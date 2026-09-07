"""Unit tests for scripts/cleanup_expired_items.py (coordination#183, Task 3).

The cleanup script discovers items whose STAC ``expires`` is in the past and
drains them (S3 delete -> validate 0 remaining -> STAC delete). Safety is the
whole point, so the tests focus on the guards and the dry-run default.

boto3 and the STAC session are mocked — no network.
"""

import json
import time
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import requests
from botocore.exceptions import ClientError, EndpointConnectionError
from cleanup_expired_items import (
    MAX_BUDGET_SECONDS,
    MAX_ITEMS_CEILING,
    _monotonic,
    build_search_kwargs,
    evaluate_guards,
    main,
    process_item,
    run_cleanup,
)

BUCKET = "esa-zarr-sentinel-explorer-fra"
NOW = datetime(2026, 7, 10, 0, 0, 0, tzinfo=UTC)
FIXTURES = Path(__file__).parent.parent / "fixtures" / "cleanup_expired"


def _fixture(name: str) -> dict:
    return json.loads((FIXTURES / f"{name}.json").read_text())


@pytest.fixture
def expired_item() -> dict:
    return _fixture("expired")


@pytest.fixture
def no_expires_item() -> dict:
    return _fixture("no_expires")


@pytest.fixture
def wrong_bucket_item() -> dict:
    return _fixture("wrong_bucket")


def _paginator(pages: list[list[str]]) -> MagicMock:
    """A get_paginator mock whose paginate() yields the given key-pages,
    one list of keys per successive call (side_effect)."""
    paginator = MagicMock()
    paginator.paginate.side_effect = [[{"Contents": [{"Key": k} for k in keys]}] for keys in pages]
    return paginator


def _response(status_code: int) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    return resp


# === Discovery query (CQL2 / sort / cap) ===


def test_build_search_kwargs_uses_cql2_expires_less_than_now() -> None:
    kwargs = build_search_kwargs("sentinel-2-l2a-staging", NOW, 25)
    assert kwargs["collections"] == ["sentinel-2-l2a-staging"]
    assert kwargs["filter_lang"] == "cql2-json"
    assert kwargs["filter"] == {
        "op": "<",
        "args": [{"property": "expires"}, "2026-07-10T00:00:00Z"],
    }


def test_build_search_kwargs_sorts_and_caps() -> None:
    kwargs = build_search_kwargs("sentinel-2-l2a-staging", NOW, 25)
    # Oldest-expiry first, with `id` as a unique tiebreaker: many items can share
    # the same `expires` (a whole backfill batch expires the same day), and keyset
    # pagination silently under-returns across pages without a total order.
    assert kwargs["sortby"] == ["+properties.expires", "+id"]
    assert kwargs["max_items"] == 25


# === Guards ===


def test_guard_allows_expired_item_in_allowed_bucket(expired_item: dict) -> None:
    ok, reason = evaluate_guards(expired_item, now=NOW, exclude_ids=set(), allowed_bucket=BUCKET)
    assert (ok, reason) == (True, "ok")


def test_guard_refuses_item_without_expires(no_expires_item: dict) -> None:
    ok, reason = evaluate_guards(no_expires_item, now=NOW, exclude_ids=set(), allowed_bucket=BUCKET)
    assert ok is False
    assert reason == "no_expires"


def test_guard_refuses_item_not_yet_expired(expired_item: dict) -> None:
    expired_item["properties"]["expires"] = "2099-01-01T00:00:00Z"
    ok, reason = evaluate_guards(expired_item, now=NOW, exclude_ids=set(), allowed_bucket=BUCKET)
    assert ok is False
    assert reason == "not_expired"


def test_guard_refuses_excluded_id(expired_item: dict) -> None:
    ok, reason = evaluate_guards(
        expired_item,
        now=NOW,
        exclude_ids={"S2_expired_item"},
        allowed_bucket=BUCKET,
    )
    assert ok is False
    assert reason == "excluded"


def test_guard_refuses_asset_outside_allowed_bucket(wrong_bucket_item: dict) -> None:
    ok, reason = evaluate_guards(
        wrong_bucket_item, now=NOW, exclude_ids=set(), allowed_bucket=BUCKET
    )
    assert ok is False
    assert reason == "wrong_bucket"


# === process_item: dry-run is the default behaviour ===


def test_dry_run_makes_no_delete_calls(expired_item: dict) -> None:
    s3 = MagicMock()
    s3.get_paginator.return_value = _paginator([["a", "b", "c"]])
    session = MagicMock()

    rec = process_item(
        expired_item,
        now=NOW,
        exclude_ids=set(),
        allowed_bucket=BUCKET,
        s3_client=s3,
        session=session,
        stac_base_url="https://stac.example.com",
        dry_run=True,
    )

    s3.delete_objects.assert_not_called()
    session.delete.assert_not_called()
    assert rec["dry_run"] is True
    assert rec["status"] == "dry_run"
    assert rec["s3_remaining"] == 3  # count of objects that WOULD be deleted


def test_dry_run_skips_guarded_item_without_counting(no_expires_item: dict) -> None:
    s3 = MagicMock()
    session = MagicMock()

    rec = process_item(
        no_expires_item,
        now=NOW,
        exclude_ids=set(),
        allowed_bucket=BUCKET,
        s3_client=s3,
        session=session,
        stac_base_url="https://stac.example.com",
        dry_run=True,
    )

    s3.delete_objects.assert_not_called()
    session.delete.assert_not_called()
    assert rec["status"] == "no_expires"
    assert rec["stac_deleted"] is False


# === process_item: real deletion path ===


def test_execute_deletes_s3_then_stac_when_validation_clean(
    expired_item: dict,
) -> None:
    s3 = MagicMock()
    # First paginate (delete listing) returns 2 keys; second (count) returns none.
    s3.get_paginator.return_value = _paginator([["a", "b"], []])
    s3.delete_objects.return_value = {
        "Deleted": [{"Key": "a"}, {"Key": "b"}],
        "Errors": [],
    }
    session = MagicMock()
    session.delete.return_value = _response(204)

    rec = process_item(
        expired_item,
        now=NOW,
        exclude_ids=set(),
        allowed_bucket=BUCKET,
        s3_client=s3,
        session=session,
        stac_base_url="https://stac.example.com",
        dry_run=False,
    )

    s3.delete_objects.assert_called_once()
    session.delete.assert_called_once()
    assert rec["status"] == "deleted"
    assert rec["stac_deleted"] is True
    assert rec["s3_objects_deleted"] == 2
    assert rec["s3_remaining"] == 0


def test_execute_retains_stac_item_when_s3_validation_fails(
    expired_item: dict,
) -> None:
    s3 = MagicMock()
    s3.get_paginator.return_value = _paginator([["a", "b"]])
    # One object fails to delete -> failed > 0 -> must not touch STAC.
    s3.delete_objects.return_value = {
        "Deleted": [{"Key": "a"}],
        "Errors": [{"Key": "b", "Code": "AccessDenied"}],
    }
    session = MagicMock()

    rec = process_item(
        expired_item,
        now=NOW,
        exclude_ids=set(),
        allowed_bucket=BUCKET,
        s3_client=s3,
        session=session,
        stac_base_url="https://stac.example.com",
        dry_run=False,
    )

    session.delete.assert_not_called()
    assert rec["status"] == "s3_validation_failed"
    assert rec["stac_deleted"] is False


def _https_only(key: str = "data") -> dict:
    """A data asset whose only href is HTTPS (no alternate.s3) -> extraction
    yields nothing, so the item's S3 storage is unresolvable."""
    return {
        key: {
            "href": (
                "https://s3.example.com/esa-zarr-sentinel-explorer-fra/tests-output/x.zarr/data"
            ),
            "type": "application/vnd+zarr",
            "roles": ["data"],
        }
    }


def _client_error(code: str = "InternalError") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, "ListObjectsV2")


def test_execute_refuses_item_with_unresolvable_s3(expired_item: dict) -> None:
    """Review finding F1: an expired item whose assets yield no s3:// URL must
    NOT be STAC-deleted — that would orphan its data. Fail closed."""
    expired_item["assets"] = _https_only()
    s3 = MagicMock()
    session = MagicMock()

    rec = process_item(
        expired_item,
        now=NOW,
        exclude_ids=set(),
        allowed_bucket=BUCKET,
        s3_client=s3,
        session=session,
        stac_base_url="https://stac.example.com",
        dry_run=False,
    )

    s3.delete_objects.assert_not_called()
    session.delete.assert_not_called()
    assert rec["status"] == "no_s3_urls"
    assert rec["stac_deleted"] is False


def test_dry_run_flags_unresolvable_s3_as_no_s3_urls(expired_item: dict) -> None:
    expired_item["assets"] = _https_only()
    rec = process_item(
        expired_item,
        now=NOW,
        exclude_ids=set(),
        allowed_bucket=BUCKET,
        s3_client=MagicMock(),
        session=MagicMock(),
        stac_base_url="https://stac.example.com",
        dry_run=True,
    )
    assert rec["status"] == "no_s3_urls"


def test_execute_allows_expired_item_with_no_assets(expired_item: dict) -> None:
    """Boundary: an expired item with NO assets has nothing to orphan, so the
    fail-closed guard must not block its STAC deletion."""
    expired_item["assets"] = {}
    session = MagicMock()
    session.delete.return_value = _response(204)

    rec = process_item(
        expired_item,
        now=NOW,
        exclude_ids=set(),
        allowed_bucket=BUCKET,
        s3_client=MagicMock(),
        session=session,
        stac_base_url="https://stac.example.com",
        dry_run=False,
    )

    assert rec["status"] == "deleted"
    assert rec["stac_deleted"] is True


def test_execute_retains_stac_item_when_s3_listing_fails(expired_item: dict) -> None:
    """Review finding F2: if S3 listing errors mid-run, 'remaining' must not be
    read as 0 — we cannot validate, so keep the STAC item."""
    s3 = MagicMock()
    paginator = MagicMock()
    paginator.paginate.side_effect = _client_error()
    s3.get_paginator.return_value = paginator
    session = MagicMock()

    rec = process_item(
        expired_item,
        now=NOW,
        exclude_ids=set(),
        allowed_bucket=BUCKET,
        s3_client=s3,
        session=session,
        stac_base_url="https://stac.example.com",
        dry_run=False,
    )

    session.delete.assert_not_called()
    assert rec["status"] == "s3_validation_failed"
    assert rec["stac_deleted"] is False


def test_execute_reports_auth_required_on_403(expired_item: dict) -> None:
    s3 = MagicMock()
    s3.get_paginator.return_value = _paginator([["a"], []])
    s3.delete_objects.return_value = {"Deleted": [{"Key": "a"}], "Errors": []}
    session = MagicMock()
    session.delete.return_value = _response(403)

    rec = process_item(
        expired_item,
        now=NOW,
        exclude_ids=set(),
        allowed_bucket=BUCKET,
        s3_client=s3,
        session=session,
        stac_base_url="https://stac.example.com",
        dry_run=False,
    )

    assert rec["status"] == "auth_required"
    assert rec["stac_deleted"] is False


def test_execute_treats_404_stac_delete_as_success(expired_item: dict) -> None:
    s3 = MagicMock()
    s3.get_paginator.return_value = _paginator([["a"], []])
    s3.delete_objects.return_value = {"Deleted": [{"Key": "a"}], "Errors": []}
    session = MagicMock()
    session.delete.return_value = _response(404)

    rec = process_item(
        expired_item,
        now=NOW,
        exclude_ids=set(),
        allowed_bucket=BUCKET,
        s3_client=s3,
        session=session,
        stac_base_url="https://stac.example.com",
        dry_run=False,
    )

    assert rec["status"] == "deleted"
    assert rec["stac_deleted"] is True


# === Audit records are JSON-serialisable ===


def test_audit_record_is_json_serialisable(expired_item: dict) -> None:
    s3 = MagicMock()
    s3.get_paginator.return_value = _paginator([["a", "b", "c"]])
    rec = process_item(
        expired_item,
        now=NOW,
        exclude_ids=set(),
        allowed_bucket=BUCKET,
        s3_client=s3,
        session=MagicMock(),
        stac_base_url="https://stac.example.com",
        dry_run=True,
    )
    line = json.dumps(rec)
    assert json.loads(line)["item_id"] == "S2_expired_item"


# === run_cleanup orchestration (review finding 2) ===


def _args(
    execute: bool = False,
    max_items: int = 100,
    max_runtime_seconds: int | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        stac_api_url="https://stac.example.com",
        collection="sentinel-2-l2a-staging",
        s3_endpoint=None,
        allowed_bucket=BUCKET,
        max_items=max_items,
        max_runtime_seconds=max_runtime_seconds,
        exclude_file=None,
        execute=execute,
    )


def _run_with(
    stale_items, *, get_status, get_body=None, s3=None, session_delete=200, execute=False
):
    """Drive run_cleanup with a mocked STAC client / HTTP session / S3 client.

    get_status: HTTP status the re-fetch GET returns.
    get_body:   JSON body for a 200 re-fetch (defaults to the stale item).
    """
    client = MagicMock()
    client.self_href = "https://stac.example.com"
    client.search.return_value.items_as_dicts.return_value = iter(stale_items)

    session = MagicMock()

    def _get(url, timeout=30):
        resp = MagicMock()
        resp.status_code = get_status
        resp.json.return_value = get_body if get_body is not None else stale_items[0]
        return resp

    session.get.side_effect = _get
    session.delete.return_value = MagicMock(status_code=session_delete)

    s3 = s3 or MagicMock()

    with (
        patch("cleanup_expired_items.Client.open", return_value=client),
        patch("cleanup_expired_items._session", return_value=session),
        patch("cleanup_expired_items._s3_client", return_value=s3),
    ):
        code = run_cleanup(_args(execute=execute))
    return code, session, s3


def _capture_lines(capsys) -> list[dict]:
    out = capsys.readouterr().out.strip().splitlines()
    return [json.loads(line) for line in out]  # every line MUST be JSON


def test_run_cleanup_dry_run_emits_json_and_makes_no_deletes(expired_item, capsys) -> None:
    s3 = MagicMock()
    s3.get_paginator.return_value = _paginator([["a", "b"]])  # dry-run counts only

    code, session, s3 = _run_with([expired_item], get_status=200, s3=s3, execute=False)

    records = _capture_lines(capsys)
    assert code == 0
    assert [r["event"] for r in records] == ["cleanup_item", "cleanup_summary"]
    assert records[0]["status"] == "dry_run"
    s3.delete_objects.assert_not_called()
    session.delete.assert_not_called()


def test_run_cleanup_execute_validation_failure_exits_1(expired_item, capsys) -> None:
    s3 = MagicMock()
    s3.get_paginator.return_value = _paginator([["a", "b"]])
    s3.delete_objects.return_value = {
        "Deleted": [],
        "Errors": [{"Key": "a", "Code": "AccessDenied"}],
    }

    code, session, _ = _run_with([expired_item], get_status=200, s3=s3, execute=True)

    records = _capture_lines(capsys)
    assert code == 1
    assert records[0]["status"] == "s3_validation_failed"
    session.delete.assert_not_called()  # STAC item retained


def test_run_cleanup_refetch_404_is_already_gone_not_a_failure(expired_item, capsys) -> None:
    code, _, s3 = _run_with([expired_item], get_status=404, execute=True)

    records = _capture_lines(capsys)
    assert code == 0  # idempotent success
    assert records[0]["status"] == "already_gone"
    s3.delete_objects.assert_not_called()  # never acted on stale data


def test_run_cleanup_refetch_error_skips_and_exits_1(expired_item, capsys) -> None:
    code, _, s3 = _run_with([expired_item], get_status=500, execute=True)

    records = _capture_lines(capsys)
    assert code == 1
    assert records[0]["status"] == "refetch_failed"
    s3.delete_objects.assert_not_called()  # did NOT fall back to stale + delete


def test_run_cleanup_paginates_fully_before_deleting(expired_item) -> None:
    """Regression: discovery must be materialised before any delete. The search
    paginates with a keyset token anchored on the last item; deleting mid-
    iteration removes that anchor and the next page 404s the token."""
    items = [dict(expired_item, id="a"), dict(expired_item, id="b")]
    yielded: list[str] = []

    def gen():
        for it in items:
            yielded.append(it["id"])
            yield it

    client = MagicMock()
    client.self_href = "https://stac.example.com"
    client.search.return_value.items_as_dicts.side_effect = gen

    session = MagicMock()

    def _get(url, timeout=30):
        # First re-fetch (before any delete) must already see the whole page set.
        assert yielded == ["a", "b"], "search was not fully paginated before mutating"
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = items[0]
        return resp

    session.get.side_effect = _get
    session.delete.return_value = MagicMock(status_code=204)

    s3 = MagicMock()
    paginator = MagicMock()
    paginator.paginate.return_value = [{"Contents": []}]  # nothing in S3
    s3.get_paginator.return_value = paginator

    with (
        patch("cleanup_expired_items.Client.open", return_value=client),
        patch("cleanup_expired_items._session", return_value=session),
        patch("cleanup_expired_items._s3_client", return_value=s3),
    ):
        run_cleanup(_args(execute=True))

    assert yielded == ["a", "b"]


def test_run_cleanup_survives_a_stac_delete_timeout(expired_item, capsys) -> None:
    """One ReadTimeout must cost one item, not the rest of the batch.

    The STAC DELETE was the last unguarded call in the loop; at 300 items a run
    it is the difference between losing one deletion and losing the run.
    """
    second = json.loads(json.dumps(expired_item))
    second["id"] = f"{expired_item['id']}_SECOND"

    client = MagicMock()
    client.self_href = "https://stac.example.com"
    client.search.return_value.items_as_dicts.return_value = iter([expired_item, second])

    session = MagicMock()

    def _get(url, timeout=30):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = second if url.endswith("_SECOND") else expired_item
        return resp

    session.get.side_effect = _get
    session.delete.side_effect = [
        requests.exceptions.ReadTimeout("timed out"),
        MagicMock(status_code=204),
    ]

    s3 = MagicMock()
    # Two paginate calls per item: the delete listing, then the validation recount.
    s3.get_paginator.return_value = _paginator([["a"], [], ["b"], []])
    s3.delete_objects.side_effect = [
        {"Deleted": [{"Key": "a"}], "Errors": []},
        {"Deleted": [{"Key": "b"}], "Errors": []},
    ]

    with (
        patch("cleanup_expired_items.Client.open", return_value=client),
        patch("cleanup_expired_items._session", return_value=session),
        patch("cleanup_expired_items._s3_client", return_value=s3),
    ):
        code = run_cleanup(_args(execute=True))

    records = _capture_lines(capsys)
    items = [r for r in records if r["event"] == "cleanup_item"]

    # The batch ran to completion and the summary was still emitted — a missing
    # cleanup_summary is the operator's real-failure signal.
    assert [r["status"] for r in items] == ["stac_delete_error", "deleted"]
    assert records[-1]["event"] == "cleanup_summary"
    assert records[-1]["processed"] == 2
    assert code == 1  # the timed-out item is still counted as a failure
    assert items[0]["stac_deleted"] is False


# === --max-runtime-seconds: the bound that lives in the tool ===
#
# `activeDeadlineSeconds` kills the pod mid-item, and the per-item unit (S3
# delete -> recount -> STAC delete -> audit line) is not atomic: a kill between
# the S3 delete and the STAC delete leaves an item pointing at data that is
# gone. These tests pin the in-tool alternative down to the exact boundary it
# stops on, because "stops eventually" is not the property that matters.


# `time.monotonic()` is seconds since boot — thousands on a laptop, far more on a
# long-lived node. Fakes that start at 0 cannot tell `deadline = _monotonic() +
# budget` from `deadline = budget`: both are "small", so the dropped-origin bug
# looks fine in tests and stalls every run in production. Every clock here starts
# at BOOT.
BOOT = 10_000.0


_CLI_BASE = [
    "--stac-api-url",
    "https://stac.example.com",
    "--collection",
    "sentinel-2-l2a-staging",
]


def _clock(values: list[float]):
    """Fake `_monotonic`: hands out `values` in order, then repeats the last.

    Repeating rather than raising keeps the tests from asserting on a call
    count, which is an implementation detail; what each test pins is how many
    ITEMS were processed.
    """
    remaining = list(values)
    last = [values[-1]]

    def _tick() -> float:
        if remaining:
            last[0] = remaining.pop(0)
        return last[0]

    return _tick


def _items(expired_item: dict, n: int) -> list[dict]:
    out = []
    for i in range(n):
        copy = json.loads(json.dumps(expired_item))
        copy["id"] = f"{expired_item['id']}_{i}"
        out.append(copy)
    return out


def _run_budgeted(stale_items, *, budget, clock_values, search_ticks=0):
    """Drive a dry-run `run_cleanup` against a fake clock.

    search_ticks: how many times the discovery search itself consumes the
    clock — i.e. how slow the query is. Used to prove the budget covers
    discovery and not just the loop.
    """
    tick = _clock(clock_values)

    client = MagicMock()
    client.self_href = "https://stac.example.com"
    search = MagicMock()
    search.items_as_dicts.return_value = iter(stale_items)

    def _search(**_kwargs):
        for _ in range(search_ticks):
            tick()
        return search

    client.search.side_effect = _search

    session = MagicMock()
    by_id = {i["id"]: i for i in stale_items}

    def _get(url, timeout=30):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = by_id[url.rsplit("/", 1)[-1]]
        return resp

    session.get.side_effect = _get

    s3 = MagicMock()
    s3.get_paginator.return_value = _paginator([["a"]] * (2 * len(stale_items)))

    with (
        patch("cleanup_expired_items.Client.open", return_value=client),
        patch("cleanup_expired_items._session", return_value=session),
        patch("cleanup_expired_items._s3_client", return_value=s3),
        patch("cleanup_expired_items._monotonic", side_effect=tick),
    ):
        code = run_cleanup(_args(max_runtime_seconds=budget))
    return code, session


def test_run_cleanup_stops_at_the_item_boundary_when_the_budget_is_spent(
    expired_item, capsys
) -> None:
    """Two items done, the third never touched — not even re-fetched.

    The assertion that matters is `processed == 2`: if the check moved to the
    bottom of the loop, item 3 would be fully deleted before the run noticed,
    which is the tear this flag exists to prevent.
    """
    items = _items(expired_item, 3)
    # deadline calc at 0 (-> 100), then the per-item checks.
    code, session = _run_budgeted(
        items,
        budget=100,
        # deadline=BOOT+100 · post-discovery · item1 · item2 · item3 (spent)
        clock_values=[BOOT, BOOT + 10, BOOT + 20, BOOT + 30, BOOT + 150],
    )

    records = _capture_lines(capsys)
    summary = records[-1]
    assert code == 0  # a spent budget is a clean yield, NOT a failure
    assert summary["event"] == "cleanup_summary"
    assert summary["processed"] == 2
    assert summary["discovered"] == 3
    assert summary["time_budget_reached"] is True
    assert [r["item_id"] for r in records[:-1]] == [items[0]["id"], items[1]["id"]]
    # The third item was not even re-fetched: nothing about it was begun.
    assert session.get.call_count == 2


def test_run_cleanup_budget_covers_discovery_not_just_the_loop(expired_item) -> None:
    """A slow search spends the budget too.

    The point of a time budget over a fixed item count is that it absorbs the
    per-run overhead — and that overhead grows: after the T8 wave the discovery
    query scans ~112k rows instead of ~500. If the clock started after the
    search, a query that ate the whole hour would still go on to delete a full
    batch on top of it.
    """
    items = _items(expired_item, 3)
    # The search burns 200 s before the first item is even considered.
    code, session = _run_budgeted(
        items,
        budget=100,
        # deadline=BOOT+100 · the search burns 200 s · post-discovery (spent)
        clock_values=[BOOT, BOOT + 200, BOOT + 210],
        search_ticks=1,
    )

    assert code == 0
    assert session.get.call_count == 0  # not one item was started


def test_run_cleanup_stops_when_elapsed_exactly_equals_the_budget(expired_item) -> None:
    """The boundary is `>=`: at exactly the budget, stop."""
    items = _items(expired_item, 2)
    code, session = _run_budgeted(items, budget=100, clock_values=[BOOT, BOOT + 100])

    assert code == 0
    assert session.get.call_count == 0


def test_run_cleanup_without_a_budget_never_reads_the_clock(expired_item, capsys) -> None:
    """No budget must mean no behaviour change at all for existing callers."""
    items = _items(expired_item, 3)
    tick = _clock([BOOT])

    client = MagicMock()
    client.self_href = "https://stac.example.com"
    client.search.return_value.items_as_dicts.return_value = iter(items)

    session = MagicMock()
    by_id = {i["id"]: i for i in items}

    def _get(url, timeout=30):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = by_id[url.rsplit("/", 1)[-1]]
        return resp

    session.get.side_effect = _get

    s3 = MagicMock()
    s3.get_paginator.return_value = _paginator([["a"]] * 6)

    with (
        patch("cleanup_expired_items.Client.open", return_value=client),
        patch("cleanup_expired_items._session", return_value=session),
        patch("cleanup_expired_items._s3_client", return_value=s3),
        patch("cleanup_expired_items._monotonic", side_effect=tick) as monotonic,
    ):
        code = run_cleanup(_args(max_runtime_seconds=None))

    summary = _capture_lines(capsys)[-1]
    assert code == 0
    assert summary["processed"] == 3
    assert summary["time_budget_reached"] is False
    monotonic.assert_not_called()


def test_run_cleanup_flags_a_budget_spent_by_discovery_alone(expired_item, capsys) -> None:
    """A slow search that finds nothing must NOT look like a quiet hour.

    `discovered: 0, processed: 0` is exactly what an idle tick emits, so the
    only thing separating "nothing was due" from "the query ate the whole
    budget and we did no work" is this flag — and it used to be set inside the
    loop, which never runs here.
    """
    code, session = _run_budgeted(
        [],
        budget=100,
        # deadline=BOOT+100 · the search burns 4000 s · post-discovery (spent)
        clock_values=[BOOT, BOOT + 4000, BOOT + 4010],
        search_ticks=1,
    )

    summary = _capture_lines(capsys)[-1]
    assert code == 0
    assert summary["discovered"] == 0
    assert summary["processed"] == 0
    assert summary["time_budget_reached"] is True
    assert session.get.call_count == 0


def test_budget_boundary_leaves_no_item_half_deleted(expired_item, capsys) -> None:
    """The atomicity claim, exercised with --execute through the real delete path.

    Every other budget test runs dry, where `process_item` returns before it
    touches S3 or STAC — so none of them can see a stop that lands between the
    S3 delete and the STAC delete. This one asserts the invariant that matters:
    item 1 is deleted in BOTH stores, item 2 in neither.
    """
    items = _items(expired_item, 2)
    tick = _clock([BOOT, BOOT + 10, BOOT + 20, BOOT + 150])

    client = MagicMock()
    client.self_href = "https://stac.example.com"
    client.search.return_value.items_as_dicts.return_value = iter(items)

    session = MagicMock()
    by_id = {i["id"]: i for i in items}

    def _get(url, timeout=30):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = by_id[url.rsplit("/", 1)[-1]]
        return resp

    session.get.side_effect = _get
    session.delete.return_value = MagicMock(status_code=204)

    s3 = MagicMock()
    # Two paginate calls per item: the delete listing, then the validation recount.
    s3.get_paginator.return_value = _paginator([["a"], [], ["b"], []])
    s3.delete_objects.return_value = {"Deleted": [{"Key": "a"}], "Errors": []}

    with (
        patch("cleanup_expired_items.Client.open", return_value=client),
        patch("cleanup_expired_items._session", return_value=session),
        patch("cleanup_expired_items._s3_client", return_value=s3),
        patch("cleanup_expired_items._monotonic", side_effect=tick),
    ):
        code = run_cleanup(_args(execute=True, max_runtime_seconds=100))

    records = _capture_lines(capsys)
    audited = [r for r in records if r["event"] == "cleanup_item"]
    summary = records[-1]

    assert code == 0
    assert summary["processed"] == 1 and summary["discovered"] == 2
    assert summary["time_budget_reached"] is True

    # Item 1: both halves done, and audited as such.
    assert [r["item_id"] for r in audited] == [items[0]["id"]]
    assert audited[0]["status"] == "deleted"
    assert audited[0]["stac_deleted"] is True
    assert audited[0]["s3_remaining"] == 0

    # Item 2: neither half. One S3 delete, one STAC delete, for item 1 only —
    # a stop inside the per-item unit would show 2 and 1, or 1 and 0 with the
    # second item audited.
    assert s3.delete_objects.call_count == 1
    assert session.delete.call_count == 1
    assert items[1]["id"] not in {r["item_id"] for r in audited}


@pytest.mark.parametrize("budget", [0, -1])
def test_run_cleanup_refuses_a_budget_below_one_second(budget: int) -> None:
    """A budget of 0 must not read as "no budget" — that would silently remove
    the bound on a prod run."""
    # Patched even though the guard should raise first: without this, a
    # regression in the guard turns this unit test into a live HTTPS call to
    # stac.example.com (5 urllib3 retries, ~4.5 s) and reports as a timeout
    # rather than an assertion failure. The file's contract is "no network".
    with (
        patch("cleanup_expired_items.Client.open") as client_open,
        patch("cleanup_expired_items._session"),
        patch("cleanup_expired_items._s3_client"),
        pytest.raises(ValueError, match="max_runtime_seconds"),
    ):
        run_cleanup(_args(max_runtime_seconds=budget))
    client_open.assert_not_called()


@pytest.mark.parametrize(
    ("bad", "expected"),
    [
        ("abc", "whole number of seconds"),
        ("3000s", "whole number of seconds"),
        ("5.5", "whole number of seconds"),
        ("1" + "0" * 400, "<= 86400 seconds"),  # parses as int, overflows a float deadline
        ("90000", "<= 86400 seconds"),
    ],
)
def test_cli_rejects_unusable_budget_values_with_a_readable_message(
    bad: str, expected: str, capsys
) -> None:
    """The message must name the problem, not leak the private callable's name."""
    with pytest.raises(SystemExit) as exc:
        main([*_CLI_BASE, "--max-runtime-seconds", bad])
    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert expected in err
    assert "_budget_seconds" not in err


def test_cli_treats_an_empty_budget_as_off() -> None:
    """`value: ""` is how this fleet's Argo templates spell an unset optional.

    The parameter is spliced into argv unconditionally, so an empty string has
    to mean "no budget" — otherwise the conventional wiring hard-fails the pod
    at parse time on every tick.
    """
    with patch("cleanup_expired_items.run_cleanup", return_value=0) as run:
        main([*_CLI_BASE, "--max-runtime-seconds", ""])
    assert run.call_args.args[0].max_runtime_seconds is None


@pytest.mark.parametrize("bad", ["0", "-1"])
def test_cli_rejects_a_budget_below_one_second_as_a_usage_error(bad: str, capsys) -> None:
    """A typo'd bound must fail loudly at parse time, before anything is deleted.

    argparse exits 2 on a bad value; the `run_cleanup` ValueError still guards
    every non-CLI caller (see the test above).
    """
    with pytest.raises(SystemExit) as exc:
        main([*_CLI_BASE, "--max-runtime-seconds", bad])
    assert exc.value.code == 2
    assert "must be >= 1 second" in capsys.readouterr().err


def test_cli_defaults_the_budget_to_off_and_parses_it_when_given() -> None:
    with patch("cleanup_expired_items.run_cleanup", return_value=0) as run:
        main(_CLI_BASE)
        assert run.call_args.args[0].max_runtime_seconds is None

        main([*_CLI_BASE, "--max-runtime-seconds", "3000"])
        assert run.call_args.args[0].max_runtime_seconds == 3000


# === Transport failures on the S3 side (the orphan #392 left open) ===
#
# botocore raises transport errors as BotoCoreError, which is NOT a ClientError
# and NOT a requests.RequestException — so before the handler these tests cover,
# they escaped every `except` in this module, killed the process mid-item, and
# left the S3 objects deleted, the STAC item intact and no audit line at all.


def _transport_error() -> EndpointConnectionError:
    return EndpointConnectionError(endpoint_url="https://s3.example.com")


def test_execute_retains_stac_item_when_the_recount_hits_a_transport_error(
    expired_item,
) -> None:
    """The dangerous half: S3 objects are already gone when this fires."""
    s3 = MagicMock()
    paginator = MagicMock()
    # First paginate() lists what to delete; the second is the validation
    # recount, and that is where the endpoint drops.
    paginator.paginate.side_effect = [
        [{"Contents": [{"Key": "a"}]}],
        _transport_error(),
    ]
    s3.get_paginator.return_value = paginator
    s3.delete_objects.return_value = {"Deleted": [{"Key": "a"}], "Errors": []}
    session = MagicMock()

    record = process_item(
        expired_item,
        now=NOW,
        exclude_ids=set(),
        allowed_bucket=BUCKET,
        s3_client=s3,
        session=session,
        stac_base_url="https://stac.example.com",
        dry_run=False,
    )

    assert record["status"] == "s3_transport_error"
    assert record["stac_deleted"] is False
    session.delete.assert_not_called()  # the item survives to be retried

    # The counts must be null, NOT 0. The helper unwinds with its per-batch tally,
    # so we do not know how many of this item's objects are gone — and `_audit`
    # defaults `s3_remaining` to 0, which is the field the validate-before-delete
    # gate is named after. Reporting 0 here would say "nothing happened" about an
    # item that may have just lost a thousand objects from a bucket with no
    # versioning.
    assert record["s3_objects_deleted"] is None
    assert record["s3_remaining"] is None


def test_execute_reports_a_transport_error_on_the_delete_itself(expired_item) -> None:
    s3 = MagicMock()
    s3.get_paginator.return_value = _paginator([["a"]])
    s3.delete_objects.side_effect = _transport_error()
    session = MagicMock()

    record = process_item(
        expired_item,
        now=NOW,
        exclude_ids=set(),
        allowed_bucket=BUCKET,
        s3_client=s3,
        session=session,
        stac_base_url="https://stac.example.com",
        dry_run=False,
    )

    assert record["status"] == "s3_transport_error"
    session.delete.assert_not_called()


def test_a_transport_error_costs_one_item_not_the_run(expired_item, capsys) -> None:
    """The whole point: the run continues and still emits its summary.

    Before the fix this killed the process — no `cleanup_summary`, which is the
    signal the README calls the real failure alarm, fired for a single bad item.
    """
    second = json.loads(json.dumps(expired_item))
    second["id"] = f"{expired_item['id']}_SECOND"

    client = MagicMock()
    client.self_href = "https://stac.example.com"
    client.search.return_value.items_as_dicts.return_value = iter([expired_item, second])

    session = MagicMock()
    by_id = {expired_item["id"]: expired_item, second["id"]: second}

    def _get(url, timeout=30):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = by_id[url.rsplit("/", 1)[-1]]
        return resp

    session.get.side_effect = _get
    session.delete.return_value = MagicMock(status_code=204)

    s3 = MagicMock()
    paginator = MagicMock()
    paginator.paginate.side_effect = [
        [{"Contents": [{"Key": "a"}]}],  # item 1: delete listing
        _transport_error(),  # item 1: recount -> transport error
        [{"Contents": [{"Key": "b"}]}],  # item 2: delete listing
        [],  # item 2: recount, clean
    ]
    s3.get_paginator.return_value = paginator
    s3.delete_objects.return_value = {"Deleted": [{"Key": "a"}], "Errors": []}

    with (
        patch("cleanup_expired_items.Client.open", return_value=client),
        patch("cleanup_expired_items._session", return_value=session),
        patch("cleanup_expired_items._s3_client", return_value=s3),
    ):
        code = run_cleanup(_args(execute=True))

    records = _capture_lines(capsys)
    items = [r for r in records if r["event"] == "cleanup_item"]

    assert [r["status"] for r in items] == ["s3_transport_error", "deleted"]
    assert records[-1]["event"] == "cleanup_summary"  # the run finished
    assert records[-1]["processed"] == 2
    assert code == 1  # and it is loud about it


# === --max-items: 0 is unlimited, not zero ===


def test_run_cleanup_refuses_a_max_items_below_one() -> None:
    """`--max-items 0` removes the cap; the tool must refuse it, not obey it."""
    with (
        patch("cleanup_expired_items.Client.open") as client_open,
        patch("cleanup_expired_items._session"),
        patch("cleanup_expired_items._s3_client"),
        pytest.raises(ValueError, match=r"max_items must be 1\.\."),
    ):
        run_cleanup(_args(max_items=0))
    client_open.assert_not_called()


@pytest.mark.parametrize("bad", ["0", "-5"])
def test_cli_rejects_max_items_below_one(bad: str, capsys) -> None:
    with pytest.raises(SystemExit) as exc:
        main([*_CLI_BASE, "--max-items", bad])
    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "UNLIMITED" in err
    assert "_positive_int" not in err


# === Bounds live in the tool, not only at the CLI ===


def test_monotonic_really_is_monotonic() -> None:
    """The seam must wrap `time.monotonic`, not `time.time`.

    Every other budget test patches `_monotonic`, so nothing else in this file
    can tell the two apart — and the difference is the whole documented reason
    the seam exists: a wall clock lets an NTP step extend or truncate a live
    budget. (Their absolute values differ by decades, so this is not a close
    call.)
    """
    assert abs(_monotonic() - time.monotonic()) < 1.0


def test_run_cleanup_enforces_the_budget_ceiling_not_just_the_cli() -> None:
    """A ceiling that exists only in argparse is a convenience, not a bound.

    Reachable in production only via a library caller, but the value that
    motivated the ceiling — a huge integer — raises OverflowError deep inside
    the deadline arithmetic rather than at the guard, which is exactly the
    failure the ceiling is documented to prevent.
    """
    with (
        patch("cleanup_expired_items.Client.open") as client_open,
        patch("cleanup_expired_items._session"),
        patch("cleanup_expired_items._s3_client"),
        pytest.raises(ValueError, match=r"max_runtime_seconds must be 1\.\."),
    ):
        run_cleanup(_args(max_runtime_seconds=MAX_BUDGET_SECONDS + 1))
    client_open.assert_not_called()

    with (
        patch("cleanup_expired_items.Client.open"),
        patch("cleanup_expired_items._session"),
        patch("cleanup_expired_items._s3_client"),
        pytest.raises(ValueError),
    ):
        run_cleanup(_args(max_runtime_seconds=int("1" + "0" * 400)))


def test_run_cleanup_enforces_the_item_ceiling() -> None:
    """The item cap is also the memory cap; an OOMKill cannot be yielded on."""
    with (
        patch("cleanup_expired_items.Client.open") as client_open,
        patch("cleanup_expired_items._session"),
        patch("cleanup_expired_items._s3_client"),
        pytest.raises(ValueError, match=r"max_items must be 1\.\."),
    ):
        run_cleanup(_args(max_items=MAX_ITEMS_CEILING + 1))
    client_open.assert_not_called()


def test_cli_rejects_an_item_cap_above_the_ceiling(capsys) -> None:
    """`100000` is a plausible typo for `10000` and would materialise ~4.5 GB."""
    with pytest.raises(SystemExit) as exc:
        main([*_CLI_BASE, "--max-items", "100000"])
    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "memory cap" in err
    assert "_item_cap" not in err
