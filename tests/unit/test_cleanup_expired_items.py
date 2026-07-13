"""Unit tests for scripts/cleanup_expired_items.py (coordination#183, Task 3).

The cleanup script discovers items whose STAC ``expires`` is in the past and
drains them (S3 delete -> validate 0 remaining -> STAC delete). Safety is the
whole point, so the tests focus on the guards and the dry-run default.

boto3 and the STAC session are mocked — no network.
"""

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from cleanup_expired_items import (
    build_search_kwargs,
    evaluate_guards,
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
    assert kwargs["sortby"] == "+properties.expires"
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
                "https://s3.example.com/esa-zarr-sentinel-explorer-fra/" "tests-output/x.zarr/data"
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


def _args(execute: bool = False, max_items: int = 100) -> SimpleNamespace:
    return SimpleNamespace(
        stac_api_url="https://stac.example.com",
        collection="sentinel-2-l2a-staging",
        s3_endpoint=None,
        allowed_bucket=BUCKET,
        max_items=max_items,
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
