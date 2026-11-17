"""Unit tests for the bounded, confined `manage_collections.py clean` path.

`clean` is the operator purge tool: it deletes every item in a collection and,
with --clean-s3, their Zarr stores. Two properties carry the safety here and
both are tested below:

- the confinement sweep sees the WHOLE collection, even on a bounded run, so a
  rogue href never hides in a batch the run has not reached yet;
- --max-items bounds what is DELETED, inside the tool rather than via an
  external timeout or kill;
- --datetime-before protects anything acquired at or after a threshold, and
  anything the tool cannot date, so an item registered mid-drain is never the
  next run's first deletion.

No network: the item manager and S3 client are mocked.
"""

import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import click
import pytest
import requests
from click.testing import CliRunner
from packaging.version import Version

OPERATOR_TOOLS = Path(__file__).parent.parent.parent / "operator-tools"
if str(OPERATOR_TOOLS) not in sys.path:
    sys.path.insert(0, str(OPERATOR_TOOLS))

from manage_collections import (  # noqa: E402
    STACCollectionManager,
    _report_confinement_sweep,
    cli,
    parse_threshold,
)
from manage_item import STACItemManager  # noqa: E402

BUCKET = "esa-zarr-sentinel-explorer-fra"
STAGING = (BUCKET, "tests-output/sentinel-2-l2a-staging/")
PROD = (BUCKET, "tests-output/sentinel-2-l2a/")


@pytest.fixture(autouse=True)
def demo_list(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin the demo denylist `clean` reads, so no test depends on $EXPIRES_EXCLUDE_FILE
    or the baked file. It lists no item these tests create."""
    path = tmp_path / "demo_exclude_ids.txt"
    path.write_text("S2A_MSIL2A_DEMO_NOT_IN_THESE_TESTS\n")
    monkeypatch.setenv("EXPIRES_EXCLUDE_FILE", str(path))


def _item(i: int, href: str | None = None, acquired: str | None = None) -> dict:
    href = href or f"s3://{BUCKET}/tests-output/sentinel-2-l2a-staging/scene{i}/data.zarr/"
    item = {"id": f"ITEM_{i:05d}", "assets": {"data": {"alternate": {"s3": {"href": href}}}}}
    if acquired is not None:
        item["properties"] = {"datetime": acquired}
    return item


def _response(status: int, body: object = None) -> requests.Response:
    """A real Response, so raise_for_status() and json() behave as in production; bytes
    are sent as the raw body."""
    response = requests.Response()
    response.status_code = status
    response._content = body if isinstance(body, bytes) else json.dumps(body).encode()
    return response


def _manager(items: list[dict]) -> STACCollectionManager:
    mgr = STACCollectionManager("https://stac.example.com")
    mgr.get_collection_items = MagicMock(return_value=items)  # type: ignore[method-assign]
    mgr.item_manager = MagicMock()
    mgr.item_manager.delete_item.return_value = (True, 1, 0)
    return mgr


# === the sweep ===


def test_sweep_passes_when_every_url_is_in_bounds() -> None:
    _report_confinement_sweep([_item(i) for i in range(50)], [STAGING])


def test_sweep_aborts_on_an_adjacent_prod_href() -> None:
    items = [_item(i) for i in range(50)]
    items[37] = _item(37, f"s3://{BUCKET}/tests-output/sentinel-2-l2a/PROD/data.zarr/")
    with pytest.raises(click.ClickException) as exc:
        _report_confinement_sweep(items, [STAGING])
    assert "outside the declared confinement" in str(exc.value)


def test_sweep_aborts_on_a_store_orphaning_bare_zarr_href() -> None:
    items = [_item(i) for i in range(5)]
    items[2] = _item(2, f"s3://{BUCKET}/tests-output/sentinel-2-l2a-staging/x/data.zarr")
    with pytest.raises(click.ClickException):
        _report_confinement_sweep(items, [STAGING])


# === the bound ===


def test_max_items_limits_deletions() -> None:
    mgr = _manager([_item(i) for i in range(100)])
    deleted, _, _ = mgr.clean_collection(
        "sentinel-2-l2a-staging",
        clean_s3=True,
        s3_client=MagicMock(),
        confinement=[STAGING],
        max_items=10,
    )
    assert deleted == 10
    assert mgr.item_manager.delete_item.call_count == 10


def test_bounded_run_still_sweeps_the_whole_collection() -> None:
    """The rogue item sits at index 60, well outside a --max-items 10 batch.

    Sweeping only the batch would delete 10 items and leave the landmine for a
    later run; sweeping everything refuses now, before anything is touched.
    """
    items = [_item(i) for i in range(100)]
    items[60] = _item(60, f"s3://{BUCKET}/tests-output/sentinel-2-l2a/PROD/data.zarr/")
    mgr = _manager(items)

    with pytest.raises(click.ClickException, match="outside the declared confinement"):
        mgr.clean_collection(
            "sentinel-2-l2a-staging",
            clean_s3=True,
            s3_client=MagicMock(),
            confinement=[STAGING],
            max_items=10,
        )
    mgr.item_manager.delete_item.assert_not_called()


def test_no_max_items_deletes_everything() -> None:
    mgr = _manager([_item(i) for i in range(25)])
    deleted, _, _ = mgr.clean_collection(
        "sentinel-2-l2a-staging",
        clean_s3=True,
        s3_client=MagicMock(),
        confinement=[STAGING],
    )
    assert deleted == 25


def test_max_items_larger_than_the_collection_is_harmless() -> None:
    mgr = _manager([_item(i) for i in range(5)])
    deleted, _, _ = mgr.clean_collection(
        "sentinel-2-l2a-staging",
        clean_s3=True,
        s3_client=MagicMock(),
        confinement=[STAGING],
        max_items=1000,
    )
    assert deleted == 5


def test_clean_s3_without_confinement_is_refused() -> None:
    """Fail closed at the library level too, not only in the CLI."""
    mgr = _manager([_item(i) for i in range(5)])
    with pytest.raises(ValueError, match="explicit confinement"):
        mgr.clean_collection("sentinel-2-l2a-staging", clean_s3=True, s3_client=MagicMock())
    mgr.item_manager.delete_item.assert_not_called()


def test_multiple_confinements_are_a_union() -> None:
    items = [_item(i) for i in range(10)]
    items[3] = _item(3, f"s3://{BUCKET}/tests-output/sentinel-2-l2a/ok/data.zarr/")
    _report_confinement_sweep(items, [STAGING, PROD])


# === the threshold ===

THRESHOLD = datetime(2026, 9, 1, tzinfo=UTC)
OLD = "2026-04-06T10:00:51.024000Z"
RECENT = "2026-09-07T10:12:00Z"


def _deleted_ids(mgr: STACCollectionManager) -> list[str]:
    return [c.kwargs["item_id"] for c in mgr.item_manager.delete_item.call_args_list]


def _clean(mgr: STACCollectionManager, **kwargs: object) -> int:
    deleted, _, _ = mgr.clean_collection(
        "sentinel-2-l2a-staging",
        clean_s3=True,
        s3_client=MagicMock(),
        confinement=[STAGING],
        **kwargs,  # type: ignore[arg-type]
    )
    return deleted


def test_items_at_or_after_the_threshold_are_protected() -> None:
    """The API lists newest first, so the recent items sit at the head of the
    list — exactly where an unguarded run would start deleting."""
    items = [_item(i, acquired=RECENT) for i in range(4)] + [
        _item(i, acquired=OLD) for i in range(4, 10)
    ]
    mgr = _manager(items)
    assert _clean(mgr, datetime_before=THRESHOLD) == 6
    assert _deleted_ids(mgr) == [f"ITEM_{i:05d}" for i in range(4, 10)]


def test_threshold_boundary_is_exclusive() -> None:
    at = _item(0, acquired="2026-09-01T00:00:00Z")
    just_before = _item(1, acquired="2026-08-31T23:59:59Z")
    mgr = _manager([at, just_before])
    assert _clean(mgr, datetime_before=THRESHOLD) == 1
    assert _deleted_ids(mgr) == ["ITEM_00001"]


def test_offset_aware_item_datetimes_compare_correctly() -> None:
    # 2026-09-01T01:00+02:00 is 2026-08-31T23:00Z — before the threshold.
    mgr = _manager([_item(0, acquired="2026-09-01T01:00:00+02:00")])
    assert _clean(mgr, datetime_before=THRESHOLD) == 1


def test_undated_items_are_protected_when_a_threshold_is_set() -> None:
    """A guard that cannot decide must fail closed."""
    items = [_item(0), _item(1, acquired="not-a-date"), _item(2, acquired=OLD)]
    items[0]["properties"] = {"start_datetime": OLD}  # range-only item: datable
    mgr = _manager(items)
    assert _clean(mgr, datetime_before=THRESHOLD) == 2
    assert _deleted_ids(mgr) == ["ITEM_00000", "ITEM_00002"]


def test_no_threshold_leaves_undated_items_eligible() -> None:
    mgr = _manager([_item(i) for i in range(3)])
    assert _clean(mgr) == 3


def test_threshold_is_applied_before_the_bound() -> None:
    """30 recent items head the list; --max-items 10 must still delete 10 OLD
    items, not spend the bound on protected ones and delete nothing."""
    items = [_item(i, acquired=RECENT) for i in range(30)] + [
        _item(i, acquired=OLD) for i in range(30, 100)
    ]
    mgr = _manager(items)
    assert _clean(mgr, datetime_before=THRESHOLD, max_items=10) == 10
    assert _deleted_ids(mgr) == [f"ITEM_{i:05d}" for i in range(30, 40)]


def test_everything_protected_deletes_nothing() -> None:
    mgr = _manager([_item(i, acquired=RECENT) for i in range(5)])
    assert _clean(mgr, datetime_before=THRESHOLD) == 0
    mgr.item_manager.delete_item.assert_not_called()


def test_threshold_does_not_skip_the_sweep() -> None:
    """A rogue href on a PROTECTED item still aborts: the sweep is a statement
    about the collection, not about this run's batch."""
    items = [_item(0, acquired=OLD), _item(1, acquired=RECENT)]
    items[1] = _item(1, f"s3://{BUCKET}/tests-output/sentinel-2-l2a/PROD/data.zarr/", RECENT)
    mgr = _manager(items)
    with pytest.raises(click.ClickException, match="outside the declared confinement"):
        _clean(mgr, datetime_before=THRESHOLD)
    mgr.item_manager.delete_item.assert_not_called()


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("2026-09-01", datetime(2026, 9, 1, tzinfo=UTC)),
        ("2026-09-01T00:00:00Z", datetime(2026, 9, 1, tzinfo=UTC)),
        ("2026-09-01T02:00:00+02:00", datetime(2026, 9, 1, tzinfo=UTC)),
        ("2026-09-01T12:30:00", datetime(2026, 9, 1, 12, 30, tzinfo=UTC)),
    ],
)
def test_parse_threshold(raw: str, expected: datetime) -> None:
    assert parse_threshold(raw) == expected


@pytest.mark.parametrize("raw", ["yesterday", "2026-13-01", "", "1788480000"])
def test_parse_threshold_rejects_garbage(raw: str) -> None:
    with pytest.raises(ValueError, match="--datetime-before"):
        parse_threshold(raw)


# === the CLI wiring: the cron manifest passes flags, so the flag→kwarg path is the contract ===


def test_cli_passes_the_threshold_to_clean_collection(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_clean(self: STACCollectionManager, collection_id: str, **kwargs: object) -> tuple:
        captured.update(kwargs, collection_id=collection_id)
        return 0, 0, 0

    monkeypatch.setattr(STACCollectionManager, "clean_collection", fake_clean)
    result = CliRunner().invoke(
        cli,
        [
            "--api-url",
            "https://stac.example.com",
            "clean",
            "sentinel-2-l2a-staging",
            "-y",
            "--max-items",
            "2000",
            "--datetime-before",
            "2026-09-01",
        ],
    )
    assert result.exit_code == 0, result.output
    assert captured["collection_id"] == "sentinel-2-l2a-staging"
    assert captured["max_items"] == 2000
    assert captured["datetime_before"] == THRESHOLD


def test_cli_refuses_an_unparseable_threshold(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        STACCollectionManager, "clean_collection", MagicMock(side_effect=AssertionError)
    )
    result = CliRunner().invoke(
        cli,
        ["--api-url", "https://stac.example.com", "clean", "c", "-y", "--datetime-before", "soon"],
    )
    assert result.exit_code != 0
    assert "--datetime-before" in result.output


def test_the_collection_walk_uses_the_shared_resilient_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The purge walk must page /search on the retrying client, not bare Client.open.

    This is the deployed destructive walker (the 6-hourly s2-staging-purge, --max-items
    2000), and it was the last read path left on a client with no timeout and no status
    retries -- the follow-up named in #418.
    """
    import manage_collections
    import stac_auth

    catalog = MagicMock()
    catalog.search.return_value.items.return_value = []
    opened: list[str] = []

    def _open(url: str):
        opened.append(url)
        return catalog

    monkeypatch.setattr(manage_collections.stac_auth, "open_resilient_client", _open)

    STACCollectionManager("https://stac.example.com").get_collection_items("c")

    assert opened == ["https://stac.example.com"], "did not go through open_resilient_client"
    # A page, not the server's default of 10, which is what made a walk 30+ round trips.
    assert catalog.search.call_args.kwargs["limit"] == stac_auth.DEFAULT_PAGE_SIZE


def test_the_resilient_client_never_carries_the_write_session() -> None:
    """Retries must not reach a DELETE/PUT: the write session is built separately."""
    manager = STACCollectionManager("https://stac.example.com")
    # The session used for every write carries no urllib3 status-retry policy.
    for adapter in manager.session.adapters.values():
        assert not getattr(adapter.max_retries, "status_forcelist", None)


# === --keep-software-min: keep a minimum processing:software version (catalogue-only) ===

FLOOR = ("EOPF_GEOZARR", Version("0.13.0"))
NEW = "sentinel-2-l2a-new"


def _sw(i: int, software: object = None, *, encode: bool = False) -> dict:
    """An item whose processing:software is `software` (left out when None); `encode`
    sends it JSON-encoded, as some API responses do."""
    item = _item(i)
    if software is not None:
        item["properties"] = {"processing:software": json.dumps(software) if encode else software}
    return item


def _geozarr(version: str) -> dict:
    return {"EOPF_CPM": "3.0.0", "EOPF_GEOZARR": version}


def _keep(mgr: STACCollectionManager, **kwargs: object) -> int:
    deleted, _, _ = mgr.clean_collection(NEW, keep_software_min=FLOOR, **kwargs)  # type: ignore[arg-type]
    return deleted


@pytest.mark.parametrize(
    ("version", "kept"),
    [
        ("0.13.0", True),  # the floor itself
        ("0.13.1", True),
        ("0.14.0", True),
        ("1.0.0", True),
        ("0.13", True),  # PEP 440: 0.13 == 0.13.0
        ("0.12.0", False),
        ("0.9.9", False),
        ("0.13.0rc1", False),  # PEP 440: a pre-release sorts below its release
    ],
)
def test_keep_software_min_compares_versions_not_strings(version: str, kept: bool) -> None:
    """As strings "0.9.9" > "0.13.0"; as versions it is below the floor."""
    mgr = _manager([_sw(0, _geozarr("0.13.1")), _sw(1, _geozarr(version))])
    assert _keep(mgr) == (0 if kept else 1)
    assert _deleted_ids(mgr) == ([] if kept else ["ITEM_00001"])


def test_keep_software_min_reads_an_object_and_a_json_encoded_string() -> None:
    items = [
        _sw(0, _geozarr("0.13.0")),
        _sw(1, _geozarr("0.13.1"), encode=True),
        _sw(2, _geozarr("0.12.0")),
        _sw(3, _geozarr("0.12.0"), encode=True),
    ]
    mgr = _manager(items)
    assert _keep(mgr) == 2
    assert _deleted_ids(mgr) == ["ITEM_00002", "ITEM_00003"]


def test_keep_software_min_deletes_items_without_the_key_or_the_property() -> None:
    """The census's 23,802 items without EOPF_GEOZARR are deletion candidates, not kept."""
    items = [
        _sw(0, _geozarr("0.13.1")),
        _sw(1, {"EOPF_CPM": "3.0.0", "EOPF_MODEL": "1.0.0"}),
        _sw(2, {"EOPF_CPM": "3.0.0"}, encode=True),
        _sw(3),  # no properties at all
        {**_item(4), "properties": {"datetime": OLD}},  # properties, no processing:software
    ]
    mgr = _manager(items)
    assert _keep(mgr) == 4
    assert _deleted_ids(mgr) == [f"ITEM_{i:05d}" for i in range(1, 5)]


def test_keep_software_min_is_applied_before_the_bound() -> None:
    """The API lists newest first, so the kept items head the list: --max-items 10 must
    still delete 10 candidates, not spend the bound on kept items and delete nothing."""
    items = [_sw(i, _geozarr("0.13.1")) for i in range(30)] + [
        _sw(i, _geozarr("0.12.0")) for i in range(30, 100)
    ]
    mgr = _manager(items)
    assert _keep(mgr, max_items=10) == 10
    assert _deleted_ids(mgr) == [f"ITEM_{i:05d}" for i in range(30, 40)]


@pytest.mark.parametrize(
    "software",
    [
        "EOPF_GEOZARR=0.13.1",
        '{"EOPF_GEOZARR": "0.13.1"',
        ["0.13.1"],
        13,
        {"EOPF_GEOZARR": 0.131},
        {"EOPF_GEOZARR": "latest"},
    ],
    ids=["not-json", "truncated-json", "list", "number", "non-string-value", "not-a-version"],
)
def test_keep_software_min_refuses_a_value_it_cannot_read(software: object) -> None:
    """Fail closed: not provably below the floor either. The whole run is refused before
    any delete, and the offending id is named."""
    mgr = _manager([_sw(0, _geozarr("0.13.1")), _sw(1, _geozarr("0.12.0")), _sw(2, software)])
    with pytest.raises(click.ClickException, match="not a readable version, e.g. ITEM_00002"):
        _keep(mgr)
    mgr.item_manager.delete_item.assert_not_called()


@pytest.mark.parametrize(
    "floor", [("EOPF_GEOZAR", Version("0.13.0")), ("EOPF_GEOZARR", Version("1.13.0"))]
)
def test_keep_software_min_keeping_no_item_refuses(floor: tuple[str, Version]) -> None:
    """A typo in KEY or VERSION keeps nothing, which would delete the whole collection."""
    mgr = _manager([_sw(i, _geozarr("0.13.1")) for i in range(5)] + [_sw(5, _geozarr("0.12.0"))])
    with pytest.raises(click.ClickException, match="keeps no item"):
        mgr.clean_collection(NEW, keep_software_min=floor)
    mgr.item_manager.delete_item.assert_not_called()


def test_keep_software_min_with_nothing_to_delete_is_a_no_op(
    capsys: pytest.CaptureFixture[str],
) -> None:
    mgr = _manager([_sw(i, _geozarr("0.13.1")) for i in range(5)])
    assert _keep(mgr) == 0
    mgr.item_manager.delete_item.assert_not_called()
    assert "Every item is kept; nothing deleted" in capsys.readouterr().out


def test_keep_software_min_still_keeps_demo_scenes(capsys: pytest.CaptureFixture[str]) -> None:
    """A demo-listed item is kept even when its version makes it a candidate, and is
    reported as a demo scene, not counted in the per-value tally."""
    demo = {**_sw(0, _geozarr("0.12.0")), "id": "S2A_MSIL2A_DEMO_NOT_IN_THESE_TESTS"}
    mgr = _manager([demo, _sw(1, _geozarr("0.13.1")), _sw(2, _geozarr("0.12.0"))])
    assert _keep(mgr) == 1
    assert _deleted_ids(mgr) == ["ITEM_00002"]
    out = capsys.readouterr().out
    assert "Protected demo scenes (demo denylist): 1" in out
    assert "delete '0.12.0': 1\n" in out


def test_keep_software_min_dry_run_prints_the_count_per_value_and_deletes_nothing(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The operator compares these lines with the census before any real run."""
    items = (
        [_sw(i, _geozarr("0.13.0"), encode=True) for i in range(3)]
        + [_sw(i, _geozarr("0.13.1")) for i in range(3, 5)]
        + [_sw(5, _geozarr("0.12.0"))]
        + [_sw(i, {"EOPF_CPM": "3.0.0"}) for i in range(6, 10)]
    )
    mgr = _manager(items)
    assert _keep(mgr, dry_run=True, max_items=4) == 0
    mgr.item_manager.delete_item.assert_not_called()
    out = capsys.readouterr().out
    assert (
        "processing:software['EOPF_GEOZARR'] per item (keeping >= 0.13.0):\n"
        "    delete (missing): 4\n"
        "    keep   '0.13.0': 3\n"
        "    keep   '0.13.1': 2\n"
        "    delete '0.12.0': 1\n"
        "    total: keep 5, delete 5\n"
    ) in out
    assert "Bounded run: 4 of 5 items" in out
    assert "Would delete 4 STAC items" in out


def test_cli_refuses_keep_software_min_with_clean_s3(monkeypatch: pytest.MonkeyPatch) -> None:
    """Catalogue-only by design: the stores behind sentinel-2-l2a-new are EODC's."""
    clean = MagicMock(side_effect=AssertionError("clean_collection must not run"))
    monkeypatch.setattr(STACCollectionManager, "clean_collection", clean)
    boto = MagicMock(side_effect=AssertionError("no S3 client may be built"))
    monkeypatch.setattr("manage_collections.boto3.client", boto)
    result = CliRunner().invoke(
        cli,
        ["--api-url", "https://stac.example.com", "clean", NEW, "-y"]
        + ["--keep-software-min", "EOPF_GEOZARR=0.13.0"]
        + ["--clean-s3", "--confine-to", f"s3://{BUCKET}/x/"],
    )
    assert result.exit_code == 2, result.output
    assert "--keep-software-min is catalogue-only" in result.output
    clean.assert_not_called()


@pytest.mark.parametrize(
    "raw", ["EOPF_GEOZARR", "EOPF_GEOZARR=", "=0.13.0", "=", "EOPF_GEOZARR=latest"]
)
def test_cli_refuses_a_malformed_keep_software_min(
    monkeypatch: pytest.MonkeyPatch, raw: str
) -> None:
    clean = MagicMock(side_effect=AssertionError("clean_collection must not run"))
    monkeypatch.setattr(STACCollectionManager, "clean_collection", clean)
    result = CliRunner().invoke(
        cli,
        ["--api-url", "https://stac.example.com", "clean", NEW, "-y", "--keep-software-min", raw],
    )
    assert result.exit_code == 2, result.output
    assert "expected KEY=VERSION" in result.output


def test_cli_keep_software_min_lists_the_counts_before_asking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End to end: the counts are on screen when the prompt asks, and "no" deletes nothing."""
    delete_item = MagicMock(return_value=(True, 0, 0))
    monkeypatch.setattr("manage_collections.STACItemManager.delete_item", delete_item)
    walk = MagicMock(return_value=[_sw(0, _geozarr("0.13.1")), _sw(1, _geozarr("0.12.0"))])
    monkeypatch.setattr(STACCollectionManager, "get_collection_items", walk)
    result = CliRunner().invoke(
        cli,
        ["--api-url", "https://stac.example.com", "clean", NEW]
        + ["--keep-software-min", "EOPF_GEOZARR=0.13.0", "--max-items", "10"],
        input="n\n",
    )
    assert result.exit_code != 0
    assert result.output.index("delete '0.12.0': 1") < result.output.index("Continue?")
    assert (
        "delete up to 10 items without processing:software EOPF_GEOZARR >= 0.13.0, "
        "except those on the demo denylist, from collection 'sentinel-2-l2a-new'"
    ) in result.output
    delete_item.assert_not_called()


def test_cli_keep_software_min_with_nothing_to_delete_exits_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delete_item = MagicMock(side_effect=AssertionError("nothing may be deleted"))
    monkeypatch.setattr("manage_collections.STACItemManager.delete_item", delete_item)
    walk = MagicMock(return_value=[_sw(0, _geozarr("0.13.1"))])
    monkeypatch.setattr(STACCollectionManager, "get_collection_items", walk)
    result = CliRunner().invoke(
        cli,
        ["--api-url", "https://stac.example.com", "clean", NEW, "-y"]
        + ["--keep-software-min", "EOPF_GEOZARR=0.13.0"],
    )
    assert result.exit_code == 0, result.output
    assert "nothing deleted" in result.output


def test_the_delete_has_a_client_timeout() -> None:
    """No activeDeadlineSeconds backs the clean Workflow, so a stalled DELETE must fail its
    item (and the run, which converges on a re-run), not hang the run."""
    manager = STACItemManager("https://stac.example.com")
    manager.session = MagicMock()
    manager.session.delete.return_value = _response(204)
    assert manager.delete_item("c", "ITEM_00001") == (True, 0, 0)
    manager.session.delete.assert_called_once_with(
        "https://stac.example.com/collections/c/items/ITEM_00001", timeout=30
    )
