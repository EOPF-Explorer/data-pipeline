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

import sys
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import click
import pytest
from click.testing import CliRunner

OPERATOR_TOOLS = Path(__file__).parent.parent.parent / "operator-tools"
if str(OPERATOR_TOOLS) not in sys.path:
    sys.path.insert(0, str(OPERATOR_TOOLS))

from manage_collections import (  # noqa: E402
    STACCollectionManager,
    _report_confinement_sweep,
    cli,
    parse_threshold,
)

BUCKET = "esa-zarr-sentinel-explorer-fra"
STAGING = (BUCKET, "tests-output/sentinel-2-l2a-staging/")
PROD = (BUCKET, "tests-output/sentinel-2-l2a/")


def _item(i: int, href: str | None = None, acquired: str | None = None) -> dict:
    href = href or f"s3://{BUCKET}/tests-output/sentinel-2-l2a-staging/scene{i}/data.zarr/"
    item = {"id": f"ITEM_{i:05d}", "assets": {"data": {"alternate": {"s3": {"href": href}}}}}
    if acquired is not None:
        item["properties"] = {"datetime": acquired}
    return item


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

    with pytest.raises(click.ClickException):
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
    with pytest.raises(click.ClickException):
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
