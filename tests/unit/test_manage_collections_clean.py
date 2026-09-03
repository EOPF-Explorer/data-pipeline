"""Unit tests for the bounded, confined `manage_collections.py clean` path.

`clean` is the operator purge tool: it deletes every item in a collection and,
with --clean-s3, their Zarr stores. Two properties carry the safety here and
both are tested below:

- the confinement sweep sees the WHOLE collection, even on a bounded run, so a
  rogue href never hides in a batch the run has not reached yet;
- --max-items bounds what is DELETED, inside the tool rather than via an
  external timeout or kill.

No network: the item manager and S3 client are mocked.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import click
import pytest

OPERATOR_TOOLS = Path(__file__).parent.parent.parent / "operator-tools"
if str(OPERATOR_TOOLS) not in sys.path:
    sys.path.insert(0, str(OPERATOR_TOOLS))

from manage_collections import (  # noqa: E402
    STACCollectionManager,
    _report_confinement_sweep,
)

BUCKET = "esa-zarr-sentinel-explorer-fra"
STAGING = (BUCKET, "tests-output/sentinel-2-l2a-staging/")
PROD = (BUCKET, "tests-output/sentinel-2-l2a/")


def _item(i: int, href: str | None = None) -> dict:
    href = href or f"s3://{BUCKET}/tests-output/sentinel-2-l2a-staging/scene{i}/data.zarr/"
    return {"id": f"ITEM_{i:05d}", "assets": {"data": {"alternate": {"s3": {"href": href}}}}}


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
