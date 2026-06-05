"""Unit tests for scripts/watch_cdse_and_process.py."""

from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

from watch_cdse_and_process import (  # noqa: E402
    build_parser,
    is_processed,
    load_processed,
    mark_processed,
    query_cdse,
    save_processed,
    tile_bbox,
)

_MOD = "watch_cdse_and_process"


def test_tile_bbox_31tch_matches_mgrs_square() -> None:
    """31TCH bbox is the MGRS 100 km square corners (min/max), not the spec's rounded AOI.

    Validated against mgrs 1.5.4 on 2026-06-05: the four corners of 31TCH yield
    [0.533, 42.427, 1.784, 43.346] — intentionally tighter/offset vs the spec's [0,42,2,43].
    """
    bbox = tile_bbox("31TCH")
    expected = [0.533, 42.427, 1.784, 43.346]
    assert len(bbox) == 4
    for got, want in zip(bbox, expected, strict=True):
        assert got == pytest.approx(want, abs=0.05)


def test_tile_bbox_order_is_lonmin_latmin_lonmax_latmax() -> None:
    """bbox follows STAC order: [lon_min, lat_min, lon_max, lat_max]."""
    lon_min, lat_min, lon_max, lat_max = tile_bbox("31TCH")
    assert lon_min < lon_max
    assert lat_min < lat_max


@pytest.mark.parametrize("bad", ["", "ZZZZZ", "not-a-tile", "31"])
def test_tile_bbox_invalid_raises_clear_error(bad: str) -> None:
    """Adversarial: a malformed/unknown tile id raises a clear ValueError, not a leaked mgrs error."""
    with pytest.raises(ValueError, match="MGRS tile"):
        tile_bbox(bad)


def test_parser_has_no_s3_zarr_prefix() -> None:
    """Watcher aligns to the real Script B: no --s3-zarr-prefix (Zarr path derives from --collection)."""
    opt_strings = {s for a in build_parser()._actions for s in a.option_strings}
    assert "--s3-zarr-prefix" not in opt_strings


def test_parser_lists_expected_args() -> None:
    """The full sub-issue 10 interface is present."""
    opt_strings = {s for a in build_parser()._actions for s in a.option_strings}
    for expected in (
        "--tiles",
        "--orbit-direction",
        "--lookback-days",
        "--s3-bucket",
        "--s3-prefix",
        "--s3-zarr-bucket",
        "--s3-endpoint",
        "--collection",
        "--stac-api-url",
        "--raster-api-url",
        "--dry-run",
    ):
        assert expected in opt_strings


# --- query_cdse -----------------------------------------------------------------------------


def _item(item_id: str, when: dt.datetime | None, properties: dict | None = None) -> MagicMock:
    item = MagicMock()
    item.id = item_id
    item.datetime = when
    item.properties = properties or {}
    return item


def _patched_client(items: list[MagicMock]) -> MagicMock:
    """Patch Client.open -> client whose search().items() yields `items`. Returns the client mock."""
    client = MagicMock()
    client.search.return_value.items.return_value = iter(items)
    return client


def test_query_returns_parsed_products() -> None:
    items = [
        _item("S1A_IW_GRDH_A", dt.datetime(2025, 2, 5, 6, 29, tzinfo=dt.UTC)),
        _item("S1A_IW_GRDH_B", dt.datetime(2025, 2, 6, 6, 30, tzinfo=dt.UTC)),
    ]
    with patch(f"{_MOD}.Client.open", return_value=_patched_client(items)):
        products = query_cdse("https://cdse/stac", [0.5, 42.4, 1.8, 43.3], "descending", 7)
    assert products == [
        {"product_id": "S1A_IW_GRDH_A", "date": "2025-02-05"},
        {"product_id": "S1A_IW_GRDH_B", "date": "2025-02-06"},
    ]


def test_query_empty_returns_empty_list() -> None:
    with patch(f"{_MOD}.Client.open", return_value=_patched_client([])):
        assert query_cdse("https://cdse/stac", [0.5, 42.4, 1.8, 43.3], "descending", 7) == []


def test_query_skips_item_without_datetime() -> None:
    """Adversarial: an item with no datetime and no start_datetime is skipped, not crashed on."""
    items = [
        _item("good", dt.datetime(2025, 2, 5, tzinfo=dt.UTC)),
        _item("bad", None, properties={}),
    ]
    with patch(f"{_MOD}.Client.open", return_value=_patched_client(items)):
        products = query_cdse("https://cdse/stac", [0.5, 42.4, 1.8, 43.3], "descending", 7)
    assert [p["product_id"] for p in products] == ["good"]


def test_query_applies_orbit_and_collection_filter() -> None:
    """The CDSE search is scoped to SENTINEL-1-GRD, the bbox, and the orbit-state filter."""
    client = _patched_client([])
    with patch(f"{_MOD}.Client.open", return_value=client):
        query_cdse("https://cdse/stac", [0.5, 42.4, 1.8, 43.3], "descending", 7)
    kwargs = client.search.call_args.kwargs
    assert kwargs["collections"] == ["SENTINEL-1-GRD"]
    assert kwargs["bbox"] == [0.5, 42.4, 1.8, 43.3]
    assert kwargs["query"] == {"sat:orbit_state": {"eq": "descending"}}


# --- state file -----------------------------------------------------------------------------


def test_load_missing_file_returns_empty(tmp_path: Path) -> None:
    assert load_processed(tmp_path / "nope.json") == {}


def test_mark_then_is_processed_roundtrip_via_disk(tmp_path: Path) -> None:
    """mark -> save -> load -> is_processed returns True for the same tile+orbit+product."""
    path = tmp_path / "data" / ".processed_products.json"
    state = load_processed(path)
    assert not is_processed(state, "31TCH", "descending", "S1A_X")
    mark_processed(state, "31TCH", "descending", "S1A_X", "2025-02-05")
    save_processed(path, state)

    reloaded = load_processed(path)
    assert is_processed(reloaded, "31TCH", "descending", "S1A_X")


def test_is_processed_scoped_by_tile_and_orbit(tmp_path: Path) -> None:
    """A product marked under one tile/orbit is not considered processed under another."""
    state: dict = {}
    mark_processed(state, "31TCH", "descending", "S1A_X", "2025-02-05")
    assert is_processed(state, "31TCH", "descending", "S1A_X")
    assert not is_processed(state, "31TCH", "ascending", "S1A_X")
    assert not is_processed(state, "30TXM", "descending", "S1A_X")


def test_malformed_state_file_treated_as_empty(tmp_path: Path) -> None:
    """Adversarial: a corrupt/non-JSON state file degrades to empty, not a crash."""
    path = tmp_path / ".processed_products.json"
    path.write_text("{ this is not valid json ]")
    assert load_processed(path) == {}
