"""Unit tests for scripts/watch_cdse_and_process.py."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

from watch_cdse_and_process import build_parser, tile_bbox  # noqa: E402


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
