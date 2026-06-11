"""Unit tests for register_v1_s1_rtc.py -- register."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pystac
import pytest

scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

from register_v1_s1_rtc import register  # noqa: E402

# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

_STORE = "s3://bucket/prefix/s1-grd-rtc-31TCH.zarr"
_COLLECTION = "sentinel-1-grd-rtc-staging"


def _make_item() -> pystac.Item:
    item = pystac.Item(
        id="s1-rtc-31TCH",
        geometry={
            "type": "Polygon",
            "coordinates": [[[0, 44], [2, 44], [2, 46], [0, 46], [0, 44]]],
        },
        bbox=[0.0, 44.0, 2.0, 46.0],
        datetime=None,
        properties={
            "start_datetime": "2023-01-15T06:12:34+00:00",
            "end_datetime": "2023-01-27T06:12:35+00:00",
            "sar:instrument_mode": "IW",
            "sar:frequency_band": "C",
            "sar:center_frequency": 5.405,
            "sar:polarizations": ["VV", "VH"],
            "sar:product_type": "GRD",
            "sat:orbit_state": "ascending",
            "proj:code": "EPSG:32631",
        },
        stac_extensions=[
            "https://stac-extensions.github.io/sar/v1.0.0/schema.json",
            "https://stac-extensions.github.io/sat/v1.0.0/schema.json",
            "https://stac-extensions.github.io/projection/v2.0.0/schema.json",
        ],
        collection=_COLLECTION,
    )
    item.add_asset(
        "zarr-store",
        pystac.Asset(href=_STORE, media_type="application/vnd.zarr; version=3", roles=["data"]),
    )
    item.add_asset(
        "vv",
        pystac.Asset(
            href=f"{_STORE}/ascending/r10m/vv",
            media_type="application/vnd.zarr; version=3",
            roles=["data"],
        ),
    )
    item.add_asset(
        "vh",
        pystac.Asset(
            href=f"{_STORE}/ascending/r10m/vh",
            media_type="application/vnd.zarr; version=3",
            roles=["data"],
        ),
    )
    return item


_MOD = "register_v1_s1_rtc"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_upserts_item_with_correct_id() -> None:
    """upsert_item must be called once with item id 's1-rtc-31TCH'."""
    with (
        patch(f"{_MOD}.build_s1_rtc_stac_item", return_value=_make_item()),
        patch(f"{_MOD}.warm_thumbnail_cache"),
        patch(f"{_MOD}.upsert_item") as mock_upsert,
        patch(f"{_MOD}.Client"),
        patch(f"{_MOD}._titiler_render_copy"),  # TEMPORARY #246
    ):
        result = register(
            _STORE,
            _COLLECTION,
            "https://stac.example.com",
            "https://raster.example.com",
            "https://s3.example.com",
        )

    assert result == 0
    mock_upsert.assert_called_once()
    called_item = (
        mock_upsert.call_args[1]["item"]
        if mock_upsert.call_args[1]
        else mock_upsert.call_args[0][2]
    )
    assert called_item.id == "s1-rtc-31TCH"


# --- TEMPORARY #246: titiler render-copy (revert when titiler-eopf#108 lands) ---------------
# titiler-eopf reconstructs the store path as {bucket}/tests-output/{collection}/{item_id}.zarr
# and ignores the asset href, so register() copies the store there. These tests pin that contract.


def test_render_copy_targets_reconstructed_path() -> None:
    """Copy lands the store at {bucket}/tests-output/{collection}/{item_id}.zarr (item-id name)."""
    import register_v1_s1_rtc as m

    fs = MagicMock()
    fs.exists.return_value = False
    with patch("s3fs.S3FileSystem", return_value=fs):
        m._titiler_render_copy(
            "s3://esa-zarr-sentinel-explorer-fra/sentinel-1-grd-rtc-tests/s1-grd-rtc-32TLR.zarr",
            "sentinel-1-grd-rtc-tests",
            "s1-rtc-32TLR",
            "https://s3.example.com",
        )
    fs.copy.assert_called_once_with(
        "esa-zarr-sentinel-explorer-fra/sentinel-1-grd-rtc-tests/s1-grd-rtc-32TLR.zarr",
        "esa-zarr-sentinel-explorer-fra/tests-output/sentinel-1-grd-rtc-tests/s1-rtc-32TLR.zarr",
        recursive=True,
    )
    fs.rm.assert_not_called()


def test_render_copy_overwrites_existing() -> None:
    """Idempotent: an existing dst is removed before the copy."""
    import register_v1_s1_rtc as m

    fs = MagicMock()
    fs.exists.return_value = True
    with patch("s3fs.S3FileSystem", return_value=fs):
        m._titiler_render_copy(
            "s3://b/sentinel-1-grd-rtc-tests/s1-grd-rtc-32TLR.zarr",
            "sentinel-1-grd-rtc-tests",
            "s1-rtc-32TLR",
            "https://s3.example.com",
        )
    fs.rm.assert_called_once_with(
        "b/tests-output/sentinel-1-grd-rtc-tests/s1-rtc-32TLR.zarr", recursive=True
    )
    fs.copy.assert_called_once()


def test_render_copy_best_effort_on_failure() -> None:
    """A copy failure must NOT raise -- the item is already registered."""
    import register_v1_s1_rtc as m

    fs = MagicMock()
    fs.exists.return_value = False
    fs.copy.side_effect = OSError("boom")
    with patch("s3fs.S3FileSystem", return_value=fs):
        m._titiler_render_copy(
            "s3://b/c/s1-grd-rtc-32TLR.zarr", "c", "s1-rtc-32TLR", "https://s3.example.com"
        )  # must not raise


def test_render_copy_skips_non_s3_store() -> None:
    """A local (non-s3) store is a no-op -- s3fs is never constructed."""
    import register_v1_s1_rtc as m

    with patch("s3fs.S3FileSystem") as mock_fs:
        m._titiler_render_copy("local-store/s1-grd-rtc-32TLR.zarr", "c", "s1-rtc-32TLR", "")
    mock_fs.assert_not_called()


def test_register_invokes_render_copy_after_upsert() -> None:
    """register() calls the render-copy once with (store, collection, item.id, endpoint)."""
    with (
        patch(f"{_MOD}.build_s1_rtc_stac_item", return_value=_make_item()),
        patch(f"{_MOD}.warm_thumbnail_cache"),
        patch(f"{_MOD}.upsert_item"),
        patch(f"{_MOD}.Client"),
        patch(f"{_MOD}._titiler_render_copy") as mock_copy,
    ):
        register(
            _STORE,
            _COLLECTION,
            "https://stac.example.com",
            "https://raster.example.com",
            "https://s3.example.com",
        )
    mock_copy.assert_called_once_with(_STORE, _COLLECTION, "s1-rtc-31TCH", "https://s3.example.com")


# --- end TEMPORARY #246 --------------------------------------------------------------------


def test_visualization_links_called() -> None:
    """add_visualization_links must be called once with correct raster URL and collection."""
    with (
        patch(f"{_MOD}.build_s1_rtc_stac_item", return_value=_make_item()),
        patch(f"{_MOD}.warm_thumbnail_cache"),
        patch(f"{_MOD}.upsert_item"),
        patch(f"{_MOD}.Client"),
        patch(f"{_MOD}._titiler_render_copy"),  # TEMPORARY #246
        patch(f"{_MOD}.add_visualization_links") as mock_viz,
    ):
        register(
            _STORE,
            _COLLECTION,
            "https://stac.example.com",
            "https://raster.example.com",
            "https://s3.example.com",
        )

    mock_viz.assert_called_once()
    _, raster_url, coll = mock_viz.call_args[0]
    assert raster_url == "https://raster.example.com"
    assert coll == _COLLECTION


def test_s2_helpers_not_called() -> None:
    """S2-specific helpers must not be imported or present in the module namespace."""
    import register_v1_s1_rtc

    assert not hasattr(
        register_v1_s1_rtc, "consolidate_reflectance_assets"
    ), "consolidate_reflectance_assets must not be imported into register_v1_s1_rtc"
    assert not hasattr(
        register_v1_s1_rtc, "fix_zarr_asset_media_types"
    ), "fix_zarr_asset_media_types must not be imported into register_v1_s1_rtc"


def test_exits_nonzero_on_bad_store() -> None:
    """When build_s1_rtc_stac_item raises ValueError, register returns non-zero."""
    with (
        patch(f"{_MOD}.build_s1_rtc_stac_item", side_effect=ValueError("no acquisitions")),
        patch(f"{_MOD}.upsert_item") as mock_upsert,
    ):
        result = register(
            _STORE,
            _COLLECTION,
            "https://stac.example.com",
            "https://raster.example.com",
            "https://s3.example.com",
        )

    assert result != 0
    mock_upsert.assert_not_called()


def test_env_mismatch_rejected_before_build() -> None:
    """A staging collection paired with a tests-bucket store is the 32TLR footgun on the
    standalone register path -> reject before building or upserting anything."""
    bad_store = (
        "s3://esa-zarr-sentinel-explorer-tests/sentinel-1-grd-rtc-staging/s1-grd-rtc-31TCH.zarr"
    )
    with (
        patch(f"{_MOD}.build_s1_rtc_stac_item") as mock_build,
        patch(f"{_MOD}.upsert_item") as mock_upsert,
        pytest.raises(ValueError, match="mismatch"),
    ):
        register(
            bad_store,
            "sentinel-1-grd-rtc-staging",
            "https://stac.example.com",
            "https://raster.example.com",
            "https://s3.example.com",
        )

    mock_build.assert_not_called()
    mock_upsert.assert_not_called()


def test_matched_env_store_allowed() -> None:
    """A staging-bucket store + staging collection passes the guard and registers normally."""
    good_store = (
        "s3://esa-zarr-sentinel-explorer-s1-l1grd-staging/"
        "sentinel-1-grd-rtc-staging/s1-grd-rtc-31TCH.zarr"
    )
    with (
        patch(f"{_MOD}.build_s1_rtc_stac_item", return_value=_make_item()),
        patch(f"{_MOD}.warm_thumbnail_cache"),
        patch(f"{_MOD}.upsert_item") as mock_upsert,
        patch(f"{_MOD}.Client"),
        patch(f"{_MOD}._titiler_render_copy"),  # TEMPORARY #246
    ):
        result = register(
            good_store,
            "sentinel-1-grd-rtc-staging",
            "https://stac.example.com",
            "https://raster.example.com",
            "https://s3.example.com",
        )

    assert result == 0
    mock_upsert.assert_called_once()
