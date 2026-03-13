#!/usr/bin/env python3
"""Unit tests for fix_zarr_asset_media_types function."""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pystac import Asset, Item

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
from register_v1 import fix_zarr_asset_media_types


def get_fixture_files():
    """Get all STAC item fixture files."""
    fixtures_dir = Path(__file__).parent / "fixtures" / "stac_to_register"
    return list(fixtures_dir.glob("*.json"))


@pytest.fixture(params=get_fixture_files())
def stac_item(request):
    """Load a STAC item from fixtures."""
    with open(request.param) as f:
        return Item.from_dict(json.load(f))


class TestFixZarrAssetMediaTypes:
    """Test suite for fix_zarr_asset_media_types function."""

    def test_fixes_vnd_plus_zarr_media_type(self, stac_item):
        """Test that application/vnd+zarr is corrected to application/vnd.zarr; version=3."""
        # Verify fixture has wrong media type before fix
        wrong_assets = [
            key
            for key, asset in stac_item.assets.items()
            if asset.media_type == "application/vnd+zarr"
        ]
        assert len(wrong_assets) > 0, "Fixture should have assets with wrong media type"

        fix_zarr_asset_media_types(stac_item)

        # No assets should have the wrong media type after fix
        for key, asset in stac_item.assets.items():
            assert (
                asset.media_type != "application/vnd+zarr"
            ), f"Asset {key} still has wrong media type"

    def test_aot_scl_wvp_get_version_3(self, stac_item):
        """Test that AOT, SCL, WVP assets get version=3 in media type."""
        fix_zarr_asset_media_types(stac_item)

        for key in ["AOT_10m", "SCL_20m", "WVP_10m"]:
            if key in stac_item.assets:
                assert (
                    stac_item.assets[key].media_type == "application/vnd.zarr; version=3"
                ), f"Asset {key} should have 'application/vnd.zarr; version=3'"

    def test_removes_zipped_product_asset(self, stac_item):
        """Test that zipped_product asset is removed."""
        assert "zipped_product" in stac_item.assets, "Fixture should have zipped_product"

        fix_zarr_asset_media_types(stac_item)

        assert "zipped_product" not in stac_item.assets, "zipped_product should be removed"

    def test_preserves_correct_zarr_media_types(self):
        """Test that assets with correct media type are not modified."""
        item = Item(
            id="test",
            geometry={"type": "Point", "coordinates": [0, 0]},
            bbox=[0, 0, 0, 0],
            datetime=datetime(2025, 1, 1, tzinfo=UTC),
            properties={},
        )
        item.add_asset(
            "good",
            Asset(href="https://example.com/data.zarr/test", media_type="application/vnd.zarr"),
        )
        item.add_asset(
            "good_v3",
            Asset(
                href="https://example.com/data.zarr/test2",
                media_type="application/vnd.zarr; version=3",
            ),
        )

        fix_zarr_asset_media_types(item)

        assert item.assets["good"].media_type == "application/vnd.zarr"
        assert item.assets["good_v3"].media_type == "application/vnd.zarr; version=3"

    def test_preserves_non_zarr_assets(self, stac_item):
        """Test that non-zarr assets (thumbnail, etc.) are not modified."""
        # Add a non-zarr asset
        stac_item.add_asset(
            "test_png",
            Asset(href="https://example.com/thumb.png", media_type="image/png"),
        )

        fix_zarr_asset_media_types(stac_item)

        assert stac_item.assets["test_png"].media_type == "image/png"
