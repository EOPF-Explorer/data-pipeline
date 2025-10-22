"""Tests for utils.py - STAC asset URL extraction."""

import json
from unittest.mock import mock_open, patch

import pytest

from scripts.utils import get_zarr_url


class TestGetZarrUrl:
    """Test Zarr URL extraction from STAC items."""

    def test_finds_product_asset_first(self):
        """Product asset has highest priority."""
        stac_json = json.dumps(
            {
                "assets": {
                    "product": {"href": "s3://bucket/product.zarr"},
                    "zarr": {"href": "s3://bucket/other.zarr"},
                    "thumbnail": {"href": "s3://bucket/random.zarr"},
                }
            }
        )
        with patch("scripts.utils.urlopen", mock_open(read_data=stac_json.encode())):
            url = get_zarr_url("https://stac.example.com/item")
            assert url == "s3://bucket/product.zarr"

    def test_finds_zarr_asset_second(self):
        """Zarr asset used if no product asset."""
        stac_json = json.dumps(
            {
                "assets": {
                    "thumbnail": {"href": "s3://bucket/thumb.png"},
                    "zarr": {"href": "s3://bucket/data.zarr"},
                    "metadata": {"href": "s3://bucket/other.zarr"},
                }
            }
        )
        with patch("scripts.utils.urlopen", mock_open(read_data=stac_json.encode())):
            url = get_zarr_url("https://stac.example.com/item")
            assert url == "s3://bucket/data.zarr"

    def test_fallback_to_any_zarr_asset(self):
        """Falls back to any asset with .zarr in href."""
        stac_json = json.dumps(
            {
                "assets": {
                    "thumbnail": {"href": "s3://bucket/thumb.png"},
                    "data": {"href": "s3://bucket/measurements.zarr"},
                }
            }
        )
        with patch("scripts.utils.urlopen", mock_open(read_data=stac_json.encode())):
            url = get_zarr_url("https://stac.example.com/item")
            assert url == "s3://bucket/measurements.zarr"

    def test_no_zarr_asset_raises_error(self):
        """Raises RuntimeError if no Zarr asset found."""
        stac_json = json.dumps(
            {
                "assets": {
                    "thumbnail": {"href": "s3://bucket/thumb.png"},
                    "metadata": {"href": "s3://bucket/meta.json"},
                }
            }
        )
        with (
            patch("scripts.utils.urlopen", mock_open(read_data=stac_json.encode())),
            pytest.raises(RuntimeError, match="No Zarr asset found"),
        ):
            get_zarr_url("https://stac.example.com/item")

    def test_empty_assets_raises_error(self):
        """Raises RuntimeError if assets dict is empty."""
        stac_json = json.dumps({"assets": {}})
        with (
            patch("scripts.utils.urlopen", mock_open(read_data=stac_json.encode())),
            pytest.raises(RuntimeError, match="No Zarr asset found"),
        ):
            get_zarr_url("https://stac.example.com/item")

    def test_missing_assets_key_raises_error(self):
        """Raises RuntimeError if no assets key in item."""
        stac_json = json.dumps({"id": "test-item"})
        with (
            patch("scripts.utils.urlopen", mock_open(read_data=stac_json.encode())),
            pytest.raises(RuntimeError, match="No Zarr asset found"),
        ):
            get_zarr_url("https://stac.example.com/item")

    def test_product_asset_without_href(self):
        """Skips product asset if no href, falls back."""
        stac_json = json.dumps(
            {
                "assets": {
                    "product": {"type": "application/json"},
                    "data": {"href": "s3://bucket/data.zarr"},
                }
            }
        )
        with patch("scripts.utils.urlopen", mock_open(read_data=stac_json.encode())):
            url = get_zarr_url("https://stac.example.com/item")
            assert url == "s3://bucket/data.zarr"

    def test_handles_http_zarr_urls(self):
        """Works with HTTP URLs for Zarr."""
        stac_json = json.dumps(
            {
                "assets": {
                    "product": {"href": "https://example.com/data.zarr"},
                }
            }
        )
        with patch("scripts.utils.urlopen", mock_open(read_data=stac_json.encode())):
            url = get_zarr_url("https://stac.example.com/item")
            assert url == "https://example.com/data.zarr"


def test_extract_item_id_from_stac_url():
    """Test extracting item ID from STAC item URL."""
    from scripts.utils import extract_item_id

    url = "https://stac.example.com/collections/sentinel-2-l2a/items/S2A_MSIL2A_20251021T101101_N0511_R022_T32TNR_20251021T115713"
    assert extract_item_id(url) == "S2A_MSIL2A_20251021T101101_N0511_R022_T32TNR_20251021T115713"


def test_extract_item_id_with_trailing_slash():
    """Test extracting item ID from URL with trailing slash."""
    from scripts.utils import extract_item_id

    url = "https://stac.example.com/collections/sentinel-2-l2a/items/S2A_MSIL2A_20251021/"
    assert extract_item_id(url) == "S2A_MSIL2A_20251021"
