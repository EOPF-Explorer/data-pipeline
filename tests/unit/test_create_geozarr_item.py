"""Unit tests for create_geozarr_item.py."""

import json
from unittest.mock import MagicMock, patch

import pytest

from scripts.create_geozarr_item import (
    create_geozarr_item,
    find_source_zarr_base,
    main,
    normalize_asset_href,
    s3_to_https,
)


@pytest.fixture
def source_item():
    """Valid source STAC item with band assets."""
    return {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "test-item",
        "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]},
        "bbox": [0, 0, 1, 1],
        "properties": {"datetime": "2025-01-01T00:00:00Z"},
        "collection": "source-col",
        "links": [],
        "assets": {
            "B01": {"href": "s3://source/data.zarr/r10m/b01", "type": "image/tiff"},
            "B02": {"href": "s3://source/data.zarr/r10m/b02", "type": "image/tiff"},
            "B08A": {"href": "s3://source/data.zarr/r60m/b08a", "type": "image/tiff"},
        },
    }


@pytest.fixture
def mock_httpx_and_validation(source_item):
    """Mock httpx (validation removed from create_geozarr_item)."""
    with patch("scripts.create_geozarr_item.httpx.get") as mock_get:
        mock_get.return_value = MagicMock(json=lambda: source_item)
        yield mock_get


def test_s3_to_https():
    """Test S3 URL conversion."""
    assert (
        s3_to_https("s3://bucket/path/file.zarr", "https://s3.io")
        == "https://bucket.s3.io/path/file.zarr"
    )
    assert s3_to_https("https://already/https", "https://s3.io") == "https://already/https"


def test_normalize_asset_href():
    """Test asset href normalization for r60m bands."""
    # r60m bands need /0/ inserted
    assert (
        normalize_asset_href("s3://bucket/data.zarr/r60m/b08a")
        == "s3://bucket/data.zarr/r60m/0/b08a"
    )
    # Already has /0/
    assert (
        normalize_asset_href("s3://bucket/data.zarr/r60m/0/b08a")
        == "s3://bucket/data.zarr/r60m/0/b08a"
    )
    # r10m/r20m don't need changes
    assert (
        normalize_asset_href("s3://bucket/data.zarr/r10m/b02") == "s3://bucket/data.zarr/r10m/b02"
    )


def test_find_source_zarr_base():
    """Test finding Zarr base URL from assets."""
    item = {
        "assets": {
            "B01": {"href": "s3://bucket/data.zarr/r10m/b01"},
            "B02": {"href": "s3://bucket/data.zarr/r10m/b02"},
        }
    }
    assert find_source_zarr_base(item) == "s3://bucket/data.zarr"
    assert find_source_zarr_base({"assets": {}}) is None


def test_create_geozarr_item_rewrites_assets(tmp_path, mock_httpx_and_validation):
    """Test that asset hrefs are rewritten to point to GeoZarr output."""
    output = tmp_path / "item.json"

    create_geozarr_item(
        "https://stac.api/items/test",
        "geozarr-col",
        "s3://bucket/output.zarr",
        "https://s3.endpoint.io",
        str(output),
    )

    item = json.loads(output.read_text())
    assert item["collection"] == "geozarr-col"

    # Check that band assets were rewritten
    assert "B01" in item["assets"]
    assert "B02" in item["assets"]
    assert "B08A" in item["assets"]

    # r10m bands should be rewritten but not normalized
    assert item["assets"]["B01"]["href"] == "https://bucket.s3.endpoint.io/output.zarr/r10m/b01"
    assert item["assets"]["B02"]["href"] == "https://bucket.s3.endpoint.io/output.zarr/r10m/b02"

    # r60m bands should be normalized with /0/ inserted
    assert item["assets"]["B08A"]["href"] == "https://bucket.s3.endpoint.io/output.zarr/r60m/0/b08a"


def test_http_error():
    """Test HTTP error handling."""
    with (
        patch("scripts.create_geozarr_item.httpx.get", side_effect=Exception("Failed")),
        pytest.raises(Exception, match="Failed"),
    ):
        create_geozarr_item(
            "https://stac/items/test",
            "col",
            "s3://bucket/data.zarr",
            "https://s3",
            "/tmp/out.json",
        )


def test_main(tmp_path, mock_httpx_and_validation):
    """Test main() CLI."""
    output = tmp_path / "item.json"

    with patch(
        "sys.argv",
        [
            "create_geozarr_item.py",
            "--source-url",
            "https://stac/items/test",
            "--collection",
            "col",
            "--geozarr-url",
            "s3://bucket/output.zarr",
            "--s3-endpoint",
            "https://s3.io",
            "--output",
            str(output),
        ],
    ):
        main()

    assert output.exists()
