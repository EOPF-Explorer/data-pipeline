"""Pytest configuration and shared fixtures for data-pipeline tests."""

import pytest


@pytest.fixture
def sample_stac_item():
    """Return a minimal STAC item for testing."""
    return {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "test-item",
        "properties": {
            "datetime": "2025-01-01T00:00:00Z",
            "proj:epsg": 32636,
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [600000, 6290220],
                    [709800, 6290220],
                    [709800, 6400020],
                    [600000, 6400020],
                    [600000, 6290220],
                ]
            ],
        },
        "links": [],
        "assets": {
            "B01": {
                "href": "s3://bucket/data/B01.tif",
                "type": "image/tiff; application=geotiff",
                "roles": ["data"],
                "proj:epsg": 32636,
                "proj:shape": [10980, 10980],
                "proj:transform": [10, 0, 600000, 0, -10, 6400020],
            }
        },
        "collection": "test-collection",
    }


@pytest.fixture
def stac_item_with_proj_code(sample_stac_item):
    """Return a STAC item with proj:code (should be removed)."""
    item = sample_stac_item.copy()
    item["properties"]["proj:code"] = "EPSG:32636"
    item["assets"]["B01"]["proj:code"] = "EPSG:32636"
    return item


@pytest.fixture
def mock_zarr_url():
    """Return a sample GeoZarr URL."""
    return "s3://bucket/path/to/dataset.zarr"


@pytest.fixture
def mock_stac_api_url():
    """Return a mock STAC API URL."""
    return "https://api.example.com/stac"
