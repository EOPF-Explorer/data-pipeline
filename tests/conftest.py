"""Pytest configuration and shared fixtures for data-pipeline tests."""

import atexit
import sys
import warnings

import pytest

# Suppress noisy async context warnings from zarr/s3fs
warnings.filterwarnings("ignore", category=ResourceWarning)
warnings.filterwarnings("ignore", message="coroutine.*was never awaited")


# Global stderr filter that stays active even after pytest teardown
_original_stderr = sys.stderr
_suppress_traceback = False


class _FilteredStderr:
    def write(self, text):
        global _suppress_traceback

        # Start suppressing when we see async context errors
        if any(
            marker in text
            for marker in [
                "Exception ignored",
                "Traceback (most recent call last)",
                "ValueError: <Token",
                "was created in a different Context",
                "zarr/storage/",
                "s3fs/core.py",
                "aiobotocore/context.py",
            ]
        ):
            _suppress_traceback = True

        # Reset suppression on empty lines (between tracebacks)
        if not text.strip():
            _suppress_traceback = False

        # Only write if not currently suppressing
        if not _suppress_traceback:
            _original_stderr.write(text)

    def flush(self):
        _original_stderr.flush()


def _restore_stderr():
    """Restore original stderr at exit."""
    sys.stderr = _original_stderr


# Install filter at module load time
sys.stderr = _FilteredStderr()
atexit.register(_restore_stderr)


@pytest.fixture(autouse=True, scope="function")
def clear_prometheus_registry():
    """Clear Prometheus registry before each test to avoid duplicates."""
    import contextlib

    try:
        from prometheus_client import REGISTRY

        collectors = list(REGISTRY._collector_to_names.keys())
        for collector in collectors:
            with contextlib.suppress(Exception):
                REGISTRY.unregister(collector)
    except ImportError:
        pass
    yield


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
