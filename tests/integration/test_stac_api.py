"""Integration tests for STAC API workflows.

These tests verify end-to-end scenarios with mocked external dependencies.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from pystac import Asset, Item


@pytest.fixture
def test_pystac_item():
    """Sample pystac Item for testing."""
    item = Item(
        id="S2B_MSIL2A_20250101T000000_test",
        geometry=None,
        bbox=None,
        datetime=datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC),
        properties={},
        collection="sentinel-2-l2a",
    )
    item.add_asset(
        "data",
        Asset(
            href="s3://bucket/test.zarr",
            media_type="application/vnd+zarr",
            roles=["data"],
        ),
    )
    item.add_asset(
        "TCI_10m",
        Asset(
            href="s3://bucket/test.zarr/measurements/reflectance/r10m/TCI",
            media_type="application/vnd+zarr",
            roles=["visual"],
        ),
    )
    return item


@pytest.mark.integration
def test_upsert_item_creates_new(test_pystac_item):
    """Test upsert_item creates new STAC item when it doesn't exist."""
    from scripts.register import upsert_item

    # Mock client and collection
    mock_client = MagicMock()
    mock_collection = MagicMock()
    mock_client.get_collection.return_value = mock_collection

    # Mock item doesn't exist
    mock_collection.get_item.side_effect = Exception("Not found")

    # Mock successful POST
    mock_response = MagicMock()
    mock_response.status_code = 201
    mock_response.raise_for_status = MagicMock()
    mock_stac_io = MagicMock()
    mock_stac_io.session = MagicMock()
    mock_stac_io.session.post.return_value = mock_response
    mock_stac_io.timeout = 30
    mock_client._stac_io = mock_stac_io

    upsert_item(mock_client, "test-collection", test_pystac_item)

    # Verify POST was called to create item
    assert mock_stac_io.session.post.called


@pytest.mark.integration
def test_upsert_item_updates_existing(test_pystac_item):
    """Test upsert_item updates existing item."""
    from scripts.register import upsert_item

    # Mock client and collection
    mock_client = MagicMock()
    mock_collection = MagicMock()
    mock_client.get_collection.return_value = mock_collection

    # Mock item exists
    mock_collection.get_item.return_value = MagicMock()

    # Mock successful DELETE and POST
    mock_stac_io = MagicMock()
    mock_stac_io.session = MagicMock()
    mock_stac_io.timeout = 30

    mock_delete_response = MagicMock()
    mock_delete_response.status_code = 204
    mock_delete_response.raise_for_status = MagicMock()
    mock_stac_io.session.delete.return_value = mock_delete_response

    mock_post_response = MagicMock()
    mock_post_response.status_code = 201
    mock_post_response.raise_for_status = MagicMock()
    mock_stac_io.session.post.return_value = mock_post_response

    mock_client._stac_io = mock_stac_io

    upsert_item(mock_client, "test-collection", test_pystac_item)

    # Verify DELETE then POST was called
    assert mock_stac_io.session.delete.called
    assert mock_stac_io.session.post.called


@pytest.mark.integration
def test_add_visualization_links_s2(test_pystac_item):
    """Test add_visualization_links for Sentinel-2."""
    from scripts.register import add_visualization_links

    # Add visualization links
    add_visualization_links(
        item=test_pystac_item,
        raster_base="https://titiler.example.com",
        collection_id="sentinel-2-l2a",
    )

    # Verify XYZ tile link added
    xyz_links = [link for link in test_pystac_item.links if link.rel == "xyz"]
    assert len(xyz_links) > 0
    assert "titiler.example.com" in xyz_links[0].href
    href_lower = xyz_links[0].href.lower()
    assert "%2fmeasurements%2freflectance%2fr60m%3ab04" in href_lower
    assert "%2fmeasurements%2freflectance%2fr60m%3ab03" in href_lower
    assert "%2fmeasurements%2freflectance%2fr60m%3ab02" in href_lower

    # Verify viewer link added
    viewer_links = [link for link in test_pystac_item.links if link.rel == "viewer"]
    assert len(viewer_links) > 0

    # Verify EOPF Explorer link added
    explorer_links = [link for link in test_pystac_item.links if "explorer" in link.href.lower()]
    assert len(explorer_links) > 0


@pytest.mark.integration
def test_add_visualization_links_s1():
    """Test add_visualization_links for Sentinel-1."""
    from scripts.register import add_visualization_links

    # Create S1 item
    item = Item(
        id="S1A_IW_GRDH_test",
        geometry=None,
        bbox=None,
        datetime=datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC),
        properties={},
        collection="sentinel-1-grd",
    )
    item.add_asset(
        "vh",
        Asset(
            href="s3://bucket/test.zarr/measurements/VH",
            media_type="application/vnd+zarr",
            roles=["data"],
        ),
    )

    add_visualization_links(
        item=item,
        raster_base="https://titiler.example.com",
        collection_id="sentinel-1-grd",
    )

    # Verify XYZ link contains VH
    xyz_links = [link for link in item.links if link.rel == "xyz"]
    assert len(xyz_links) > 0
    assert "VH" in xyz_links[0].href or "vh" in xyz_links[0].href.lower()


@pytest.mark.integration
def test_s3_to_https_conversion():
    """Test S3 URL to HTTPS conversion."""
    from scripts.register import s3_to_https

    result = s3_to_https("s3://bucket/path/file.zarr", "https://s3.example.com")

    assert result.startswith("https://")
    assert "bucket" in result
    assert "file.zarr" in result


@pytest.mark.integration
def test_rewrite_asset_hrefs(test_pystac_item):
    """Test asset href rewriting from old to new base."""
    from scripts.register import rewrite_asset_hrefs

    rewrite_asset_hrefs(
        item=test_pystac_item,
        old_base="s3://bucket",
        new_base="s3://new-bucket",
        s3_endpoint="https://s3.example.com",
    )

    # Verify assets were rewritten
    for asset in test_pystac_item.assets.values():
        assert "new-bucket" in asset.href
        assert "bucket/" not in asset.href or "new-bucket" in asset.href
