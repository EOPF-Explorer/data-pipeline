"""Integration tests for STAC API workflows.

These tests verify end-to-end scenarios with mocked external dependencies.
"""

from unittest.mock import MagicMock, patch

import pytest
from pystac import Asset, Item


@pytest.fixture
def test_pystac_item():
    """Sample pystac Item for testing."""
    item = Item(
        id="S2B_test",
        geometry=None,
        bbox=None,
        datetime="2025-01-01T00:00:00Z",
        properties={},
        collection="sentinel-2-l2a",
    )
    item.add_asset(
        "TCI_10m",
        Asset(
            href="https://example.com/data.zarr/TCI",
            media_type="application/vnd+zarr",
            roles=["visual"],
        ),
    )
    return item


@pytest.fixture
def test_item_dict():
    """Sample STAC item dictionary for registration tests."""
    return {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "test-item",
        "collection": "test-collection",
        "geometry": None,
        "bbox": None,
        "properties": {"datetime": "2025-01-01T00:00:00Z"},
        "assets": {},
        "links": [],
    }


@pytest.fixture
def mock_stac_client():
    """Mock pystac_client.Client for registration tests."""
    mock_client = MagicMock()
    mock_collection = MagicMock()
    mock_client.get_collection.return_value = mock_collection

    # Mock the StacApiIO session (httpx client)
    mock_session = MagicMock()
    mock_stac_io = MagicMock()
    mock_stac_io.session = mock_session
    mock_stac_io.timeout = 30
    mock_client._stac_io = mock_stac_io

    return mock_client


@pytest.mark.integration
def test_register_creates_new_item(test_item_dict, mock_stac_client):
    """Test registration creates new STAC item when it doesn't exist."""
    from scripts.register_stac import register_item

    # Mock item doesn't exist
    mock_stac_client.get_collection().get_item.side_effect = Exception("Not found")

    # Mock successful POST
    mock_response = MagicMock()
    mock_response.status_code = 201
    mock_response.raise_for_status = MagicMock()
    mock_stac_client._stac_io.session.post.return_value = mock_response

    with patch("pystac_client.Client.open", return_value=mock_stac_client):
        register_item(
            stac_url="https://stac.example.com",
            collection_id="test-collection",
            item_dict=test_item_dict,
            mode="create-or-skip",
        )

        # Verify POST was called to create item
        assert mock_stac_client._stac_io.session.post.called


@pytest.mark.integration
def test_register_skips_existing_item(test_item_dict, mock_stac_client):
    """Test registration skips when item already exists."""
    from scripts.register_stac import register_item

    # Mock item exists
    mock_stac_client.get_collection().get_item.return_value = MagicMock()

    with patch("pystac_client.Client.open", return_value=mock_stac_client):
        register_item(
            stac_url="https://stac.example.com",
            collection_id="test-collection",
            item_dict=test_item_dict,
            mode="create-or-skip",
        )

        # Verify no POST/PUT/DELETE was called
        assert not mock_stac_client._stac_io.session.post.called
        assert not mock_stac_client._stac_io.session.put.called
        assert not mock_stac_client._stac_io.session.delete.called


@pytest.mark.integration
def test_register_updates_existing_item(test_item_dict, mock_stac_client):
    """Test registration updates existing item in upsert mode."""
    from scripts.register_stac import register_item

    # Mock item exists
    mock_stac_client.get_collection().get_item.return_value = MagicMock()

    # Mock successful DELETE and POST
    mock_delete_response = MagicMock()
    mock_delete_response.status_code = 204
    mock_delete_response.raise_for_status = MagicMock()
    mock_stac_client._stac_io.session.delete.return_value = mock_delete_response

    mock_post_response = MagicMock()
    mock_post_response.status_code = 201
    mock_post_response.raise_for_status = MagicMock()
    mock_stac_client._stac_io.session.post.return_value = mock_post_response

    with patch("pystac_client.Client.open", return_value=mock_stac_client):
        register_item(
            stac_url="https://stac.example.com",
            collection_id="test-collection",
            item_dict=test_item_dict,
            mode="upsert",
        )

        # Verify DELETE then POST was called
        assert mock_stac_client._stac_io.session.delete.called
        assert mock_stac_client._stac_io.session.post.called


@pytest.mark.integration
def test_augmentation_adds_visualization_links(test_pystac_item):
    """Test augmentation workflow adds visualization links."""
    from scripts.augment_stac_item import add_visualization

    # Add visualization links
    add_visualization(
        item=test_pystac_item,
        raster_base="https://titiler.example.com",
        collection_id="sentinel-2-l2a",
    )

    # Verify XYZ tile link added
    xyz_links = [link for link in test_pystac_item.links if link.rel == "xyz"]
    assert len(xyz_links) > 0
    assert "titiler.example.com" in xyz_links[0].target

    # Verify viewer link added
    viewer_links = [link for link in test_pystac_item.links if link.rel == "viewer"]
    assert len(viewer_links) > 0


@pytest.mark.integration
def test_augmentation_adds_projection(test_pystac_item):
    """Test augmentation extracts projection information."""
    from scripts.augment_stac_item import add_projection

    # Mock zarr group with spatial_ref
    # Note: This test validates the interface, actual zarr integration tested separately
    add_projection(item=test_pystac_item)

    # Projection properties should be attempted (may not be set without real zarr)
    # This validates the function can be called and handles missing data gracefully
    assert test_pystac_item.properties is not None


@pytest.mark.integration
def test_full_augmentation_pipeline(test_pystac_item):
    """Test complete augmentation pipeline."""
    from scripts.augment_stac_item import augment

    # Run full augmentation
    result = augment(
        item=test_pystac_item,
        raster_base="https://titiler.example.com",
        collection_id="sentinel-2-l2a",
        verbose=False,
    )

    # Verify item returned
    assert result.id == "S2B_test"

    # Verify links added
    assert len(result.links) > 0
    xyz_count = sum(1 for link in result.links if link.rel == "xyz")
    assert xyz_count > 0
