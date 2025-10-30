"""Unit tests for register_stac.py (simplified implementation)."""

import json

import pytest

from scripts.register_stac import main, register_item


@pytest.fixture
def valid_stac_item():
    """Minimal valid STAC item for testing."""
    return {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "test-item-123",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
        },
        "bbox": [0, 0, 1, 1],
        "properties": {"datetime": "2025-01-01T00:00:00Z"},
        "links": [],
        "assets": {
            "data": {
                "href": "s3://bucket/data.zarr",
                "type": "application/vnd+zarr",
            }
        },
    }


def test_register_item_create_new(mocker, valid_stac_item):
    """Test register_item creates new item when it doesn't exist."""
    # Mock STAC client
    mock_client = mocker.Mock()
    mock_collection = mocker.Mock()
    mock_collection.get_item.side_effect = Exception("Not found")
    mock_client.get_collection.return_value = mock_collection

    # Mock StacApiIO session for POST
    mock_response = mocker.Mock()
    mock_response.status_code = 201
    mock_session = mocker.Mock()
    mock_session.post.return_value = mock_response
    mock_client._stac_io.session = mock_session
    mock_client._stac_io.timeout = 30

    # Patch Client class
    mock_client_class = mocker.patch("pystac_client.Client")
    mock_client_class.open.return_value = mock_client

    mock_metrics = mocker.patch("scripts.register_stac.STAC_REGISTRATION_TOTAL")

    register_item(
        stac_url="http://stac.example.com",
        collection_id="test-collection",
        item_dict=valid_stac_item,
        mode="create-or-skip",
    )

    # Verify POST was called
    mock_session.post.assert_called_once()
    mock_metrics.labels.assert_called()


def test_register_item_skip_existing(mocker, valid_stac_item):
    """Test register_item skips existing item in create-or-skip mode."""
    # Mock existing item
    mock_client = mocker.Mock()
    mock_collection = mocker.Mock()
    mock_collection.get_item.return_value = mocker.Mock()  # Item exists
    mock_client.get_collection.return_value = mock_collection
    mock_client.add_item = mocker.Mock()

    # Patch Client class - this is production-grade pytest-mock
    mock_client_class = mocker.patch("pystac_client.Client")
    mock_client_class.open.return_value = mock_client

    mock_metrics = mocker.patch("scripts.register_stac.STAC_REGISTRATION_TOTAL")

    register_item(
        stac_url="http://stac.example.com",
        collection_id="test-collection",
        item_dict=valid_stac_item,
        mode="create-or-skip",
    )

    # Verify item was NOT added
    mock_client.add_item.assert_not_called()
    # Verify skip metric recorded
    mock_metrics.labels.assert_called_with(collection="test-collection", status="success")


def test_register_item_upsert_mode(mocker, valid_stac_item):
    """Test register_item replaces existing item in upsert mode."""
    # Mock existing item
    mock_client = mocker.Mock()
    mock_collection = mocker.Mock()
    mock_collection.get_item.return_value = mocker.Mock()  # Item exists
    mock_client.get_collection.return_value = mock_collection

    # Mock StacApiIO session for DELETE and POST
    mock_delete_response = mocker.Mock()
    mock_delete_response.status_code = 204
    mock_post_response = mocker.Mock()
    mock_post_response.status_code = 201
    mock_session = mocker.Mock()
    mock_session.delete.return_value = mock_delete_response
    mock_session.post.return_value = mock_post_response
    mock_client._stac_io.session = mock_session
    mock_client._stac_io.timeout = 30

    # Patch Client class
    mock_client_class = mocker.patch("pystac_client.Client")
    mock_client_class.open.return_value = mock_client

    mock_metrics = mocker.patch("scripts.register_stac.STAC_REGISTRATION_TOTAL")

    register_item(
        stac_url="http://stac.example.com",
        collection_id="test-collection",
        item_dict=valid_stac_item,
        mode="upsert",
    )

    # Verify item was deleted then created via POST
    mock_session.delete.assert_called_once()
    mock_session.post.assert_called_once()
    # Verify replace metric recorded
    mock_metrics.labels.assert_called()


def test_main_reads_item_from_file(mocker, tmp_path, valid_stac_item):
    """Test main() reads item from JSON file."""
    # Write test item to file
    item_file = tmp_path / "item.json"
    item_file.write_text(json.dumps(valid_stac_item))

    mock_register = mocker.patch("scripts.register_stac.register_item")
    mocker.patch(
        "sys.argv",
        [
            "register_stac.py",
            "--stac-api",
            "http://stac.example.com",
            "--collection",
            "test-collection",
            "--item-json",
            str(item_file),
            "--mode",
            "create-or-skip",
        ],
    )

    main()

    # Verify register_item was called with correct args
    mock_register.assert_called_once()
    call_args = mock_register.call_args
    assert call_args[0][0] == "http://stac.example.com"
    assert call_args[0][1] == "test-collection"
    assert call_args[0][2] == valid_stac_item
    assert call_args[0][3] == "create-or-skip"


def test_register_item_delete_warning(mocker, valid_stac_item):
    """Test register_item logs warning on delete failure."""
    # Mock existing item
    mock_client = mocker.Mock()
    mock_collection = mocker.Mock()
    mock_collection.get_item.return_value = mocker.Mock()
    mock_client.get_collection.return_value = mock_collection

    # Mock DELETE failure
    mock_delete_response = mocker.Mock()
    mock_delete_response.status_code = 404  # Not 200 or 204
    mock_post_response = mocker.Mock()
    mock_post_response.status_code = 201
    mock_session = mocker.Mock()
    mock_session.delete.return_value = mock_delete_response
    mock_session.post.return_value = mock_post_response
    mock_client._stac_io.session = mock_session
    mock_client._stac_io.timeout = 30

    mock_client_class = mocker.patch("pystac_client.Client")
    mock_client_class.open.return_value = mock_client
    mocker.patch("scripts.register_stac.STAC_REGISTRATION_TOTAL")

    # Should log warning but still proceed
    register_item(
        stac_url="http://stac.example.com",
        collection_id="test-col",
        item_dict=valid_stac_item,
        mode="upsert",
    )


def test_register_item_delete_exception(mocker, valid_stac_item):
    """Test register_item handles delete exception gracefully."""
    mock_client = mocker.Mock()
    mock_collection = mocker.Mock()
    mock_collection.get_item.return_value = mocker.Mock()
    mock_client.get_collection.return_value = mock_collection

    # Mock DELETE exception
    mock_post_response = mocker.Mock()
    mock_post_response.status_code = 201
    mock_session = mocker.Mock()
    mock_session.delete.side_effect = Exception("Network error")
    mock_session.post.return_value = mock_post_response
    mock_client._stac_io.session = mock_session
    mock_client._stac_io.timeout = 30

    mock_client_class = mocker.patch("pystac_client.Client")
    mock_client_class.open.return_value = mock_client
    mocker.patch("scripts.register_stac.STAC_REGISTRATION_TOTAL")

    # Should log warning but still proceed
    register_item(
        stac_url="http://stac.example.com",
        collection_id="test-col",
        item_dict=valid_stac_item,
        mode="replace",
    )


def test_register_item_post_failure(mocker, valid_stac_item):
    """Test register_item raises on POST failure."""
    mock_client = mocker.Mock()
    mock_collection = mocker.Mock()
    mock_collection.get_item.side_effect = Exception("Not found")
    mock_client.get_collection.return_value = mock_collection

    # Mock POST failure
    mock_session = mocker.Mock()
    mock_session.post.side_effect = Exception("POST failed")
    mock_client._stac_io.session = mock_session
    mock_client._stac_io.timeout = 30

    mock_client_class = mocker.patch("pystac_client.Client")
    mock_client_class.open.return_value = mock_client
    mock_metrics = mocker.patch("scripts.register_stac.STAC_REGISTRATION_TOTAL")

    with pytest.raises(Exception, match="POST failed"):
        register_item(
            stac_url="http://stac.example.com",
            collection_id="test-col",
            item_dict=valid_stac_item,
            mode="create-or-skip",
        )

    # Verify failure metric recorded
    mock_metrics.labels.assert_called_with(collection="test-col", status="failure")
