"""Unit tests for augment_stac_item.py."""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from pystac import Asset, Item

from scripts.augment_stac_item import add_projection, add_visualization, augment, main


@pytest.fixture
def item():
    """Create test STAC item."""
    return Item("test", geometry=None, bbox=None, datetime=datetime.now(UTC), properties={})


@pytest.fixture
def mock_httpx_success():
    """Mock successful httpx requests."""
    with patch("scripts.augment_stac_item.httpx.Client") as mock_client:
        mock_ctx = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_ctx.get.return_value = mock_response
        mock_ctx.put.return_value = mock_response
        mock_client.return_value.__enter__.return_value = mock_ctx
        mock_client.return_value.__exit__.return_value = None
        yield mock_ctx


def test_add_projection_extracts_epsg(item):
    """Test projection extraction from zarr."""
    item.add_asset("product", Asset(href="s3://test.zarr", media_type="application/vnd+zarr"))

    mock_store = MagicMock()
    # The actual code reads spatial_ref dict which contains "spatial_ref" key with EPSG value
    mock_store.attrs.get.return_value = {"spatial_ref": "32632", "crs_wkt": "PROJCS[...]"}

    with patch("scripts.augment_stac_item.zarr.open", return_value=mock_store):
        add_projection(item)

    # Projection extension sets proj:code based on EPSG
    assert (
        item.properties.get("proj:code") == "EPSG:32632"
        or item.properties.get("proj:epsg") == 32632
    )
    assert "proj:wkt2" in item.properties


def test_add_projection_handles_errors(item):
    """Test add_projection error handling."""
    item.add_asset("product", Asset(href="s3://test.zarr", media_type="application/vnd+zarr"))
    with patch("scripts.augment_stac_item.zarr.open", side_effect=Exception):
        add_projection(item)  # Should not raise
        assert "proj:epsg" not in item.properties


def test_add_projection_no_zarr_assets(item):
    """Test add_projection with no zarr assets."""
    add_projection(item)
    assert "proj:epsg" not in item.properties


@pytest.mark.parametrize(
    "collection,expected_asset",
    [
        ("sentinel-2-l2a", "TCI_10m"),
    ],
)
def test_add_visualization(item, collection, expected_asset):
    """Test visualization links for S1/S2."""
    add_visualization(item, "https://raster.api", collection)

    links = {link.rel: link for link in item.links}
    assert all(rel in links for rel in ["viewer", "xyz", "tilejson", "via"])

    # Verify asset in xyz URL
    assert expected_asset in links["xyz"].href

    # Verify proper URL encoding (/ should be %2F, : should be %3A)
    assert "%2F" in links["xyz"].href  # Forward slashes are encoded
    assert "%3A" in links["xyz"].href  # Colons are encoded

    # Verify titles are present
    assert links["xyz"].title is not None
    assert links["tilejson"].title is not None
    assert links["viewer"].title is not None


def test_augment_verbose(item):
    """Test augment with verbose output."""
    with (
        patch("scripts.augment_stac_item.add_projection"),
        patch("scripts.augment_stac_item.add_visualization"),
        patch("builtins.print") as mock_print,
    ):
        augment(item, raster_base="https://api", collection_id="col", verbose=True)
        mock_print.assert_called_once()


def test_main_success(mock_httpx_success):
    """Test main() success flow."""
    item_dict = Item(
        "test", geometry=None, bbox=None, datetime=datetime.now(UTC), properties={}
    ).to_dict()
    item_dict["collection"] = "test-col"
    mock_httpx_success.get.return_value.json.return_value = item_dict

    with patch("scripts.augment_stac_item.augment") as mock_aug:
        mock_aug.return_value = Item.from_dict(item_dict)
        exit_code = main(
            ["--stac", "https://stac", "--collection", "test-col", "--item-id", "test"]
        )

    assert exit_code == 0


def test_main_get_failure():
    """Test main() GET failure."""
    with patch("scripts.augment_stac_item.httpx.Client") as mock:
        mock.return_value.__enter__.return_value.get.side_effect = Exception("Failed")
        exit_code = main(["--stac", "https://stac", "--collection", "col", "--item-id", "test"])

    assert exit_code == 1


def test_main_put_failure(mock_httpx_success):
    """Test main() PUT failure."""
    item_dict = Item(
        "test", geometry=None, bbox=None, datetime=datetime.now(UTC), properties={}
    ).to_dict()
    mock_httpx_success.get.return_value.json.return_value = item_dict
    mock_httpx_success.put.side_effect = Exception("Failed")

    with patch("scripts.augment_stac_item.augment", return_value=Item.from_dict(item_dict)):
        exit_code = main(["--stac", "https://stac", "--collection", "col", "--item-id", "test"])

    assert exit_code == 1


def test_main_with_bearer_token(mock_httpx_success):
    """Test main() with bearer token."""
    item_dict = Item(
        "test", geometry=None, bbox=None, datetime=datetime.now(UTC), properties={}
    ).to_dict()
    item_dict["collection"] = "col"
    mock_httpx_success.get.return_value.json.return_value = item_dict

    with patch("scripts.augment_stac_item.augment", return_value=Item.from_dict(item_dict)):
        main(
            [
                "--stac",
                "https://stac",
                "--collection",
                "col",
                "--item-id",
                "test",
                "--bearer",
                "token",
            ]
        )

    call = mock_httpx_success.get.call_args
    assert call.kwargs["headers"]["Authorization"] == "Bearer token"
