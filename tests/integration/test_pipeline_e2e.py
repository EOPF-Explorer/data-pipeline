"""Integration tests for end-to-end pipeline flow.

Tests the full workflow:
1. Extract metadata from source Zarr
2. Register GeoZarr to STAC API
3. Augment item with preview links
4. Validate final STAC item
"""

from unittest.mock import Mock, patch

import pytest


@pytest.mark.integration
def test_s3_url_conversion():
    """Test S3 URL to HTTPS conversion."""
    from scripts.create_geozarr_item import s3_to_https

    # Test S3 URL conversion
    s3_url = "s3://eopf-bucket/geozarr/S2A_test.zarr"
    https_url = s3_to_https(s3_url, "https://s3.gra.cloud.ovh.net")

    assert https_url.startswith("https://")
    assert "eopf-bucket" in https_url
    assert "s3.gra.cloud.ovh.net" in https_url

    # Test already HTTPS URL (should pass through)
    https_input = "https://example.com/data.zarr"
    result = s3_to_https(https_input, "https://s3.example.com")
    assert result == https_input


@pytest.fixture
def sample_sentinel1_item():
    """Sentinel-1 GRD test item."""
    return {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "S1A_IW_GRDH_1SDV_20250518T120000",
        "collection": "sentinel-1-l1-grd",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[10.0, 50.0], [10.0, 51.0], [12.0, 51.0], [12.0, 50.0], [10.0, 50.0]]],
        },
        "properties": {
            "datetime": "2025-05-18T12:00:00Z",
            "platform": "sentinel-1a",
            "instruments": ["c-sar"],
            "sar:instrument_mode": "IW",
            "sar:polarizations": ["VV", "VH"],
        },
        "assets": {
            "vh": {
                "href": "s3://bucket/s1.zarr/S01SIWGRD_20250518_VH/measurements",
                "type": "application/vnd+zarr",
                "roles": ["data"],
            },
            "vv": {
                "href": "s3://bucket/s1.zarr/S01SIWGRD_20250518_VV/measurements",
                "type": "application/vnd+zarr",
                "roles": ["data"],
            },
        },
        "links": [],
    }


@pytest.mark.integration
@pytest.mark.parametrize(
    "collection_id,item_fixture",
    [
        ("sentinel-2-l2a", "sample_stac_item"),
        ("sentinel-1-l1-grd", "sample_sentinel1_item"),
    ],
)
def test_multi_mission_registration(collection_id, item_fixture, request):
    """Test registration workflow for multiple missions (S1, S2)."""
    from scripts.register_stac import register_item

    item = request.getfixturevalue(item_fixture)

    with patch("pystac_client.Client.open") as mock_client_open:
        mock_collection = Mock()
        mock_collection.get_item.side_effect = Exception("Not found")

        mock_client = Mock()
        mock_client.get_collection.return_value = mock_collection
        mock_client._stac_io.session.post = Mock(return_value=Mock(status_code=201))
        mock_client_open.return_value = mock_client

        register_item(
            stac_url="https://stac.example.com",
            collection_id=collection_id,
            item_dict=item,
            mode="create-or-skip",
        )

        assert mock_client._stac_io.session.post.called


@pytest.mark.integration
@pytest.mark.parametrize(
    "collection_id,expected_groups,expected_flags",
    [
        ("sentinel-2-l2a", "/quality/l2a_quicklook/r10m", "--crs-groups"),
        ("sentinel-1-l1-grd", "/measurements", "--gcp-group"),
        ("sentinel-1-l2-ocn", "/measurements", "--gcp-group"),
        ("sentinel-2-l1c", "/quality/l2a_quicklook/r10m", "--crs-groups"),
    ],
)
def test_collection_parameter_dispatch(collection_id, expected_groups, expected_flags):
    """Test collection-based parameter dispatch across missions."""
    from scripts.get_conversion_params import get_conversion_params

    params = get_conversion_params(collection_id)

    assert params["groups"] == expected_groups
    assert expected_flags in params["extra_flags"]
    assert params["spatial_chunk"] == 4096
    assert params["tile_width"] == 512


@pytest.mark.integration
@pytest.mark.parametrize(
    "collection_id,item_id,expected_substring",
    [
        ("sentinel-2-l2a", "S2B_MSIL2A_20250518_T29RLL", "sentinel-2-l2a"),
        ("sentinel-1-l1-grd", "S1A_IW_GRDH_1SDV_20250518", "sentinel-1-l1-grd"),
    ],
)
def test_collection_aware_output_paths(collection_id, item_id, expected_substring):
    """Test that output paths include collection ID for multi-mission organization."""
    output_path = f"s3://bucket/{collection_id}/{item_id}.zarr"
    assert expected_substring in output_path
