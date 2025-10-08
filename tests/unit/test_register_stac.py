"""Unit tests for register_stac.py."""


def test_remove_proj_code_from_properties(stac_item_with_proj_code):
    """Test that proj:code is removed from item properties."""
    from scripts.register_stac import create_geozarr_item

    # Mock minimal inputs
    item = create_geozarr_item(
        source_item=stac_item_with_proj_code,
        geozarr_url="s3://bucket/output.zarr",
        item_id=None,
        collection_id=None,
        s3_endpoint="https://s3.example.com",
    )

    # Verify proj:code removed from properties
    assert "proj:code" not in item["properties"]
    # But proj:epsg should remain
    assert "proj:epsg" in item["properties"]


def test_remove_proj_epsg_from_assets(stac_item_with_proj_code):
    """Test that proj:epsg and proj:code are removed from assets."""
    from scripts.register_stac import create_geozarr_item

    item = create_geozarr_item(
        source_item=stac_item_with_proj_code,
        geozarr_url="s3://bucket/output.zarr",
        item_id=None,
        collection_id=None,
        s3_endpoint="https://s3.example.com",
    )

    # Check all assets have NO proj:epsg or proj:code
    for asset_key, asset_value in item["assets"].items():
        assert "proj:epsg" not in asset_value, f"Asset {asset_key} has proj:epsg"
        assert "proj:code" not in asset_value, f"Asset {asset_key} has proj:code"


def test_remove_storage_options_from_assets(sample_stac_item):
    """Test that storage:options is removed from assets."""
    from scripts.register_stac import create_geozarr_item

    # Add storage:options to source item
    source = sample_stac_item.copy()
    source["assets"]["B01"]["storage:options"] = {"anon": True}

    item = create_geozarr_item(
        source_item=source,
        geozarr_url="s3://bucket/output.zarr",
        item_id=None,
        collection_id=None,
        s3_endpoint="https://s3.example.com",
    )

    # Verify storage:options removed
    for asset_value in item["assets"].values():
        assert "storage:options" not in asset_value


def test_s3_to_https_conversion():
    """Test S3 URL to HTTPS conversion."""
    from scripts.register_stac import s3_to_https

    result = s3_to_https("s3://mybucket/path/to/file.zarr", "https://s3.example.com")
    assert result == "https://mybucket.s3.example.com/path/to/file.zarr"


def test_derived_from_link_added(sample_stac_item):
    """Test that derived_from link is added."""
    from scripts.register_stac import create_geozarr_item

    # Add self link to source
    source = sample_stac_item.copy()
    source["links"] = [
        {
            "rel": "self",
            "href": "https://api.example.com/items/test-item",
            "type": "application/json",
        }
    ]

    item = create_geozarr_item(
        source_item=source,
        geozarr_url="s3://bucket/output.zarr",
        item_id=None,
        collection_id=None,
        s3_endpoint="https://s3.example.com",
    )

    # Check derived_from link exists
    derived_links = [link for link in item["links"] if link["rel"] == "derived_from"]
    assert len(derived_links) == 1
    assert derived_links[0]["href"] == "https://api.example.com/items/test-item"


def test_r60m_overview_path_rewrite():
    """Test that r60m band assets get /0 inserted for overview level."""
    from scripts.register_stac import create_geozarr_item

    source = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "test",
        "properties": {"datetime": "2025-01-01T00:00:00Z", "proj:epsg": 32636},
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "links": [],
        "assets": {
            "B01_60m": {
                "href": "s3://bucket/source.zarr/r60m/b01",
                "type": "image/tiff",
                "roles": ["data"],
            }
        },
        "collection": "test",
    }

    item = create_geozarr_item(
        source_item=source,
        geozarr_url="s3://bucket/output.zarr",
        item_id=None,
        collection_id=None,
        s3_endpoint="https://s3.example.com",
    )

    # Verify /0 was inserted for r60m
    assert "/r60m/0/b01" in item["assets"]["B01_60m"]["href"]


def test_r10m_no_overview_path():
    """Test that r10m/r20m bands do NOT get /0 inserted."""
    from scripts.register_stac import create_geozarr_item

    source = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "test",
        "properties": {"datetime": "2025-01-01T00:00:00Z", "proj:epsg": 32636},
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "links": [],
        "assets": {
            "B02_10m": {
                "href": "s3://bucket/source.zarr/r10m/b02",
                "type": "image/tiff",
                "roles": ["data"],
            }
        },
        "collection": "test",
    }

    item = create_geozarr_item(
        source_item=source,
        geozarr_url="s3://bucket/output.zarr",
        item_id=None,
        collection_id=None,
        s3_endpoint="https://s3.example.com",
    )

    # Verify NO /0 for r10m
    assert "/r10m/b02" in item["assets"]["B02_10m"]["href"]
    assert "/0/" not in item["assets"]["B02_10m"]["href"]


def test_keep_proj_spatial_fields_on_assets(sample_stac_item):
    """Test that proj:bbox, proj:shape, proj:transform are kept on assets."""
    from scripts.register_stac import create_geozarr_item

    # Add spatial fields to source asset
    source = sample_stac_item.copy()
    source["assets"]["B01"]["proj:bbox"] = [600000, 6290220, 709800, 6400020]
    source["assets"]["B01"]["proj:shape"] = [10980, 10980]
    source["assets"]["B01"]["proj:transform"] = [10, 0, 600000, 0, -10, 6400020]

    item = create_geozarr_item(
        source_item=source,
        geozarr_url="s3://bucket/output.zarr",
        item_id=None,
        collection_id=None,
        s3_endpoint="https://s3.example.com",
    )

    # These should be preserved
    asset = item["assets"]["B01"]
    assert "proj:bbox" in asset
    assert "proj:shape" in asset
    assert "proj:transform" in asset


def test_normalize_asset_href_basic():
    """Test normalize_asset_href for simple r60m paths."""
    from scripts.register_stac import normalize_asset_href

    # Should insert /0 for r60m bands
    result = normalize_asset_href("s3://bucket/data.zarr/r60m/b01")
    assert result == "s3://bucket/data.zarr/r60m/0/b01"

    result = normalize_asset_href("s3://bucket/data.zarr/r60m/b09")
    assert result == "s3://bucket/data.zarr/r60m/0/b09"


def test_normalize_asset_href_complex_paths():
    """Test normalize_asset_href with complex base paths."""
    from scripts.register_stac import normalize_asset_href

    # Complex S3 path
    result = normalize_asset_href(
        "s3://eodc-sentinel-2/products/2025/S2A_MSIL2A.zarr/measurements/reflectance/r60m/b01"
    )
    expected = (
        "s3://eodc-sentinel-2/products/2025/S2A_MSIL2A.zarr/measurements/reflectance/r60m/0/b01"
    )
    assert result == expected

    # HTTPS path
    result = normalize_asset_href("https://example.com/data.zarr/quality/r60m/scene_classification")
    expected = "https://example.com/data.zarr/quality/r60m/0/scene_classification"
    assert result == expected


def test_clean_stac_item_metadata():
    """Test cleaning invalid projection metadata from STAC item."""
    from scripts.register_stac import clean_stac_item_metadata

    item = {
        "id": "test-item",
        "properties": {
            "datetime": "2025-01-01T00:00:00Z",
            "proj:bbox": [0, 0, 100, 100],
            "proj:epsg": 32632,
            "proj:shape": [1024, 1024],
            "proj:transform": [10, 0, 0, 0, -10, 0],
            "proj:code": "EPSG:32632",
        },
        "assets": {
            "band1": {
                "href": "s3://bucket/data.zarr/b01",
                "proj:epsg": 32632,
                "proj:code": "EPSG:32632",
                "storage:options": {"anon": True},
            },
            "band2": {
                "href": "s3://bucket/data.zarr/b02",
                "proj:epsg": 32632,
            },
        },
    }

    clean_stac_item_metadata(item)

    # Check properties cleaned
    assert "proj:shape" not in item["properties"]
    assert "proj:transform" not in item["properties"]
    assert "proj:code" not in item["properties"]
    assert "proj:bbox" in item["properties"]  # Should be kept
    assert "proj:epsg" in item["properties"]  # Should be kept

    # Check assets cleaned
    assert "proj:epsg" not in item["assets"]["band1"]
    assert "proj:code" not in item["assets"]["band1"]
    assert "storage:options" not in item["assets"]["band1"]
    assert "href" in item["assets"]["band1"]  # Should be kept

    assert "proj:epsg" not in item["assets"]["band2"]
    assert "href" in item["assets"]["band2"]


def test_find_source_zarr_base():
    """Test extracting base Zarr URL from source item assets."""
    from scripts.register_stac import find_source_zarr_base

    # Test with .zarr/ in path
    source_item = {
        "assets": {
            "product": {"href": "s3://bucket/data.zarr/measurements/b01"},
            "metadata": {"href": "https://example.com/metadata.json"},
        }
    }
    result = find_source_zarr_base(source_item)
    assert result == "s3://bucket/data.zarr/"

    # Test with .zarr at end
    source_item = {"assets": {"product": {"href": "s3://bucket/data.zarr"}}}
    result = find_source_zarr_base(source_item)
    assert result == "s3://bucket/data.zarr/"

    # Test with no zarr assets
    source_item = {"assets": {"metadata": {"href": "https://example.com/metadata.json"}}}
    result = find_source_zarr_base(source_item)
    assert result is None

    # Test with no assets
    source_item = {}
    result = find_source_zarr_base(source_item)
    assert result is None
