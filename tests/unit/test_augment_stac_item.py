"""Unit tests for augment_stac_item.py (refactored using STAC extensions)."""

from pystac import Asset, Item

from scripts.augment_stac_item import _build_tilejson_query, _get_s1_preview_query, add_projection


def test_build_tilejson_query_basic():
    """Test TiTiler query string generation."""
    variables = ["/measurements/reflectance/r10m/0:b04"]
    query = _build_tilejson_query(variables, rescale="0,0.1")

    assert "variables=" in query
    assert "rescale=0%2C0.1" in query


def test_build_tilejson_query_multiple_variables():
    """Test TiTiler query with multiple variables (S2 true color)."""
    variables = [
        "/measurements/reflectance/r10m/0:b04",
        "/measurements/reflectance/r10m/0:b03",
        "/measurements/reflectance/r10m/0:b02",
    ]
    query = _build_tilejson_query(variables)

    assert query.count("variables=") == 3
    assert "b04" in query
    assert "b03" in query
    assert "b02" in query


def test_get_s1_preview_query():
    """Test S1 GRD preview query generation."""
    item = Item(
        id="test",
        geometry={"type": "Point", "coordinates": [0, 0]},
        bbox=[0, 0, 1, 1],
        datetime=None,
        properties={},
    )
    item.add_asset(
        "vh",
        Asset(
            href="s3://eopf-devseed/sentinel-1-grd/S1A_IW_GRDH_1SDV_20250101T000000_20250101T000000_012345_012345_1234.zarr/measurements/grd/iw_grdh_1sdv",
            media_type="application/vnd+zarr",
        ),
    )

    query = _get_s1_preview_query(item)

    # Should extract vh and use default rescale
    assert "variables=" in query
    assert "vh" in query.lower()
    assert "rescale=" in query


def test_add_projection_requires_zarr_asset():
    """Test add_projection returns early if no zarr assets."""
    item = Item(
        id="test",
        geometry={"type": "Point", "coordinates": [0, 0]},
        bbox=[0, 0, 1, 1],
        datetime=None,
        properties={},
    )
    item.add_asset("not_zarr", Asset(href="http://example.com", media_type="image/tiff"))

    # Should not raise, just return early
    add_projection(item)

    # No projection extension should be added
    assert "proj:epsg" not in item.properties
