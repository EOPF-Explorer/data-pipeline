"""Unit tests for augment_stac_item.py."""


def test_encode_true_color_query():
    """Test true color query string encoding."""
    from scripts.augment_stac_item import _encode_true_color_query

    result = _encode_true_color_query("0,0.1")

    # Should include all 3 bands (URL encoded)
    assert "variables=%2Fmeasurements%2Freflectance%2Fr10m%2F0%3Ab04" in result
    assert "variables=%2Fmeasurements%2Freflectance%2Fr10m%2F0%3Ab03" in result
    assert "variables=%2Fmeasurements%2Freflectance%2Fr10m%2F0%3Ab02" in result
    assert "rescale=0%2C0.1" in result
    assert "color_formula=Gamma+RGB+1.4" in result


def test_encode_quicklook_query():
    """Test quicklook query string encoding."""
    from scripts.augment_stac_item import _encode_quicklook_query

    result = _encode_quicklook_query()

    # Should reference TCI (URL encoded)
    assert "variables=%2Fquality%2Fl2a_quicklook%2Fr10m%3Atci" in result
    assert "bidx=1" in result
    assert "bidx=2" in result
    assert "bidx=3" in result


def test_coerce_epsg_from_string():
    """Test EPSG code coercion from string."""
    from scripts.augment_stac_item import _coerce_epsg

    assert _coerce_epsg("32636") == 32636
    assert _coerce_epsg("EPSG:32636") == 32636
    assert _coerce_epsg("epsg:32636") == 32636


def test_coerce_epsg_invalid():
    """Test EPSG code coercion returns None for invalid input."""
    from scripts.augment_stac_item import _coerce_epsg

    assert _coerce_epsg(None) is None
    assert _coerce_epsg("") is None
    assert _coerce_epsg("invalid") is None
    assert _coerce_epsg(True) is None


def test_resolve_preview_query_default():
    """Test preview query resolution uses default when env is None."""
    from scripts.augment_stac_item import _resolve_preview_query

    result = _resolve_preview_query(None, default_query="default")
    assert result == "default"


def test_resolve_preview_query_custom():
    """Test preview query resolution uses env value when provided."""
    from scripts.augment_stac_item import _resolve_preview_query

    result = _resolve_preview_query("custom=value", default_query="default")
    assert result == "custom=value"


def test_resolve_preview_query_strips_whitespace():
    """Test preview query resolution strips whitespace."""
    from scripts.augment_stac_item import _resolve_preview_query

    result = _resolve_preview_query("  custom=value  ", default_query="default")
    assert result == "custom=value"


def test_normalize_collection_slug():
    """Test collection ID normalization."""
    from scripts.augment_stac_item import _normalize_collection_slug

    assert _normalize_collection_slug("sentinel-2-l2a") == "sentinel-2-l2a"
    assert _normalize_collection_slug("Sentinel 2 L2A") == "sentinel 2 l2a"
    assert _normalize_collection_slug("SENTINEL_2_L2A") == "sentinel_2_l2a"


def test_normalize_href_scheme_s3_passthrough():
    """Test that s3:// URLs pass through unchanged."""
    from scripts.augment_stac_item import normalize_href_scheme

    assert normalize_href_scheme("s3://mybucket/data.zarr") == "s3://mybucket/data.zarr"


def test_normalize_href_scheme_ovh_s3_subdomain():
    """Test OVH S3 virtual-hosted style URL normalization."""
    from scripts.augment_stac_item import normalize_href_scheme

    result = normalize_href_scheme("https://mybucket.s3.gra.cloud.ovh.net/path/to/data.zarr")
    assert result == "s3://mybucket/path/to/data.zarr"


def test_normalize_href_scheme_ovh_s3_path_style():
    """Test OVH S3 path-style URL normalization."""
    from scripts.augment_stac_item import normalize_href_scheme

    result = normalize_href_scheme("https://s3.gra.cloud.ovh.net/mybucket/path/to/data.zarr")
    assert result == "s3://mybucket/path/to/data.zarr"


def test_normalize_href_scheme_ovh_io_subdomain():
    """Test OVH IO Cloud virtual-hosted style URL normalization."""
    from scripts.augment_stac_item import normalize_href_scheme

    result = normalize_href_scheme("https://mybucket.s3.io.cloud.ovh.net/data.zarr")
    assert result == "s3://mybucket/data.zarr"


def test_normalize_href_scheme_non_ovh_unchanged():
    """Test non-OVH URLs remain unchanged."""
    from scripts.augment_stac_item import normalize_href_scheme

    url = "https://example.com/data.zarr"
    assert normalize_href_scheme(url) == url


def test_normalize_href_scheme_invalid_scheme():
    """Test non-http(s) schemes remain unchanged."""
    from scripts.augment_stac_item import normalize_href_scheme

    ftp_url = "ftp://example.com/data.zarr"
    assert normalize_href_scheme(ftp_url) == ftp_url


def test_resolve_preview_asset_href_converts_preview():
    """Test preview path resolution to full-resolution dataset."""
    from scripts.augment_stac_item import resolve_preview_asset_href

    preview = "s3://bucket/previews/S2B_MSIL2A_20250518_preview.zarr/measurements/b04"
    result = resolve_preview_asset_href(preview)
    assert result == "s3://bucket/sentinel-2-l2a/S2B_MSIL2A_20250518.zarr/measurements/b04"


def test_resolve_preview_asset_href_passthrough_full_res():
    """Test full-resolution paths remain unchanged."""
    from scripts.augment_stac_item import resolve_preview_asset_href

    full = "s3://bucket/sentinel-2-l2a/S2B_MSIL2A_20250518.zarr/measurements/b04"
    assert resolve_preview_asset_href(full) == full


def test_resolve_preview_asset_href_passthrough_no_preview_suffix():
    """Test paths in previews directory without _preview.zarr suffix remain unchanged."""
    from scripts.augment_stac_item import resolve_preview_asset_href

    no_suffix = "s3://bucket/previews/S2B_MSIL2A_20250518.zarr/data"
    assert resolve_preview_asset_href(no_suffix) == no_suffix


def test_resolve_preview_asset_href_passthrough_non_s3():
    """Test non-S3 URLs remain unchanged."""
    from scripts.augment_stac_item import resolve_preview_asset_href

    https_url = "https://example.com/previews/data_preview.zarr/b04"
    assert resolve_preview_asset_href(https_url) == https_url


def test_resolve_preview_asset_href_malformed_path():
    """Test malformed preview paths return original href."""
    from scripts.augment_stac_item import resolve_preview_asset_href

    # Missing store name after previews/
    malformed = "s3://bucket/previews/"
    assert resolve_preview_asset_href(malformed) == malformed


def test_normalize_asset_alternate_schemes_normalizes_s3():
    """Test alternate hrefs are normalized to s3:// scheme."""
    from pystac import Asset

    from scripts.augment_stac_item import normalize_asset_alternate_schemes

    asset = Asset(
        href="s3://bucket/data.zarr",
        extra_fields={
            "alternate": {
                "s3": {"href": "https://bucket.s3.gra.io.cloud.ovh.net/data.zarr"},
                "https": {"href": "https://example.com/data.zarr"},
            }
        },
    )

    normalize_asset_alternate_schemes(asset)

    alternates = asset.extra_fields.get("alternate", {})
    assert alternates["s3"]["href"] == "s3://bucket/data.zarr"
    assert alternates["https"]["href"] == "https://example.com/data.zarr"


def test_normalize_asset_alternate_schemes_resolves_previews():
    """Test alternate preview paths are resolved to full datasets."""
    from pystac import Asset

    from scripts.augment_stac_item import normalize_asset_alternate_schemes

    asset = Asset(
        href="s3://bucket/sentinel-2-l2a/data.zarr",
        extra_fields={
            "alternate": {
                "s3": {"href": "s3://bucket/previews/data_preview.zarr"},
            }
        },
    )

    normalize_asset_alternate_schemes(asset)

    alternates = asset.extra_fields.get("alternate", {})
    assert alternates["s3"]["href"] == "s3://bucket/sentinel-2-l2a/data.zarr"


def test_normalize_asset_alternate_schemes_removes_empty():
    """Test empty alternates are removed after normalization."""
    from pystac import Asset

    from scripts.augment_stac_item import normalize_asset_alternate_schemes

    # Start with empty dict
    asset = Asset(href="s3://bucket/data.zarr", extra_fields={"alternate": {}})

    normalize_asset_alternate_schemes(asset)

    assert "alternate" not in asset.extra_fields


def test_normalize_asset_alternate_schemes_no_extra_fields():
    """Test assets without extra_fields are handled safely."""
    from pystac import Asset

    from scripts.augment_stac_item import normalize_asset_alternate_schemes

    asset = Asset(href="s3://bucket/data.zarr")

    # Should not raise
    normalize_asset_alternate_schemes(asset)

    assert asset.extra_fields == {}


def test_normalize_asset_alternate_schemes_invalid_alternate_type():
    """Test non-dict alternate values are skipped."""
    from pystac import Asset

    from scripts.augment_stac_item import normalize_asset_alternate_schemes

    asset = Asset(href="s3://bucket/data.zarr", extra_fields={"alternate": "invalid"})

    normalize_asset_alternate_schemes(asset)

    # Invalid type is left unchanged
    assert asset.extra_fields.get("alternate") == "invalid"


def test_normalize_asset_alternate_schemes_missing_href():
    """Test alternate entries without href are skipped."""
    from pystac import Asset

    from scripts.augment_stac_item import normalize_asset_alternate_schemes

    asset = Asset(
        href="s3://bucket/data.zarr",
        extra_fields={
            "alternate": {
                "s3": {"title": "S3 access"},  # no href
                "https": {"href": "https://example.com/data.zarr"},
            }
        },
    )

    normalize_asset_alternate_schemes(asset)

    alternates = asset.extra_fields.get("alternate", {})
    # Entry without href is unchanged
    assert alternates["s3"] == {"title": "S3 access"}
    # Entry with href is normalized (unchanged in this case)
    assert alternates["https"]["href"] == "https://example.com/data.zarr"


def test_normalize_asset_alternate_schemes_combined_transformations():
    """Test both normalization and preview resolution work together."""
    from pystac import Asset

    from scripts.augment_stac_item import normalize_asset_alternate_schemes

    asset = Asset(
        href="s3://bucket/sentinel-2-l2a/data.zarr",
        extra_fields={
            "alternate": {
                "s3": {"href": "https://bucket.s3.gra.io.cloud.ovh.net/previews/data_preview.zarr"},
            }
        },
    )

    normalize_asset_alternate_schemes(asset)

    alternates = asset.extra_fields.get("alternate", {})
    # Should be normalized from HTTPS AND resolved from preview
    assert alternates["s3"]["href"] == "s3://bucket/sentinel-2-l2a/data.zarr"
