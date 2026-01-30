"""Unit tests for S3 gateway format and alternate asset extension."""

from pystac import Asset, Item

from scripts.register_v1 import (
    add_alternate_s3_assets,
    https_to_s3,
    rewrite_asset_hrefs,
    s3_to_https,
)


class TestS3ToHttps:
    """Test s3_to_https conversion function."""

    def test_simple_path(self):
        """Test conversion of simple S3 path."""
        s3_url = "s3://my-bucket/path/to/file.zarr"
        expected = "https://s3.explorer.eopf.copernicus.eu/my-bucket/path/to/file.zarr"
        assert s3_to_https(s3_url) == expected

    def test_deep_nested_path(self):
        """Test conversion of deeply nested S3 path."""
        s3_url = "s3://esa-zarr-sentinel-explorer-fra/tests-output/sentinel-2-l2a-staging/S2A_MSIL2A_20250831T103701_N0511_R008_T31TFL_20250831T145420.zarr/quality/atmosphere/r10m/aot"
        expected = "https://s3.explorer.eopf.copernicus.eu/esa-zarr-sentinel-explorer-fra/tests-output/sentinel-2-l2a-staging/S2A_MSIL2A_20250831T103701_N0511_R008_T31TFL_20250831T145420.zarr/quality/atmosphere/r10m/aot"
        assert s3_to_https(s3_url) == expected

    def test_bucket_only(self):
        """Test conversion of S3 URL with bucket only (no path)."""
        s3_url = "s3://my-bucket"
        expected = "https://s3.explorer.eopf.copernicus.eu/my-bucket"
        assert s3_to_https(s3_url) == expected

    def test_bucket_with_trailing_slash(self):
        """Test conversion of S3 URL with trailing slash."""
        s3_url = "s3://my-bucket/"
        expected = "https://s3.explorer.eopf.copernicus.eu/my-bucket/"
        assert s3_to_https(s3_url) == expected

    def test_https_url_passthrough(self):
        """Test that HTTPS URLs are passed through unchanged."""
        https_url = "https://example.com/path/to/file"
        assert s3_to_https(https_url) == https_url

    def test_custom_gateway_url(self):
        """Test conversion with custom gateway URL."""
        s3_url = "s3://my-bucket/path/file.zarr"
        custom_gateway = "https://custom.gateway.com"
        expected = "https://custom.gateway.com/my-bucket/path/file.zarr"
        assert s3_to_https(s3_url, custom_gateway) == expected

    def test_gateway_url_with_trailing_slash(self):
        """Test that trailing slash in gateway URL is handled correctly."""
        s3_url = "s3://my-bucket/path/file.zarr"
        gateway_with_slash = "https://s3.explorer.eopf.copernicus.eu/"
        expected = "https://s3.explorer.eopf.copernicus.eu/my-bucket/path/file.zarr"
        assert s3_to_https(s3_url, gateway_with_slash) == expected

    def test_special_characters_in_path(self):
        """Test conversion with special characters in path."""
        s3_url = "s3://my-bucket/path/with spaces/and_underscores/file.zarr"
        expected = "https://s3.explorer.eopf.copernicus.eu/my-bucket/path/with spaces/and_underscores/file.zarr"
        assert s3_to_https(s3_url) == expected


class TestHttpsToS3:
    """Test https_to_s3 conversion function."""

    def test_new_gateway_format_simple(self):
        """Test conversion from new gateway format - simple path."""
        https_url = "https://s3.explorer.eopf.copernicus.eu/my-bucket/path/to/file.zarr"
        expected = "s3://my-bucket/path/to/file.zarr"
        assert https_to_s3(https_url) == expected

    def test_new_gateway_format_deep_path(self):
        """Test conversion from new gateway format - deep nested path."""
        https_url = "https://s3.explorer.eopf.copernicus.eu/esa-zarr-sentinel-explorer-fra/tests-output/sentinel-2-l2a-staging/file.zarr"
        expected = (
            "s3://esa-zarr-sentinel-explorer-fra/tests-output/sentinel-2-l2a-staging/file.zarr"
        )
        assert https_to_s3(https_url) == expected

    def test_new_gateway_format_bucket_only(self):
        """Test conversion from new gateway format - bucket only."""
        https_url = "https://s3.explorer.eopf.copernicus.eu/my-bucket"
        expected = "s3://my-bucket"
        assert https_to_s3(https_url) == expected

    def test_new_gateway_format_bucket_with_slash(self):
        """Test conversion from new gateway format - bucket with trailing slash."""
        https_url = "https://s3.explorer.eopf.copernicus.eu/my-bucket/"
        expected = "s3://my-bucket/"
        assert https_to_s3(https_url) == expected

    def test_old_s3_format_de_region(self):
        """Test backwards compatibility with old S3 format (de region)."""
        https_url = (
            "https://esa-zarr-sentinel-explorer-fra.s3.de.io.cloud.ovh.net/tests-output/file.zarr"
        )
        expected = "s3://esa-zarr-sentinel-explorer-fra/tests-output/file.zarr"
        assert https_to_s3(https_url) == expected

    def test_old_s3_format_gra_region(self):
        """Test backwards compatibility with old S3 format (gra region)."""
        https_url = "https://my-bucket.s3.gra.io.cloud.ovh.net/path/to/file.zarr"
        expected = "s3://my-bucket/path/to/file.zarr"
        assert https_to_s3(https_url) == expected

    def test_old_s3_format_sbg_region(self):
        """Test backwards compatibility with old S3 format (sbg region)."""
        https_url = "https://my-bucket.s3.sbg.io.cloud.ovh.net/path/to/file.zarr"
        expected = "s3://my-bucket/path/to/file.zarr"
        assert https_to_s3(https_url) == expected

    def test_old_s3_format_aws_style(self):
        """Test backwards compatibility with AWS-style S3 URLs."""
        https_url = "https://my-bucket.s3.amazonaws.com/path/to/file.zarr"
        expected = "s3://my-bucket/path/to/file.zarr"
        assert https_to_s3(https_url) == expected

    def test_non_s3_url_returns_none(self):
        """Test that non-S3 HTTPS URLs return None."""
        https_url = "https://example.com/path/to/file"
        assert https_to_s3(https_url) is None

    def test_api_url_returns_none(self):
        """Test that API URLs return None."""
        https_url = "https://api.explorer.eopf.copernicus.eu/stac/collections/test"
        assert https_to_s3(https_url) is None

    def test_http_url_returns_none(self):
        """Test that HTTP URLs (non-HTTPS) return None."""
        http_url = "http://s3.explorer.eopf.copernicus.eu/bucket/path"
        assert https_to_s3(http_url) is None

    def test_custom_gateway_url(self):
        """Test conversion with custom gateway URL."""
        https_url = "https://custom.gateway.com/my-bucket/path/file.zarr"
        custom_gateway = "https://custom.gateway.com"
        expected = "s3://my-bucket/path/file.zarr"
        assert https_to_s3(https_url, custom_gateway) == expected

    def test_roundtrip_conversion(self):
        """Test that S3 -> HTTPS -> S3 conversion is lossless."""
        original = "s3://my-bucket/path/to/file.zarr"
        https_url = s3_to_https(original)
        result = https_to_s3(https_url)
        assert result == original

    def test_roundtrip_conversion_complex_path(self):
        """Test roundtrip conversion with complex path."""
        original = "s3://esa-zarr-sentinel-explorer-fra/tests-output/sentinel-2-l2a-staging/S2A_MSIL2A_20250831T103701_N0511_R008_T31TFL_20250831T145420.zarr/measurements/reflectance"
        https_url = s3_to_https(original)
        result = https_to_s3(https_url)
        assert result == original


class TestRewriteAssetHrefs:
    """Test rewrite_asset_hrefs function."""

    def test_rewrite_with_s3_urls(self):
        """Test rewriting asset hrefs with S3 URLs."""
        item = Item(
            id="test-item",
            geometry={"type": "Point", "coordinates": [0, 0]},
            bbox=[0, 0, 1, 1],
            datetime="2025-01-01T00:00:00Z",
            properties={},
        )
        item.add_asset(
            "data",
            Asset(
                href="s3://old-bucket/old-prefix/file.zarr",
                media_type="application/vnd+zarr",
            ),
        )

        old_base = "s3://old-bucket/old-prefix"
        new_base = "s3://new-bucket/new-prefix"

        rewrite_asset_hrefs(item, old_base, new_base)

        expected = "https://s3.explorer.eopf.copernicus.eu/new-bucket/new-prefix/file.zarr"
        assert item.assets["data"].href == expected

    def test_rewrite_with_https_urls(self):
        """Test rewriting asset hrefs with HTTPS URLs."""
        item = Item(
            id="test-item",
            geometry={"type": "Point", "coordinates": [0, 0]},
            bbox=[0, 0, 1, 1],
            datetime="2025-01-01T00:00:00Z",
            properties={},
        )
        item.add_asset(
            "data",
            Asset(
                href="https://old.example.com/old-prefix/file.zarr",
                media_type="application/vnd+zarr",
            ),
        )

        old_base = "https://old.example.com/old-prefix"
        new_base = "https://new.example.com/new-prefix"

        rewrite_asset_hrefs(item, old_base, new_base)

        assert item.assets["data"].href == "https://new.example.com/new-prefix/file.zarr"

    def test_no_rewrite_for_non_matching_href(self):
        """Test that non-matching hrefs are not rewritten."""
        item = Item(
            id="test-item",
            geometry={"type": "Point", "coordinates": [0, 0]},
            bbox=[0, 0, 1, 1],
            datetime="2025-01-01T00:00:00Z",
            properties={},
        )
        original_href = "https://other.example.com/other/file.zarr"
        item.add_asset(
            "data",
            Asset(href=original_href, media_type="application/vnd+zarr"),
        )

        old_base = "s3://old-bucket/old-prefix"
        new_base = "s3://new-bucket/new-prefix"

        rewrite_asset_hrefs(item, old_base, new_base)

        assert item.assets["data"].href == original_href

    def test_rewrite_multiple_assets(self):
        """Test rewriting multiple assets."""
        item = Item(
            id="test-item",
            geometry={"type": "Point", "coordinates": [0, 0]},
            bbox=[0, 0, 1, 1],
            datetime="2025-01-01T00:00:00Z",
            properties={},
        )
        item.add_asset(
            "asset1",
            Asset(
                href="s3://old-bucket/old-prefix/file1.zarr",
                media_type="application/vnd+zarr",
            ),
        )
        item.add_asset(
            "asset2",
            Asset(
                href="s3://old-bucket/old-prefix/file2.zarr",
                media_type="application/vnd+zarr",
            ),
        )

        old_base = "s3://old-bucket/old-prefix"
        new_base = "s3://new-bucket/new-prefix"

        rewrite_asset_hrefs(item, old_base, new_base)

        assert (
            item.assets["asset1"].href
            == "https://s3.explorer.eopf.copernicus.eu/new-bucket/new-prefix/file1.zarr"
        )
        assert (
            item.assets["asset2"].href
            == "https://s3.explorer.eopf.copernicus.eu/new-bucket/new-prefix/file2.zarr"
        )


class TestAddAlternateS3Assets:
    """Test add_alternate_s3_assets function."""

    def test_add_alternate_to_gateway_url(self):
        """Test adding alternate S3 URL to asset with gateway HTTPS URL."""
        item = Item(
            id="test-item",
            geometry={"type": "Point", "coordinates": [0, 0]},
            bbox=[0, 0, 1, 1],
            datetime="2025-01-01T00:00:00Z",
            properties={},
        )
        item.add_asset(
            "data",
            Asset(
                href="https://s3.explorer.eopf.copernicus.eu/my-bucket/path/file.zarr",
                media_type="application/vnd+zarr",
                roles=["data"],
            ),
        )

        s3_endpoint = "https://s3.de.io.cloud.ovh.net"
        add_alternate_s3_assets(item, s3_endpoint)

        # Check extensions were added
        assert (
            "https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json"
            in item.stac_extensions
        )
        assert (
            "https://stac-extensions.github.io/storage/v2.0.0/schema.json" in item.stac_extensions
        )

        # Check alternate was added
        asset = item.assets["data"]
        assert "alternate" in asset.extra_fields
        assert "s3" in asset.extra_fields["alternate"]

        s3_alt = asset.extra_fields["alternate"]["s3"]
        assert s3_alt["href"] == "s3://my-bucket/path/file.zarr"
        assert "storage:scheme" in s3_alt
        scheme = s3_alt["storage:scheme"]
        assert scheme["platform"] == "OVHcloud"
        assert scheme["region"] == "de"
        assert scheme["requester_pays"] is False

    def test_add_alternate_to_old_s3_url(self):
        """Test adding alternate S3 URL to asset with old S3 format URL."""
        item = Item(
            id="test-item",
            geometry={"type": "Point", "coordinates": [0, 0]},
            bbox=[0, 0, 1, 1],
            datetime="2025-01-01T00:00:00Z",
            properties={},
        )
        item.add_asset(
            "data",
            Asset(
                href="https://my-bucket.s3.gra.io.cloud.ovh.net/path/file.zarr",
                media_type="application/vnd+zarr",
                roles=["data"],
            ),
        )

        s3_endpoint = "https://s3.gra.io.cloud.ovh.net"
        add_alternate_s3_assets(item, s3_endpoint)

        asset = item.assets["data"]
        s3_alt = asset.extra_fields["alternate"]["s3"]
        assert s3_alt["href"] == "s3://my-bucket/path/file.zarr"
        assert "storage:scheme" in s3_alt
        assert s3_alt["storage:scheme"]["region"] == "gra"

    def test_skip_thumbnail_asset(self):
        """Test that thumbnail assets are skipped."""
        item = Item(
            id="test-item",
            geometry={"type": "Point", "coordinates": [0, 0]},
            bbox=[0, 0, 1, 1],
            datetime="2025-01-01T00:00:00Z",
            properties={},
        )
        item.add_asset(
            "thumbnail",
            Asset(
                href="https://s3.explorer.eopf.copernicus.eu/my-bucket/thumb.png",
                media_type="image/png",
                roles=["thumbnail"],
            ),
        )

        s3_endpoint = "https://s3.de.io.cloud.ovh.net"
        add_alternate_s3_assets(item, s3_endpoint)

        # Thumbnail should not have alternate
        assert "alternate" not in item.assets["thumbnail"].extra_fields

    def test_skip_non_s3_url(self):
        """Test that non-S3 URLs are skipped."""
        item = Item(
            id="test-item",
            geometry={"type": "Point", "coordinates": [0, 0]},
            bbox=[0, 0, 1, 1],
            datetime="2025-01-01T00:00:00Z",
            properties={},
        )
        item.add_asset(
            "data",
            Asset(
                href="https://api.example.com/data/file",
                media_type="application/json",
                roles=["data"],
            ),
        )

        s3_endpoint = "https://s3.de.io.cloud.ovh.net"
        add_alternate_s3_assets(item, s3_endpoint)

        # Should not have alternate since it's not an S3 URL
        assert "alternate" not in item.assets["data"].extra_fields

    def test_multiple_assets(self):
        """Test adding alternates to multiple assets."""
        item = Item(
            id="test-item",
            geometry={"type": "Point", "coordinates": [0, 0]},
            bbox=[0, 0, 1, 1],
            datetime="2025-01-01T00:00:00Z",
            properties={},
        )
        item.add_asset(
            "data1",
            Asset(
                href="https://s3.explorer.eopf.copernicus.eu/bucket/file1.zarr",
                media_type="application/vnd+zarr",
                roles=["data"],
            ),
        )
        item.add_asset(
            "data2",
            Asset(
                href="https://s3.explorer.eopf.copernicus.eu/bucket/file2.zarr",
                media_type="application/vnd+zarr",
                roles=["data"],
            ),
        )
        item.add_asset(
            "thumbnail",
            Asset(
                href="https://s3.explorer.eopf.copernicus.eu/bucket/thumb.png",
                media_type="image/png",
                roles=["thumbnail"],
            ),
        )

        s3_endpoint = "https://s3.sbg.io.cloud.ovh.net"
        add_alternate_s3_assets(item, s3_endpoint)

        # Data assets should have alternates
        assert "alternate" in item.assets["data1"].extra_fields
        assert "alternate" in item.assets["data2"].extra_fields
        assert (
            item.assets["data1"].extra_fields["alternate"]["s3"]["storage:scheme"]["region"]
            == "sbg"
        )
        assert (
            item.assets["data2"].extra_fields["alternate"]["s3"]["storage:scheme"]["region"]
            == "sbg"
        )

        # Thumbnail should not
        assert "alternate" not in item.assets["thumbnail"].extra_fields

    def test_region_detection_de(self):
        """Test region detection for DE region."""
        item = Item(
            id="test-item",
            geometry={"type": "Point", "coordinates": [0, 0]},
            bbox=[0, 0, 1, 1],
            datetime="2025-01-01T00:00:00Z",
            properties={},
        )
        item.add_asset(
            "data",
            Asset(
                href="https://s3.explorer.eopf.copernicus.eu/bucket/file.zarr",
                media_type="application/vnd+zarr",
                roles=["data"],
            ),
        )

        add_alternate_s3_assets(item, "https://s3.de.io.cloud.ovh.net")
        assert (
            item.assets["data"].extra_fields["alternate"]["s3"]["storage:scheme"]["region"] == "de"
        )

    def test_region_detection_gra(self):
        """Test region detection for GRA region."""
        item = Item(
            id="test-item",
            geometry={"type": "Point", "coordinates": [0, 0]},
            bbox=[0, 0, 1, 1],
            datetime="2025-01-01T00:00:00Z",
            properties={},
        )
        item.add_asset(
            "data",
            Asset(
                href="https://s3.explorer.eopf.copernicus.eu/bucket/file.zarr",
                media_type="application/vnd+zarr",
                roles=["data"],
            ),
        )

        add_alternate_s3_assets(item, "https://s3.gra.io.cloud.ovh.net")
        assert (
            item.assets["data"].extra_fields["alternate"]["s3"]["storage:scheme"]["region"] == "gra"
        )

    def test_region_detection_unknown(self):
        """Test region detection for unknown region."""
        item = Item(
            id="test-item",
            geometry={"type": "Point", "coordinates": [0, 0]},
            bbox=[0, 0, 1, 1],
            datetime="2025-01-01T00:00:00Z",
            properties={},
        )
        item.add_asset(
            "data",
            Asset(
                href="https://s3.explorer.eopf.copernicus.eu/bucket/file.zarr",
                media_type="application/vnd+zarr",
                roles=["data"],
            ),
        )

        add_alternate_s3_assets(item, "https://s3.amazonaws.com")
        assert (
            item.assets["data"].extra_fields["alternate"]["s3"]["storage:scheme"]["region"]
            == "unknown"
        )

    def test_extensions_not_duplicated(self):
        """Test that extensions are not duplicated if already present."""
        item = Item(
            id="test-item",
            geometry={"type": "Point", "coordinates": [0, 0]},
            bbox=[0, 0, 1, 1],
            datetime="2025-01-01T00:00:00Z",
            properties={},
        )
        item.stac_extensions = [
            "https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json"
        ]
        item.add_asset(
            "data",
            Asset(
                href="https://s3.explorer.eopf.copernicus.eu/bucket/file.zarr",
                media_type="application/vnd+zarr",
                roles=["data"],
            ),
        )

        add_alternate_s3_assets(item, "https://s3.de.io.cloud.ovh.net")

        # Count occurrences of alternate-assets extension
        alternate_count = sum(1 for ext in item.stac_extensions if "alternate-assets" in ext)
        assert alternate_count == 1
        assert (
            "https://stac-extensions.github.io/storage/v2.0.0/schema.json" in item.stac_extensions
        )


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_https_url_with_query_params(self):
        """Test HTTPS URL with query parameters (query params are stripped for S3 URIs)."""
        # Note: S3 URIs don't have query parameters. This tests that query params
        # in HTTPS URLs are properly stripped during conversion to S3 URIs
        https_url = "https://s3.explorer.eopf.copernicus.eu/bucket/path/file.zarr?version=123"
        result = https_to_s3(https_url)
        # Query params should be stripped since S3 URIs don't support them
        assert result == "s3://bucket/path/file.zarr"

    def test_empty_path_components(self):
        """Test handling of empty path components."""
        s3_url = "s3://bucket//path//file.zarr"
        https_url = s3_to_https(s3_url)
        result = https_to_s3(https_url)
        assert result == s3_url

    def test_unicode_in_path(self):
        """Test handling of Unicode characters in path."""
        s3_url = "s3://bucket/path/文件.zarr"
        https_url = s3_to_https(s3_url)
        result = https_to_s3(https_url)
        assert result == s3_url

    def test_very_long_path(self):
        """Test handling of very long paths."""
        long_path = "/".join([f"dir{i}" for i in range(100)])
        s3_url = f"s3://bucket/{long_path}/file.zarr"
        https_url = s3_to_https(s3_url)
        result = https_to_s3(https_url)
        assert result == s3_url

    def test_bucket_name_with_dots(self):
        """Test handling of bucket names with dots."""
        s3_url = "s3://my.bucket.name/path/file.zarr"
        https_url = s3_to_https(s3_url)
        result = https_to_s3(https_url)
        assert result == s3_url

    def test_bucket_name_with_dashes(self):
        """Test handling of bucket names with dashes."""
        s3_url = "s3://my-bucket-name-123/path/file.zarr"
        https_url = s3_to_https(s3_url)
        result = https_to_s3(https_url)
        assert result == s3_url
