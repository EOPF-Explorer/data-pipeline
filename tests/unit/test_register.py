"""Unit tests for register.py - STAC registration and augmentation."""

from unittest.mock import MagicMock, patch

from pystac import Asset, Item
from pystac.extensions.projection import ProjectionExtension

from scripts.register import (
    add_projection_from_zarr,
    add_visualization_links,
    rewrite_asset_hrefs,
    s3_to_https,
)


class TestS3URLConversion:
    """Test S3 URL to HTTPS conversion."""

    def test_s3_to_https_conversion(self):
        """Test basic S3 to HTTPS conversion."""
        result = s3_to_https("s3://bucket/path/file.zarr", "https://s3.example.com")
        assert result == "https://bucket.s3.example.com/path/file.zarr"

    def test_s3_to_https_with_trailing_slash(self):
        """Test S3 conversion with trailing slash in endpoint."""
        result = s3_to_https("s3://bucket/file.zarr", "https://s3.example.com/")
        assert result == "https://bucket.s3.example.com/file.zarr"

    def test_s3_to_https_already_https(self):
        """Test that HTTPS URLs are returned unchanged."""
        https_url = "https://example.com/data.zarr"
        result = s3_to_https(https_url, "https://s3.example.com")
        assert result == https_url


class TestAssetRewriting:
    """Test asset href rewriting."""

    def test_rewrite_asset_hrefs(self):
        """Test asset href rewriting from old to new base."""
        item = Item(
            id="test",
            geometry=None,
            bbox=None,
            datetime="2025-01-01T00:00:00Z",
            properties={},
        )
        item.add_asset(
            "data",
            Asset(
                href="s3://old-bucket/old.zarr/path/data",
                media_type="application/vnd+zarr",
            ),
        )

        rewrite_asset_hrefs(
            item,
            old_base="s3://old-bucket/old.zarr",
            new_base="s3://new-bucket/new.zarr",
            s3_endpoint="https://s3.example.com",
        )

        # Should rewrite to HTTPS URL with new base
        new_href = item.assets["data"].href
        assert new_href.startswith("https://new-bucket.s3.example.com")
        assert "new.zarr" in new_href


class TestProjectionExtension:
    """Test projection extension from zarr."""

    @patch("scripts.register.zarr.open")
    def test_add_projection_from_zarr_with_spatial_ref(self, mock_zarr_open):
        """Test adding projection extension when spatial_ref exists."""
        # Mock zarr store with spatial_ref
        mock_store = MagicMock()
        mock_store.attrs = {
            "spatial_ref": {
                "spatial_ref": "32633",  # EPSG code
                "crs_wkt": "PROJCS[...]",
            }
        }
        mock_zarr_open.return_value = mock_store

        item = Item(
            id="test",
            geometry=None,
            bbox=None,
            datetime="2025-01-01T00:00:00Z",
            properties={},
        )
        item.add_asset(
            "data",
            Asset(
                href="https://example.com/data.zarr",
                media_type="application/vnd+zarr",
            ),
        )

        add_projection_from_zarr(item)

        # Should have projection extension
        proj = ProjectionExtension.ext(item)
        assert proj.epsg == 32633
        assert proj.wkt2 is not None

    @patch("scripts.register.zarr.open")
    def test_add_projection_no_spatial_ref(self, mock_zarr_open):
        """Test that function handles missing spatial_ref gracefully."""
        # Mock zarr store without spatial_ref
        mock_store = MagicMock()
        mock_store.attrs = {}
        mock_zarr_open.return_value = mock_store

        item = Item(
            id="test",
            geometry=None,
            bbox=None,
            datetime="2025-01-01T00:00:00Z",
            properties={},
        )
        item.add_asset(
            "data",
            Asset(
                href="https://example.com/data.zarr",
                media_type="application/vnd+zarr",
            ),
        )

        # Should not raise an exception
        add_projection_from_zarr(item)

        # Should not have projection extension
        assert "proj:epsg" not in item.properties


class TestVisualizationLinks:
    """Test visualization link generation."""

    def test_add_visualization_links_s2(self):
        """Test S2 True Color visualization links."""
        item = Item(
            id="S2A_MSIL2A_20250518_test",
            geometry=None,
            bbox=None,
            datetime="2025-05-18T00:00:00Z",
            properties={},
        )
        item.add_asset(
            "TCI_10m",
            Asset(
                href="https://example.com/data.zarr/quality/l2a_quicklook/r10m",
                media_type="application/vnd+zarr",
            ),
        )

        add_visualization_links(
            item,
            raster_base="https://api.example.com/raster",
            collection_id="sentinel-2-l2a",
        )

        # Should have viewer link
        viewer_links = [link for link in item.links if link.rel == "viewer"]
        assert len(viewer_links) == 1
        assert "viewer" in viewer_links[0].href

        # Should have XYZ tile link
        xyz_links = [link for link in item.links if link.rel == "xyz"]
        assert len(xyz_links) == 1
        assert "tiles" in xyz_links[0].href

        # Should have TileJSON link
        tilejson_links = [link for link in item.links if link.rel == "tilejson"]
        assert len(tilejson_links) == 1
        assert "tilejson.json" in tilejson_links[0].href

    def test_add_visualization_links_s1(self):
        """Test S1 VH visualization links."""
        item = Item(
            id="S1A_IW_GRDH_test",
            geometry=None,
            bbox=None,
            datetime="2025-05-18T00:00:00Z",
            properties={},
        )
        item.add_asset(
            "vh",
            Asset(
                href="https://example.com/data.zarr/measurements/vh",
                media_type="application/vnd+zarr",
            ),
        )

        add_visualization_links(
            item,
            raster_base="https://api.example.com/raster",
            collection_id="sentinel-1-l1-grd",
        )

        # Should have viewer link
        viewer_links = [link for link in item.links if link.rel == "viewer"]
        assert len(viewer_links) == 1

        # Should have XYZ tile link for VH
        xyz_links = [link for link in item.links if link.rel == "xyz"]
        assert len(xyz_links) == 1
        assert "VH" in xyz_links[0].title or "vh" in xyz_links[0].href.lower()

    def test_add_eopf_explorer_link(self):
        """Test EOPF Explorer via link is added."""
        item = Item(
            id="test-item",
            geometry=None,
            bbox=None,
            datetime="2025-01-01T00:00:00Z",
            properties={},
        )

        add_visualization_links(
            item,
            raster_base="https://api.example.com/raster",
            collection_id="sentinel-2-l2a",
        )

        # Should have via link to EOPF Explorer
        via_links = [link for link in item.links if link.rel == "via"]
        assert len(via_links) == 1
        assert "explorer.eopf.copernicus.eu" in via_links[0].href
        assert "EOPF Explorer" in via_links[0].title
