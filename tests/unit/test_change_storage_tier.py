"""Unit tests for change_storage_tier.py script."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from scripts.change_storage_tier import (
    change_object_storage_class,
    extract_s3_urls,
    filter_paths,
    get_zarr_root,
    list_objects,
    main,
    process_stac_item,
    validate_storage_class,
)


class TestValidateStorageClass:
    """Tests for storage class validation."""

    def test_valid_standard(self) -> None:
        """Test STANDARD is valid."""
        assert validate_storage_class("STANDARD") is True

    def test_valid_glacier(self) -> None:
        """Test GLACIER is valid."""
        assert validate_storage_class("GLACIER") is True

    def test_valid_express_onezone(self) -> None:
        """Test EXPRESS_ONEZONE is valid."""
        assert validate_storage_class("EXPRESS_ONEZONE") is True

    def test_invalid_storage_class(self) -> None:
        """Test invalid storage class returns False."""
        assert validate_storage_class("INVALID") is False
        assert validate_storage_class("glacier") is False  # case sensitive
        assert validate_storage_class("") is False

    def test_valid_storage_classes_list(self) -> None:
        """Test all valid storage classes."""
        valid_classes = ["STANDARD", "GLACIER", "EXPRESS_ONEZONE"]
        for sc in valid_classes:
            assert validate_storage_class(sc) is True


class TestExtractS3Urls:
    """Tests for S3 URL extraction from STAC items."""

    def test_extract_s3_urls_basic(self) -> None:
        """Test basic S3 URL extraction from a Sentinel-2 STAC item."""
        stac_item = {
            "assets": {
                "B02_10m": {
                    "alternate": {
                        "s3": {
                            "href": "s3://eopf-zarr-store/sentinel-2/test.zarr/measurements/reflectance/r10m/B02"
                        }
                    }
                }
            }
        }
        urls = extract_s3_urls(stac_item)
        assert len(urls) == 1
        assert "s3://eopf-zarr-store/sentinel-2/test.zarr/measurements/reflectance/r10m/B02" in urls

    def test_extract_s3_urls_multiple_assets(self) -> None:
        """Test extraction from multiple Sentinel-2 band assets."""
        stac_item = {
            "assets": {
                "B02_10m": {
                    "alternate": {
                        "s3": {
                            "href": "s3://eopf-zarr-store/geozarr/S2A_MSIL2A.zarr/measurements/reflectance/r10m/B02"
                        }
                    }
                },
                "B03_10m": {
                    "alternate": {
                        "s3": {
                            "href": "s3://eopf-zarr-store/geozarr/S2A_MSIL2A.zarr/measurements/reflectance/r10m/B03"
                        }
                    }
                },
                "B04_10m": {
                    "alternate": {
                        "s3": {
                            "href": "s3://eopf-zarr-store/geozarr/S2A_MSIL2A.zarr/measurements/reflectance/r10m/B04"
                        }
                    }
                },
            }
        }
        urls = extract_s3_urls(stac_item)
        assert len(urls) == 3

    def test_extract_s3_urls_skips_thumbnails(self) -> None:
        """Test that thumbnail assets are skipped."""
        stac_item = {
            "assets": {
                "thumbnail": {
                    "roles": ["thumbnail"],
                    "alternate": {
                        "s3": {"href": "s3://eopf-zarr-store/thumbnails/S2A_MSIL2A_thumbnail.png"}
                    },
                },
                "B02_10m": {
                    "alternate": {
                        "s3": {
                            "href": "s3://eopf-zarr-store/geozarr/S2A_MSIL2A.zarr/measurements/reflectance/r10m/B02"
                        }
                    }
                },
            }
        }
        urls = extract_s3_urls(stac_item)
        assert len(urls) == 1
        assert (
            "s3://eopf-zarr-store/geozarr/S2A_MSIL2A.zarr/measurements/reflectance/r10m/B02" in urls
        )

    def test_extract_s3_urls_no_alternate(self) -> None:
        """Test handling assets without alternate field."""
        stac_item = {
            "assets": {
                "B02_10m": {"href": "https://api.example.com/data/S2A_MSIL2A.zarr/B02"},
            }
        }
        urls = extract_s3_urls(stac_item)
        assert len(urls) == 0

    def test_extract_s3_urls_empty_assets(self) -> None:
        """Test handling empty assets."""
        stac_item: dict = {"assets": {}}
        urls = extract_s3_urls(stac_item)
        assert len(urls) == 0

    def test_extract_s3_urls_no_assets_key(self) -> None:
        """Test handling missing assets key."""
        stac_item: dict = {}
        urls = extract_s3_urls(stac_item)
        assert len(urls) == 0

    def test_extract_s3_urls_non_s3_href(self) -> None:
        """Test that non-S3 URLs are ignored."""
        stac_item = {
            "assets": {
                "B02_10m": {
                    "alternate": {
                        "s3": {"href": "https://api.example.com/data/S2A_MSIL2A.zarr/B02"}
                    }
                },
            }
        }
        urls = extract_s3_urls(stac_item)
        assert len(urls) == 0

    def test_extract_s3_urls_deduplication(self) -> None:
        """Test that duplicate URLs are deduplicated."""
        stac_item = {
            "assets": {
                "B02_10m": {
                    "alternate": {
                        "s3": {
                            "href": "s3://eopf-zarr-store/geozarr/S2A_MSIL2A.zarr/measurements/reflectance/r10m/B02"
                        }
                    }
                },
                "B02_10m_copy": {
                    "alternate": {
                        "s3": {
                            "href": "s3://eopf-zarr-store/geozarr/S2A_MSIL2A.zarr/measurements/reflectance/r10m/B02"
                        }
                    }
                },
            }
        }
        urls = extract_s3_urls(stac_item)
        assert len(urls) == 1


class TestGetZarrRoot:
    """Tests for Zarr root extraction."""

    def test_get_zarr_root_basic(self) -> None:
        """Test basic Zarr root extraction from Sentinel-2 URL."""
        urls = {"s3://eopf-zarr-store/sentinel-2/test.zarr/measurements/reflectance/r10m/B02"}
        root = get_zarr_root(urls)
        assert root == "s3://eopf-zarr-store/sentinel-2/test.zarr"

    def test_get_zarr_root_multiple_urls(self) -> None:
        """Test with multiple URLs pointing to same Zarr root."""
        urls = {
            "s3://eopf-zarr-store/geozarr/S2A_MSIL2A.zarr/measurements/reflectance/r10m/B02",
            "s3://eopf-zarr-store/geozarr/S2A_MSIL2A.zarr/measurements/reflectance/r10m/B03",
            "s3://eopf-zarr-store/geozarr/S2A_MSIL2A.zarr/measurements/quality/cloud_mask",
        }
        root = get_zarr_root(urls)
        assert root == "s3://eopf-zarr-store/geozarr/S2A_MSIL2A.zarr"

    def test_get_zarr_root_no_zarr(self) -> None:
        """Test handling URLs without .zarr pattern."""
        urls = {"s3://eopf-zarr-store/sentinel-2/S2A_MSIL2A_20231015/metadata.json"}
        root = get_zarr_root(urls)
        assert root is None

    def test_get_zarr_root_empty_set(self) -> None:
        """Test handling empty URL set."""
        urls: set[str] = set()
        root = get_zarr_root(urls)
        assert root is None


class TestFilterPaths:
    """Tests for path filtering functionality."""

    @pytest.fixture
    def sample_zarr_paths(self) -> list[str]:
        """Create realistic Zarr object paths from S3 listing."""
        zarr_prefix = "geozarr/S2A_test.zarr/"
        return [
            f"{zarr_prefix}.zattrs",
            f"{zarr_prefix}.zgroup",
            f"{zarr_prefix}measurements/reflectance/r10m/B02/0",
            f"{zarr_prefix}measurements/reflectance/r10m/B02/.zarray",
            f"{zarr_prefix}measurements/reflectance/r10m/B03/0",
            f"{zarr_prefix}measurements/reflectance/r20m/B05/0",
            f"{zarr_prefix}measurements/reflectance/r60m/B01/0",
            f"{zarr_prefix}measurements/quality/cloud_mask/0",
            f"{zarr_prefix}metadata/product_info/.zattrs",
        ]

    def test_filter_paths_no_patterns(self, sample_zarr_paths: list[str]) -> None:
        """Test that all paths pass when no patterns specified."""
        # Convert paths to (key, storage_class) tuples
        objects = [(path, "STANDARD") for path in sample_zarr_paths]
        filtered, excluded = filter_paths(objects)
        assert filtered == objects
        assert excluded == []

    def test_filter_paths_include_pattern(self, sample_zarr_paths: list[str]) -> None:
        """Test include pattern to select only 10m resolution bands."""
        zarr_prefix = "geozarr/S2A_test.zarr/"
        objects = [(path, "STANDARD") for path in sample_zarr_paths]
        filtered, excluded = filter_paths(
            objects,
            include_patterns=["measurements/reflectance/r10m/*"],
            zarr_prefix=zarr_prefix,
        )
        assert len(filtered) == 3
        assert all("r10m" in key for key, _ in filtered)
        assert len(excluded) == 6

    def test_filter_paths_exclude_pattern(self, sample_zarr_paths: list[str]) -> None:
        """Test exclude pattern to skip Zarr metadata files."""
        zarr_prefix = "geozarr/S2A_test.zarr/"
        objects = [(path, "STANDARD") for path in sample_zarr_paths]
        filtered, excluded = filter_paths(
            objects,
            exclude_patterns=["*.zattrs", "*.zgroup", "*.zarray"],
            zarr_prefix=zarr_prefix,
        )
        assert len(filtered) == 5
        assert all(not key.endswith((".zattrs", ".zgroup", ".zarray")) for key, _ in filtered)

    def test_filter_paths_include_and_exclude(self, sample_zarr_paths: list[str]) -> None:
        """Test combined include (10m bands) and exclude (60m resolution)."""
        zarr_prefix = "geozarr/S2A_test.zarr/"
        paths = [
            f"{zarr_prefix}measurements/reflectance/r10m/B02/0",
            f"{zarr_prefix}measurements/reflectance/r10m/B03/0",
            f"{zarr_prefix}measurements/reflectance/r20m/B05/0",
            f"{zarr_prefix}measurements/reflectance/r60m/B01/0",
        ]
        objects = [(path, "STANDARD") for path in paths]
        filtered, excluded = filter_paths(
            objects,
            include_patterns=["measurements/reflectance/*"],
            exclude_patterns=["*/r60m/*"],
            zarr_prefix=zarr_prefix,
        )
        assert len(filtered) == 3
        assert all("r60m" not in key for key, _ in filtered)

    def test_filter_paths_multiple_include_patterns(self) -> None:
        """Test multiple include patterns (OR logic) for reflectance and quality."""
        zarr_prefix = "geozarr/S2A_MSIL2A.zarr/"
        paths = [
            f"{zarr_prefix}measurements/reflectance/r10m/B02/0",
            f"{zarr_prefix}measurements/quality/cloud_mask/0",
            f"{zarr_prefix}metadata/product_info/data",
        ]
        objects = [(path, "STANDARD") for path in paths]
        filtered, excluded = filter_paths(
            objects,
            include_patterns=["measurements/reflectance/*", "measurements/quality/*"],
            zarr_prefix=zarr_prefix,
        )
        assert len(filtered) == 2
        assert f"{zarr_prefix}metadata/product_info/data" in excluded

    def test_filter_paths_wildcard_patterns(self) -> None:
        """Test single character wildcard pattern matching for resolutions."""
        zarr_prefix = "geozarr/S2A_MSIL2A.zarr/"
        paths = [
            f"{zarr_prefix}measurements/reflectance/r10m/B02/0",
            f"{zarr_prefix}measurements/reflectance/r20m/B05/0",
            f"{zarr_prefix}measurements/reflectance/r60m/B01/0",
        ]
        objects = [(path, "STANDARD") for path in paths]
        filtered, excluded = filter_paths(
            objects, include_patterns=["measurements/reflectance/r?0m/*"], zarr_prefix=zarr_prefix
        )
        assert len(filtered) == 3  # All match r?0m pattern


class TestListObjects:
    """Tests for S3 object listing."""

    BUCKET = "eopf-zarr-store"
    PREFIX = "geozarr/S2A_test.zarr/"

    @pytest.fixture
    def mock_s3_client(self) -> MagicMock:
        """Create a mock S3 client with paginator and default response."""
        mock_client = MagicMock()
        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator
        # Default response simulating realistic S3 listing with multiple pages
        # StorageClass is included in list_objects_v2 response
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {
                        "Key": f"{self.PREFIX}measurements/reflectance/r10m/B02/0",
                        "StorageClass": "STANDARD",
                    },
                    {
                        "Key": f"{self.PREFIX}measurements/reflectance/r10m/B02/.zarray",
                        "StorageClass": "STANDARD",
                    },
                ]
            },
            {
                "Contents": [
                    {
                        "Key": f"{self.PREFIX}measurements/reflectance/r10m/B03/0",
                        "StorageClass": "GLACIER",
                    },
                ]
            },
            {
                "Contents": [
                    {
                        "Key": f"{self.PREFIX}measurements/reflectance/r20m/B05/0",
                        "StorageClass": "STANDARD",
                    },
                    {
                        "Key": f"{self.PREFIX}measurements/reflectance/r20m/B05/.zarray",
                        "StorageClass": "STANDARD",
                    },
                ]
            },
        ]
        mock_client._paginator = mock_paginator  # Store reference for test access
        return mock_client

    def test_list_objects_calls_paginator_with_correct_params(
        self, mock_s3_client: MagicMock
    ) -> None:
        """Test that list_objects calls S3 paginator with correct bucket and prefix."""
        objects = list_objects(mock_s3_client, self.BUCKET, self.PREFIX)

        # Verify correct paginator is requested
        mock_s3_client.get_paginator.assert_called_once_with("list_objects_v2")
        # Verify paginate is called with correct bucket and prefix
        mock_s3_client._paginator.paginate.assert_called_once_with(
            Bucket=self.BUCKET, Prefix=self.PREFIX
        )
        assert len(objects) == 5

    def test_list_objects_aggregates_multiple_pages(self, mock_s3_client: MagicMock) -> None:
        """Test that objects from multiple pages are aggregated correctly."""
        objects = list_objects(mock_s3_client, self.BUCKET, self.PREFIX)

        assert len(objects) == 5
        assert objects == [
            (f"{self.PREFIX}measurements/reflectance/r10m/B02/0", "STANDARD"),
            (f"{self.PREFIX}measurements/reflectance/r10m/B02/.zarray", "STANDARD"),
            (f"{self.PREFIX}measurements/reflectance/r10m/B03/0", "GLACIER"),
            (f"{self.PREFIX}measurements/reflectance/r20m/B05/0", "STANDARD"),
            (f"{self.PREFIX}measurements/reflectance/r20m/B05/.zarray", "STANDARD"),
        ]

    def test_list_objects_handles_empty_prefix(self, mock_s3_client: MagicMock) -> None:
        """Test listing with empty prefix passes correct params."""

        objects = list_objects(mock_s3_client, self.BUCKET, "")

        mock_s3_client._paginator.paginate.assert_called_once_with(Bucket=self.BUCKET, Prefix="")
        assert len(objects) == 5

    def test_list_objects_returns_empty_list_when_no_contents(
        self, mock_s3_client: MagicMock
    ) -> None:
        """Test that empty Contents returns empty list (nonexistent prefix)."""
        # S3 returns pages without Contents key when prefix doesn't exist
        mock_s3_client._paginator.paginate.return_value = [{}]

        objects = list_objects(mock_s3_client, self.BUCKET, "nonexistent-prefix/")

        assert objects == []


class TestChangeObjectStorageClass:
    """Tests for storage class change operation."""

    BUCKET = "eopf-zarr-store"
    OBJECT_KEY = "geozarr/S2A_test.zarr/measurements/reflectance/r10m/B02/0"

    def test_dry_run_mode(self) -> None:
        """Test dry run doesn't modify objects (no API calls needed)."""
        mock_client = MagicMock()

        success, current_class = change_object_storage_class(
            mock_client, self.BUCKET, self.OBJECT_KEY, "STANDARD", "GLACIER", dry_run=True
        )
        assert success is True
        assert current_class == "STANDARD"
        # No head_object call needed - storage class already known from list_objects
        mock_client.head_object.assert_not_called()
        mock_client.copy_object.assert_not_called()

    def test_already_correct_storage_class(self) -> None:
        """Test skipping objects already in GLACIER storage class."""
        mock_client = MagicMock()

        success, current_class = change_object_storage_class(
            mock_client, self.BUCKET, self.OBJECT_KEY, "GLACIER", "GLACIER", dry_run=False
        )
        assert success is True
        assert current_class == "GLACIER"
        # No API calls needed - already correct storage class
        mock_client.head_object.assert_not_called()
        mock_client.copy_object.assert_not_called()

    def test_change_storage_class_success(self) -> None:
        """Test successful storage class change from STANDARD to GLACIER."""
        mock_client = MagicMock()

        success, current_class = change_object_storage_class(
            mock_client, self.BUCKET, self.OBJECT_KEY, "STANDARD", "GLACIER", dry_run=False
        )
        assert success is True
        assert current_class == "STANDARD"
        # Only copy_object call needed to change storage class
        mock_client.head_object.assert_not_called()
        mock_client.copy_object.assert_called_once_with(
            Bucket=self.BUCKET,
            Key=self.OBJECT_KEY,
            CopySource={"Bucket": self.BUCKET, "Key": self.OBJECT_KEY},
            StorageClass="GLACIER",
            MetadataDirective="COPY",
        )

    def test_change_storage_class_error(self) -> None:
        """Test handling S3 AccessDenied error during copy_object."""
        from botocore.exceptions import ClientError

        mock_client = MagicMock()
        mock_client.copy_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
            "CopyObject",
        )

        success, current_class = change_object_storage_class(
            mock_client, self.BUCKET, self.OBJECT_KEY, "STANDARD", "GLACIER", dry_run=False
        )
        assert success is False
        assert current_class == "STANDARD"  # Returns the known current class


class TestProcessStacItem:
    """Tests for the main processing function."""

    STAC_API_URL = (
        "https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a/items/test"
    )
    S3_ENDPOINT = "https://s3.sbg.perf.cloud.ovh.net"

    @patch("scripts.change_storage_tier.httpx.Client")
    @patch("scripts.change_storage_tier.boto3.client")
    def test_process_stac_item_no_s3_urls(
        self, mock_boto_client: MagicMock, mock_httpx_client: MagicMock
    ) -> None:
        """Test handling STAC item with no S3 URLs (only HTTP assets)."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"assets": {}}
        mock_httpx_client.return_value.__enter__.return_value.get.return_value = mock_response

        stats = process_stac_item(
            self.STAC_API_URL,
            "GLACIER",
            dry_run=False,
            s3_endpoint=None,
        )
        assert stats["processed"] == 0
        assert stats["succeeded"] == 0
        assert stats["failed"] == 0

    @patch("scripts.change_storage_tier.httpx.Client")
    @patch("scripts.change_storage_tier.boto3.client")
    @patch("scripts.change_storage_tier.list_objects")
    @patch("scripts.change_storage_tier.change_object_storage_class")
    def test_process_stac_item_success(
        self,
        mock_change: MagicMock,
        mock_list: MagicMock,
        mock_boto_client: MagicMock,
        mock_httpx_client: MagicMock,
    ) -> None:
        """Test successful processing of Sentinel-2 STAC item."""
        # Setup realistic STAC item response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "id": "test",
            "assets": {
                "B02_10m": {
                    "alternate": {
                        "s3": {
                            "href": "s3://eopf-zarr-store/geozarr/S2A_MSIL2A.zarr/measurements/reflectance/r10m/B02"
                        }
                    }
                }
            },
        }
        mock_httpx_client.return_value.__enter__.return_value.get.return_value = mock_response
        mock_list.return_value = [
            ("geozarr/S2A_MSIL2A.zarr/measurements/reflectance/r10m/B02/0", "STANDARD"),
            ("geozarr/S2A_MSIL2A.zarr/measurements/reflectance/r10m/B02/.zarray", "STANDARD"),
        ]
        mock_change.return_value = (True, "STANDARD")

        stats = process_stac_item(
            self.STAC_API_URL,
            "GLACIER",
            dry_run=False,
            s3_endpoint=self.S3_ENDPOINT,
        )
        assert stats["processed"] == 2
        assert stats["succeeded"] == 2
        assert stats["failed"] == 0


class TestMain:
    """Tests for CLI entry point."""

    STAC_API_URL = (
        "https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a/items/test"
    )

    def test_main_missing_required_args(self) -> None:
        """Test that missing required arguments causes error."""
        with pytest.raises(SystemExit):
            main([])

    def test_main_invalid_storage_class(self) -> None:
        """Test that invalid storage class causes error."""
        with pytest.raises(SystemExit):
            main(["--stac-item-url", self.STAC_API_URL, "--storage-class", "INVALID"])

    @patch("scripts.change_storage_tier.process_stac_item")
    def test_main_success(self, mock_process: MagicMock) -> None:
        """Test successful main execution with dry-run."""
        mock_process.return_value = {"processed": 10, "succeeded": 10, "failed": 0}

        result = main(
            [
                "--stac-item-url",
                self.STAC_API_URL,
                "--storage-class",
                "GLACIER",
                "--dry-run",
            ]
        )
        assert result == 0

    @patch("scripts.change_storage_tier.process_stac_item")
    def test_main_with_failures(self, mock_process: MagicMock) -> None:
        """Test main returns error code when some objects fail."""
        mock_process.return_value = {"processed": 10, "succeeded": 8, "failed": 2}

        result = main(
            [
                "--stac-item-url",
                self.STAC_API_URL,
                "--storage-class",
                "GLACIER",
            ]
        )
        assert result == 1

    @patch("scripts.change_storage_tier.process_stac_item")
    def test_main_with_patterns(self, mock_process: MagicMock) -> None:
        """Test main passes filter patterns for selecting specific bands."""
        mock_process.return_value = {"processed": 5, "succeeded": 5, "failed": 0}

        result = main(
            [
                "--stac-item-url",
                self.STAC_API_URL,
                "--storage-class",
                "GLACIER",
                "--include-pattern",
                "measurements/reflectance/r10m/*",
                "--exclude-pattern",
                "*.zattrs",
            ]
        )
        assert result == 0
        mock_process.assert_called_once()
        call_args = mock_process.call_args
        # Positional args: (stac_item_url, storage_class, dry_run, s3_endpoint, include_patterns, exclude_patterns)
        stac_item_url, storage_class, dry_run, s3_endpoint, include_patterns, exclude_patterns = (
            call_args[0]
        )
        assert stac_item_url == self.STAC_API_URL
        assert storage_class == "GLACIER"
        assert dry_run is False
        assert s3_endpoint is None
        assert include_patterns == ["measurements/reflectance/r10m/*"]
        assert exclude_patterns == ["*.zattrs"]
