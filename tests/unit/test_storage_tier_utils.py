"""Unit tests for storage_tier_utils module."""

import sys
from pathlib import Path
from unittest.mock import Mock, patch

from botocore.exceptions import ClientError

# Add scripts directory to path
scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

from storage_tier_utils import (  # noqa: E402
    extract_region_from_endpoint,
    get_s3_storage_class,
    get_s3_storage_info,
)


class TestGetS3StorageClass:
    """Tests for get_s3_storage_class function."""

    @patch("storage_tier_utils.boto3")
    def test_single_file_standard_tier(self, mock_boto3):
        """Test querying a single file with STANDARD tier (no StorageClass field)."""
        # Setup mock
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client
        mock_client.head_object.return_value = {}  # No StorageClass = STANDARD

        # Test
        result = get_s3_storage_class("s3://bucket/path/file.txt", "https://s3.endpoint.com")

        # Verify
        assert result == "STANDARD"
        mock_client.head_object.assert_called_once_with(Bucket="bucket", Key="path/file.txt")

    @patch("storage_tier_utils.boto3")
    def test_single_file_standard_ia_tier(self, mock_boto3):
        """Test querying a single file with STANDARD_IA tier."""
        # Setup mock
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client
        mock_client.head_object.return_value = {"StorageClass": "STANDARD_IA"}

        # Test
        result = get_s3_storage_class("s3://bucket/path/file.txt", "https://s3.endpoint.com")

        # Verify
        assert result == "STANDARD_IA"

    @patch("storage_tier_utils.boto3")
    def test_single_file_express_onezone(self, mock_boto3):
        """Test querying a single file with EXPRESS_ONEZONE tier."""
        # Setup mock
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client
        mock_client.head_object.return_value = {"StorageClass": "EXPRESS_ONEZONE"}

        # Test
        result = get_s3_storage_class("s3://bucket/path/file.txt", "https://s3.endpoint.com")

        # Verify
        assert result == "EXPRESS_ONEZONE"

    @patch("storage_tier_utils.boto3")
    def test_zarr_directory_all_same_class(self, mock_boto3):
        """Test Zarr directory where all files have same storage class."""
        # Setup mock
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client

        # First head_object fails with 404 (it's a directory)
        mock_client.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")

        # list_objects_v2 returns files with STANDARD_IA class
        mock_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "data.zarr/0.0", "StorageClass": "STANDARD_IA"},
                {"Key": "data.zarr/0.1", "StorageClass": "STANDARD_IA"},
                {"Key": "data.zarr/.zarray", "StorageClass": "STANDARD_IA"},
            ]
        }

        # Test
        result = get_s3_storage_class("s3://bucket/data.zarr", "https://s3.endpoint.com")

        # Verify
        assert result == "STANDARD_IA"
        mock_client.list_objects_v2.assert_called_once()

    @patch("storage_tier_utils.boto3")
    def test_zarr_directory_mixed_storage(self, mock_boto3, caplog):
        """Test Zarr directory with mixed storage classes - returns most common."""

        # Setup mock
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client

        # First head_object fails with 404
        mock_client.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")

        # list_objects_v2 returns mixed storage classes (3 STANDARD_IA, 2 STANDARD)
        mock_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "data.zarr/0.0", "StorageClass": "STANDARD_IA"},
                {"Key": "data.zarr/0.1", "StorageClass": "STANDARD_IA"},
                {"Key": "data.zarr/0.2", "StorageClass": "STANDARD_IA"},
                {"Key": "data.zarr/.zarray"},  # STANDARD (no StorageClass)
                {"Key": "data.zarr/.zattrs"},  # STANDARD
            ]
        }

        # Test
        result = get_s3_storage_class("s3://bucket/data.zarr", "https://s3.endpoint.com")

        # Verify - should return most common (STANDARD_IA: 3 vs STANDARD: 2)
        assert result == "STANDARD_IA"
        assert "Mixed storage classes detected" in caplog.text
        assert "STANDARD_IA: 3/5" in caplog.text
        assert "STANDARD: 2/5" in caplog.text

    @patch("storage_tier_utils.boto3")
    def test_zarr_directory_empty(self, mock_boto3):
        """Test Zarr directory with no files."""
        # Setup mock
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client

        # First head_object fails with 404
        mock_client.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")

        # list_objects_v2 returns empty
        mock_client.list_objects_v2.return_value = {}

        # Test
        result = get_s3_storage_class("s3://bucket/data.zarr", "https://s3.endpoint.com")

        # Verify
        assert result is None

    @patch("storage_tier_utils.boto3")
    def test_non_s3_url(self, mock_boto3):
        """Test with non-S3 URL."""
        result = get_s3_storage_class("https://example.com/file", "https://s3.endpoint.com")
        assert result is None

    @patch("storage_tier_utils.boto3")
    def test_s3_url_no_key(self, mock_boto3):
        """Test with S3 URL but no key (root bucket)."""
        result = get_s3_storage_class("s3://bucket/", "https://s3.endpoint.com")
        assert result is None

    @patch("storage_tier_utils.boto3")
    def test_permission_error(self, mock_boto3):
        """Test handling of permission errors."""
        # Setup mock
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client
        mock_client.head_object.side_effect = ClientError({"Error": {"Code": "403"}}, "HeadObject")

        # Test
        result = get_s3_storage_class("s3://bucket/file.txt", "https://s3.endpoint.com")

        # Verify
        assert result is None

    @patch("storage_tier_utils.boto3")
    def test_endpoint_parameter_used(self, mock_boto3):
        """Test that s3_endpoint parameter is used for boto3 client."""
        # Setup mock
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client
        mock_client.head_object.return_value = {"StorageClass": "STANDARD"}

        # Test with specific endpoint
        endpoint = "https://custom.s3.endpoint.com"
        get_s3_storage_class("s3://bucket/file.txt", endpoint)

        # Verify endpoint was used
        mock_boto3.client.assert_called_once_with("s3", endpoint_url=endpoint)

    @patch("storage_tier_utils.boto3")
    @patch("storage_tier_utils.os.getenv")
    def test_fallback_to_env_endpoint(self, mock_getenv, mock_boto3):
        """Test fallback to AWS_ENDPOINT_URL environment variable."""
        # Setup mocks
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client
        mock_client.head_object.return_value = {}
        mock_getenv.return_value = "https://env.endpoint.com"

        # Test with no endpoint parameter
        get_s3_storage_class("s3://bucket/file.txt", "")

        # Verify env endpoint was used
        mock_boto3.client.assert_called_once_with("s3", endpoint_url="https://env.endpoint.com")


class TestExtractRegionFromEndpoint:
    """Tests for extract_region_from_endpoint function."""

    def test_de_region(self):
        """Test extracting DE region."""
        assert extract_region_from_endpoint("https://s3.de.io.cloud.ovh.net") == "de"

    def test_gra_region(self):
        """Test extracting GRA region."""
        assert extract_region_from_endpoint("https://s3.gra.io.cloud.ovh.net") == "gra"

    def test_sbg_region(self):
        """Test extracting SBG region."""
        assert extract_region_from_endpoint("https://s3.sbg.io.cloud.ovh.net") == "sbg"

    def test_uk_region(self):
        """Test extracting UK region."""
        assert extract_region_from_endpoint("https://s3.uk.io.cloud.ovh.net") == "uk"

    def test_ca_region(self):
        """Test extracting CA region."""
        assert extract_region_from_endpoint("https://s3.ca.io.cloud.ovh.net") == "ca"

    def test_unknown_region(self):
        """Test unknown region returns 'unknown'."""
        assert extract_region_from_endpoint("https://s3.unknown-region.com") == "unknown"

    def test_endpoint_without_protocol(self):
        """Test endpoint without https:// protocol."""
        assert extract_region_from_endpoint("s3.de.io.cloud.ovh.net") == "de"

    def test_empty_endpoint(self):
        """Test empty endpoint."""
        assert extract_region_from_endpoint("") == "unknown"


class TestGetS3StorageInfo:
    """Tests for get_s3_storage_info function."""

    @patch("storage_tier_utils.boto3")
    def test_single_file_standard_tier(self, mock_boto3):
        """Test querying a single file with STANDARD tier."""
        # Setup mock
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client
        mock_client.head_object.return_value = {}  # No StorageClass = STANDARD

        # Test
        result = get_s3_storage_info("s3://bucket/path/file.txt", "https://s3.endpoint.com")

        # Verify
        assert result == {"tier": "STANDARD", "distribution": None}
        mock_client.head_object.assert_called_once_with(Bucket="bucket", Key="path/file.txt")

    @patch("storage_tier_utils.boto3")
    def test_single_file_standard_ia_tier(self, mock_boto3):
        """Test querying a single file with STANDARD_IA tier."""
        # Setup mock
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client
        mock_client.head_object.return_value = {"StorageClass": "STANDARD_IA"}

        # Test
        result = get_s3_storage_info("s3://bucket/path/file.txt", "https://s3.endpoint.com")

        # Verify
        assert result == {"tier": "STANDARD_IA", "distribution": None}

    @patch("storage_tier_utils.boto3")
    def test_zarr_directory_uniform_standard_ia(self, mock_boto3):
        """Test Zarr directory where all files have same storage class."""
        # Setup mock
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client

        # First head_object fails with 404 (it's a directory)
        mock_client.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")

        # list_objects_v2 returns files with STANDARD_IA class
        mock_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "data.zarr/0.0", "StorageClass": "STANDARD_IA"},
                {"Key": "data.zarr/0.1", "StorageClass": "STANDARD_IA"},
                {"Key": "data.zarr/.zarray", "StorageClass": "STANDARD_IA"},
            ]
        }

        # Test
        result = get_s3_storage_info("s3://bucket/data.zarr", "https://s3.endpoint.com")

        # Verify
        assert result == {"tier": "STANDARD_IA", "distribution": {"STANDARD_IA": 3}}

    @patch("storage_tier_utils.boto3")
    def test_zarr_directory_uniform_standard(self, mock_boto3):
        """Test Zarr directory where all files are STANDARD."""
        # Setup mock
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client

        # First head_object fails with 404
        mock_client.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")

        # list_objects_v2 returns files without StorageClass field
        mock_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "data.zarr/0.0"},  # No StorageClass = STANDARD
                {"Key": "data.zarr/0.1"},
                {"Key": "data.zarr/.zarray"},
            ]
        }

        # Test
        result = get_s3_storage_info("s3://bucket/data.zarr", "https://s3.endpoint.com")

        # Verify
        assert result == {"tier": "STANDARD", "distribution": {"STANDARD": 3}}

    @patch("storage_tier_utils.boto3")
    def test_zarr_directory_mixed_storage(self, mock_boto3, caplog):
        """Test Zarr directory with mixed storage classes - should return MIXED."""
        import logging

        # Setup mock
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client

        # First head_object fails with 404
        mock_client.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")

        # list_objects_v2 returns mixed storage classes
        mock_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "data.zarr/0.0", "StorageClass": "STANDARD_IA"},
                {"Key": "data.zarr/0.1", "StorageClass": "STANDARD_IA"},
                {"Key": "data.zarr/0.2", "StorageClass": "STANDARD_IA"},
                {"Key": "data.zarr/.zarray"},  # STANDARD (no StorageClass)
                {"Key": "data.zarr/.zattrs"},  # STANDARD
            ]
        }

        # Set log level to capture INFO messages
        caplog.set_level(logging.INFO, logger="storage_tier_utils")

        # Test
        result = get_s3_storage_info("s3://bucket/data.zarr", "https://s3.endpoint.com")

        # Verify
        assert result["tier"] == "MIXED"
        assert result["distribution"] == {"STANDARD_IA": 3, "STANDARD": 2}
        assert "Mixed storage classes detected" in caplog.text

    @patch("storage_tier_utils.boto3")
    def test_zarr_directory_large_mixed_storage(self, mock_boto3):
        """Test Zarr directory with many files and mixed storage."""
        # Setup mock
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client

        # First head_object fails with 404
        mock_client.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")

        # Simulate 100 files: 60 STANDARD_IA, 40 STANDARD
        contents = []
        for i in range(60):
            contents.append({"Key": f"data.zarr/chunk_{i}", "StorageClass": "STANDARD_IA"})
        for i in range(40):
            contents.append({"Key": f"data.zarr/meta_{i}"})  # STANDARD (no StorageClass)

        mock_client.list_objects_v2.return_value = {"Contents": contents}

        # Test
        result = get_s3_storage_info("s3://bucket/data.zarr", "https://s3.endpoint.com")

        # Verify
        assert result["tier"] == "MIXED"
        assert result["distribution"] == {"STANDARD_IA": 60, "STANDARD": 40}

    @patch("storage_tier_utils.boto3")
    def test_zarr_directory_empty(self, mock_boto3):
        """Test Zarr directory with no files."""
        # Setup mock
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client

        # First head_object fails with 404
        mock_client.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")

        # list_objects_v2 returns empty
        mock_client.list_objects_v2.return_value = {}

        # Test
        result = get_s3_storage_info("s3://bucket/data.zarr", "https://s3.endpoint.com")

        # Verify
        assert result is None

    @patch("storage_tier_utils.boto3")
    def test_max_samples_parameter(self, mock_boto3):
        """Test that max_samples parameter is respected."""
        # Setup mock
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client

        # First head_object fails with 404
        mock_client.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")

        mock_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": f"data.zarr/{i}", "StorageClass": "STANDARD_IA"} for i in range(50)
            ]
        }

        # Test with custom max_samples
        result = get_s3_storage_info(
            "s3://bucket/data.zarr", "https://s3.endpoint.com", max_samples=50
        )

        # Verify MaxKeys parameter was passed
        mock_client.list_objects_v2.assert_called_once_with(
            Bucket="bucket", Prefix="data.zarr/", MaxKeys=50
        )
        assert result == {"tier": "STANDARD_IA", "distribution": {"STANDARD_IA": 50}}

    @patch("storage_tier_utils.boto3")
    def test_invalid_url(self, mock_boto3):
        """Test with non-S3 URL."""
        result = get_s3_storage_info("https://example.com/file", "https://s3.endpoint.com")
        assert result is None

    @patch("storage_tier_utils.boto3")
    def test_no_key_in_url(self, mock_boto3):
        """Test with S3 URL containing only bucket."""
        result = get_s3_storage_info("s3://bucket/", "https://s3.endpoint.com")
        assert result is None

    @patch("storage_tier_utils.boto3")
    def test_permission_error(self, mock_boto3):
        """Test when access is denied (403 error)."""
        # Setup mock
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client
        mock_client.head_object.side_effect = ClientError({"Error": {"Code": "403"}}, "HeadObject")

        # Test
        result = get_s3_storage_info("s3://bucket/file.txt", "https://s3.endpoint.com")

        # Verify
        assert result is None
