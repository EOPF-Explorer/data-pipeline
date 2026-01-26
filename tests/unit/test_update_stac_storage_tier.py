"""Unit tests for update_stac_storage_tier.py script."""

import json
import sys
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

# Add scripts directory to path
scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

from pystac import Item  # noqa: E402

# Fixtures directory
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "update_storage_tier"


@pytest.fixture
def stac_item_before():
    """STAC item with existing alternate.s3 (STANDARD tier)."""
    with open(FIXTURES_DIR / "stac_item_before.json") as f:
        return Item.from_dict(json.load(f))


@pytest.fixture
def stac_item_legacy():
    """Legacy STAC item without alternate.s3."""
    with open(FIXTURES_DIR / "stac_item_legacy.json") as f:
        return Item.from_dict(json.load(f))


@pytest.fixture
def s3_responses():
    """Mock S3 storage tier responses."""
    with open(FIXTURES_DIR / "s3_storage_responses.json") as f:
        return json.load(f)


class TestUpdateItemStorageTiers:
    """Tests for update_item_storage_tiers function."""

    @patch("update_stac_storage_tier.get_s3_storage_info")
    def test_tier_change_updates_asset(self, mock_get_info, stac_item_before):
        """Test updating storage tier when it changes."""
        from update_stac_storage_tier import update_item_storage_tiers

        mock_get_info.return_value = {"tier": "GLACIER", "distribution": None}

        updated, with_alt, with_tier, added, skipped, failed = update_item_storage_tiers(
            stac_item_before, "https://s3.endpoint.com", add_missing=False
        )

        assert updated == 1
        assert with_alt == 1
        assert with_tier == 1
        s3_info = stac_item_before.assets["reflectance"].extra_fields["alternate"]["s3"]
        assert s3_info["storage:scheme"]["tier"] == "GLACIER"

    @patch("update_stac_storage_tier.get_s3_storage_info")
    def test_no_update_when_tier_unchanged(self, mock_get_info, stac_item_before):
        """Test no update when tier hasn't changed."""
        from update_stac_storage_tier import update_item_storage_tiers

        mock_get_info.return_value = {"tier": "STANDARD", "distribution": None}

        updated, with_alt, with_tier, added, skipped, failed = update_item_storage_tiers(
            stac_item_before, "https://s3.endpoint.com", add_missing=False
        )

        assert updated == 0
        assert with_tier == 1

    @patch("update_stac_storage_tier.get_s3_storage_info")
    def test_removes_tier_on_s3_query_failure(self, mock_get_info, stac_item_before):
        """Test tier removal when S3 query fails."""
        from update_stac_storage_tier import update_item_storage_tiers

        mock_get_info.return_value = None

        updated, with_alt, with_tier, added, skipped, failed = update_item_storage_tiers(
            stac_item_before, "https://s3.endpoint.com", add_missing=False
        )

        assert updated == 1
        assert failed == 1
        s3_info = stac_item_before.assets["reflectance"].extra_fields["alternate"]["s3"]
        assert "storage:scheme" in s3_info
        assert "tier" not in s3_info["storage:scheme"]

    @patch("update_stac_storage_tier.get_s3_storage_info")
    @patch("update_stac_storage_tier.https_to_s3")
    def test_add_missing_creates_alternate_s3(
        self, mock_https_to_s3, mock_get_info, stac_item_legacy
    ):
        """Test add_missing mode creates alternate.s3 structure."""
        from update_stac_storage_tier import update_item_storage_tiers

        mock_https_to_s3.return_value = "s3://bucket/data.zarr/measurements/reflectance"
        mock_get_info.return_value = {"tier": "STANDARD", "distribution": None}

        updated, with_alt, with_tier, added, skipped, failed = update_item_storage_tiers(
            stac_item_legacy, "https://s3.de.io.cloud.ovh.net", add_missing=True
        )

        assert updated == 1
        assert added == 1
        s3_info = stac_item_legacy.assets["reflectance"].extra_fields["alternate"]["s3"]
        assert s3_info["href"] == "s3://bucket/data.zarr/measurements/reflectance"
        assert s3_info["storage:scheme"]["tier"] == "STANDARD"
        assert s3_info["storage:scheme"]["platform"] == "OVHcloud"

    @patch("update_stac_storage_tier.get_s3_storage_info")
    @patch("update_stac_storage_tier.https_to_s3")
    def test_add_missing_skips_on_s3_failure(
        self, mock_https_to_s3, mock_get_info, stac_item_legacy
    ):
        """Test add_missing mode skips asset when S3 query fails."""
        from update_stac_storage_tier import update_item_storage_tiers

        mock_https_to_s3.return_value = "s3://bucket/data.zarr/measurements/reflectance"
        mock_get_info.return_value = None

        updated, with_alt, with_tier, added, skipped, failed = update_item_storage_tiers(
            stac_item_legacy, "https://s3.endpoint.com", add_missing=True
        )

        assert updated == 0
        assert skipped == 1
        assert failed == 1
        assert "alternate" not in stac_item_legacy.assets["reflectance"].extra_fields

    @patch("update_stac_storage_tier.get_s3_storage_info")
    def test_mixed_storage_adds_distribution(self, mock_get_info, stac_item_before):
        """Test mixed storage adds distribution metadata."""
        from update_stac_storage_tier import update_item_storage_tiers

        mock_get_info.return_value = {
            "tier": "MIXED",
            "distribution": {"STANDARD": 450, "GLACIER": 608},
        }

        updated, with_alt, with_tier, added, skipped, failed = update_item_storage_tiers(
            stac_item_before, "https://s3.endpoint.com", add_missing=False
        )

        assert updated == 1
        s3_info = stac_item_before.assets["reflectance"].extra_fields["alternate"]["s3"]
        assert s3_info["storage:scheme"]["tier"] == "MIXED"
        assert s3_info["ovh:storage_tier_distribution"] == {"STANDARD": 450, "GLACIER": 608}

    @patch("update_stac_storage_tier.get_s3_storage_info")
    def test_uniform_zarr_adds_distribution(self, mock_get_info, stac_item_before):
        """Test uniform Zarr includes distribution."""
        from update_stac_storage_tier import update_item_storage_tiers

        mock_get_info.return_value = {"tier": "GLACIER", "distribution": {"GLACIER": 100}}

        updated, with_alt, with_tier, added, skipped, failed = update_item_storage_tiers(
            stac_item_before, "https://s3.endpoint.com", add_missing=False
        )

        assert updated == 1
        s3_info = stac_item_before.assets["reflectance"].extra_fields["alternate"]["s3"]
        assert s3_info["storage:scheme"]["tier"] == "GLACIER"
        assert s3_info["ovh:storage_tier_distribution"] == {"GLACIER": 100}

    @patch("update_stac_storage_tier.get_s3_storage_info")
    def test_single_file_no_distribution(self, mock_get_info, stac_item_before):
        """Test single file doesn't add distribution."""
        from update_stac_storage_tier import update_item_storage_tiers

        mock_get_info.return_value = {"tier": "GLACIER", "distribution": None}

        updated, with_alt, with_tier, added, skipped, failed = update_item_storage_tiers(
            stac_item_before, "https://s3.endpoint.com", add_missing=False
        )

        assert updated == 1
        s3_info = stac_item_before.assets["reflectance"].extra_fields["alternate"]["s3"]
        assert s3_info["storage:scheme"]["tier"] == "GLACIER"
        assert "ovh:storage_tier_distribution" not in s3_info

    @patch("update_stac_storage_tier.get_s3_storage_info")
    def test_removes_distribution_when_becomes_single_file(self, mock_get_info, stac_item_before):
        """Test distribution removal when Zarr becomes single file."""
        from update_stac_storage_tier import update_item_storage_tiers

        # Add existing distribution
        s3_info = stac_item_before.assets["reflectance"].extra_fields["alternate"]["s3"]
        if "storage:scheme" not in s3_info:
            s3_info["storage:scheme"] = {}
        s3_info["storage:scheme"]["tier"] = "MIXED"
        s3_info["ovh:storage_tier_distribution"] = {"STANDARD": 450, "GLACIER": 608}

        mock_get_info.return_value = {"tier": "GLACIER", "distribution": None}

        updated, with_alt, with_tier, added, skipped, failed = update_item_storage_tiers(
            stac_item_before, "https://s3.endpoint.com", add_missing=False
        )

        assert updated == 1
        s3_info = stac_item_before.assets["reflectance"].extra_fields["alternate"]["s3"]
        assert s3_info["storage:scheme"]["tier"] == "GLACIER"
        assert "ovh:storage_tier_distribution" not in s3_info

    @patch("update_stac_storage_tier.get_s3_storage_info")
    def test_skips_thumbnail_assets(self, mock_get_info, stac_item_before):
        """Test thumbnail assets are skipped."""
        from update_stac_storage_tier import update_item_storage_tiers

        update_item_storage_tiers(stac_item_before, "https://s3.endpoint.com", add_missing=False)

        # Should only be called once (for reflectance, not thumbnail)
        assert mock_get_info.call_count == 1


class TestUpdateStacItem:
    """Tests for update_stac_item function."""

    @patch("update_stac_storage_tier.httpx.Client")
    @patch("update_stac_storage_tier.update_item_storage_tiers")
    def test_dry_run_skips_stac_update(self, mock_update_tiers, mock_httpx, stac_item_before):
        """Test dry run doesn't update STAC API."""
        from update_stac_storage_tier import update_stac_item

        mock_response = Mock()
        mock_response.json.return_value = stac_item_before.to_dict()
        mock_http_client = Mock()
        mock_http_client.get.return_value = mock_response
        mock_http_client.__enter__ = Mock(return_value=mock_http_client)
        mock_http_client.__exit__ = Mock(return_value=False)
        mock_httpx.return_value = mock_http_client

        mock_update_tiers.return_value = (1, 1, 1, 0, 0, 0)

        result = update_stac_item(
            "https://stac.api.com/collections/test/items/test-item",
            "https://stac.api.com",
            "https://s3.endpoint.com",
            dry_run=True,
        )

        assert result["updated"] == 1

    @patch("update_stac_storage_tier.httpx.Client")
    @patch("update_stac_storage_tier.update_item_storage_tiers")
    @patch("update_stac_storage_tier.Client")
    def test_updates_stac_when_changes_made(
        self, mock_client_class, mock_update_tiers, mock_httpx, stac_item_before
    ):
        """Test STAC API is updated when changes are made."""
        from update_stac_storage_tier import update_stac_item

        mock_response = Mock()
        mock_response.json.return_value = stac_item_before.to_dict()
        mock_http_client = Mock()
        mock_http_client.get.return_value = mock_response
        mock_http_client.__enter__ = Mock(return_value=mock_http_client)
        mock_http_client.__exit__ = Mock(return_value=False)
        mock_httpx.return_value = mock_http_client

        mock_update_tiers.return_value = (1, 1, 1, 0, 0, 0)

        mock_stac_client = Mock()
        mock_stac_client.self_href = "https://stac.api.com"
        mock_session = Mock()
        mock_post_response = Mock()
        mock_post_response.status_code = 201
        mock_session.post.return_value = mock_post_response
        mock_session.delete.return_value = Mock()
        mock_stac_client._stac_io.session = mock_session
        mock_client_class.open.return_value = mock_stac_client

        result = update_stac_item(
            "https://stac.api.com/collections/test/items/test-item",
            "https://stac.api.com",
            "https://s3.endpoint.com",
            dry_run=False,
        )

        assert result["updated"] == 1
        mock_session.delete.assert_called_once()
        mock_session.post.assert_called_once()

    @patch("update_stac_storage_tier.httpx.Client")
    @patch("update_stac_storage_tier.update_item_storage_tiers")
    def test_no_stac_update_when_no_changes(self, mock_update_tiers, mock_httpx, stac_item_before):
        """Test STAC API not updated when no changes."""
        from update_stac_storage_tier import update_stac_item

        mock_response = Mock()
        mock_response.json.return_value = stac_item_before.to_dict()
        mock_http_client = Mock()
        mock_http_client.get.return_value = mock_response
        mock_http_client.__enter__ = Mock(return_value=mock_http_client)
        mock_http_client.__exit__ = Mock(return_value=False)
        mock_httpx.return_value = mock_http_client

        mock_update_tiers.return_value = (0, 1, 1, 0, 0, 0)

        result = update_stac_item(
            "https://stac.api.com/collections/test/items/test-item",
            "https://stac.api.com",
            "https://s3.endpoint.com",
            dry_run=False,
        )

        assert result["updated"] == 0


class TestMain:
    """Tests for main function."""

    @patch("update_stac_storage_tier.update_stac_item")
    def test_main_success(self, mock_update):
        """Test main function success."""
        from update_stac_storage_tier import main

        mock_update.return_value = {"updated": 1, "with_tier": 1, "added": 0}

        exit_code = main(
            [
                "--stac-item-url",
                "https://stac.api.com/collections/col/items/item",
                "--stac-api-url",
                "https://stac.api.com",
                "--s3-endpoint",
                "https://s3.endpoint.com",
            ]
        )

        assert exit_code == 0

    @patch("update_stac_storage_tier.update_stac_item")
    def test_main_with_flags(self, mock_update):
        """Test main with --add-missing and --dry-run flags."""
        from update_stac_storage_tier import main

        mock_update.return_value = {"updated": 1, "with_tier": 1, "added": 1}

        exit_code = main(
            [
                "--stac-item-url",
                "https://stac.api.com/collections/col/items/item",
                "--stac-api-url",
                "https://stac.api.com",
                "--s3-endpoint",
                "https://s3.endpoint.com",
                "--add-missing",
                "--dry-run",
            ]
        )

        assert exit_code == 0
        call_args = mock_update.call_args[0]
        assert call_args[3] is True  # dry_run
        assert call_args[4] is True  # add_missing

    @patch("update_stac_storage_tier.update_stac_item")
    def test_main_handles_exception(self, mock_update):
        """Test main handles exceptions."""
        from update_stac_storage_tier import main

        mock_update.side_effect = Exception("Test error")

        exit_code = main(
            [
                "--stac-item-url",
                "https://stac.api.com/collections/col/items/item",
                "--stac-api-url",
                "https://stac.api.com",
                "--s3-endpoint",
                "https://s3.endpoint.com",
            ]
        )

        assert exit_code == 1
