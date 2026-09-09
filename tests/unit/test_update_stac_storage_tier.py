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
def stac_item_legacy_storage_scheme():
    """Legacy STAC item with alternate.s3 in old format (storage:scheme, tier, tier_distribution)."""
    with open(FIXTURES_DIR / "stac_item_legacy_storage_scheme.json") as f:
        return Item.from_dict(json.load(f))


@pytest.fixture
def stac_item_legacy_storage_scheme_expected():
    """Expected STAC item after migrating legacy storage:scheme to new format."""
    with open(FIXTURES_DIR / "stac_item_legacy_storage_scheme_after_update.json") as f:
        return Item.from_dict(json.load(f))


@pytest.fixture
def s3_responses():
    """Mock S3 storage tier responses."""
    with open(FIXTURES_DIR / "s3_storage_responses.json") as f:
        return json.load(f)


class TestUpdateItemStorageTiers:
    """Tests for update_item_storage_tiers function.

    Patches are applied where the script uses the functions (update_stac_storage_tier
    module): get_s3_storage_info (from storage_tier_utils), https_to_s3 (from register_v1).
    """

    @patch("update_stac_storage_tier.get_s3_storage_info")
    def test_tier_change_updates_asset(self, mock_get_info, stac_item_before):
        """Test updating storage tier when it changes."""
        from update_stac_storage_tier import update_item_storage_tiers

        mock_get_info.return_value = {"tier": "STANDARD_IA", "distribution": None}

        updated, with_alt, with_tier, added, skipped, failed = update_item_storage_tiers(
            stac_item_before, "https://s3.endpoint.com", add_missing=False
        )

        assert updated == 1
        assert with_alt == 1
        assert with_tier == 1
        s3_info = stac_item_before.assets["reflectance"].extra_fields["alternate"]["s3"]
        assert s3_info["storage:refs"] == ["glacier"]

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
        """Test ref set to standard and legacy storage:scheme removed when S3 query fails."""
        from update_stac_storage_tier import update_item_storage_tiers

        mock_get_info.return_value = None
        # Simulate legacy item with storage:scheme (script will remove it)
        s3_info = stac_item_before.assets["reflectance"].extra_fields["alternate"]["s3"]
        s3_info["storage:scheme"] = {"tier": "STANDARD", "platform": "OVHcloud"}

        updated, with_alt, with_tier, added, skipped, failed = update_item_storage_tiers(
            stac_item_before, "https://s3.endpoint.com", add_missing=False
        )

        assert updated == 1
        assert failed == 1
        s3_info = stac_item_before.assets["reflectance"].extra_fields["alternate"]["s3"]
        assert s3_info["storage:refs"] == ["standard"]
        assert "storage:scheme" not in s3_info

    @patch("update_stac_storage_tier.get_s3_storage_info")
    def test_legacy_storage_scheme_migrates_to_new_format(
        self,
        mock_get_info,
        stac_item_legacy_storage_scheme,
        stac_item_legacy_storage_scheme_expected,
    ):
        """Test legacy item with storage:scheme (tier, tier_distribution) migrates to new format."""
        from update_stac_storage_tier import update_item_storage_tiers

        # Match legacy fixture: tier STANDARD, uniform Zarr with distribution
        mock_get_info.return_value = {
            "tier": "STANDARD",
            "distribution": {"STANDARD": 450},
        }

        updated, with_alt, with_tier, added, skipped, failed = update_item_storage_tiers(
            stac_item_legacy_storage_scheme, "https://s3.de.io.cloud.ovh.net", add_missing=False
        )

        assert updated == 1
        assert with_alt == 1
        assert with_tier == 1
        assert failed == 0
        # Item-level: new format has storage:schemes
        assert "storage:schemes" in stac_item_legacy_storage_scheme.properties
        expected_props = stac_item_legacy_storage_scheme_expected.properties
        assert (
            stac_item_legacy_storage_scheme.properties["storage:schemes"]
            == expected_props["storage:schemes"]
        )
        # Asset-level: storage:refs, objects_per_storage_class, no legacy storage:scheme
        s3_info = stac_item_legacy_storage_scheme.assets["reflectance"].extra_fields["alternate"][
            "s3"
        ]
        expected_s3 = stac_item_legacy_storage_scheme_expected.assets["reflectance"].extra_fields[
            "alternate"
        ]["s3"]
        assert s3_info["storage:refs"] == expected_s3["storage:refs"]
        assert s3_info["objects_per_storage_class"] == expected_s3["objects_per_storage_class"]
        assert "storage:scheme" not in s3_info
        assert s3_info["href"] == expected_s3["href"]

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
        assert s3_info["storage:refs"] == ["standard"]
        assert "storage:schemes" in stac_item_legacy.properties

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
        """Test mixed storage adds objects_per_storage_class and mixed ref."""
        from update_stac_storage_tier import update_item_storage_tiers

        # Per storage_tier_utils.get_s3_storage_info: tier must be MIXED when distribution has multiple storage classes
        mock_get_info.return_value = {
            "tier": "MIXED",
            "distribution": {"STANDARD": 450, "STANDARD_IA": 608},
        }

        updated, with_alt, with_tier, added, skipped, failed = update_item_storage_tiers(
            stac_item_before, "https://s3.endpoint.com", add_missing=False
        )

        assert updated == 1
        s3_info = stac_item_before.assets["reflectance"].extra_fields["alternate"]["s3"]
        assert s3_info["storage:refs"] == ["mixed"]
        assert s3_info["objects_per_storage_class"] == {"STANDARD": 450, "STANDARD_IA": 608}

    @patch("update_stac_storage_tier.get_s3_storage_info")
    def test_uniform_zarr_adds_distribution(self, mock_get_info, stac_item_before):
        """Test uniform Zarr includes objects_per_storage_class."""
        from update_stac_storage_tier import update_item_storage_tiers

        # Per get_s3_storage_info: uniform Zarr has one class in distribution, tier is that class
        mock_get_info.return_value = {"tier": "STANDARD_IA", "distribution": {"STANDARD_IA": 100}}

        updated, with_alt, with_tier, added, skipped, failed = update_item_storage_tiers(
            stac_item_before, "https://s3.endpoint.com", add_missing=False
        )

        assert updated == 1
        s3_info = stac_item_before.assets["reflectance"].extra_fields["alternate"]["s3"]
        assert s3_info["storage:refs"] == ["glacier"]
        assert s3_info["objects_per_storage_class"] == {"STANDARD_IA": 100}

    @patch("update_stac_storage_tier.get_s3_storage_info")
    def test_single_file_no_distribution(self, mock_get_info, stac_item_before):
        """Test single file doesn't add objects_per_storage_class."""
        from update_stac_storage_tier import update_item_storage_tiers

        mock_get_info.return_value = {"tier": "STANDARD_IA", "distribution": None}

        updated, with_alt, with_tier, added, skipped, failed = update_item_storage_tiers(
            stac_item_before, "https://s3.endpoint.com", add_missing=False
        )

        assert updated == 1
        s3_info = stac_item_before.assets["reflectance"].extra_fields["alternate"]["s3"]
        assert s3_info["storage:refs"] == ["glacier"]
        assert "objects_per_storage_class" not in s3_info

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

    def _make_stac_client(self) -> tuple[Mock, Mock]:
        mock_stac_client = Mock()
        mock_stac_client.self_href = "https://stac.api.com"
        mock_session = Mock()
        mock_session.put.return_value = Mock(status_code=200)
        mock_session.post.return_value = Mock(status_code=201)
        mock_stac_client._stac_io.session = mock_session
        return mock_stac_client, mock_session

    def _mock_httpx(self, mock_httpx, stac_item_before) -> None:
        mock_response = Mock()
        mock_response.json.return_value = stac_item_before.to_dict()
        mock_http_client = Mock()
        mock_http_client.get.return_value = mock_response
        mock_http_client.__enter__ = Mock(return_value=mock_http_client)
        mock_http_client.__exit__ = Mock(return_value=False)
        mock_httpx.return_value = mock_http_client

    @patch("update_stac_storage_tier.httpx.Client")
    @patch("update_stac_storage_tier.update_item_storage_tiers")
    @patch("update_stac_storage_tier.stac_auth.open_client")
    def test_posts_when_item_new(
        self, mock_open_client, mock_update_tiers, mock_httpx, stac_item_before
    ):
        """First POST succeeds (item did not exist) → single POST; no PUT, no DELETE."""
        from update_stac_storage_tier import update_stac_item

        self._mock_httpx(mock_httpx, stac_item_before)
        mock_update_tiers.return_value = (1, 1, 1, 0, 0, 0)
        mock_stac_client, mock_session = self._make_stac_client()
        mock_open_client.return_value = mock_stac_client

        result = update_stac_item(
            "https://stac.api.com/collections/test/items/test-item",
            "https://stac.api.com",
            "https://s3.endpoint.com",
            dry_run=False,
        )

        assert result["updated"] == 1
        mock_session.post.assert_called_once()
        post_call = mock_session.post.call_args
        assert post_call.args[0] == "https://stac.api.com/collections/test/items"
        mock_session.delete.assert_not_called()
        mock_session.put.assert_not_called()

    @patch("update_stac_storage_tier.httpx.Client")
    @patch("update_stac_storage_tier.update_item_storage_tiers")
    @patch("update_stac_storage_tier.stac_auth.open_client")
    def test_put_replace_on_409(
        self, mock_open_client, mock_update_tiers, mock_httpx, stac_item_before
    ):
        """POST 409 (item exists) → single atomic PUT to the item URL; no DELETE."""
        from update_stac_storage_tier import update_stac_item

        self._mock_httpx(mock_httpx, stac_item_before)
        mock_update_tiers.return_value = (1, 1, 1, 0, 0, 0)
        mock_stac_client, mock_session = self._make_stac_client()
        mock_session.post.return_value = Mock(status_code=409)
        mock_open_client.return_value = mock_stac_client

        result = update_stac_item(
            "https://stac.api.com/collections/test/items/test-item",
            "https://stac.api.com",
            "https://s3.endpoint.com",
            dry_run=False,
        )

        assert result["updated"] == 1
        mock_session.post.assert_called_once()
        mock_session.put.assert_called_once()
        put_call = mock_session.put.call_args
        assert put_call.args[0] == "https://stac.api.com/collections/test/items/test-item"
        mock_session.delete.assert_not_called()

    @patch("update_stac_storage_tier.httpx.Client")
    @patch("update_stac_storage_tier.update_item_storage_tiers")
    @patch("update_stac_storage_tier.stac_auth.open_client")
    def test_put_failure_raises(
        self, mock_open_client, mock_update_tiers, mock_httpx, stac_item_before
    ):
        """A failed PUT must raise — never swallowed like the old DELETE was."""
        import requests
        from update_stac_storage_tier import update_stac_item

        self._mock_httpx(mock_httpx, stac_item_before)
        mock_update_tiers.return_value = (1, 1, 1, 0, 0, 0)
        mock_stac_client, mock_session = self._make_stac_client()
        mock_session.post.return_value = Mock(status_code=409)
        mock_session.put.return_value.raise_for_status.side_effect = requests.HTTPError("500 Error")
        mock_open_client.return_value = mock_stac_client

        with pytest.raises(requests.HTTPError):
            update_stac_item(
                "https://stac.api.com/collections/test/items/test-item",
                "https://stac.api.com",
                "https://s3.endpoint.com",
                dry_run=False,
            )

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


# --- #374: raster write-back guard --------------------------------------------

RASTER_API = "https://api.explorer.eopf.copernicus.eu/raster"
# What the stac-auth-proxy link-rewrite bug (platform-deploy#343) served back.
CORRUPT_RASTER = "https://api.explorer.eopf.copernicus.eu/stac/raster"


def raster_item(base: str = RASTER_API, thumbnail_href: str | None = None) -> dict:
    """Bare STAC item dict carrying the three raster rels, plus a thumbnail asset."""
    return {
        "id": "item-1",
        "links": [
            {"rel": "self", "href": "https://api.explorer.eopf.copernicus.eu/stac/x"},
            {"rel": "viewer", "href": f"{base}/collections/c/items/item-1/viewer"},
            {
                "rel": "xyz",
                "href": f"{base}/collections/c/items/item-1/tiles/{{z}}/{{x}}/{{y}}.png",
            },
            {"rel": "tilejson", "href": f"{base}/collections/c/items/item-1/tilejson.json"},
        ],
        "assets": {
            "thumbnail": {
                "href": thumbnail_href or f"{base}/collections/c/items/item-1/preview.png",
                "roles": ["thumbnail"],
            }
        },
    }


def real_item_with_raster_links(item: Item, base: str = RASTER_API) -> dict:
    """A pystac-valid fixture item, given the raster links a registered item carries."""
    doc = item.to_dict()
    template = raster_item(base)
    doc["links"] = template["links"]
    doc["assets"]["thumbnail"] = template["assets"]["thumbnail"]
    return doc


class TestCheckRasterLinks:
    """Tests for the #374 write-back guard itself."""

    def test_clean_item_passes(self):
        from update_stac_storage_tier import check_raster_links

        check_raster_links(raster_item(), RASTER_API, "test")

    def test_trailing_slash_on_expected_url_is_tolerated(self):
        from update_stac_storage_tier import check_raster_links

        check_raster_links(raster_item(), RASTER_API + "/", "test")

    def test_corrupted_links_raise_and_name_every_offender(self, caplog):
        from update_stac_storage_tier import RasterLinkMismatchError, check_raster_links

        with pytest.raises(RasterLinkMismatchError):
            check_raster_links(raster_item(base=CORRUPT_RASTER), RASTER_API, "test")

        # V-C3 greps these lines to tell a fired guard from an argparse error.
        for rel in ("viewer", "xyz", "tilejson", "asset:thumbnail"):
            assert rel in caplog.text
        assert "/stac/raster/" in caplog.text

    def test_item_without_raster_links_passes(self, caplog):
        """S1 / S3-OLCI items have no xyz/tilejson/viewer. Must not be refused."""
        from update_stac_storage_tier import check_raster_links

        check_raster_links({"id": "s1-item", "links": [], "assets": {}}, RASTER_API, "test")
        check_raster_links({"id": "s1-item"}, RASTER_API, "test")
        assert caplog.text == ""

    def test_thumbnail_on_another_host_is_not_judged(self):
        """A thumbnail served from S3 or elsewhere is legitimate, not corruption."""
        from update_stac_storage_tier import check_raster_links

        item = raster_item(thumbnail_href="https://s3.example.com/bucket/thumb.png")
        check_raster_links(item, RASTER_API, "test")

    def test_corrupted_thumbnail_alone_raises(self):
        from update_stac_storage_tier import RasterLinkMismatchError, check_raster_links

        item = raster_item(thumbnail_href=f"{CORRUPT_RASTER}/collections/c/items/i/preview.png")
        with pytest.raises(RasterLinkMismatchError):
            check_raster_links(item, RASTER_API, "test")


class TestRasterGuardWiring:
    """The guard, as reached through update_stac_item."""

    def _mock_httpx_with(self, mock_httpx, item_dict) -> None:
        mock_response = Mock()
        mock_response.json.return_value = item_dict
        mock_http_client = Mock()
        mock_http_client.get.return_value = mock_response
        mock_http_client.__enter__ = Mock(return_value=mock_http_client)
        mock_http_client.__exit__ = Mock(return_value=False)
        mock_httpx.return_value = mock_http_client

    def _stac_client(self) -> tuple[Mock, Mock]:
        mock_stac_client = Mock()
        mock_stac_client.self_href = "https://stac.api.com"
        mock_session = Mock()
        mock_session.post.return_value = Mock(status_code=409)
        mock_session.put.return_value = Mock(status_code=200)
        mock_stac_client._stac_io.session = mock_session
        return mock_stac_client, mock_session

    @patch("update_stac_storage_tier.httpx.Client")
    @patch("update_stac_storage_tier.update_item_storage_tiers")
    @patch("update_stac_storage_tier.stac_auth.open_client")
    def test_corrupted_item_is_never_written(
        self, mock_open_client, mock_update_tiers, mock_httpx, stac_item_before
    ):
        """The point of #374: proxy-corrupted links must not round-trip into pgstac."""
        from update_stac_storage_tier import RasterLinkMismatchError, update_stac_item

        self._mock_httpx_with(
            mock_httpx, real_item_with_raster_links(stac_item_before, CORRUPT_RASTER)
        )
        mock_update_tiers.return_value = (1, 1, 1, 0, 0, 0)
        mock_stac_client, mock_session = self._stac_client()
        mock_open_client.return_value = mock_stac_client

        with pytest.raises(RasterLinkMismatchError):
            update_stac_item(
                "https://stac.api.com/collections/c/items/item-1",
                "https://stac.api.com",
                "https://s3.endpoint.com",
                dry_run=False,
                raster_api_url=RASTER_API,
            )

        mock_session.put.assert_not_called()
        mock_session.post.assert_not_called()

    @patch("update_stac_storage_tier.httpx.Client")
    @patch("update_stac_storage_tier.update_item_storage_tiers")
    @patch("update_stac_storage_tier.stac_auth.open_client")
    def test_clean_item_still_writes(
        self, mock_open_client, mock_update_tiers, mock_httpx, stac_item_before
    ):
        from update_stac_storage_tier import update_stac_item

        self._mock_httpx_with(mock_httpx, real_item_with_raster_links(stac_item_before))
        mock_update_tiers.return_value = (1, 1, 1, 0, 0, 0)
        mock_stac_client, mock_session = self._stac_client()
        mock_open_client.return_value = mock_stac_client

        result = update_stac_item(
            "https://stac.api.com/collections/c/items/item-1",
            "https://stac.api.com",
            "https://s3.endpoint.com",
            dry_run=False,
            raster_api_url=RASTER_API,
        )

        assert result["updated"] == 1
        mock_session.put.assert_called_once()

    @patch("update_stac_storage_tier.httpx.Client")
    @patch("update_stac_storage_tier.update_item_storage_tiers")
    @patch("update_stac_storage_tier.stac_auth.open_client")
    def test_guard_off_writes_corrupted_item_but_warns(
        self, mock_open_client, mock_update_tiers, mock_httpx, caplog, stac_item_before
    ):
        """Unset --raster-api-url keeps today's behaviour - but never silently."""
        from update_stac_storage_tier import update_stac_item

        self._mock_httpx_with(
            mock_httpx, real_item_with_raster_links(stac_item_before, CORRUPT_RASTER)
        )
        mock_update_tiers.return_value = (1, 1, 1, 0, 0, 0)
        mock_stac_client, mock_session = self._stac_client()
        mock_open_client.return_value = mock_stac_client

        update_stac_item(
            "https://stac.api.com/collections/c/items/item-1",
            "https://stac.api.com",
            "https://s3.endpoint.com",
            dry_run=False,
        )

        mock_session.put.assert_called_once()
        assert "guard NOT active" in caplog.text

    @patch("update_stac_storage_tier.update_stac_item")
    def test_main_forwards_the_flag(self, mock_update):
        from update_stac_storage_tier import main

        mock_update.return_value = {"updated": 0, "with_tier": 0, "added": 0}

        assert (
            main(
                [
                    "--stac-item-url",
                    "https://stac.api.com/collections/c/items/item-1",
                    "--stac-api-url",
                    "https://stac.api.com",
                    "--s3-endpoint",
                    "https://s3.endpoint.com",
                    "--raster-api-url",
                    RASTER_API,
                ]
            )
            == 0
        )
        assert mock_update.call_args.kwargs["raster_api_url"] == RASTER_API

    @patch("update_stac_storage_tier.httpx.Client")
    @patch("update_stac_storage_tier.update_item_storage_tiers")
    @patch("update_stac_storage_tier.stac_auth.open_client")
    def test_a_fired_guard_exits_1_not_2(
        self, mock_open_client, mock_update_tiers, mock_httpx, stac_item_before
    ):
        """V-C3 disambiguation: argparse exits 2, so a fired guard must exit 1."""
        from update_stac_storage_tier import main

        self._mock_httpx_with(
            mock_httpx, real_item_with_raster_links(stac_item_before, CORRUPT_RASTER)
        )
        mock_update_tiers.return_value = (1, 1, 1, 0, 0, 0)
        mock_stac_client, _ = self._stac_client()
        mock_open_client.return_value = mock_stac_client

        assert (
            main(
                [
                    "--stac-item-url",
                    "https://stac.api.com/collections/c/items/item-1",
                    "--stac-api-url",
                    "https://stac.api.com",
                    "--s3-endpoint",
                    "https://s3.endpoint.com",
                    "--raster-api-url",
                    RASTER_API,
                ]
            )
            == 1
        )
