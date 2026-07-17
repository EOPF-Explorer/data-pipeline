"""Unit tests for the file-local _replace_item helpers (issue #352).

Item updates must be a single idempotent PUT — never DELETE-then-POST, which
leaves a window where the item exists nowhere. Covers the helper directly and
the CLI call sites that route through it.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
import requests
from click.testing import CliRunner

# ---------------------------------------------------------------------------
# Module loading
# operator-tools uses a hyphen in the directory name so we cannot use a
# standard import statement; use importlib to load by file path instead.
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).parent.parent.parent
OPERATOR_TOOLS = REPO_ROOT / "operator-tools"
SCRIPTS_DIR = REPO_ROOT / "scripts"

for _p in (str(SCRIPTS_DIR), str(OPERATOR_TOOLS)):
    if _p not in sys.path:
        sys.path.insert(0, _p)


def _load(module_name: str, file_path: Path):
    # Reuse an already-loaded instance: replacing sys.modules[module_name] would
    # break patch() targets in other test modules that loaded the same file.
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


manage_item_module = _load("manage_item", OPERATOR_TOOLS / "manage_item.py")
manage_collections_module = _load("manage_collections", OPERATOR_TOOLS / "manage_collections.py")

item_cli = manage_item_module.cli


# ---------------------------------------------------------------------------
# Shared fixtures / helpers
# ---------------------------------------------------------------------------
API_URL = "https://api.example.com/stac"
S3_ENDPOINT = "https://s3.example.com"
COLLECTION_ID = "sentinel-2-l2a-staging"
ITEM_ID = "test-item-001"

FAKE_ITEM_DICT = {
    "type": "Feature",
    "stac_version": "1.0.0",
    "stac_extensions": [],
    "id": ITEM_ID,
    "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
    "bbox": [0.0, 0.0, 0.0, 0.0],
    "properties": {"datetime": "2024-01-15T00:00:00Z"},
    "assets": {},
    "links": [],
    "collection": COLLECTION_ID,
}

# update_item_storage_tiers 6-tuple: (updated, with_alternate_s3, with_tier,
# added, skipped, s3_failed) — updated > 0 makes the CLI write the item back.
TIERS_UPDATED = (1, 1, 1, 0, 0, 0)

SYNC_ARGS = [
    "--api-url",
    API_URL,
    "sync-storage-tiers",
    COLLECTION_ID,
    ITEM_ID,
    "--s3-endpoint",
    S3_ENDPOINT,
]


def _make_response(status_code: int) -> Mock:
    resp = Mock(spec=requests.Response)
    resp.status_code = status_code
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(
            f"{status_code} Error", response=resp
        )
    else:
        resp.raise_for_status.return_value = None
    return resp


def _make_item(item_id: str = ITEM_ID) -> MagicMock:
    item = MagicMock()
    item.id = item_id
    item.to_dict.return_value = {"id": item_id, "type": "Feature"}
    return item


# ---------------------------------------------------------------------------
# manage_item._replace_item: direct helper tests
# ---------------------------------------------------------------------------
class TestManageItemReplaceItemHelper:
    def test_issues_single_put_to_item_url(self):
        session = MagicMock(spec=requests.Session)
        session.put.return_value = _make_response(200)
        item = _make_item()

        manage_item_module._replace_item(session, API_URL, COLLECTION_ID, item)

        session.put.assert_called_once_with(
            f"{API_URL}/collections/{COLLECTION_ID}/items/{ITEM_ID}",
            json=item.to_dict(),
            timeout=30,
        )
        session.delete.assert_not_called()
        session.post.assert_not_called()

    @pytest.mark.parametrize("status_code", [403, 404, 500, 503])
    def test_raises_on_http_error(self, status_code):
        session = MagicMock(spec=requests.Session)
        session.put.return_value = _make_response(status_code)

        with pytest.raises(requests.HTTPError):
            manage_item_module._replace_item(session, API_URL, COLLECTION_ID, _make_item())


# ---------------------------------------------------------------------------
# manage_collections._replace_item: direct helper tests
# ---------------------------------------------------------------------------
class TestManageCollectionsReplaceItemHelper:
    def test_issues_single_put_to_item_url(self):
        session = MagicMock(spec=requests.Session)
        session.put.return_value = _make_response(200)
        item = _make_item()

        manage_collections_module._replace_item(session, API_URL, COLLECTION_ID, item)

        session.put.assert_called_once_with(
            f"{API_URL}/collections/{COLLECTION_ID}/items/{ITEM_ID}",
            json=item.to_dict(),
            timeout=30,
        )
        session.delete.assert_not_called()
        session.post.assert_not_called()

    @pytest.mark.parametrize("status_code", [404, 500])
    def test_raises_on_http_error(self, status_code):
        session = MagicMock(spec=requests.Session)
        session.put.return_value = _make_response(status_code)

        with pytest.raises(requests.HTTPError):
            manage_collections_module._replace_item(session, API_URL, COLLECTION_ID, _make_item())


# ---------------------------------------------------------------------------
# manage_collections.STACCollectionManager.sync_storage_tiers per-item loop
# ---------------------------------------------------------------------------
class TestCollectionSyncStorageTiersLoop:
    """The per-item write loop had no unit coverage before #352."""

    TIERS_UPDATED = (1, 1, 1, 0, 0, 0)

    def _run_sync(self, item_dicts: list[dict], put_side_effect):
        manager = manage_collections_module.STACCollectionManager(API_URL)
        with (
            patch.object(manager, "get_collection_items", return_value=item_dicts),
            patch(
                "update_stac_storage_tier.update_item_storage_tiers",
                return_value=self.TIERS_UPDATED,
            ),
            patch("requests.Session.put", side_effect=put_side_effect) as mock_put,
            patch("requests.Session.delete") as mock_delete,
            patch("requests.Session.post") as mock_post,
        ):
            stats = manager.sync_storage_tiers(COLLECTION_ID, S3_ENDPOINT)
        return stats, mock_put, mock_delete, mock_post

    @staticmethod
    def _item_dict(item_id: str) -> dict:
        return {**FAKE_ITEM_DICT, "id": item_id}

    def test_updates_use_put_only(self):
        items = [self._item_dict("item-000"), self._item_dict("item-001")]
        stats, mock_put, mock_delete, mock_post = self._run_sync(
            items, lambda *a, **k: _make_response(200)
        )

        assert stats["items_updated"] == 2
        assert stats["items_failed"] == 0
        assert mock_put.call_count == 2
        mock_delete.assert_not_called()
        mock_post.assert_not_called()

    def test_put_failure_counts_item_failed_and_continues(self):
        items = [self._item_dict("item-000"), self._item_dict("item-001")]

        def _put(url, *args, **kwargs):
            return _make_response(500 if "item-000" in url else 200)

        stats, mock_put, _, _ = self._run_sync(items, _put)

        assert stats["items_failed"] == 1
        assert stats["items_updated"] == 1
        assert mock_put.call_count == 2  # loop continues past the failure


# ---------------------------------------------------------------------------
# manage_item.py: sync-storage-tiers call site
# ---------------------------------------------------------------------------
class TestSyncStorageTiersWrite:
    def _invoke(self, put_response: Mock):
        runner = CliRunner()
        with (
            patch("manage_item.STACItemManager.get_item", return_value=FAKE_ITEM_DICT),
            patch(
                "update_stac_storage_tier.update_item_storage_tiers",
                return_value=TIERS_UPDATED,
            ),
            patch("requests.Session.put", return_value=put_response) as mock_put,
            patch("requests.Session.delete") as mock_delete,
            patch("requests.Session.post") as mock_post,
        ):
            result = runner.invoke(item_cli, SYNC_ARGS)
        return result, mock_put, mock_delete, mock_post

    def test_update_uses_put_only(self):
        result, mock_put, mock_delete, mock_post = self._invoke(_make_response(200))

        assert result.exit_code == 0
        mock_put.assert_called_once()
        assert mock_put.call_args.args[0] == (
            f"{API_URL}/collections/{COLLECTION_ID}/items/{ITEM_ID}"
        )
        mock_delete.assert_not_called()
        mock_post.assert_not_called()

    def test_write_failure_aborts(self):
        result, _, _, _ = self._invoke(_make_response(500))

        assert result.exit_code != 0
        assert "Failed to update STAC item" in result.output

    def test_ghost_item_404_aborts(self):
        """PUT on a missing id 404s — a real error, never a silent create."""
        result, _, _, mock_post = self._invoke(_make_response(404))

        assert result.exit_code != 0
        mock_post.assert_not_called()
