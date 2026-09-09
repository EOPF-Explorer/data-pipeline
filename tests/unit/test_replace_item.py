"""Unit tests for the shared _replace_item helper (issue #352).

Item updates must be a single idempotent PUT — never DELETE-then-POST, which
leaves a window where the item exists nowhere. Covers the helper directly and
the CLI call sites that route through it. The helper lives in manage_item and
manage_collections imports it; there is no second copy.
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
# manage_collections re-exports the SAME helper — there is no second copy
# ---------------------------------------------------------------------------
class TestManageCollectionsReusesTheHelper:
    def test_is_the_same_object_as_manage_item(self):
        """The helper is defined once, in manage_item, and imported here.

        Asserting identity rather than re-testing the behaviour is the point: a
        future re-introduction of a file-local copy would drift silently from
        the original, which is exactly what this consolidation removed.
        """
        assert manage_collections_module._replace_item is manage_item_module._replace_item


# ---------------------------------------------------------------------------
# manage_collections.STACCollectionManager.sync_storage_tiers per-item loop
# ---------------------------------------------------------------------------
class TestCollectionSyncStorageTiersLoop:
    """The per-item write loop had no unit coverage before #352."""

    def _run_sync(self, item_dicts: list[dict], put_side_effect, dry_run: bool = False):
        manager = manage_collections_module.STACCollectionManager(API_URL)
        with (
            patch.object(manager, "get_collection_items", return_value=item_dicts),
            patch(
                "update_stac_storage_tier.update_item_storage_tiers",
                return_value=TIERS_UPDATED,
            ),
            patch("requests.Session.put", side_effect=put_side_effect) as mock_put,
            patch("requests.Session.delete") as mock_delete,
            patch("requests.Session.post") as mock_post,
        ):
            stats = manager.sync_storage_tiers(COLLECTION_ID, S3_ENDPOINT, dry_run=dry_run)
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

    def test_failed_write_is_not_reported_as_correction(self):
        """An item whose PUT failed must not appear in the corrections summary."""
        items = [self._item_dict("item-000"), self._item_dict("item-001")]

        def _put(url, *args, **kwargs):
            return _make_response(500 if "item-000" in url else 200)

        stats, _, _, _ = self._run_sync(items, _put)

        corrected_ids = [c["item_id"] for c in stats["corrections"]]
        assert corrected_ids == ["item-001"]

    def test_dry_run_still_reports_would_be_corrections(self):
        """Dry-run performs no writes but must still list the would-be corrections."""
        items = [self._item_dict("item-000")]

        stats, mock_put, _, _ = self._run_sync(items, None, dry_run=True)

        mock_put.assert_not_called()
        assert stats["items_updated"] == 1
        assert [c["item_id"] for c in stats["corrections"]] == ["item-000"]


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


# ---------------------------------------------------------------------------
# #408: the #374 write-back raster-link guard, applied at the shared helper so
# all four operator-tools read-modify-write sites are covered at once.
# ---------------------------------------------------------------------------
RASTER_API = "https://api.example.com/raster"
CORRUPT_RASTER = "https://api.example.com/stac/raster"  # the #343 rewrite shape
GUARD_OFF_WARNING = "write-back link guard NOT active"

collection_cli = manage_collections_module.cli


def _group_args(raster_api_url: str | None) -> list[str]:
    """CLI group args, with --raster-api-url only when the guard is meant to be on."""
    args = ["--api-url", API_URL]
    return args + ["--raster-api-url", raster_api_url] if raster_api_url else args


def _item_with_xyz(base: str, item_id: str = ITEM_ID) -> dict:
    """FAKE_ITEM_DICT carrying one xyz link — a rel the guard judges by construction."""
    return {
        **FAKE_ITEM_DICT,
        "id": item_id,
        "links": [
            {
                "rel": "xyz",
                "href": f"{base}/collections/{COLLECTION_ID}/items/{item_id}/tiles/{{z}}/{{x}}/{{y}}",
            }
        ],
    }


class TestReplaceItemRasterGuard:
    def _replace(self, base: str, raster_api_url: str | None) -> MagicMock:
        session = MagicMock(spec=requests.Session)
        session.put.return_value = _make_response(200)
        item = _make_item()
        item.to_dict.return_value = _item_with_xyz(base)
        manage_item_module._replace_item(
            session, API_URL, COLLECTION_ID, item, raster_api_url=raster_api_url
        )
        return session

    def test_corrupted_item_is_refused_before_put(self):
        from update_stac_storage_tier import RasterLinkMismatchError

        session = MagicMock(spec=requests.Session)
        item = _make_item()
        item.to_dict.return_value = _item_with_xyz(CORRUPT_RASTER)
        with pytest.raises(RasterLinkMismatchError):
            manage_item_module._replace_item(
                session, API_URL, COLLECTION_ID, item, raster_api_url=RASTER_API
            )
        session.put.assert_not_called()

    def test_clean_item_still_writes_exactly_as_before(self):
        session = self._replace(RASTER_API, RASTER_API)

        session.put.assert_called_once_with(
            f"{API_URL}/collections/{COLLECTION_ID}/items/{ITEM_ID}",
            json=_item_with_xyz(RASTER_API),
            timeout=30,
        )

    def test_guard_is_inert_when_raster_api_url_unset(self):
        """Today's behaviour is preserved: no URL, no judgement, the PUT goes out."""
        with patch("update_stac_storage_tier.check_raster_links") as mock_check:
            session = self._replace(CORRUPT_RASTER, None)

        mock_check.assert_not_called()
        session.put.assert_called_once()


class TestCollectionSyncStorageTiersRasterGuard:
    """A fired guard ABORTS the bulk loop; it is not a per-item failure.

    Contrast with TestCollectionSyncStorageTiersLoop: a failing PUT is one bad
    item and the run continues, but a proxy fault corrupts every item, so
    counting-and-continuing would log thousands of failures and still exit 0.
    """

    def _run_sync(self, item_dicts: list[dict], dry_run: bool = False):
        manager = manage_collections_module.STACCollectionManager(API_URL)
        with (
            patch.object(manager, "get_collection_items", return_value=item_dicts),
            patch(
                "update_stac_storage_tier.update_item_storage_tiers",
                return_value=TIERS_UPDATED,
            ) as mock_update,
            patch("requests.Session.put", return_value=_make_response(200)) as mock_put,
        ):
            stats = manager.sync_storage_tiers(
                COLLECTION_ID, S3_ENDPOINT, dry_run=dry_run, raster_api_url=RASTER_API
            )
        return stats, mock_update, mock_put

    def test_guard_fire_aborts_run_at_first_item(self):
        """The item is judged as read, before any S3 work, and the loop stops there.

        Partial stats come back rather than an exception so the command can still
        print what was already written before the abort.
        """
        items = [_item_with_xyz(CORRUPT_RASTER, f"item-{i:03d}") for i in range(3)]

        stats, mock_update, mock_put = self._run_sync(items)

        assert stats["guard_abort"]  # names why the run stopped
        assert "item-000" in stats["guard_abort"]
        mock_update.assert_not_called()  # refused as read: no S3 work for item-000, none after
        mock_put.assert_not_called()

    def test_dry_run_surveys_every_offender_without_aborting(self):
        """Dry-run is the pre-flight: it must find every corrupt item, not stop at one."""
        items = [_item_with_xyz(CORRUPT_RASTER, f"item-{i:03d}") for i in range(3)]
        items.append(_item_with_xyz(RASTER_API, "item-clean"))

        stats, mock_update, mock_put = self._run_sync(items, dry_run=True)

        assert stats["raster_link_offenders"] == 3
        assert not stats["guard_abort"]
        assert mock_update.call_count == 1  # only the clean item was surveyed
        mock_put.assert_not_called()

    def test_clean_items_still_write(self):
        items = [_item_with_xyz(RASTER_API, f"item-{i:03d}") for i in range(2)]

        stats, _, mock_put = self._run_sync(items)

        assert stats["items_updated"] == 2
        assert stats["items_failed"] == 0
        assert stats["raster_link_offenders"] == 0
        assert not stats["guard_abort"]
        assert mock_put.call_count == 2


class TestItemSyncStorageTiersRasterGuardCli:
    """manage_item.py --raster-api-url reaches the sync-storage-tiers write."""

    def _invoke(self, item_dict: dict, raster_api_url: str | None, *extra_args: str):
        group_args = _group_args(raster_api_url)
        runner = CliRunner()
        with (
            patch("manage_item.STACItemManager.get_item", return_value=item_dict),
            patch(
                "update_stac_storage_tier.update_item_storage_tiers",
                return_value=TIERS_UPDATED,
            ) as mock_update,
            patch("requests.Session.put", return_value=_make_response(200)) as mock_put,
        ):
            result = runner.invoke(item_cli, group_args + SYNC_ARGS[2:] + list(extra_args))
        return result, mock_put, mock_update

    def test_corrupted_item_refused_as_read_before_any_s3_work(self):
        result, mock_put, mock_update = self._invoke(_item_with_xyz(CORRUPT_RASTER), RASTER_API)

        assert result.exit_code != 0
        assert "(as read)" in result.output
        mock_update.assert_not_called()
        mock_put.assert_not_called()

    def test_dry_run_still_exercises_the_guard(self):
        """A dry run during a proxy fault must fail, not report 'would update' and exit 0."""
        result, mock_put, _ = self._invoke(_item_with_xyz(CORRUPT_RASTER), RASTER_API, "--dry-run")

        assert result.exit_code != 0
        assert "(as read)" in result.output
        mock_put.assert_not_called()

    def test_clean_item_written(self):
        result, mock_put, _ = self._invoke(_item_with_xyz(RASTER_API), RASTER_API)

        assert result.exit_code == 0
        mock_put.assert_called_once()
        assert GUARD_OFF_WARNING not in result.output

    def test_warns_once_when_raster_api_url_unset(self):
        """A skipped guard must never be mistaken for a passing one."""
        result, mock_put, _ = self._invoke(_item_with_xyz(CORRUPT_RASTER), None)

        assert result.exit_code == 0
        mock_put.assert_called_once()
        assert result.output.count(GUARD_OFF_WARNING) == 1


class TestCollectionSyncStorageTiersRasterGuardCli:
    """manage_collections.py --raster-api-url reaches the bulk sync loop."""

    def _invoke(self, item_dicts: list[dict], raster_api_url: str | None, *extra_args: str):
        group_args = _group_args(raster_api_url)
        runner = CliRunner()
        with (
            patch(
                "manage_collections.STACCollectionManager.get_collection_items",
                return_value=item_dicts,
            ),
            patch(
                "update_stac_storage_tier.update_item_storage_tiers",
                return_value=TIERS_UPDATED,
            ),
            patch("requests.Session.put", return_value=_make_response(200)) as mock_put,
        ):
            result = runner.invoke(
                collection_cli,
                group_args
                + ["sync-storage-tiers", COLLECTION_ID, "--s3-endpoint", S3_ENDPOINT, "-y"]
                + list(extra_args),
            )
        return result, mock_put

    def test_guard_fire_aborts_with_reason_and_nonzero_exit(self):
        """The run stops, but the operator still gets the summary of what was written."""
        items = [_item_with_xyz(CORRUPT_RASTER, f"item-{i:03d}") for i in range(3)]

        result, mock_put = self._invoke(items, RASTER_API)

        assert result.exit_code != 0
        assert "Aborting" in result.output
        assert "SYNC SUMMARY" in result.output
        mock_put.assert_not_called()

    def test_dry_run_reports_offender_count_and_exits_nonzero(self):
        items = [_item_with_xyz(CORRUPT_RASTER, f"item-{i:03d}") for i in range(3)]

        result, mock_put = self._invoke(items, RASTER_API, "--dry-run")

        assert result.exit_code != 0
        assert "SYNC SUMMARY" in result.output
        assert "3 item(s) with corrupted raster links" in result.output
        mock_put.assert_not_called()

    def test_warns_once_when_raster_api_url_unset(self):
        items = [_item_with_xyz(CORRUPT_RASTER, f"item-{i:03d}") for i in range(3)]

        result, mock_put = self._invoke(items, None)

        assert result.exit_code == 0
        assert mock_put.call_count == 3
        assert result.output.count(GUARD_OFF_WARNING) == 1
