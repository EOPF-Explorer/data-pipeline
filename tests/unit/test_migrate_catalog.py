"""Unit tests for the migrate_catalog package."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from migrate_catalog.history import load_history, record_run, was_migration_run
from migrate_catalog.migrations.fix_url_encoding import fix_url_encoding
from migrate_catalog.migrations.fix_zarr_media_type import fix_zarr_media_type
from migrate_catalog.runner import STACMigrationRunner, compose_migrations
from migrate_catalog.types import MigrationResult

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "migrate_catalog"


@pytest.fixture
def item_with_plus_urls():
    with open(FIXTURES_DIR / "stac_item_with_plus_urls.json") as f:
        return json.load(f)


@pytest.fixture
def item_with_wrong_media_type():
    with open(FIXTURES_DIR / "stac_item_with_wrong_media_type.json") as f:
        return json.load(f)


@pytest.fixture
def item_clean():
    with open(FIXTURES_DIR / "stac_item_clean.json") as f:
        return json.load(f)


@pytest.fixture
def migration_result():
    return MigrationResult(
        migration_name="fix_zarr_media_type",
        collection_id="sentinel-2-l2a",
        started_at="2026-03-12T10:00:00+00:00",
        completed_at="2026-03-12T10:05:00+00:00",
        items_processed=100,
        items_modified=50,
        items_skipped=50,
        items_failed=0,
        dry_run=False,
        errors=[],
    )


class TestFixUrlEncoding:
    def test_replaces_plus_in_asset_query_string(self, item_with_plus_urls):
        result = fix_url_encoding(item_with_plus_urls)
        assert result is not None
        query = result["assets"]["thumbnail"]["href"].split("?")[1]
        assert "+" not in query
        assert "%20" in query

    def test_replaces_plus_in_link_href(self, item_with_plus_urls):
        result = fix_url_encoding(item_with_plus_urls)
        assert result is not None
        preview_link = next(link for link in result["links"] if link["rel"] == "preview")
        query = preview_link["href"].split("?")[1]
        assert "+" not in query
        assert "%20" in query

    def test_does_not_touch_path_segment(self, item_with_plus_urls):
        # The data asset has no query string — href must be unchanged
        result = fix_url_encoding(item_with_plus_urls)
        assert result is not None
        assert result["assets"]["data"]["href"] == item_with_plus_urls["assets"]["data"]["href"]

    def test_returns_none_when_no_plus_in_query(self, item_clean):
        assert fix_url_encoding(item_clean) is None

    def test_does_not_mutate_input(self, item_with_plus_urls):
        original_href = item_with_plus_urls["assets"]["thumbnail"]["href"]
        fix_url_encoding(item_with_plus_urls)
        assert item_with_plus_urls["assets"]["thumbnail"]["href"] == original_href

    def test_idempotent(self, item_with_plus_urls):
        result1 = fix_url_encoding(item_with_plus_urls)
        assert result1 is not None
        result2 = fix_url_encoding(result1)
        assert result2 is None


class TestFixZarrMediaType:
    def test_replaces_vnd_plus_zarr(self, item_with_wrong_media_type):
        result = fix_zarr_media_type(item_with_wrong_media_type)
        assert result is not None
        assert result["assets"]["SR_10m"]["type"] == "application/vnd.zarr"

    def test_replaces_vnd_plus_zarr_with_version_suffix(self, item_with_wrong_media_type):
        result = fix_zarr_media_type(item_with_wrong_media_type)
        assert result is not None
        assert result["assets"]["SR_20m"]["type"] == "application/vnd.zarr; version=3"

    def test_does_not_modify_non_zarr_assets(self, item_with_wrong_media_type):
        result = fix_zarr_media_type(item_with_wrong_media_type)
        assert result is not None
        assert result["assets"]["thumbnail"]["type"] == "image/png"

    def test_returns_none_when_already_correct(self, item_clean):
        assert fix_zarr_media_type(item_clean) is None

    def test_does_not_mutate_input(self, item_with_wrong_media_type):
        original_type = item_with_wrong_media_type["assets"]["SR_10m"]["type"]
        fix_zarr_media_type(item_with_wrong_media_type)
        assert item_with_wrong_media_type["assets"]["SR_10m"]["type"] == original_type

    def test_idempotent(self, item_with_wrong_media_type):
        result1 = fix_zarr_media_type(item_with_wrong_media_type)
        assert result1 is not None
        result2 = fix_zarr_media_type(result1)
        assert result2 is None


class TestSTACMigrationRunner:
    def _make_runner(self):
        runner = STACMigrationRunner("https://api.example.com/stac")
        runner._update_item = MagicMock()
        return runner

    def test_dry_run_counts_modified_without_updating(self, item_with_wrong_media_type):
        runner = self._make_runner()
        runner.get_items = MagicMock(return_value=[item_with_wrong_media_type])

        result = runner.run_migration(
            "test-col", fix_zarr_media_type, "fix_zarr_media_type", dry_run=True
        )

        assert result.items_modified == 1
        assert result.items_skipped == 0
        assert result.items_failed == 0
        assert result.dry_run is True
        runner._update_item.assert_not_called()

    def test_applies_migration_with_corrected_item(self, item_with_wrong_media_type):
        runner = self._make_runner()
        runner.get_items = MagicMock(return_value=[item_with_wrong_media_type])

        result = runner.run_migration("test-col", fix_zarr_media_type, "fix_zarr_media_type")

        assert result.items_modified == 1
        assert result.items_failed == 0
        runner._update_item.assert_called_once()
        _, _, posted_item = runner._update_item.call_args[0]
        assert posted_item["assets"]["SR_10m"]["type"] == "application/vnd.zarr"

    def test_skips_items_with_no_changes(self, item_clean):
        runner = self._make_runner()
        runner.get_items = MagicMock(return_value=[item_clean])

        result = runner.run_migration("test-col", fix_zarr_media_type, "fix_zarr_media_type")

        assert result.items_skipped == 1
        assert result.items_modified == 0
        runner._update_item.assert_not_called()

    def test_records_failure_on_update_error(self, item_with_wrong_media_type):
        runner = self._make_runner()
        runner.get_items = MagicMock(return_value=[item_with_wrong_media_type])
        runner._update_item.side_effect = Exception("API error")

        result = runner.run_migration("test-col", fix_zarr_media_type, "fix_zarr_media_type")

        assert result.items_failed == 1
        assert result.items_modified == 0
        assert len(result.errors) == 1
        assert result.errors[0]["error"] == "API error"

    def test_processes_multiple_items(self, item_with_wrong_media_type, item_clean):
        runner = self._make_runner()
        runner.get_items = MagicMock(return_value=[item_with_wrong_media_type, item_clean])

        result = runner.run_migration("test-col", fix_zarr_media_type, "fix_zarr_media_type")

        assert result.items_processed == 2
        assert result.items_modified == 1
        assert result.items_skipped == 1
        runner._update_item.assert_called_once()

    def test_clone_collection_copies_metadata_and_items(self):
        runner = STACMigrationRunner("https://api.example.com/stac")
        runner.get_items = MagicMock(
            return_value=[
                {"id": "item-1", "collection": "source-col", "links": [], "assets": {}},
                {"id": "item-2", "collection": "source-col", "links": [], "assets": {}},
            ]
        )

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "id": "source-col",
            "type": "Collection",
            "description": "Test",
            "links": [],
        }
        mock_resp.raise_for_status = MagicMock()

        with (
            patch.object(runner.session, "get", return_value=mock_resp),
            patch.object(runner.session, "post", return_value=mock_resp) as mock_post,
        ):
            copied, failed = runner.clone_collection("source-col", "target-col")

        assert copied == 2
        assert failed == 0
        # collection creation + 2 item posts = 3 POSTs
        assert mock_post.call_count == 3
        # items should have collection field updated
        for call in mock_post.call_args_list[1:]:
            assert call.kwargs["json"]["collection"] == "target-col"

    def test_clone_collection_counts_failed_items(self):
        runner = STACMigrationRunner("https://api.example.com/stac")
        runner.get_items = MagicMock(
            return_value=[{"id": "item-1", "collection": "source-col", "links": [], "assets": {}}]
        )

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "id": "source-col",
            "type": "Collection",
            "description": "Test",
            "links": [],
        }
        mock_resp.raise_for_status = MagicMock()

        # collection POST succeeds, item POST fails
        def post_side_effect(url, **kwargs):
            if "items" in url:
                raise Exception("item post failed")
            return mock_resp

        with (
            patch.object(runner.session, "get", return_value=mock_resp),
            patch.object(runner.session, "post", side_effect=post_side_effect),
        ):
            copied, failed = runner.clone_collection("source-col", "target-col")

        assert copied == 0
        assert failed == 1


class TestRecoveryFile:
    def test_recovery_file_written_before_delete(self, tmp_path, item_with_wrong_media_type):
        runner = STACMigrationRunner("https://api.example.com/stac", recovery_dir=tmp_path)

        recovery_existed_before_delete = []

        def delete_side_effect(*args, **kwargs):
            files = list(tmp_path.glob(".migration_recovery_*.jsonl"))
            recovery_existed_before_delete.append(bool(files))
            return MagicMock()

        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()

        with (
            patch.object(runner.session, "delete", side_effect=delete_side_effect),
            patch.object(runner.session, "post", return_value=mock_resp),
        ):
            runner._update_item("test-col", "item-1", item_with_wrong_media_type)

        assert recovery_existed_before_delete == [
            True
        ], "Recovery file must exist before delete is called"

        files = list(tmp_path.glob(".migration_recovery_*.jsonl"))
        assert len(files) == 1
        with open(files[0]) as f:
            saved = json.loads(f.readline())
        assert saved["id"] == item_with_wrong_media_type["id"]

    def test_no_recovery_file_without_recovery_dir(self, item_with_wrong_media_type):
        runner = STACMigrationRunner("https://api.example.com/stac")

        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()

        with (
            patch.object(runner.session, "delete", return_value=MagicMock()),
            patch.object(runner.session, "post", return_value=mock_resp),
        ):
            runner._update_item("test-col", "item-1", item_with_wrong_media_type)

        assert runner._recovery_file is None

    def test_multiple_updates_append_to_same_recovery_file(
        self, tmp_path, item_with_wrong_media_type, item_clean
    ):
        runner = STACMigrationRunner("https://api.example.com/stac", recovery_dir=tmp_path)

        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()

        with (
            patch.object(runner.session, "delete", return_value=MagicMock()),
            patch.object(runner.session, "post", return_value=mock_resp),
        ):
            runner._update_item("test-col", "item-1", item_with_wrong_media_type)
            runner._update_item("test-col", "item-2", item_clean)

        files = list(tmp_path.glob(".migration_recovery_*.jsonl"))
        assert len(files) == 1
        lines = files[0].read_text().strip().splitlines()
        assert len(lines) == 2


class TestComposeMigrations:
    def test_changed_by_both(self):
        item = {
            "id": "test-both",
            "assets": {
                "data": {
                    "href": "https://example.com/data?scale=0+1",
                    "type": "application/vnd+zarr",
                }
            },
            "links": [],
        }
        composed = compose_migrations([fix_url_encoding, fix_zarr_media_type])
        result = composed(item)
        assert result is not None
        assert "%20" in result["assets"]["data"]["href"]
        assert "+" not in result["assets"]["data"]["href"].split("?")[1]
        assert result["assets"]["data"]["type"] == "application/vnd.zarr"

    def test_changed_by_one_only(self, item_with_plus_urls):
        # item_with_plus_urls has correct media types — only url fix applies
        composed = compose_migrations([fix_url_encoding, fix_zarr_media_type])
        result = composed(item_with_plus_urls)
        assert result is not None
        query = result["assets"]["thumbnail"]["href"].split("?")[1]
        assert "+" not in query
        assert "%20" in query

    def test_changed_by_none(self, item_clean):
        composed = compose_migrations([fix_url_encoding, fix_zarr_media_type])
        assert composed(item_clean) is None

    def test_does_not_mutate_input(self):
        item = {
            "id": "test-mutate",
            "assets": {"data": {"href": "https://ex.com?q=a+b", "type": "application/vnd+zarr"}},
            "links": [],
        }
        original_href = item["assets"]["data"]["href"]
        original_type = item["assets"]["data"]["type"]
        compose_migrations([fix_url_encoding, fix_zarr_media_type])(item)
        assert item["assets"]["data"]["href"] == original_href
        assert item["assets"]["data"]["type"] == original_type

    def test_composed_name_uses_plus_separator(self):
        from migrate_catalog.migrations import MIGRATIONS

        migration_name = "+".join(["fix_url_encoding", "fix_zarr_media_type"])
        assert migration_name == "fix_url_encoding+fix_zarr_media_type"
        assert "fix_url_encoding" in MIGRATIONS
        assert "fix_zarr_media_type" in MIGRATIONS


class TestHistoryTracking:
    def test_load_history_returns_empty_for_missing_file(self, tmp_path):
        history = load_history(tmp_path / "history.json")
        assert history == {"runs": []}

    def test_record_run_writes_to_file(self, tmp_path, migration_result):
        history_file = tmp_path / "history.json"
        record_run(history_file, migration_result)

        history = load_history(history_file)
        assert len(history["runs"]) == 1
        run = history["runs"][0]
        assert run["migration_name"] == "fix_zarr_media_type"
        assert run["items_modified"] == 50
        assert run["dry_run"] is False

    def test_record_run_appends_to_existing(self, tmp_path, migration_result):
        history_file = tmp_path / "history.json"
        record_run(history_file, migration_result)
        record_run(history_file, migration_result)

        assert len(load_history(history_file)["runs"]) == 2

    def test_was_migration_run_detects_completed_run(self, tmp_path, migration_result):
        history_file = tmp_path / "history.json"
        record_run(history_file, migration_result)

        assert was_migration_run(history_file, "fix_zarr_media_type", "sentinel-2-l2a")

    def test_was_migration_run_ignores_dry_runs(self, tmp_path, migration_result):
        history_file = tmp_path / "history.json"
        migration_result.dry_run = True
        record_run(history_file, migration_result)

        assert not was_migration_run(history_file, "fix_zarr_media_type", "sentinel-2-l2a")

    def test_was_migration_run_false_for_different_collection(self, tmp_path, migration_result):
        history_file = tmp_path / "history.json"
        record_run(history_file, migration_result)

        assert not was_migration_run(history_file, "fix_zarr_media_type", "sentinel-1-slc")

    def test_was_migration_run_false_for_different_migration(self, tmp_path, migration_result):
        history_file = tmp_path / "history.json"
        record_run(history_file, migration_result)

        assert not was_migration_run(history_file, "fix_url_encoding", "sentinel-2-l2a")
