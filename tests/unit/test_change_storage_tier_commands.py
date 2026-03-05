"""Unit tests for the change-storage-tier CLI commands.

Covers manage_item.py and manage_collections.py change-storage-tier subcommands.
All external calls (S3, STAC API, pystac_client) are mocked.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

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
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


# Load manage_item first (manage_collections imports from it)
manage_item_module = _load("manage_item", OPERATOR_TOOLS / "manage_item.py")
manage_collections_module = _load("manage_collections", OPERATOR_TOOLS / "manage_collections.py")

item_cli = manage_item_module.cli
collection_cli = manage_collections_module.cli


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

S3_SUCCESS_STATS = {"processed": 100, "succeeded": 100, "failed": 0}
S3_FAILURE_STATS = {"processed": 100, "succeeded": 90, "failed": 10}
S3_ZERO_STATS = {"processed": 0, "succeeded": 0, "failed": 0}

BASE_ARGS = ["--api-url", API_URL, "change-storage-tier"]
ITEM_BASE_ARGS = BASE_ARGS + [COLLECTION_ID, ITEM_ID]
COLL_BASE_ARGS = BASE_ARGS + [COLLECTION_ID]


def _fake_pystac_item(item_id: str) -> MagicMock:
    mock = MagicMock()
    mock.id = item_id
    return mock


def _mock_catalog(items: list) -> MagicMock:
    mock_search = MagicMock()
    mock_search.items.return_value = items
    mock_catalog = MagicMock()
    mock_catalog.search.return_value = mock_search
    return mock_catalog


# ---------------------------------------------------------------------------
# manage_item.py: change-storage-tier
# ---------------------------------------------------------------------------
class TestManageItemChangeStorageTier:
    def test_help_output(self):
        runner = CliRunner()
        result = runner.invoke(item_cli, BASE_ARGS + ["--help"])
        assert result.exit_code == 0
        for flag in (
            "--storage-class",
            "--s3-endpoint",
            "--include-pattern",
            "--exclude-pattern",
            "--dry-run",
        ):
            assert flag in result.output

    def test_missing_storage_class(self):
        runner = CliRunner()
        result = runner.invoke(
            item_cli,
            ITEM_BASE_ARGS + ["--s3-endpoint", S3_ENDPOINT],
        )
        assert result.exit_code != 0

    def test_invalid_storage_class(self):
        runner = CliRunner()
        result = runner.invoke(
            item_cli,
            ITEM_BASE_ARGS + ["--storage-class", "GLACIER", "--s3-endpoint", S3_ENDPOINT],
        )
        assert result.exit_code != 0

    def test_missing_s3_endpoint(self):
        runner = CliRunner()
        result = runner.invoke(
            item_cli,
            ITEM_BASE_ARGS + ["--storage-class", "STANDARD_IA"],
            env={"AWS_ENDPOINT_URL": ""},
        )
        assert "S3 endpoint required" in result.output

    def test_s3_endpoint_from_env(self):
        runner = CliRunner()
        with patch("change_storage_tier.process_stac_item", return_value=S3_ZERO_STATS):
            result = runner.invoke(
                item_cli,
                ITEM_BASE_ARGS + ["--storage-class", "STANDARD_IA", "--dry-run"],
                env={"AWS_ENDPOINT_URL": S3_ENDPOINT},
            )
        assert result.exit_code == 0

    def test_dry_run_skips_confirmation(self):
        """With --dry-run, no confirmation prompt is shown (no input needed)."""
        runner = CliRunner()
        with patch("change_storage_tier.process_stac_item", return_value=S3_ZERO_STATS):
            result = runner.invoke(
                item_cli,
                ITEM_BASE_ARGS
                + ["--storage-class", "STANDARD_IA", "--s3-endpoint", S3_ENDPOINT, "--dry-run"],
            )
        assert result.exit_code == 0
        assert "Continue?" not in result.output

    def test_confirmation_prompt_shown(self):
        """Without --dry-run or -y, a confirmation prompt must appear."""
        runner = CliRunner()
        with patch("change_storage_tier.process_stac_item", return_value=S3_ZERO_STATS):
            result = runner.invoke(
                item_cli,
                ITEM_BASE_ARGS + ["--storage-class", "STANDARD_IA", "--s3-endpoint", S3_ENDPOINT],
                input="n\n",
            )
        assert "Continue?" in result.output

    def test_dry_run_calls_process_stac_item(self):
        runner = CliRunner()
        with patch("change_storage_tier.process_stac_item", return_value=S3_ZERO_STATS) as mock_psi:
            runner.invoke(
                item_cli,
                ITEM_BASE_ARGS
                + ["--storage-class", "STANDARD_IA", "--s3-endpoint", S3_ENDPOINT, "--dry-run"],
            )
        mock_psi.assert_called_once()
        assert mock_psi.call_args.args[2] is True  # dry_run positional argument

    def test_dry_run_skips_stac_update(self):
        """In dry-run mode, STAC DELETE+POST must not be called."""
        runner = CliRunner()
        with (
            patch("change_storage_tier.process_stac_item", return_value=S3_SUCCESS_STATS),
            patch("update_stac_storage_tier.update_item_storage_tiers") as mock_update,
            patch("requests.Session.delete") as mock_delete,
            patch("requests.Session.post") as mock_post,
        ):
            runner.invoke(
                item_cli,
                ITEM_BASE_ARGS
                + [
                    "--storage-class",
                    "STANDARD_IA",
                    "--s3-endpoint",
                    S3_ENDPOINT,
                    "--dry-run",
                ],
            )
        mock_update.assert_not_called()
        mock_delete.assert_not_called()
        mock_post.assert_not_called()

    def test_success_calls_update_stac_metadata(self):
        """When S3 succeeds (failed=0), update_item_storage_tiers must be called."""
        runner = CliRunner()
        with (
            patch("change_storage_tier.process_stac_item", return_value=S3_SUCCESS_STATS),
            patch("manage_item.STACItemManager.get_item", return_value=FAKE_ITEM_DICT),
            patch("update_stac_storage_tier.update_item_storage_tiers") as mock_update,
            patch("requests.Session.delete", return_value=MagicMock(status_code=200)),
            patch("requests.Session.post", return_value=MagicMock(status_code=201)),
        ):
            result = runner.invoke(
                item_cli,
                ITEM_BASE_ARGS
                + [
                    "--storage-class",
                    "STANDARD_IA",
                    "--s3-endpoint",
                    S3_ENDPOINT,
                    "-y",
                ],
            )
        mock_update.assert_called_once()
        assert result.exit_code == 0

    def test_s3_failure_skips_stac_update(self):
        """When S3 has failures (failed>0), update_item_storage_tiers must NOT be called."""
        runner = CliRunner()
        with (
            patch("change_storage_tier.process_stac_item", return_value=S3_FAILURE_STATS),
            patch("update_stac_storage_tier.update_item_storage_tiers") as mock_update,
        ):
            runner.invoke(
                item_cli,
                ITEM_BASE_ARGS
                + ["--storage-class", "STANDARD_IA", "--s3-endpoint", S3_ENDPOINT, "-y"],
            )
        mock_update.assert_not_called()

    def test_include_exclude_patterns_passed(self):
        """--include-pattern and --exclude-pattern are forwarded to process_stac_item."""
        runner = CliRunner()
        with patch("change_storage_tier.process_stac_item", return_value=S3_ZERO_STATS) as mock_psi:
            runner.invoke(
                item_cli,
                ITEM_BASE_ARGS
                + [
                    "--storage-class",
                    "STANDARD_IA",
                    "--s3-endpoint",
                    S3_ENDPOINT,
                    "--dry-run",
                    "--include-pattern",
                    "*.zarr",
                    "--exclude-pattern",
                    "*.metadata",
                ],
            )
        mock_psi.assert_called_once()
        call_args = mock_psi.call_args.args
        assert call_args[4] == ["*.zarr"]  # include_patterns
        assert call_args[5] == ["*.metadata"]  # exclude_patterns


# ---------------------------------------------------------------------------
# manage_collections.py: change-storage-tier
# ---------------------------------------------------------------------------
class TestManageCollectionsChangeStorageTier:
    def test_help_output(self):
        runner = CliRunner()
        result = runner.invoke(collection_cli, BASE_ARGS + ["--help"])
        assert result.exit_code == 0
        for flag in ("--storage-class", "--start-date", "--end-date", "--s3-endpoint", "--dry-run"):
            assert flag in result.output

    def test_missing_storage_class(self):
        runner = CliRunner()
        result = runner.invoke(
            collection_cli,
            COLL_BASE_ARGS + ["--s3-endpoint", S3_ENDPOINT],
        )
        assert result.exit_code != 0

    def test_invalid_date_format(self):
        runner = CliRunner()
        result = runner.invoke(
            collection_cli,
            COLL_BASE_ARGS
            + [
                "--storage-class",
                "STANDARD_IA",
                "--s3-endpoint",
                S3_ENDPOINT,
                "--start-date",
                "01-2024-01",
            ],
        )
        assert "Invalid" in result.output or result.exit_code != 0

    def test_no_date_filter_fetches_all(self):
        """Without dates, catalog.search() is called without a CQL2 filter."""
        runner = CliRunner()
        mock_catalog = _mock_catalog([_fake_pystac_item("item-001")])
        with (
            patch("pystac_client.Client.open", return_value=mock_catalog),
            patch("change_storage_tier.process_stac_item", return_value=S3_ZERO_STATS),
        ):
            runner.invoke(
                collection_cli,
                COLL_BASE_ARGS
                + [
                    "--storage-class",
                    "STANDARD_IA",
                    "--s3-endpoint",
                    S3_ENDPOINT,
                    "--dry-run",
                    "-y",
                ],
            )
        call_kwargs = mock_catalog.search.call_args.kwargs
        assert "filter" not in call_kwargs

    def test_date_filter_builds_cql2_between(self):
        """With start+end dates, catalog.search() receives the correct CQL2 between filter."""
        runner = CliRunner()
        mock_catalog = _mock_catalog([_fake_pystac_item("item-001")])
        with (
            patch("pystac_client.Client.open", return_value=mock_catalog),
            patch("change_storage_tier.process_stac_item", return_value=S3_ZERO_STATS),
        ):
            runner.invoke(
                collection_cli,
                COLL_BASE_ARGS
                + [
                    "--storage-class",
                    "STANDARD_IA",
                    "--s3-endpoint",
                    S3_ENDPOINT,
                    "--start-date",
                    "2024-01-01",
                    "--end-date",
                    "2024-03-31",
                    "--dry-run",
                    "-y",
                ],
            )
        call_kwargs = mock_catalog.search.call_args.kwargs
        filter_arg = call_kwargs["filter"]
        assert filter_arg["op"] == "between"
        args_list = str(filter_arg)
        assert "2024-01-01T00:00:00Z" in args_list
        assert "2024-03-31T23:59:59Z" in args_list

    def test_start_date_only(self):
        """Only --start-date: range goes from start to far future."""
        runner = CliRunner()
        mock_catalog = _mock_catalog([_fake_pystac_item("item-001")])
        with (
            patch("pystac_client.Client.open", return_value=mock_catalog),
            patch("change_storage_tier.process_stac_item", return_value=S3_ZERO_STATS),
        ):
            runner.invoke(
                collection_cli,
                COLL_BASE_ARGS
                + [
                    "--storage-class",
                    "STANDARD_IA",
                    "--s3-endpoint",
                    S3_ENDPOINT,
                    "--start-date",
                    "2024-01-01",
                    "--dry-run",
                    "-y",
                ],
            )
        filter_arg = mock_catalog.search.call_args.kwargs["filter"]
        args_str = str(filter_arg)
        assert "2024-01-01T00:00:00Z" in args_str
        assert "2100-12-31T23:59:59Z" in args_str

    def test_end_date_only(self):
        """Only --end-date: range goes from far past to end."""
        runner = CliRunner()
        mock_catalog = _mock_catalog([_fake_pystac_item("item-001")])
        with (
            patch("pystac_client.Client.open", return_value=mock_catalog),
            patch("change_storage_tier.process_stac_item", return_value=S3_ZERO_STATS),
        ):
            runner.invoke(
                collection_cli,
                COLL_BASE_ARGS
                + [
                    "--storage-class",
                    "STANDARD_IA",
                    "--s3-endpoint",
                    S3_ENDPOINT,
                    "--end-date",
                    "2024-03-31",
                    "--dry-run",
                    "-y",
                ],
            )
        filter_arg = mock_catalog.search.call_args.kwargs["filter"]
        args_str = str(filter_arg)
        assert "1900-01-01T00:00:00Z" in args_str
        assert "2024-03-31T23:59:59Z" in args_str

    def test_dry_run_no_stac_update(self):
        """In dry-run mode, STAC DELETE+POST must not be called for any item."""
        runner = CliRunner()
        items = [_fake_pystac_item(f"item-{i:03d}") for i in range(3)]
        mock_catalog = _mock_catalog(items)
        with (
            patch("pystac_client.Client.open", return_value=mock_catalog),
            patch("change_storage_tier.process_stac_item", return_value=S3_SUCCESS_STATS),
            patch("update_stac_storage_tier.update_item_storage_tiers") as mock_update,
            patch("requests.Session.delete") as mock_delete,
            patch("requests.Session.post") as mock_post,
        ):
            runner.invoke(
                collection_cli,
                COLL_BASE_ARGS
                + [
                    "--storage-class",
                    "STANDARD_IA",
                    "--s3-endpoint",
                    S3_ENDPOINT,
                    "--dry-run",
                    "-y",
                ],
            )
        mock_update.assert_not_called()
        mock_delete.assert_not_called()
        mock_post.assert_not_called()

    def test_per_item_failure_tracking(self):
        """One item fails S3 change; summary reports correct counts and failed item ID."""
        runner = CliRunner()
        items = [_fake_pystac_item(f"item-{i:03d}") for i in range(3)]
        mock_catalog = _mock_catalog(items)

        def _psi_side_effect(url, *args, **kwargs):
            return S3_FAILURE_STATS if "item-000" in url else S3_SUCCESS_STATS

        with (
            patch("pystac_client.Client.open", return_value=mock_catalog),
            patch("change_storage_tier.process_stac_item", side_effect=_psi_side_effect),
            patch("manage_item.STACItemManager.get_item", return_value=FAKE_ITEM_DICT),
            patch("update_stac_storage_tier.update_item_storage_tiers"),
            patch("requests.Session.delete", return_value=MagicMock()),
            patch("requests.Session.post", return_value=MagicMock()),
        ):
            result = runner.invoke(
                collection_cli,
                COLL_BASE_ARGS
                + [
                    "--storage-class",
                    "STANDARD_IA",
                    "--s3-endpoint",
                    S3_ENDPOINT,
                    "-y",
                ],
            )
        assert "Items failed: 1" in result.output
        assert "item-000" in result.output
        assert "Items changed: 2" in result.output

    def test_confirmation_with_item_count(self):
        """Confirmation prompt includes the number of matched items."""
        runner = CliRunner()
        items = [_fake_pystac_item(f"item-{i:03d}") for i in range(5)]
        mock_catalog = _mock_catalog(items)
        with patch("pystac_client.Client.open", return_value=mock_catalog):
            result = runner.invoke(
                collection_cli,
                COLL_BASE_ARGS + ["--storage-class", "STANDARD_IA", "--s3-endpoint", S3_ENDPOINT],
                input="n\n",
            )
        assert "5" in result.output
        assert "Continue?" in result.output

    def test_empty_search_results(self):
        """When no items match, exit gracefully with a message."""
        runner = CliRunner()
        mock_catalog = _mock_catalog([])
        with patch("pystac_client.Client.open", return_value=mock_catalog):
            result = runner.invoke(
                collection_cli,
                COLL_BASE_ARGS
                + ["--storage-class", "STANDARD_IA", "--s3-endpoint", S3_ENDPOINT, "-y"],
            )
        assert result.exit_code == 0
        assert "No items matched" in result.output
