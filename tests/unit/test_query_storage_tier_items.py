"""Tests for query_storage_tier_items.py script."""

import json
from datetime import datetime
from io import StringIO
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from pystac import Asset, Item
from stac_auth import DEFAULT_PAGE_SIZE

from scripts.query_storage_tier_items import (
    get_storage_ref,
    is_already_migrated,
    main,
    query_items,
)

COLLECTION = "sentinel-2-l2a-staging"
STAC_API_URL = "https://stac.example.com/stac"


def create_stac_item(
    item_id: str,
    storage_refs: list[str] | None = None,
    has_s3_alternate: bool = True,
) -> Item:
    """Create a STAC item with optional storage:refs on its assets.

    Args:
        item_id: Item identifier.
        storage_refs: storage:refs list for alternate.s3 (None = no alternate.s3).
        has_s3_alternate: Whether to include alternate.s3 on assets.
    """
    item = Item(
        id=item_id,
        geometry={"type": "Point", "coordinates": [0, 0]},
        bbox=[0, 0, 0, 0],
        datetime=datetime(2024, 1, 1),
        properties={},
    )

    asset = Asset(href="https://example.com/data.zarr")
    extra: dict = {}
    if has_s3_alternate:
        s3_info: dict = {"href": "s3://bucket/data.zarr"}
        if storage_refs is not None:
            s3_info["storage:refs"] = storage_refs
        extra["alternate"] = {"s3": s3_info}
    asset.extra_fields = extra
    item.assets["data"] = asset

    return item


class FakeItemSearch:
    """Simulates STAC search results."""

    def __init__(self, items: list[Item]):
        self._items = items

    def pages(self):
        return [SimpleNamespace(items=self._items)]


class FakeStacClient:
    """Simulates STAC API client."""

    def __init__(self, items: list[Item]):
        self.items = items
        self.search_kwargs: dict | None = None

    def search(self, **kwargs):
        self.search_kwargs = kwargs
        return FakeItemSearch(self.items)


# --- Tests for get_storage_ref ---


class TestGetStorageRef:
    def test_list_single(self):
        assert get_storage_ref({"storage:refs": ["glacier"]}) == "glacier"

    def test_list_multiple(self):
        assert get_storage_ref({"storage:refs": ["glacier", "standard"]}) == "glacier"

    def test_empty_list(self):
        assert get_storage_ref({"storage:refs": []}) is None

    def test_missing_key(self):
        assert get_storage_ref({}) is None

    def test_string_value(self):
        """Defensive: handle storage:refs as a bare string."""
        assert get_storage_ref({"storage:refs": "glacier"}) == "glacier"


# --- Tests for is_already_migrated ---


class TestIsAlreadyMigrated:
    def test_all_assets_match(self):
        item = create_stac_item("item-1", storage_refs=["glacier"])
        assert is_already_migrated(item, "glacier") is True

    def test_no_assets_match(self):
        item = create_stac_item("item-1", storage_refs=["standard"])
        assert is_already_migrated(item, "glacier") is False

    def test_no_storage_refs(self):
        """Assets with alternate.s3 but no storage:refs → needs work."""
        item = create_stac_item("item-1", storage_refs=None, has_s3_alternate=True)
        assert is_already_migrated(item, "glacier") is False

    def test_no_s3_alternate(self):
        """No alternate.s3 at all → needs work (safe default)."""
        item = create_stac_item("item-1", has_s3_alternate=False)
        assert is_already_migrated(item, "glacier") is False

    def test_partial_match(self):
        """One asset matches, another doesn't → needs work."""
        item = create_stac_item("item-1", storage_refs=["glacier"])
        # Add a second asset with different tier
        asset2 = Asset(href="https://example.com/other.zarr")
        asset2.extra_fields = {
            "alternate": {"s3": {"href": "s3://bucket/other.zarr", "storage:refs": ["standard"]}}
        }
        item.assets["other"] = asset2
        assert is_already_migrated(item, "glacier") is False

    def test_multiple_assets_all_match(self):
        """Multiple assets all at target tier → already migrated."""
        item = create_stac_item("item-1", storage_refs=["glacier"])
        asset2 = Asset(href="https://example.com/other.zarr")
        asset2.extra_fields = {
            "alternate": {"s3": {"href": "s3://bucket/other.zarr", "storage:refs": ["glacier"]}}
        }
        item.assets["other"] = asset2
        assert is_already_migrated(item, "glacier") is True


# --- Tests for query_items ---


class TestQueryItems:
    def test_filters_migrated_items(self):
        items = [
            create_stac_item("item-1", storage_refs=["glacier"]),  # already migrated
            create_stac_item("item-2", storage_refs=["standard"]),  # needs work
            create_stac_item("item-3", storage_refs=["glacier"]),  # already migrated
            create_stac_item("item-4", storage_refs=["standard"]),  # needs work
            create_stac_item("item-5", has_s3_alternate=False),  # needs work (no s3)
        ]
        client = FakeStacClient(items)

        with patch(
            "scripts.query_storage_tier_items.stac_auth.open_resilient_client", return_value=client
        ):
            result = query_items(STAC_API_URL, COLLECTION, 7, "glacier", 100)

        assert result == ["item-2", "item-4", "item-5"]

    def test_caps_at_max_batch_size(self):
        items = [create_stac_item(f"item-{i}", storage_refs=["standard"]) for i in range(10)]
        client = FakeStacClient(items)

        with patch(
            "scripts.query_storage_tier_items.stac_auth.open_resilient_client", return_value=client
        ):
            result = query_items(STAC_API_URL, COLLECTION, 7, "glacier", 3)

        assert len(result) == 3
        assert result == ["item-0", "item-1", "item-2"]

    def test_empty_collection(self):
        client = FakeStacClient([])

        with patch(
            "scripts.query_storage_tier_items.stac_auth.open_resilient_client", return_value=client
        ):
            result = query_items(STAC_API_URL, COLLECTION, 7, "glacier", 100)

        assert result == []

    def test_search_passes_an_explicit_page_size(self):
        """`limit` is the page size and is always sent; `max_batch_size` stays the cap.

        This script and the cleanup cron were the two fleet searches that omitted
        `limit`; walking a 36 h window at the server's default page (10) was hundreds
        of /search POSTs. Not evidence that 100 avoids the gateway's 15 s upstream
        timeout: the tier-down cron that died on it 2026-09-18 runs
        submit_storage_tier_workflows, which already passed limit=100 (this script has
        no manifest). The knob exists so that can be measured either way.
        """
        items = [create_stac_item(f"item-{i}", storage_refs=["standard"]) for i in range(10)]
        client = FakeStacClient(items)

        with patch(
            "scripts.query_storage_tier_items.stac_auth.open_resilient_client", return_value=client
        ):
            result = query_items(STAC_API_URL, COLLECTION, 7, "glacier", 3)
        assert client.search_kwargs["limit"] == DEFAULT_PAGE_SIZE == 100
        assert len(result) == 3, "the cap is still max_batch_size, not the page size"

        with patch(
            "scripts.query_storage_tier_items.stac_auth.open_resilient_client", return_value=client
        ):
            query_items(STAC_API_URL, COLLECTION, 7, "glacier", 3, page_size=500)
        assert client.search_kwargs["limit"] == 500

    def test_all_migrated(self):
        items = [
            create_stac_item("item-1", storage_refs=["glacier"]),
            create_stac_item("item-2", storage_refs=["glacier"]),
        ]
        client = FakeStacClient(items)

        with patch(
            "scripts.query_storage_tier_items.stac_auth.open_resilient_client", return_value=client
        ):
            result = query_items(STAC_API_URL, COLLECTION, 7, "glacier", 100)

        assert result == []

    def test_excluded_ids_never_selected(self):
        """Items on the demo denylist are skipped even when they need work."""
        items = [
            create_stac_item("demo-1", storage_refs=["standard"]),  # needs work but excluded
            create_stac_item("item-2", storage_refs=["standard"]),  # needs work
        ]
        client = FakeStacClient(items)

        with patch(
            "scripts.query_storage_tier_items.stac_auth.open_resilient_client", return_value=client
        ):
            result = query_items(
                STAC_API_URL, COLLECTION, 7, "glacier", 100, exclude_ids={"demo-1"}
            )

        assert result == ["item-2"]

    def test_exclusion_does_not_consume_batch_capacity(self):
        """Excluded items must not eat into max_batch_size."""
        items = [create_stac_item("demo-1", storage_refs=["standard"])] + [
            create_stac_item(f"item-{i}", storage_refs=["standard"]) for i in range(3)
        ]
        client = FakeStacClient(items)

        with patch(
            "scripts.query_storage_tier_items.stac_auth.open_resilient_client", return_value=client
        ):
            result = query_items(STAC_API_URL, COLLECTION, 7, "glacier", 3, exclude_ids={"demo-1"})

        assert result == ["item-0", "item-1", "item-2"]


# --- Tests for main ---


class TestMain:
    def test_output_format(self):
        items = [
            create_stac_item("item-1", storage_refs=["standard"]),
            create_stac_item("item-2", storage_refs=["standard"]),
        ]
        client = FakeStacClient(items)

        with (
            patch(
                "scripts.query_storage_tier_items.stac_auth.open_resilient_client",
                return_value=client,
            ),
            patch("sys.stdout", new_callable=StringIO) as stdout,
        ):
            exit_code = main(
                [
                    "--stac-api-url",
                    STAC_API_URL,
                    "--collection",
                    COLLECTION,
                    "--age-days",
                    "7",
                    "--to-storage-class",
                    "STANDARD_IA",
                    "--max-batch-size",
                    "100",
                ]
            )

        assert exit_code == 0
        output = json.loads(stdout.getvalue())
        assert isinstance(output, list)
        assert output == ["item-1", "item-2"]

    def test_empty_result(self):
        client = FakeStacClient([])

        with (
            patch(
                "scripts.query_storage_tier_items.stac_auth.open_resilient_client",
                return_value=client,
            ),
            patch("sys.stdout", new_callable=StringIO) as stdout,
        ):
            exit_code = main(
                [
                    "--stac-api-url",
                    STAC_API_URL,
                    "--collection",
                    COLLECTION,
                    "--age-days",
                    "7",
                    "--to-storage-class",
                    "STANDARD_IA",
                ]
            )

        assert exit_code == 0
        assert json.loads(stdout.getvalue()) == []

    def test_default_max_batch_size(self):
        """Default max_batch_size is 100."""
        items = [create_stac_item(f"item-{i}", storage_refs=["standard"]) for i in range(150)]
        client = FakeStacClient(items)

        with (
            patch(
                "scripts.query_storage_tier_items.stac_auth.open_resilient_client",
                return_value=client,
            ),
            patch("sys.stdout", new_callable=StringIO) as stdout,
        ):
            exit_code = main(
                [
                    "--stac-api-url",
                    STAC_API_URL,
                    "--collection",
                    COLLECTION,
                    "--age-days",
                    "7",
                    "--to-storage-class",
                    "STANDARD_IA",
                ]
            )

        assert exit_code == 0
        output = json.loads(stdout.getvalue())
        assert len(output) == 100

    def test_baked_demo_denylist_applied_by_default(self, monkeypatch):
        """Without --exclude-file or env var, the baked demo list still protects."""
        monkeypatch.delenv("EXPIRES_EXCLUDE_FILE", raising=False)
        from scripts.s3_item_cleanup import BAKED_EXCLUDE_FILE, load_exclude_ids

        demo_id = sorted(load_exclude_ids(str(BAKED_EXCLUDE_FILE)))[0]
        items = [
            create_stac_item(demo_id, storage_refs=["performance"]),  # baked demo id
            create_stac_item("item-2", storage_refs=["performance"]),
        ]
        client = FakeStacClient(items)

        with (
            patch(
                "scripts.query_storage_tier_items.stac_auth.open_resilient_client",
                return_value=client,
            ),
            patch("sys.stdout", new_callable=StringIO) as stdout,
        ):
            exit_code = main(
                [
                    "--stac-api-url",
                    STAC_API_URL,
                    "--collection",
                    COLLECTION,
                    "--age-days",
                    "180",
                    "--to-storage-class",
                    "STANDARD",
                ]
            )

        assert exit_code == 0
        assert json.loads(stdout.getvalue()) == ["item-2"]

    def test_explicit_exclude_file(self, tmp_path):
        """--exclude-file takes precedence and is honored."""
        exclude_file = tmp_path / "exclude.txt"
        exclude_file.write_text("# comment\nitem-1\n")
        items = [
            create_stac_item("item-1", storage_refs=["standard"]),
            create_stac_item("item-2", storage_refs=["standard"]),
        ]
        client = FakeStacClient(items)

        with (
            patch(
                "scripts.query_storage_tier_items.stac_auth.open_resilient_client",
                return_value=client,
            ),
            patch("sys.stdout", new_callable=StringIO) as stdout,
        ):
            exit_code = main(
                [
                    "--stac-api-url",
                    STAC_API_URL,
                    "--collection",
                    COLLECTION,
                    "--age-days",
                    "7",
                    "--to-storage-class",
                    "STANDARD_IA",
                    "--exclude-file",
                    str(exclude_file),
                ]
            )

        assert exit_code == 0
        assert json.loads(stdout.getvalue()) == ["item-2"]

    def test_error_returns_nonzero(self):
        with patch(
            "scripts.query_storage_tier_items.stac_auth.open_resilient_client",
            side_effect=Exception("Connection failed"),
        ):
            exit_code = main(
                [
                    "--stac-api-url",
                    STAC_API_URL,
                    "--collection",
                    COLLECTION,
                    "--age-days",
                    "7",
                    "--to-storage-class",
                    "STANDARD_IA",
                ]
            )

        assert exit_code == 1

    @pytest.mark.parametrize("bad", ["0", "-5", "20000", "ten"])
    def test_cli_rejects_a_bad_page_size_before_any_network_call(self, bad, capsys):
        """Same validator as the cleanup cron. Unvalidated `type=int` let these reach
        `search()` and die after a network round trip inside pystac-client with a bare
        `Exception("Invalid limit of 0, ...")` -> rc 1 and a traceback; the cleanup twin
        was a clean usage error at parse time."""
        with (
            patch("scripts.query_storage_tier_items.stac_auth.open_resilient_client") as opener,
            pytest.raises(SystemExit) as exc,
        ):
            main([*_CLI_BASE, "--page-size", bad])
        assert exc.value.code == 2
        assert "--page-size" in capsys.readouterr().err
        opener.assert_not_called()

    def test_cli_treats_an_empty_page_size_as_the_default(self):
        """`""` is this fleet's spelling of an unset optional Argo parameter."""
        client = FakeStacClient([])
        with (
            patch(
                "scripts.query_storage_tier_items.stac_auth.open_resilient_client",
                return_value=client,
            ),
            patch("sys.stdout", new_callable=StringIO),
        ):
            assert main([*_CLI_BASE, "--page-size", ""]) == 0
        assert client.search_kwargs["limit"] == DEFAULT_PAGE_SIZE == 100


_CLI_BASE = [
    "--stac-api-url",
    STAC_API_URL,
    "--collection",
    COLLECTION,
    "--age-days",
    "7",
    "--to-storage-class",
    "STANDARD_IA",
]
