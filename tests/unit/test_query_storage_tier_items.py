"""Tests for query_storage_tier_items.py script."""

import json
from datetime import datetime
from io import StringIO
from types import SimpleNamespace
from unittest.mock import patch

from pystac import Asset, Item

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

    def search(self, **kwargs):
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

        with patch("scripts.query_storage_tier_items.Client.open", return_value=client):
            result = query_items(STAC_API_URL, COLLECTION, 7, "glacier", 100)

        assert result == ["item-2", "item-4", "item-5"]

    def test_caps_at_max_batch_size(self):
        items = [create_stac_item(f"item-{i}", storage_refs=["standard"]) for i in range(10)]
        client = FakeStacClient(items)

        with patch("scripts.query_storage_tier_items.Client.open", return_value=client):
            result = query_items(STAC_API_URL, COLLECTION, 7, "glacier", 3)

        assert len(result) == 3
        assert result == ["item-0", "item-1", "item-2"]

    def test_empty_collection(self):
        client = FakeStacClient([])

        with patch("scripts.query_storage_tier_items.Client.open", return_value=client):
            result = query_items(STAC_API_URL, COLLECTION, 7, "glacier", 100)

        assert result == []

    def test_all_migrated(self):
        items = [
            create_stac_item("item-1", storage_refs=["glacier"]),
            create_stac_item("item-2", storage_refs=["glacier"]),
        ]
        client = FakeStacClient(items)

        with patch("scripts.query_storage_tier_items.Client.open", return_value=client):
            result = query_items(STAC_API_URL, COLLECTION, 7, "glacier", 100)

        assert result == []


# --- Tests for main ---


class TestMain:
    def test_output_format(self):
        items = [
            create_stac_item("item-1", storage_refs=["standard"]),
            create_stac_item("item-2", storage_refs=["standard"]),
        ]
        client = FakeStacClient(items)

        with (
            patch("scripts.query_storage_tier_items.Client.open", return_value=client),
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
                    "--target-storage-ref",
                    "glacier",
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
            patch("scripts.query_storage_tier_items.Client.open", return_value=client),
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
                    "--target-storage-ref",
                    "glacier",
                ]
            )

        assert exit_code == 0
        assert json.loads(stdout.getvalue()) == []

    def test_default_max_batch_size(self):
        """Default max_batch_size is 100."""
        items = [create_stac_item(f"item-{i}", storage_refs=["standard"]) for i in range(150)]
        client = FakeStacClient(items)

        with (
            patch("scripts.query_storage_tier_items.Client.open", return_value=client),
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
                    "--target-storage-ref",
                    "glacier",
                ]
            )

        assert exit_code == 0
        output = json.loads(stdout.getvalue())
        assert len(output) == 100

    def test_error_returns_nonzero(self):
        with patch(
            "scripts.query_storage_tier_items.Client.open",
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
                    "--target-storage-ref",
                    "glacier",
                ]
            )

        assert exit_code == 1
