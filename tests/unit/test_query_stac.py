"""Simple tests for query_stac.py script."""

import json
from datetime import datetime
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from pystac import Item, Link

# Test collection names
SOURCE_COLLECTION = "test"
TARGET_COLLECTION = "test-staging"


def create_stac_item(item_id: str, collection_id: str, has_self_link: bool = True) -> Item:
    """Create a minimal STAC item for testing."""
    item = Item(
        id=item_id,
        geometry={"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]},
        bbox=[0, 0, 1, 1],
        datetime=datetime(2023, 12, 8, 10, 0, 0),
        properties={},
    )

    if has_self_link:
        item.add_link(
            Link(
                rel="self",
                target=f"https://stac.example.com/collections/{collection_id}/items/{item_id}",
            )
        )

    return item


def load_fixtures():
    """Load source and target collections from JSON fixtures."""
    fixtures_dir = Path(__file__).parent.parent / "fixtures"

    with open(fixtures_dir / "stac_source_collection.json") as f:
        source_data = json.load(f)

    with open(fixtures_dir / "stac_target_collection.json") as f:
        target_data = json.load(f)

    source_items = [
        create_stac_item(item["id"], source_data["collection_id"]) for item in source_data["items"]
    ]
    target_items = [
        create_stac_item(item["id"], target_data["collection_id"]) for item in target_data["items"]
    ]

    return source_items, target_items


class FakeItemSearch:
    """Simulates STAC search results."""

    def __init__(self, items: list[Item]):
        self._items = items

    def items(self):
        return iter(self._items)

    def pages(self):
        from types import SimpleNamespace

        return [SimpleNamespace(items=self._items)]


class FakeStacClient:
    """Simulates STAC API client."""

    def __init__(
        self,
        source_items: list[Item],
        target_items: list[Item],
        source_collection: str,
        target_collection: str,
        raise_error_on_target: bool = False,
    ):
        self.source_items = source_items
        self.target_items = target_items
        self.source_collection = source_collection
        self.target_collection = target_collection
        self.raise_error_on_target = raise_error_on_target
        self.searches = []

    def search(self, collections: list[str] | None = None, ids: list[str] | None = None, **kwargs):
        self.searches.append({"collections": collections, "ids": ids, "kwargs": kwargs})

        if collections and self.source_collection in collections:
            return FakeItemSearch(self.source_items)

        if collections and self.target_collection in collections:
            if self.raise_error_on_target:
                raise Exception("Simulated API error")

            if ids:
                matching = [item for item in self.target_items if item.id in ids]
                return FakeItemSearch(matching)
            return FakeItemSearch(self.target_items)

        return FakeItemSearch([])


def run_script(
    source_items: list[Item], target_items: list[Item], raise_error_on_target: bool = False
) -> dict:
    """Helper to run the script with test data."""
    fake_client = FakeStacClient(
        source_items=source_items,
        target_items=target_items,
        source_collection=SOURCE_COLLECTION,
        target_collection=TARGET_COLLECTION,
        raise_error_on_target=raise_error_on_target,
    )

    with (
        patch("scripts.query_stac.Client.open", return_value=fake_client),
        patch(
            "sys.argv",
            [
                "query_stac.py",
                "https://stac.example.com/",
                SOURCE_COLLECTION,
                TARGET_COLLECTION,
                "0",
                "3",
                "[-5.14, 41.33, 9.56, 51.09]",
            ],
        ),
        patch("sys.stdout", StringIO()) as stdout,
        patch("sys.stderr", StringIO()) as stderr,
    ):
        from scripts.query_stac import main

        main()

        return {
            "output": json.loads(stdout.getvalue()),
            "stderr": stderr.getvalue(),
            "client": fake_client,
        }


class TestQueryStac:
    """Test suite for query_stac.py script."""

    def test_returns_all_items_when_target_empty(self, caplog):
        import logging

        caplog.set_level(logging.INFO)
        """All source items should be returned when target collection is empty."""
        source, _ = load_fixtures()
        target = []

        result = run_script(source, target)

        assert len(result["output"]) == 3
        assert {item["item_id"] for item in result["output"]} == {
            "item-001",
            "item-002",
            "item-003",
        }
        assert "Checked 3 items, 3 to process" in caplog.text

    def test_excludes_items_already_in_target(self, caplog):
        """Items already in target collection should be excluded."""
        import logging

        caplog.set_level(logging.INFO)
        source, target = load_fixtures()

        result = run_script(source, target)

        assert len(result["output"]) == 2
        assert {item["item_id"] for item in result["output"]} == {"item-001", "item-003"}
        assert "Already converted" in caplog.text
        assert "Checked 3 items, 2 to process" in caplog.text

    def test_skips_items_without_self_link(self, caplog):
        """Items without a self link should be skipped."""
        import logging

        caplog.set_level(logging.INFO)
        source = [
            create_stac_item("item-001", SOURCE_COLLECTION, has_self_link=True),
            create_stac_item("item-002", SOURCE_COLLECTION, has_self_link=False),
            create_stac_item("item-003", SOURCE_COLLECTION, has_self_link=True),
        ]
        target = []

        result = run_script(source, target)

        assert len(result["output"]) == 2
        assert {item["item_id"] for item in result["output"]} == {"item-001", "item-003"}
        assert "No self link" in caplog.text

    def test_handles_empty_source_collection(self, caplog):
        """Empty source collection should return empty result."""
        import logging

        caplog.set_level(logging.INFO)
        result = run_script([], [])

        assert result["output"] == []
        assert "Checked 0 items, 0 to process" in caplog.text

    def test_handles_error_checking_target(self, caplog):
        """When target check fails, item should still be processed (safe default)."""
        import logging

        caplog.set_level(logging.INFO)
        source = [create_stac_item("item-001", SOURCE_COLLECTION)]
        target = []

        result = run_script(source, target, raise_error_on_target=True)

        assert len(result["output"]) == 1
        assert result["output"][0]["item_id"] == "item-001"
        assert "Could not check" in caplog.text
        assert "Simulated API error" in caplog.text

    def test_output_format(self):
        """Output should have correct structure for Argo workflow."""
        source = [create_stac_item("item-001", SOURCE_COLLECTION)]
        target = []

        result = run_script(source, target)

        assert len(result["output"]) == 1
        item = result["output"][0]
        assert "source_url" in item
        assert "collection" in item
        assert "item_id" in item
        assert item["item_id"] == "item-001"
        assert item["collection"] == TARGET_COLLECTION
        assert "https://stac.example.com" in item["source_url"]

    def test_search_parameters(self):
        """Search should be called with correct parameters."""
        source = []
        target = []

        result = run_script(source, target)

        assert len(result["client"].searches) > 0
        first_search = result["client"].searches[0]
        assert "datetime" in first_search["kwargs"]
        assert "bbox" in first_search["kwargs"]
        assert first_search["collections"] == [SOURCE_COLLECTION]

    def test_datetime_format(self):
        """Datetime parameter should be in correct ISO format with Z suffix."""
        result = run_script([], [])

        datetime_param = result["client"].searches[0]["kwargs"]["datetime"]

        # Should be in format: "YYYY-MM-DDTHH:MM:SS.ffffffZ/YYYY-MM-DDTHH:MM:SS.ffffffZ"
        assert "/" in datetime_param
        start_str, end_str = datetime_param.split("/")

        # Both parts should end with Z (not +00:00Z)
        assert start_str.endswith("Z"), f"Start time should end with Z, got: {start_str}"
        assert end_str.endswith("Z"), f"End time should end with Z, got: {end_str}"

        # Should not have both timezone offset and Z
        assert "+00:00Z" not in start_str, "Should not have both +00:00 and Z"
        assert "+00:00Z" not in end_str, "Should not have both +00:00 and Z"

        # Verify they're valid ISO format timestamps
        from datetime import datetime

        # Remove Z and parse
        datetime.fromisoformat(start_str.rstrip("Z"))
        datetime.fromisoformat(end_str.rstrip("Z"))
