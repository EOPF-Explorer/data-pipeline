"""Simple tests for query_stac.py script."""

import json
import tempfile
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
        items: list[Item],
        collection: str,
        raise_error: bool = False,
    ):
        self.items = items
        self.collection = collection
        self.raise_error = raise_error
        self.searches = []

    def search(
        self,
        collections: list[str] | None = None,
        ids: list[str] | None = None,
        filter=None,
        filter_lang=None,
        **kwargs,
    ):
        self.searches.append(
            {
                "collections": collections,
                "ids": ids,
                "filter": filter,
                "filter_lang": filter_lang,
                "kwargs": kwargs,
            }
        )

        if self.raise_error:
            raise Exception("Simulated API error")

        if collections and self.collection in collections:
            if ids:
                matching = [item for item in self.items if item.id in ids]
                return FakeItemSearch(matching)
            return FakeItemSearch(self.items)

        return FakeItemSearch([])


def run_script(
    source_items: list[Item],
    target_items: list[Item],
    raise_error_on_target: bool = False,
    batch_size: int = 200,
) -> dict:
    """Helper to run `discover` mode with test data and read back the manifest files."""
    source_client = FakeStacClient(
        items=source_items,
        collection=SOURCE_COLLECTION,
        raise_error=False,
    )

    target_client = FakeStacClient(
        items=target_items,
        collection=TARGET_COLLECTION,
        raise_error=raise_error_on_target,
    )

    def mock_client_open(url: str):
        """Return appropriate client based on URL."""
        if "source" in url:
            return source_client
        return target_client

    out_dir = tempfile.mkdtemp()
    with (
        patch("scripts.query_stac.Client.open", side_effect=mock_client_open),
        patch(
            "sys.argv",
            [
                "query_stac.py",
                "discover",
                "https://source-stac.example.com/",
                SOURCE_COLLECTION,
                "https://target-stac.example.com/",
                TARGET_COLLECTION,
                "2024-01-01T12:00:00Z",  # ISO timestamp instead of "0"
                "3",
                "[-5.14, 41.33, 9.56, 51.09]",
                "--out-dir",
                out_dir,
                "--batch-size",
                str(batch_size),
            ],
        ),
        patch("sys.stdout", StringIO()) as stdout,
        patch("sys.stderr", StringIO()) as stderr,
    ):
        from scripts.query_stac import main

        main()

    return {
        "output": json.loads((Path(out_dir) / "items.json").read_text()),
        "count": int((Path(out_dir) / "count").read_text()),
        "num_batches": int((Path(out_dir) / "num_batches").read_text()),
        "stdout": stdout.getvalue(),
        "stderr": stderr.getvalue(),
        "out_dir": out_dir,
        "source_client": source_client,
        "target_client": target_client,
    }


def run_read_batch(items: list[dict], index: int, batch_size: int = 200) -> list[dict]:
    """Run `read-batch` mode against a temp items file; return the emitted slice."""
    out_dir = tempfile.mkdtemp()
    items_file = Path(out_dir) / "items.json"
    items_file.write_text(json.dumps(items))

    with (
        patch(
            "sys.argv",
            [
                "query_stac.py",
                "read-batch",
                str(items_file),
                str(index),
                "--batch-size",
                str(batch_size),
            ],
        ),
        patch("sys.stdout", StringIO()) as stdout,
    ):
        from scripts.query_stac import main

        main()

    return json.loads(stdout.getvalue())


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
        assert "Processed 1 pages, checked 3 items, 3 to process" in caplog.text

    def test_excludes_items_already_in_target(self, caplog):
        """Items already in target collection should be excluded."""
        import logging

        caplog.set_level(logging.INFO)
        source, target = load_fixtures()

        result = run_script(source, target)

        assert len(result["output"]) == 2
        assert {item["item_id"] for item in result["output"]} == {"item-001", "item-003"}
        assert "Already converted" in caplog.text
        assert "Processed 1 pages, checked 3 items, 2 to process" in caplog.text

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
        # Allow for either 0 or 1 pages depending on STAC client behavior
        assert (
            "Processed 0 pages, checked 0 items, 0 to process" in caplog.text
            or "Processed 1 pages, checked 0 items, 0 to process" in caplog.text
        )

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

        assert len(result["source_client"].searches) > 0
        first_search = result["source_client"].searches[0]
        assert first_search["filter"] is not None
        assert first_search["filter_lang"] == "cql2-json"
        assert "bbox" in first_search["kwargs"]
        assert first_search["collections"] == [SOURCE_COLLECTION]

    def test_cql2_filter_format(self):
        """CQL2 filter should have correct structure for updated property query."""
        result = run_script([], [])

        filter_param = result["source_client"].searches[0]["filter"]

        # Should be a CQL2 between filter
        assert filter_param["op"] == "between"
        assert len(filter_param["args"]) == 3

        # First arg should be the property
        assert filter_param["args"][0] == {"property": "updated"}

        # Both timestamps should end with Z
        start_str, end_str = filter_param["args"][1], filter_param["args"][2]
        assert start_str.endswith("Z"), f"Start time should end with Z, got: {start_str}"
        assert end_str.endswith("Z"), f"End time should end with Z, got: {end_str}"

        # Verify they're valid ISO format timestamps
        from datetime import datetime

        datetime.fromisoformat(start_str.rstrip("Z"))
        datetime.fromisoformat(end_str.rstrip("Z"))

    def test_queries_updated_property_not_datetime(self):
        """Verify that we query the 'updated' property for harvesting, not acquisition datetime."""
        result = run_script([], [])

        filter_param = result["source_client"].searches[0]["filter"]

        # Should target the 'updated' property specifically
        assert filter_param["args"][0]["property"] == "updated"

        # Should NOT use datetime parameter (which queries acquisition date)
        assert "datetime" not in result["source_client"].searches[0]["kwargs"]

        # Should use CQL2 filter language
        assert result["source_client"].searches[0]["filter_lang"] == "cql2-json"


class TestBatchedOutput:
    """`discover` writes a manifest + items file; `read-batch` emits bounded slices."""

    def test_discover_writes_manifest_files(self):
        """discover writes items.json, count, and num_batches = ceil(count/batch_size)."""
        source = [create_stac_item(f"item-{i:03d}", SOURCE_COLLECTION) for i in range(5)]

        result = run_script(source, [], batch_size=2)

        assert result["count"] == 5
        assert len(result["output"]) == 5
        assert result["num_batches"] == 3  # ceil(5/2)

    def test_discover_does_not_emit_list_to_stdout(self):
        """The full list must NOT go to stdout — that was the 256 KB withParam gate."""
        source = [create_stac_item(f"item-{i:04d}", SOURCE_COLLECTION) for i in range(300)]

        result = run_script(source, [], batch_size=200)

        assert "source_url" not in result["stdout"]

    def test_num_batches_boundaries(self):
        """Exactly-full and one-over batch counts compute the right num_batches."""
        s4 = [create_stac_item(f"i{n:02d}", SOURCE_COLLECTION) for n in range(4)]
        s5 = [create_stac_item(f"i{n:02d}", SOURCE_COLLECTION) for n in range(5)]

        assert run_script(s4, [], batch_size=2)["num_batches"] == 2
        assert run_script(s5, [], batch_size=2)["num_batches"] == 3

    def test_empty_discovery_zero_batches(self):
        """No items → count 0, num_batches 0, empty items file."""
        result = run_script([], [], batch_size=200)

        assert result["count"] == 0
        assert result["num_batches"] == 0
        assert result["output"] == []

    def test_read_batch_slices_within_bounds(self):
        """read-batch returns exactly items [index*size : (index+1)*size]."""
        items = [
            {"source_url": f"u{i}", "collection": "c", "item_id": f"item-{i:03d}"} for i in range(5)
        ]

        assert [i["item_id"] for i in run_read_batch(items, 0, 2)] == ["item-000", "item-001"]
        assert [i["item_id"] for i in run_read_batch(items, 1, 2)] == ["item-002", "item-003"]
        assert [i["item_id"] for i in run_read_batch(items, 2, 2)] == ["item-004"]

    def test_read_batch_is_lossless_across_all_batches(self):
        """The union of every batch equals the full list — nothing dropped or duplicated."""
        items = [
            {"source_url": f"u{i}", "collection": "c", "item_id": f"item-{i:04d}"}
            for i in range(450)
        ]
        batch_size = 200
        num_batches = (len(items) + batch_size - 1) // batch_size

        union: list[dict] = []
        for idx in range(num_batches):
            batch = run_read_batch(items, idx, batch_size)
            assert len(batch) <= batch_size
            union += batch

        assert union == items

    def test_read_batch_out_of_range_is_empty(self):
        """An index past the end yields an empty list (clean short-circuit)."""
        items = [{"source_url": "u", "collection": "c", "item_id": "x"}]

        assert run_read_batch(items, 5, 2) == []

    def test_read_batch_bound_is_count_based_not_byte_based(self):
        """Large/unicode payloads don't change the per-batch item count bound."""
        items = [
            {"source_url": "u" * 500, "collection": "c", "item_id": "💧" * 50 + str(i)}
            for i in range(10)
        ]

        assert len(run_read_batch(items, 0, 4)) == 4
