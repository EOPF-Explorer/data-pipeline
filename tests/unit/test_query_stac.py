"""Simple tests for query_stac.py script."""

import json
import tempfile
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import pytest
from pystac import Item, Link

# Test collection names
SOURCE_COLLECTION = "test"
TARGET_COLLECTION = "test-staging"

# Sentinel so run_script can distinguish "don't pass --max-cloud-cover" from "pass it blank".
_UNSET = object()


def create_stac_item(
    item_id: str,
    collection_id: str,
    has_self_link: bool = True,
    dt: datetime | None = datetime(2023, 12, 8, 10, 0, 0),
) -> Item:
    """Create a minimal STAC item for testing."""
    # pystac requires start/end when datetime is None; item.datetime still resolves to None.
    properties = (
        {}
        if dt is not None
        else {"start_datetime": "2024-01-01T00:00:00Z", "end_datetime": "2024-01-02T00:00:00Z"}
    )
    item = Item(
        id=item_id,
        geometry={"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]},
        bbox=[0, 0, 1, 1],
        datetime=dt,
        properties=properties,
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
    max_acquisition_age_days: int | None = None,
    max_cloud_cover: object = _UNSET,
    max_items: object = _UNSET,
) -> dict:
    """Helper to run `discover` mode with test data and read back the manifest files.

    The hardcoded scheduled end time is 2024-01-01T12:00:00Z, so an acquisition floor of
    N days lands at 2024-01-01 − N days — pick item datetimes relative to that.
    """
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
    argv = [
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
    ]
    if max_acquisition_age_days is not None:
        argv += ["--max-acquisition-age-days", str(max_acquisition_age_days)]
    # _UNSET ⇒ omit the flag entirely; any other value (including "") is passed through so
    # the blank-disables-filter path is exercisable.
    if max_cloud_cover is not _UNSET:
        argv += ["--max-cloud-cover", str(max_cloud_cover)]
    # _UNSET ⇒ omit the flag entirely (no cap); any other value (including "") is passed
    # through so the blank-disables-cap path is exercisable.
    if max_items is not _UNSET:
        argv += ["--max-items", str(max_items)]
    with (
        patch("scripts.query_stac.Client.open", side_effect=mock_client_open),
        patch("sys.argv", argv),
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
        assert (
            "Processed 1 pages, checked 3 items, 0 skipped (out of acquisition window), "
            "3 to process" in caplog.text
        )

    def test_excludes_items_already_in_target(self, caplog):
        """Items already in target collection should be excluded."""
        import logging

        caplog.set_level(logging.INFO)
        source, target = load_fixtures()

        result = run_script(source, target)

        assert len(result["output"]) == 2
        assert {item["item_id"] for item in result["output"]} == {"item-001", "item-003"}
        assert "Already converted" in caplog.text
        assert (
            "Processed 1 pages, checked 3 items, 0 skipped (out of acquisition window), "
            "2 to process" in caplog.text
        )

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
            "Processed 0 pages, checked 0 items, 0 skipped (out of acquisition window), "
            "0 to process"
            in caplog.text
            or "Processed 1 pages, checked 0 items, 0 skipped (out of acquisition window), "
            "0 to process"
            in caplog.text
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


class TestProcessingOrder:
    """`discover` orders the queue most-recent-first (acquisition datetime, descending)."""

    def test_items_sorted_most_recent_first(self):
        """Items are emitted newest acquisition datetime first, regardless of arrival order."""
        source = [
            create_stac_item("old", SOURCE_COLLECTION, dt=datetime(2024, 1, 1, 0, 0, 0)),
            create_stac_item("newest", SOURCE_COLLECTION, dt=datetime(2024, 6, 1, 0, 0, 0)),
            create_stac_item("middle", SOURCE_COLLECTION, dt=datetime(2024, 3, 1, 0, 0, 0)),
        ]

        result = run_script(source, [])

        assert [it["item_id"] for it in result["output"]] == ["newest", "middle", "old"]

    def test_sort_is_chronological_across_utc_offsets(self):
        """Sorting parses timestamps, so a later instant wins even with a larger offset.

        09:00+02:00 (07:00Z) is earlier than 08:00+00:00 (08:00Z); a lexicographic
        string sort would get this wrong, a chronological one ranks 08:00Z first.
        """
        source = [
            create_stac_item(
                "plus2", SOURCE_COLLECTION, dt=datetime.fromisoformat("2024-05-01T09:00:00+02:00")
            ),
            create_stac_item(
                "utc", SOURCE_COLLECTION, dt=datetime.fromisoformat("2024-05-01T08:00:00+00:00")
            ),
        ]

        result = run_script(source, [])

        assert [it["item_id"] for it in result["output"]] == ["utc", "plus2"]

    def test_items_without_datetime_sort_last(self):
        """An item with no acquisition datetime drains after dated items."""
        source = [
            create_stac_item("no-date", SOURCE_COLLECTION, dt=None),
            create_stac_item("dated", SOURCE_COLLECTION, dt=datetime(2024, 1, 1, 0, 0, 0)),
        ]

        result = run_script(source, [])

        assert [it["item_id"] for it in result["output"]] == ["dated", "no-date"]

    def test_output_carries_datetime_field(self):
        """Each record exposes the acquisition datetime used for ordering."""
        source = [create_stac_item("item-001", SOURCE_COLLECTION, dt=datetime(2024, 1, 1, 0, 0, 0))]

        result = run_script(source, [])

        assert result["output"][0]["datetime"] == "2024-01-01T00:00:00"


class TestMaxItems:
    """`discover --max-items N` caps the queue at the N newest, inside the tool."""

    def _eight_dated(self) -> list[Item]:
        # Distinct datetimes so newest-first order is unambiguous; supplied out of order.
        return [
            create_stac_item(f"item-{month:02d}", SOURCE_COLLECTION, dt=datetime(2024, month, 1))
            for month in (3, 8, 1, 6, 2, 7, 4, 5)
        ]

    def test_caps_to_newest_n(self):
        """With 8 candidates and --max-items 5, exactly the 5 newest survive, in order."""
        result = run_script(self._eight_dated(), [], max_items=5)

        assert [it["item_id"] for it in result["output"]] == [
            "item-08",
            "item-07",
            "item-06",
            "item-05",
            "item-04",
        ]

    def test_count_and_num_batches_reflect_truncation(self):
        """count and num_batches are computed AFTER the cap, not before."""
        result = run_script(self._eight_dated(), [], batch_size=2, max_items=5)

        assert result["count"] == 5
        assert result["num_batches"] == 3  # ceil(5/2)

    def test_no_flag_is_byte_identical(self):
        """Omitting the flag leaves all items (backward compatible)."""
        capped = run_script(self._eight_dated(), [], max_items=5)
        uncapped = run_script(self._eight_dated(), [])

        assert capped["count"] == 5
        assert uncapped["count"] == 8

    def test_blank_disables_cap(self):
        """An empty value (shared-template reuse path) means no cap."""
        result = run_script(self._eight_dated(), [], max_items="")

        assert result["count"] == 8

    def test_cap_larger_than_available_keeps_all(self):
        """A cap above the candidate count is a no-op, not an error."""
        result = run_script(self._eight_dated(), [], max_items=100)

        assert result["count"] == 8

    @pytest.mark.parametrize("bad", ["0", "-5"])
    def test_rejects_non_positive_cap(self, bad):
        """A non-positive cap would invert the bound (keep oldest / empty queue) — reject it."""
        with pytest.raises(SystemExit):
            run_script(self._eight_dated(), [], max_items=bad)


class TestAcquisitionFilter:
    """`--max-acquisition-age-days` keeps only recently-acquired items (scheduled end 2024-01-01)."""

    def test_drops_items_older_than_floor(self):
        """With a 7-day floor, an item acquired 31 days earlier is excluded; a recent one kept."""
        source = [
            create_stac_item("recent", SOURCE_COLLECTION, dt=datetime(2023, 12, 28, 12, 0, 0)),
            create_stac_item("stale", SOURCE_COLLECTION, dt=datetime(2023, 12, 1, 12, 0, 0)),
        ]

        result = run_script(source, [], max_acquisition_age_days=7)

        assert [it["item_id"] for it in result["output"]] == ["recent"]

    def test_boundary_item_at_floor_is_kept(self):
        """An item acquired exactly at the floor (end − N days) is kept (>= boundary)."""
        # floor = 2024-01-01T12:00:00Z − 7d = 2023-12-25T12:00:00Z
        source = [create_stac_item("edge", SOURCE_COLLECTION, dt=datetime(2023, 12, 25, 12, 0, 0))]

        result = run_script(source, [], max_acquisition_age_days=7)

        assert [it["item_id"] for it in result["output"]] == ["edge"]

    def test_naive_and_aware_datetimes_filter_without_raising(self):
        """Mixed naive (default factory) and tz-aware item datetimes compare safely."""
        source = [
            create_stac_item(
                "naive-recent", SOURCE_COLLECTION, dt=datetime(2023, 12, 28, 12, 0, 0)
            ),
            create_stac_item(
                "aware-recent",
                SOURCE_COLLECTION,
                dt=datetime(2023, 12, 27, 12, 0, 0, tzinfo=UTC),
            ),
            create_stac_item("aware-stale", SOURCE_COLLECTION, dt=datetime(2023, 11, 1, 12, 0, 0)),
        ]

        result = run_script(source, [], max_acquisition_age_days=7)

        assert {it["item_id"] for it in result["output"]} == {"naive-recent", "aware-recent"}

    def test_undated_item_excluded_when_filter_active(self):
        """An item with no acquisition datetime can't be proven recent → dropped when active."""
        source = [
            create_stac_item("dated", SOURCE_COLLECTION, dt=datetime(2023, 12, 28, 12, 0, 0)),
            create_stac_item("no-date", SOURCE_COLLECTION, dt=None),
        ]

        result = run_script(source, [], max_acquisition_age_days=7)

        assert [it["item_id"] for it in result["output"]] == ["dated"]

    def test_inactive_filter_passes_no_datetime_kwarg(self):
        """Without the flag, the search carries no `datetime` param (only the updated filter)."""
        source = [create_stac_item("a", SOURCE_COLLECTION, dt=datetime(2020, 1, 1, 0, 0, 0))]

        result = run_script(source, [])

        assert "datetime" not in result["source_client"].searches[0]["kwargs"]
        assert [it["item_id"] for it in result["output"]] == ["a"]  # old item still kept

    def test_active_filter_passes_datetime_floor_to_search(self):
        """When active, the source search is narrowed server-side with an open-ended floor."""
        source = [create_stac_item("a", SOURCE_COLLECTION, dt=datetime(2023, 12, 28, 12, 0, 0))]

        result = run_script(source, [], max_acquisition_age_days=7)

        assert (
            result["source_client"].searches[0]["kwargs"]["datetime"] == "2023-12-25T12:00:00Z/.."
        )

    def test_filtered_items_do_not_trigger_dedup_search(self):
        """Items dropped by the filter cost no target/dedup API call (filter runs before dedup)."""
        source = [
            create_stac_item("recent", SOURCE_COLLECTION, dt=datetime(2023, 12, 28, 12, 0, 0)),
            create_stac_item("stale", SOURCE_COLLECTION, dt=datetime(2023, 1, 1, 12, 0, 0)),
        ]

        result = run_script(source, [], max_acquisition_age_days=7)

        # Only the surviving item is checked against the target collection.
        assert len(result["target_client"].searches) == 1

    def test_survivors_remain_newest_first(self):
        """Filter then sort: kept items are still ordered most-recent acquisition first."""
        source = [
            create_stac_item("mid", SOURCE_COLLECTION, dt=datetime(2023, 12, 27, 12, 0, 0)),
            create_stac_item("stale", SOURCE_COLLECTION, dt=datetime(2023, 10, 1, 12, 0, 0)),
            create_stac_item("newest", SOURCE_COLLECTION, dt=datetime(2023, 12, 30, 12, 0, 0)),
        ]

        result = run_script(source, [], max_acquisition_age_days=14)

        assert [it["item_id"] for it in result["output"]] == ["newest", "mid"]

    def test_non_positive_age_is_rejected(self):
        """N must be > 0; a non-positive value exits rather than silently dropping everything."""
        source = [create_stac_item("a", SOURCE_COLLECTION)]

        for bad in (0, -5):
            with pytest.raises(SystemExit):
                run_script(source, [], max_acquisition_age_days=bad)


class TestCloudCoverFilter:
    """`--max-cloud-cover` adds a server-side CQL2 `eo:cloud_cover < N` predicate."""

    def test_filter_unset_stays_bare_between(self):
        """Without the flag the filter is the original bare `between` on `updated`."""
        result = run_script([], [])

        filter_param = result["source_client"].searches[0]["filter"]
        assert filter_param["op"] == "between"
        assert filter_param["args"][0] == {"property": "updated"}

    def test_filter_set_wraps_updated_and_cloud_cover_in_and(self):
        """With the flag the filter is `and(updated between …, eo:cloud_cover < N)`."""
        result = run_script([], [], max_cloud_cover=90)

        filter_param = result["source_client"].searches[0]["filter"]
        assert filter_param["op"] == "and"
        assert len(filter_param["args"]) == 2

        between_clause, cloud_clause = filter_param["args"]
        # Original updated-window predicate is preserved unchanged as the first arg.
        assert between_clause["op"] == "between"
        assert between_clause["args"][0] == {"property": "updated"}
        # Second arg is the strict-below cloud-cover predicate.
        assert cloud_clause == {"op": "<", "args": [{"property": "eo:cloud_cover"}, 90.0]}

    def test_filter_lang_still_cql2_json_when_set(self):
        """Adding the predicate must not change the declared filter language."""
        result = run_script([], [], max_cloud_cover=50)

        assert result["source_client"].searches[0]["filter_lang"] == "cql2-json"

    def test_blank_value_disables_filter(self):
        """A blank param (SAR/non-optical reuse path) parses to no cloud-cover predicate."""
        result = run_script([], [], max_cloud_cover="")

        filter_param = result["source_client"].searches[0]["filter"]
        assert filter_param["op"] == "between"  # bare between, no `and` wrapper

    def test_out_of_range_values_are_rejected(self):
        """Values outside (0, 100] exit rather than building a filter that keeps nothing/all."""
        for bad in (0, -5, 150):
            with pytest.raises(SystemExit):
                run_script([], [], max_cloud_cover=bad)

    def test_upper_boundary_is_inclusive(self):
        """100 is accepted (<= 100) and builds a strict `< 100` predicate."""
        result = run_script([], [], max_cloud_cover=100)

        filter_param = result["source_client"].searches[0]["filter"]
        assert filter_param["op"] == "and"
        assert filter_param["args"][1] == {
            "op": "<",
            "args": [{"property": "eo:cloud_cover"}, 100.0],
        }

    def test_cloud_and_acquisition_filters_coexist(self):
        """Prod runs both: `filter` gets the AND(cloud) and the `datetime` floor still applies."""
        result = run_script([], [], max_cloud_cover=90, max_acquisition_age_days=7)

        search = result["source_client"].searches[0]
        assert search["filter"]["op"] == "and"  # cloud predicate present
        assert "datetime" in search["kwargs"]  # acquisition narrowing not clobbered
