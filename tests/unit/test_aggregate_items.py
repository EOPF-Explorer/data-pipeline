"""Tests for scripts/aggregate_items.py."""

import collections
from datetime import datetime
from io import StringIO
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from pystac import Item

from scripts.aggregate_items import (
    build_daily_aggregation,
    build_monthly_aggregation,
    count_items_by_datetime,
    main,
    update_collection_links,
)


def _make_item(item_id: str, dt: datetime | None = None, dt_str: str | None = None) -> Item:
    """Create a minimal STAC item with a datetime.

    When *dt* is None, pystac requires start/end_datetime — we supply dummy
    values so the Item can be constructed for testing fallback paths.
    """
    props: dict = {}
    extra_kwargs: dict = {}
    if dt_str:
        props["datetime"] = dt_str
    if dt is None:
        # pystac enforces: if datetime is None, start/end must be provided
        extra_kwargs["start_datetime"] = datetime(2000, 1, 1)
        extra_kwargs["end_datetime"] = datetime(2000, 1, 2)
    item = Item(
        id=item_id,
        geometry={"type": "Point", "coordinates": [0, 0]},
        bbox=[0, 0, 0, 0],
        datetime=dt,
        properties=props,
        **extra_kwargs,
    )
    return item


class FakeItemSearch:
    """Simulates STAC search results for aggregation tests."""

    def __init__(self, items: list[Item]):
        self._items = items

    def pages(self):
        return [SimpleNamespace(items=self._items)]


class FakeStacClient:
    """Simulates a pystac_client.Client."""

    def __init__(self, items: list[Item]):
        self._items = items
        self.search_calls: list[dict] = []

    def search(self, **kwargs):
        self.search_calls.append(kwargs)
        return FakeItemSearch(self._items)


# --- Pure function tests (no mocking) ---


class TestBuildDailyAggregation:
    def test_format(self):
        counts: collections.Counter[str] = collections.Counter({"2026-01-15": 10, "2026-01-16": 20})
        result = build_daily_aggregation(counts)

        assert result["type"] == "AggregationCollection"
        assert len(result["aggregations"]) == 1
        agg = result["aggregations"][0]
        assert agg["key"] == "datetime_daily"
        assert agg["interval"] == "daily"
        assert len(agg["buckets"]) == 2
        assert agg["buckets"][0] == {"key": "2026-01-15T00:00:00.000Z", "value": 10}
        assert agg["buckets"][1] == {"key": "2026-01-16T00:00:00.000Z", "value": 20}

    def test_empty_counter(self):
        result = build_daily_aggregation(collections.Counter())
        assert result["aggregations"][0]["buckets"] == []

    def test_sorted_chronologically(self):
        counts: collections.Counter[str] = collections.Counter(
            {"2026-03-01": 1, "2025-12-01": 2, "2026-01-15": 3}
        )
        result = build_daily_aggregation(counts)
        keys = [b["key"] for b in result["aggregations"][0]["buckets"]]
        assert keys == [
            "2025-12-01T00:00:00.000Z",
            "2026-01-15T00:00:00.000Z",
            "2026-03-01T00:00:00.000Z",
        ]

    def test_values_are_integers(self):
        counts: collections.Counter[str] = collections.Counter({"2026-01-01": 523})
        result = build_daily_aggregation(counts)
        value = result["aggregations"][0]["buckets"][0]["value"]
        assert isinstance(value, int)
        assert value == 523


class TestBuildMonthlyAggregation:
    def test_sums_days_correctly(self):
        counts: collections.Counter[str] = collections.Counter(
            {"2026-01-15": 10, "2026-01-16": 20, "2026-02-01": 5}
        )
        result = build_monthly_aggregation(counts)

        agg = result["aggregations"][0]
        assert agg["key"] == "datetime_monthly"
        assert agg["interval"] == "monthly"
        assert len(agg["buckets"]) == 2
        assert agg["buckets"][0] == {"key": "2026-01-01T00:00:00.000Z", "value": 30}
        assert agg["buckets"][1] == {"key": "2026-02-01T00:00:00.000Z", "value": 5}

    def test_sorted_chronologically(self):
        counts: collections.Counter[str] = collections.Counter(
            {"2026-03-01": 1, "2025-12-15": 2, "2026-01-10": 3}
        )
        result = build_monthly_aggregation(counts)
        keys = [b["key"] for b in result["aggregations"][0]["buckets"]]
        assert keys == [
            "2025-12-01T00:00:00.000Z",
            "2026-01-01T00:00:00.000Z",
            "2026-03-01T00:00:00.000Z",
        ]

    def test_empty_counter(self):
        result = build_monthly_aggregation(collections.Counter())
        assert result["aggregations"][0]["buckets"] == []


# --- Mocked STAC tests ---


class TestCountItemsByDatetime:
    def test_counts_items_by_date(self):
        items = [
            _make_item("a", dt=datetime(2026, 1, 15, 10, 0)),
            _make_item("b", dt=datetime(2026, 1, 15, 11, 0)),
            _make_item("c", dt=datetime(2026, 1, 16, 9, 0)),
        ]
        client = FakeStacClient(items)

        with patch("scripts.aggregate_items.Client.open", return_value=client):
            result = count_items_by_datetime("https://stac.test", "test-collection")

        assert result["2026-01-15"] == 2
        assert result["2026-01-16"] == 1

    def test_falls_back_to_properties_datetime(self):
        items = [
            _make_item("a", dt=None, dt_str="2026-02-20T12:00:00Z"),
        ]
        client = FakeStacClient(items)

        with patch("scripts.aggregate_items.Client.open", return_value=client):
            result = count_items_by_datetime("https://stac.test", "test-collection")

        assert result["2026-02-20"] == 1

    def test_skips_items_without_datetime(self):
        items = [
            _make_item("a", dt=None),
            _make_item("b", dt=datetime(2026, 1, 1, 0, 0)),
        ]
        client = FakeStacClient(items)

        with patch("scripts.aggregate_items.Client.open", return_value=client):
            result = count_items_by_datetime("https://stac.test", "test-collection")

        assert result["2026-01-01"] == 1
        assert len(result) == 1

    def test_passes_fields_and_limit(self):
        client = FakeStacClient([])

        with patch("scripts.aggregate_items.Client.open", return_value=client):
            count_items_by_datetime("https://stac.test", "my-collection")

        assert len(client.search_calls) == 1
        call = client.search_calls[0]
        assert call["collections"] == ["my-collection"]
        assert call["limit"] == 1000
        assert "fields" in call

    def test_empty_collection(self):
        client = FakeStacClient([])

        with patch("scripts.aggregate_items.Client.open", return_value=client):
            result = count_items_by_datetime("https://stac.test", "empty")

        assert len(result) == 0


# --- Collection link tests ---


class TestUpdateCollectionLinks:
    def test_removes_old_pre_aggregation_links(self):
        collection_data = {
            "id": "test",
            "links": [
                {"rel": "self", "href": "https://api.test/collections/test"},
                {"rel": "pre-aggregation", "href": "https://old/daily.json"},
                {"rel": "root", "href": "https://api.test"},
            ],
        }

        mock_response_get = MagicMock()
        mock_response_get.json.return_value = collection_data
        mock_response_get.raise_for_status = MagicMock()

        mock_response_put = MagicMock()
        mock_response_put.raise_for_status = MagicMock()

        captured_put_data = {}

        def mock_put(url, json=None, headers=None):  # noqa: ARG001
            captured_put_data["json"] = json
            return mock_response_put

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = mock_response_get
        mock_client.put = mock_put

        with patch("scripts.aggregate_items.httpx.Client", return_value=mock_client):
            update_collection_links(
                "https://api.test/stac",
                "test",
                "https://s3.gateway.test",
                "my-bucket",
                "aggregations",
            )

        links = captured_put_data["json"]["links"]
        pre_agg_links = [lk for lk in links if lk["rel"] == "pre-aggregation"]
        other_links = [lk for lk in links if lk["rel"] != "pre-aggregation"]

        # Old pre-aggregation link removed, two new ones added
        assert len(pre_agg_links) == 2
        assert pre_agg_links[0]["aggregation:interval"] == "daily"
        assert pre_agg_links[1]["aggregation:interval"] == "monthly"

        # Other links preserved
        assert len(other_links) == 2
        rels = {lk["rel"] for lk in other_links}
        assert rels == {"self", "root"}

    def test_link_hrefs(self):
        collection_data = {"id": "s2", "links": []}

        mock_response_get = MagicMock()
        mock_response_get.json.return_value = collection_data
        mock_response_get.raise_for_status = MagicMock()

        mock_response_put = MagicMock()
        mock_response_put.raise_for_status = MagicMock()

        captured_put_data = {}

        def mock_put(url, json=None, headers=None):  # noqa: ARG001
            captured_put_data["json"] = json
            return mock_response_put

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = mock_response_get
        mock_client.put = mock_put

        with patch("scripts.aggregate_items.httpx.Client", return_value=mock_client):
            update_collection_links(
                "https://api.test/stac",
                "s2",
                "https://s3.explorer.eopf.copernicus.eu",
                "my-bucket",
                "agg",
            )

        links = captured_put_data["json"]["links"]
        assert links[0]["href"] == (
            "https://s3.explorer.eopf.copernicus.eu/my-bucket/agg/s2/daily.json"
        )
        assert links[1]["href"] == (
            "https://s3.explorer.eopf.copernicus.eu/my-bucket/agg/s2/monthly.json"
        )


# --- Integration tests (main with --dry-run) ---


class TestMainDryRun:
    def test_dry_run_outputs_json(self):
        items = [
            _make_item("a", dt=datetime(2026, 1, 15, 10, 0)),
            _make_item("b", dt=datetime(2026, 1, 16, 11, 0)),
        ]
        client = FakeStacClient(items)

        with (
            patch("scripts.aggregate_items.Client.open", return_value=client),
            patch("sys.stdout", new_callable=StringIO) as mock_stdout,
        ):
            rc = main(
                [
                    "--collection",
                    "test",
                    "--stac-api-url",
                    "https://stac.test",
                    "--s3-bucket",
                    "bucket",
                    "--dry-run",
                ]
            )

        assert rc == 0
        output = mock_stdout.getvalue()
        # Should contain two JSON documents (daily + monthly)
        assert '"AggregationCollection"' in output
        assert '"datetime_daily"' in output
        assert '"datetime_monthly"' in output

    def test_empty_collection_returns_zero(self):
        client = FakeStacClient([])

        with patch("scripts.aggregate_items.Client.open", return_value=client):
            rc = main(
                [
                    "--collection",
                    "empty",
                    "--stac-api-url",
                    "https://stac.test",
                    "--s3-bucket",
                    "bucket",
                    "--dry-run",
                ]
            )

        assert rc == 0
