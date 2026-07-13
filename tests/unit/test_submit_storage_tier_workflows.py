"""Unit tests for operator-tools/submit_storage_tier_workflows.py."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from submit_storage_tier_workflows import (
    compute_age_cutoff,
    generate_time_windows,
    query_stac_items,
    resolve_window_bounds,
    submit_batch,
)


class _FakeAsset:
    def __init__(self, ref: str | None) -> None:
        s3: dict[str, object] = {}
        if ref is not None:
            s3["storage:refs"] = [ref]
        self.extra_fields = {"alternate": {"s3": s3}}


class _FakeItem:
    def __init__(self, item_id: str, ref: str | None) -> None:
        self.id = item_id
        self.assets = {"data": _FakeAsset(ref)}


class TestGenerateTimeWindows:
    def test_single_day(self) -> None:
        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = datetime(2024, 1, 2, tzinfo=UTC)
        windows = generate_time_windows(start, end)
        assert len(windows) == 1
        assert windows[0] == ("2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z")

    def test_multi_day(self) -> None:
        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = datetime(2024, 1, 4, tzinfo=UTC)
        windows = generate_time_windows(start, end)
        assert len(windows) == 3
        assert windows[0] == ("2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z")
        assert windows[1] == ("2024-01-02T00:00:00Z", "2024-01-03T00:00:00Z")
        assert windows[2] == ("2024-01-03T00:00:00Z", "2024-01-04T00:00:00Z")

    def test_partial_last_window(self) -> None:
        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = datetime(2024, 1, 2, 12, 0, 0, tzinfo=UTC)
        windows = generate_time_windows(start, end)
        assert len(windows) == 2
        assert windows[0] == ("2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z")
        assert windows[1] == ("2024-01-02T00:00:00Z", "2024-01-02T12:00:00Z")

    def test_custom_window_hours(self) -> None:
        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = datetime(2024, 1, 2, tzinfo=UTC)
        windows = generate_time_windows(start, end, window_hours=6)
        assert len(windows) == 4
        assert windows[0] == ("2024-01-01T00:00:00Z", "2024-01-01T06:00:00Z")
        assert windows[-1] == ("2024-01-01T18:00:00Z", "2024-01-02T00:00:00Z")

    def test_same_start_end_returns_empty(self) -> None:
        start = datetime(2024, 1, 1, tzinfo=UTC)
        windows = generate_time_windows(start, start)
        assert windows == []


class TestQueryStacItems:
    def test_returns_item_ids(self) -> None:
        mock_item_a = MagicMock()
        mock_item_a.id = "item-a"
        mock_item_b = MagicMock()
        mock_item_b.id = "item-b"

        mock_search = MagicMock()
        mock_search.items.return_value = [mock_item_a, mock_item_b]

        mock_catalog = MagicMock()
        mock_catalog.search.return_value = mock_search

        with patch("submit_storage_tier_workflows.Client") as mock_client:
            mock_client.open.return_value = mock_catalog
            result = query_stac_items(
                "https://stac.example.com",
                "sentinel-2",
                "2024-01-01T00:00:00Z",
                "2024-01-02T00:00:00Z",
            )

        assert result == ["item-a", "item-b"]
        mock_client.open.assert_called_once_with("https://stac.example.com")
        mock_catalog.search.assert_called_once_with(
            collections=["sentinel-2"],
            datetime="2024-01-01T00:00:00Z/2024-01-02T00:00:00Z",
            limit=100,
        )

    def test_returns_empty_list_when_no_items(self) -> None:
        mock_search = MagicMock()
        mock_search.items.return_value = []

        mock_catalog = MagicMock()
        mock_catalog.search.return_value = mock_search

        with patch("submit_storage_tier_workflows.Client") as mock_client:
            mock_client.open.return_value = mock_catalog
            result = query_stac_items(
                "https://stac.example.com",
                "sentinel-2",
                "2024-01-01T00:00:00Z",
                "2024-01-02T00:00:00Z",
            )

        assert result == []

    def test_date_field_created_uses_cql2_between(self) -> None:
        """A non-default date_field switches to a CQL2 `between` filter on that property."""
        mock_item = MagicMock()
        mock_item.id = "item-a"
        mock_search = MagicMock()
        mock_search.items.return_value = [mock_item]

        mock_catalog = MagicMock()
        mock_catalog.search.return_value = mock_search

        with patch("submit_storage_tier_workflows.Client") as mock_client:
            mock_client.open.return_value = mock_catalog
            result = query_stac_items(
                "https://stac.example.com",
                "sentinel-2",
                "2024-01-01T00:00:00Z",
                "2024-01-02T00:00:00Z",
                date_field="created",
            )

        assert result == ["item-a"]
        mock_catalog.search.assert_called_once_with(
            collections=["sentinel-2"],
            filter={
                "op": "between",
                "args": [
                    {"property": "created"},
                    "2024-01-01T00:00:00Z",
                    "2024-01-02T00:00:00Z",
                ],
            },
            filter_lang="cql2-json",
            limit=100,
        )

    def test_default_date_field_uses_datetime_range(self) -> None:
        """The default date_field keeps the native pystac-client datetime= range query."""
        mock_search = MagicMock()
        mock_search.items.return_value = []
        mock_catalog = MagicMock()
        mock_catalog.search.return_value = mock_search

        with patch("submit_storage_tier_workflows.Client") as mock_client:
            mock_client.open.return_value = mock_catalog
            query_stac_items(
                "https://stac.example.com",
                "sentinel-2",
                "2024-01-01T00:00:00Z",
                "2024-01-02T00:00:00Z",
                date_field="datetime",
            )

        mock_catalog.search.assert_called_once_with(
            collections=["sentinel-2"],
            datetime="2024-01-01T00:00:00Z/2024-01-02T00:00:00Z",
            limit=100,
        )


class TestComputeAgeCutoff:
    def test_subtracts_days_from_injected_today(self) -> None:
        today = datetime(2026, 7, 13, tzinfo=UTC)
        assert compute_age_cutoff(90, today=today) == datetime(2026, 4, 14, tzinfo=UTC)

    def test_zero_days_is_today(self) -> None:
        today = datetime(2026, 7, 13, 12, 0, 0, tzinfo=UTC)
        assert compute_age_cutoff(0, today=today) == today


class TestResolveWindowBounds:
    def test_min_age_days_open_lower_bound(self) -> None:
        """Age mode returns (None, today - min_age_days) — single-sided."""
        today = datetime(2026, 7, 13, tzinfo=UTC)
        start, end = resolve_window_bounds(
            min_age_days=90, start_date=None, end_date=None, today=today
        )
        assert start is None
        assert end == datetime(2026, 4, 14, tzinfo=UTC)

    def test_explicit_dates_pass_through(self) -> None:
        today = datetime(2026, 7, 13, tzinfo=UTC)
        start, end = resolve_window_bounds(
            min_age_days=None,
            start_date="2026-03-03",
            end_date="2026-04-08",
            today=today,
        )
        assert start == datetime(2026, 3, 3, tzinfo=UTC)
        assert end == datetime(2026, 4, 8, tzinfo=UTC)

    def test_both_modes_is_error(self) -> None:
        today = datetime(2026, 7, 13, tzinfo=UTC)
        with pytest.raises(ValueError, match="mutually exclusive"):
            resolve_window_bounds(
                min_age_days=90,
                start_date="2026-03-03",
                end_date="2026-04-08",
                today=today,
            )

    def test_neither_mode_is_error(self) -> None:
        today = datetime(2026, 7, 13, tzinfo=UTC)
        with pytest.raises(ValueError, match="--min-age-days"):
            resolve_window_bounds(min_age_days=None, start_date=None, end_date=None, today=today)

    def test_negative_min_age_days_is_error(self) -> None:
        today = datetime(2026, 7, 13, tzinfo=UTC)
        with pytest.raises(ValueError, match=">= 0"):
            resolve_window_bounds(min_age_days=-5, start_date=None, end_date=None, today=today)

    def test_explicit_end_before_start_is_error(self) -> None:
        today = datetime(2026, 7, 13, tzinfo=UTC)
        with pytest.raises(ValueError, match="after"):
            resolve_window_bounds(
                min_age_days=None,
                start_date="2026-06-01",
                end_date="2026-01-01",
                today=today,
            )


class TestQueryStacItemsTierFilter:
    def test_excludes_items_already_in_target_tier(self) -> None:
        """With target_storage_ref set, items already at that tier are dropped."""
        already = _FakeItem("already-standard", "standard")
        needs = _FakeItem("needs-move", "performance")
        mock_search = MagicMock()
        mock_search.items.return_value = [already, needs]
        mock_catalog = MagicMock()
        mock_catalog.search.return_value = mock_search

        with patch("submit_storage_tier_workflows.Client") as mock_client:
            mock_client.open.return_value = mock_catalog
            result = query_stac_items(
                "https://stac.example.com",
                "sentinel-2",
                "2026-01-01T00:00:00Z",
                "2026-04-14T00:00:00Z",
                date_field="created",
                target_storage_ref="standard",
            )

        assert result == ["needs-move"]

    def test_all_already_standard_returns_empty(self) -> None:
        """A window where every item is already STANDARD selects nothing (idempotent re-run)."""
        mock_search = MagicMock()
        mock_search.items.return_value = [
            _FakeItem("a", "standard"),
            _FakeItem("b", "standard"),
        ]
        mock_catalog = MagicMock()
        mock_catalog.search.return_value = mock_search

        with patch("submit_storage_tier_workflows.Client") as mock_client:
            mock_client.open.return_value = mock_catalog
            result = query_stac_items(
                "https://stac.example.com",
                "sentinel-2",
                None,
                "2026-04-14T00:00:00Z",
                date_field="created",
                target_storage_ref="standard",
            )

        assert result == []

    def test_open_lower_bound_uses_less_than_filter(self) -> None:
        """window_start=None issues a single-sided CQL2 '<' filter on the date field."""
        mock_search = MagicMock()
        mock_search.items.return_value = []
        mock_catalog = MagicMock()
        mock_catalog.search.return_value = mock_search

        with patch("submit_storage_tier_workflows.Client") as mock_client:
            mock_client.open.return_value = mock_catalog
            query_stac_items(
                "https://stac.example.com",
                "sentinel-2",
                None,
                "2026-04-14T00:00:00Z",
                date_field="created",
                target_storage_ref="standard",
            )

        mock_catalog.search.assert_called_once_with(
            collections=["sentinel-2"],
            filter={
                "op": "<",
                "args": [{"property": "created"}, "2026-04-14T00:00:00Z"],
            },
            filter_lang="cql2-json",
            limit=100,
        )


class TestSubmitBatch:
    def test_dry_run_does_not_send_request(self) -> None:
        payload: dict[str, object] = {
            "action": "batch-change-storage-tier",
            "item_ids": ["item-1", "item-2"],
        }
        with patch("submit_storage_tier_workflows.requests") as mock_requests:
            result = submit_batch("http://localhost:12000/samples", payload, dry_run=True)
        mock_requests.post.assert_not_called()
        assert result is True

    def test_success_returns_true(self) -> None:
        payload: dict[str, object] = {
            "action": "batch-change-storage-tier",
            "item_ids": ["item-1", "item-2"],
        }
        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch("submit_storage_tier_workflows.requests.post", return_value=mock_response):
            result = submit_batch("http://localhost:12000/samples", payload, dry_run=False)

        assert result is True

    def test_non_200_returns_false(self) -> None:
        payload: dict[str, object] = {
            "action": "batch-change-storage-tier",
            "item_ids": ["item-1"],
        }
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"

        with patch("submit_storage_tier_workflows.requests.post", return_value=mock_response):
            result = submit_batch("http://localhost:12000/samples", payload, dry_run=False)

        assert result is False

    def test_request_exception_returns_false(self) -> None:
        payload: dict[str, object] = {
            "action": "batch-change-storage-tier",
            "item_ids": ["item-1"],
        }

        with patch(
            "submit_storage_tier_workflows.requests.post",
            side_effect=Exception("connection refused"),
        ):
            result = submit_batch("http://localhost:12000/samples", payload, dry_run=False)

        assert result is False

    def test_payload_contains_item_ids_list(self) -> None:
        payload: dict[str, object] = {
            "action": "batch-change-storage-tier",
            "item_ids": ["item-a", "item-b", "item-c"],
        }
        mock_response = MagicMock()
        mock_response.status_code = 200
        captured: list[dict[str, object]] = []

        def capture_post(url: str, json: dict[str, object], **kwargs: object) -> MagicMock:
            captured.append(json)
            return mock_response

        with patch("submit_storage_tier_workflows.requests.post", side_effect=capture_post):
            submit_batch("http://localhost:12000/samples", payload, dry_run=False)

        assert captured[0]["item_ids"] == ["item-a", "item-b", "item-c"]


class TestMainEmptyWindowSkip:
    def test_empty_window_not_submitted(self) -> None:
        """Windows with no items should not trigger a POST."""
        with (
            patch(
                "sys.argv",
                [
                    "submit_storage_tier_workflows.py",
                    "--start-date",
                    "2024-01-01",
                    "--end-date",
                    "2024-01-03",
                    "--collection",
                    "sentinel-2-l2a",
                    "--dry-run",
                ],
            ),
            patch("submit_storage_tier_workflows.query_stac_items", return_value=[]),
            patch("submit_storage_tier_workflows.submit_batch") as mock_submit,
        ):
            from submit_storage_tier_workflows import main

            main()

        mock_submit.assert_not_called()

    def test_batch_payload_structure(self) -> None:
        """Submitted payload must contain item_ids list and correct action."""
        submitted_payloads: list[dict[str, object]] = []

        def capture(url: str, payload: dict[str, object], dry_run: bool) -> bool:
            submitted_payloads.append(payload)
            return True

        with (
            patch(
                "sys.argv",
                [
                    "submit_storage_tier_workflows.py",
                    "--start-date",
                    "2024-01-01",
                    "--end-date",
                    "2024-01-02",
                    "--collection",
                    "sentinel-2-l2a",
                    "--dry-run",
                ],
            ),
            patch(
                "submit_storage_tier_workflows.query_stac_items",
                return_value=["item-1", "item-2"],
            ),
            patch("submit_storage_tier_workflows.submit_batch", side_effect=capture),
        ):
            from submit_storage_tier_workflows import main

            main()

        assert len(submitted_payloads) == 1
        assert submitted_payloads[0]["item_ids"] == ["item-1", "item-2"]
        assert submitted_payloads[0]["action"] == "batch-change-storage-tier"
        assert "parallelism" not in submitted_payloads[0]


class TestMainDateFieldForwarding:
    def test_date_field_forwarded_to_query(self) -> None:
        """`--date-field created` reaches query_stac_items."""
        captured: list[str] = []

        def capture_query(
            stac_api_url: str,
            collection: str,
            window_start: str,
            window_end: str,
            date_field: str,
            target_storage_ref: str | None,
        ) -> list[str]:
            captured.append(date_field)
            return []

        with (
            patch(
                "sys.argv",
                [
                    "submit_storage_tier_workflows.py",
                    "--start-date",
                    "2024-01-01",
                    "--end-date",
                    "2024-01-02",
                    "--collection",
                    "sentinel-2-l2a",
                    "--date-field",
                    "created",
                    "--dry-run",
                ],
            ),
            patch(
                "submit_storage_tier_workflows.query_stac_items",
                side_effect=capture_query,
            ),
        ):
            from submit_storage_tier_workflows import main

            main()

        assert captured == ["created"]

    def test_default_storage_class_is_standard(self) -> None:
        """The submitted payload defaults storage_class to STANDARD."""
        submitted_payloads: list[dict[str, object]] = []

        def capture(url: str, payload: dict[str, object], dry_run: bool) -> bool:
            submitted_payloads.append(payload)
            return True

        with (
            patch(
                "sys.argv",
                [
                    "submit_storage_tier_workflows.py",
                    "--start-date",
                    "2024-01-01",
                    "--end-date",
                    "2024-01-02",
                    "--collection",
                    "sentinel-2-l2a",
                    "--dry-run",
                ],
            ),
            patch(
                "submit_storage_tier_workflows.query_stac_items",
                return_value=["item-1"],
            ),
            patch("submit_storage_tier_workflows.submit_batch", side_effect=capture),
        ):
            from submit_storage_tier_workflows import main

            main()

        assert submitted_payloads[0]["storage_class"] == "STANDARD"


class TestMainMinAgeMode:
    def test_min_age_days_issues_single_sided_tier_aware_query(self) -> None:
        """--min-age-days runs one open-lower-bound query with the target tier ref."""
        captured: list[dict[str, object]] = []

        def capture_query(
            stac_api_url: str,
            collection: str,
            window_start: str | None,
            window_end: str,
            date_field: str,
            target_storage_ref: str | None,
        ) -> list[str]:
            captured.append(
                {
                    "window_start": window_start,
                    "window_end": window_end,
                    "date_field": date_field,
                    "target_storage_ref": target_storage_ref,
                }
            )
            return []

        with (
            patch(
                "sys.argv",
                [
                    "submit_storage_tier_workflows.py",
                    "--min-age-days",
                    "90",
                    "--collection",
                    "sentinel-2-l2a",
                    "--date-field",
                    "created",
                    "--storage-class",
                    "STANDARD",
                    "--dry-run",
                ],
            ),
            patch(
                "submit_storage_tier_workflows._utcnow",
                return_value=datetime(2026, 7, 13, tzinfo=UTC),
            ),
            patch(
                "submit_storage_tier_workflows.query_stac_items",
                side_effect=capture_query,
            ),
        ):
            from submit_storage_tier_workflows import main

            main()

        assert len(captured) == 1
        assert captured[0]["window_start"] is None
        assert captured[0]["window_end"] == "2026-04-14T00:00:00Z"
        assert captured[0]["date_field"] == "created"
        assert captured[0]["target_storage_ref"] == "standard"

    def test_min_age_days_and_explicit_dates_exit(self) -> None:
        with patch(
            "sys.argv",
            [
                "submit_storage_tier_workflows.py",
                "--min-age-days",
                "90",
                "--start-date",
                "2026-03-03",
                "--end-date",
                "2026-04-08",
                "--collection",
                "sentinel-2-l2a",
            ],
        ):
            with pytest.raises(SystemExit) as exc_info:
                from submit_storage_tier_workflows import main

                main()
            assert exc_info.value.code == 1


class TestMainDateValidation:
    def test_end_before_start_exits(self) -> None:
        with patch(
            "sys.argv",
            [
                "submit_storage_tier_workflows.py",
                "--start-date",
                "2024-06-01",
                "--end-date",
                "2024-01-01",
                "--collection",
                "sentinel-2-l2a",
            ],
        ):
            with pytest.raises(SystemExit) as exc_info:
                from submit_storage_tier_workflows import main

                main()
            assert exc_info.value.code == 1

    def test_invalid_date_format_exits(self) -> None:
        with patch(
            "sys.argv",
            [
                "submit_storage_tier_workflows.py",
                "--start-date",
                "not-a-date",
                "--end-date",
                "2024-01-01",
                "--collection",
                "sentinel-2-l2a",
            ],
        ):
            with pytest.raises(SystemExit) as exc_info:
                from submit_storage_tier_workflows import main

                main()
            assert exc_info.value.code == 1
