"""Unit tests for operator-tools/submit_storage_tier_workflows.py."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from submit_storage_tier_workflows import (
    generate_time_windows,
    query_stac_items,
    submit_batch,
)


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


class TestSubmitBatch:
    def _make_nats_mock(self) -> tuple[AsyncMock, AsyncMock, MagicMock]:
        """Return (mock_nc, mock_js, mock_ack) with seq=1.

        nc.jetstream() is a sync call in nats-py, so use MagicMock for it.
        nc.connect() and js.publish() are async.
        """
        mock_ack = MagicMock()
        mock_ack.seq = 1
        mock_js = AsyncMock()
        mock_js.publish.return_value = mock_ack
        mock_nc = AsyncMock()
        mock_nc.jetstream = MagicMock(return_value=mock_js)
        return mock_nc, mock_js, mock_ack

    def test_dry_run_does_not_connect(self) -> None:
        payload: dict[str, object] = {
            "action": "batch-change-storage-tier",
            "item_ids": ["item-1", "item-2"],
        }
        with patch("submit_storage_tier_workflows.nats") as mock_nats:
            result = asyncio.run(submit_batch("nats://localhost:4222", payload, dry_run=True))
        mock_nats.connect.assert_not_called()
        assert result is True

    def test_success_returns_true(self) -> None:
        payload: dict[str, object] = {
            "action": "batch-change-storage-tier",
            "item_ids": ["item-1", "item-2"],
        }
        mock_nc, mock_js, _ = self._make_nats_mock()

        with patch("submit_storage_tier_workflows.nats.connect", return_value=mock_nc):
            result = asyncio.run(submit_batch("nats://localhost:4222", payload, dry_run=False))

        assert result is True
        mock_js.publish.assert_called_once()

    def test_publish_failure_returns_false(self) -> None:
        payload: dict[str, object] = {
            "action": "batch-change-storage-tier",
            "item_ids": ["item-1"],
        }
        mock_js = AsyncMock()
        mock_js.publish.side_effect = Exception("stream not found")
        mock_nc = AsyncMock()
        mock_nc.jetstream = MagicMock(return_value=mock_js)

        with patch("submit_storage_tier_workflows.nats.connect", return_value=mock_nc):
            result = asyncio.run(submit_batch("nats://localhost:4222", payload, dry_run=False))

        assert result is False

    def test_connect_failure_returns_false(self) -> None:
        payload: dict[str, object] = {
            "action": "batch-change-storage-tier",
            "item_ids": ["item-1"],
        }
        with patch(
            "submit_storage_tier_workflows.nats.connect",
            side_effect=Exception("connection refused"),
        ):
            result = asyncio.run(submit_batch("nats://localhost:4222", payload, dry_run=False))

        assert result is False

    def test_publishes_correct_subject_and_payload(self) -> None:
        payload: dict[str, object] = {
            "action": "batch-change-storage-tier",
            "item_ids": ["item-a", "item-b"],
        }
        mock_nc, mock_js, _ = self._make_nats_mock()

        with patch("submit_storage_tier_workflows.nats.connect", return_value=mock_nc):
            asyncio.run(submit_batch("nats://localhost:4222", payload, dry_run=False))

        call_args = mock_js.publish.call_args
        assert call_args[0][0] == "storage-tier-changes"
        published_data = json.loads(call_args[0][1])
        assert published_data["item_ids"] == ["item-a", "item-b"]


class TestMainEmptyWindowSkip:
    def test_empty_window_not_submitted(self) -> None:
        """Windows with no items should not trigger a NATS publish."""
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

        async def capture(nats_url: str, payload: dict[str, object], dry_run: bool) -> bool:
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
            patch("submit_storage_tier_workflows.submit_batch", new=capture),
        ):
            from submit_storage_tier_workflows import main

            main()

        assert len(submitted_payloads) == 1
        assert submitted_payloads[0]["item_ids"] == ["item-1", "item-2"]
        assert submitted_payloads[0]["action"] == "batch-change-storage-tier"
        assert "parallelism" not in submitted_payloads[0]


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
