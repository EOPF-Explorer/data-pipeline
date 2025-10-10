"""Unit tests for publish_amqp.py script."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pika.exceptions
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
from publish_amqp import format_routing_key, load_payload


@pytest.fixture
def sample_payload() -> dict[str, str]:
    """Sample payload for tests."""
    return {"collection": "sentinel-2-l2a", "item_id": "test-123"}


@pytest.fixture
def payload_file(tmp_path: Path, sample_payload: dict[str, str]) -> Path:
    """Create a temporary payload file."""
    file = tmp_path / "payload.json"
    file.write_text(json.dumps(sample_payload))
    return file


class TestLoadPayload:
    """Tests for payload loading."""

    def test_valid_payload(self, payload_file: Path, sample_payload: dict[str, str]) -> None:
        """Load valid JSON payload."""
        assert load_payload(payload_file) == sample_payload

    def test_missing_file(self, tmp_path: Path) -> None:
        """Handle missing file with exit code 1."""
        with pytest.raises(SystemExit, match="1"):
            load_payload(tmp_path / "missing.json")

    def test_invalid_json(self, tmp_path: Path) -> None:
        """Handle invalid JSON with exit code 1."""
        invalid = tmp_path / "invalid.json"
        invalid.write_text("{not valid json")
        with pytest.raises(SystemExit, match="1"):
            load_payload(invalid)


class TestFormatRoutingKey:
    """Tests for routing key formatting."""

    @pytest.mark.parametrize(
        ("template", "payload", "expected"),
        [
            (
                "eopf.item.found.{collection}",
                {"collection": "sentinel-2-l2a"},
                "eopf.item.found.sentinel-2-l2a",
            ),
            (
                "{env}.{service}.{collection}",
                {"env": "prod", "service": "ingest", "collection": "s1"},
                "prod.ingest.s1",
            ),
            ("static.key", {"collection": "sentinel-2"}, "static.key"),
        ],
    )
    def test_format_templates(self, template: str, payload: dict[str, str], expected: str) -> None:
        """Format various routing key templates."""
        assert format_routing_key(template, payload) == expected

    def test_missing_field(self) -> None:
        """Handle missing field with exit code 1."""
        with pytest.raises(SystemExit, match="1"):
            format_routing_key("eopf.item.found.{collection}", {"item_id": "test"})


class TestPublishMessage:
    """Tests for message publishing (mocked)."""

    def test_publish_success(self, mocker) -> None:
        """Publish message successfully."""
        from publish_amqp import publish_message

        mock_conn = mocker.patch("publish_amqp.pika.BlockingConnection")
        mock_channel = mocker.MagicMock()
        mock_conn.return_value.channel.return_value = mock_channel

        publish_message(
            host="rabbitmq.test",
            port=5672,
            user="testuser",
            password="testpass",
            exchange="test_exchange",
            routing_key="test.key",
            payload={"test": "data"},
        )

        mock_conn.assert_called_once()
        mock_channel.basic_publish.assert_called_once()
        call = mock_channel.basic_publish.call_args.kwargs
        assert call["exchange"] == "test_exchange"
        assert call["routing_key"] == "test.key"
        assert json.loads(call["body"]) == {"test": "data"}

    def test_connection_retry(self, mocker) -> None:
        """Verify tenacity retry on transient failures."""
        from publish_amqp import publish_message

        mock_conn = mocker.patch("publish_amqp.pika.BlockingConnection")
        mock_channel = mocker.MagicMock()

        # Fail twice, succeed on third attempt
        mock_conn.side_effect = [
            pika.exceptions.AMQPConnectionError("Transient error"),
            pika.exceptions.AMQPConnectionError("Transient error"),
            mocker.MagicMock(channel=mocker.MagicMock(return_value=mock_channel)),
        ]

        publish_message(
            host="rabbitmq.test",
            port=5672,
            user="testuser",
            password="testpass",
            exchange="test_exchange",
            routing_key="test.key",
            payload={"test": "data"},
        )

        assert mock_conn.call_count == 3
