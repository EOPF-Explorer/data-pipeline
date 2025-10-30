"""Unit tests for metrics.py."""

from unittest.mock import patch

import pytest


@pytest.fixture
def mock_http_server():
    """Mock prometheus HTTP server."""
    with patch("scripts.metrics.start_http_server") as mock:
        yield mock


def test_start_metrics_server_default_port(mock_http_server):
    """Test metrics server starts on default port."""
    from scripts import metrics

    metrics.start_metrics_server()
    mock_http_server.assert_called_once_with(8000)


def test_start_metrics_server_custom_port(mock_http_server):
    """Test metrics server starts on custom port."""
    from scripts import metrics

    metrics.start_metrics_server(port=9090)
    mock_http_server.assert_called_once_with(9090)


def test_start_metrics_server_env_port(mock_http_server):
    """Test metrics server uses METRICS_PORT env var."""
    from scripts import metrics

    with patch.dict("os.environ", {"METRICS_PORT": "9999"}):
        metrics.start_metrics_server()
        mock_http_server.assert_called_once_with(9999)


def test_start_metrics_server_port_in_use(mock_http_server):
    """Test metrics server handles port already in use."""
    from scripts import metrics

    mock_http_server.side_effect = OSError("Address already in use")
    metrics.start_metrics_server(port=8000)  # Should not raise


@pytest.mark.parametrize(
    "env_value,expected",
    [
        (None, True),  # Default
        ("true", True),
        ("TRUE", True),
        ("True", True),
        ("false", False),
        ("invalid", False),
    ],
)
def test_is_metrics_enabled(env_value, expected):
    """Test metrics enabled check with various env values."""
    from scripts import metrics

    env = {"ENABLE_METRICS": env_value} if env_value else {}
    with patch.dict("os.environ", env, clear=True):
        assert metrics.is_metrics_enabled() is expected
