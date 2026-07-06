"""Unit tests for scripts/stac_auth.py — OIDC client-credentials auth helper.

Contract: a no-op when OIDC env is absent (local/dev writes stay unauthenticated),
a cached client-credentials bearer when it is present, and a clear error — never a
silent unauthenticated write — when a configured token endpoint fails.

Shared scaffolding (OIDC env, token cache reset, token_response factory) lives in
tests/unit/conftest.py.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

# Add scripts directory to path
scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

import stac_auth  # noqa: E402


def _prepared(url="https://stac.example.com/collections"):
    return requests.Request("POST", url).prepare()


# --- No env: unauthenticated back-compat -------------------------------------


def test_get_token_returns_none_without_env():
    assert stac_auth.get_token() is None


def test_auth_headers_empty_without_env():
    assert stac_auth.auth_headers() == {}


def test_bearer_auth_noop_without_env():
    assert "Authorization" not in stac_auth.bearer_auth(_prepared()).headers


def test_open_client_wires_bearer_auth_hook():
    """open_client opens unauthenticated (public landing page) and wires the per-request
    auth hook onto the session — regardless of env (the hook is itself a no-op unset)."""
    with patch("stac_auth.Client.open") as mock_open:
        client = stac_auth.open_client("https://stac.example.com")
    mock_open.assert_called_once_with("https://stac.example.com")
    assert client._stac_io.session.auth is stac_auth.bearer_auth


# --- Env set: token fetched, cached, injected --------------------------------


def test_get_token_fetches_and_caches(oidc_env, token_response):
    with patch("stac_auth.httpx.post", return_value=token_response()) as mock_post:
        assert stac_auth.get_token() == "test-token"
        assert stac_auth.get_token() == "test-token"  # served from cache
    mock_post.assert_called_once()
    # client-credentials grant with the configured client id/secret
    _, kwargs = mock_post.call_args
    assert kwargs["data"]["grant_type"] == "client_credentials"
    assert kwargs["data"]["client_id"] == "stac-writer"
    assert kwargs["data"]["client_secret"] == "s3cr3t"  # noqa: S105


def test_auth_headers_carries_bearer(oidc_env, token_response):
    with patch("stac_auth.httpx.post", return_value=token_response()):
        assert stac_auth.auth_headers() == {"Authorization": "Bearer test-token"}


def test_bearer_auth_attaches_header(oidc_env, token_response):
    with patch("stac_auth.httpx.post", return_value=token_response()):
        req = stac_auth.bearer_auth(_prepared())
    assert req.headers["Authorization"] == "Bearer test-token"


# --- Cache expiry refetch ----------------------------------------------------


def test_token_refetched_after_expiry(oidc_env, token_response):
    clock = {"now": 0.0}
    with (
        patch("stac_auth.time.monotonic", side_effect=lambda: clock["now"]),
        patch("stac_auth.httpx.post", return_value=token_response(expires_in=300)) as mock_post,
    ):
        assert stac_auth.get_token() == "test-token"  # cached until 300 - margin
        clock["now"] = 280.0  # past the safety-margin expiry
        assert stac_auth.get_token() == "test-token"
    assert mock_post.call_count == 2


# --- Adversarial: never silently unauthenticated -----------------------------


def test_token_endpoint_http_error_raises(oidc_env):
    bad = MagicMock()
    bad.raise_for_status.side_effect = RuntimeError("401 Unauthorized")
    with patch("stac_auth.httpx.post", return_value=bad), pytest.raises(RuntimeError):
        stac_auth.get_token()


def test_token_response_without_access_token_raises(oidc_env):
    empty = MagicMock()
    empty.raise_for_status.return_value = None
    empty.json.return_value = {"expires_in": 300}
    with patch("stac_auth.httpx.post", return_value=empty), pytest.raises(RuntimeError):
        stac_auth.get_token()


def test_partial_env_is_noop(monkeypatch):
    """Missing any one of the three vars → unauthenticated (no half-configured writes)."""
    monkeypatch.setenv("OIDC_TOKEN_URL", "https://kc.example.com/token")
    monkeypatch.setenv("OIDC_CLIENT_ID", "stac-writer")
    # OIDC_CLIENT_SECRET intentionally unset
    assert stac_auth.get_token() is None
