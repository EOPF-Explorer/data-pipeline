"""Unit tests for scripts/stac_auth.py — OIDC client-credentials auth helper.

Contract: a no-op when OIDC env is absent (local/dev writes stay unauthenticated),
a cached client-credentials bearer when it is present, and a clear error — never a
silent unauthenticated write — when a configured token endpoint fails.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Add scripts directory to path
scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

import stac_auth  # noqa: E402

OIDC_ENV = {
    "OIDC_TOKEN_URL": "https://kc.example.com/realms/eoxhub/protocol/openid-connect/token",
    "OIDC_CLIENT_ID": "stac-writer",
    "OIDC_CLIENT_SECRET": "s3cr3t",
}


@pytest.fixture(autouse=True)
def _reset_cache_and_env(monkeypatch):
    """Each test starts with a cleared token cache and no OIDC env."""
    for key in OIDC_ENV:
        monkeypatch.delenv(key, raising=False)
    stac_auth._cached_token = None
    stac_auth._cached_expiry = 0.0
    yield


def _set_env(monkeypatch):
    for key, value in OIDC_ENV.items():
        monkeypatch.setenv(key, value)


def _token_response(access_token="test-token", expires_in=300):  # noqa: S107
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = {"access_token": access_token, "expires_in": expires_in}
    return resp


# --- No env: unauthenticated back-compat -------------------------------------


def test_get_token_returns_none_without_env():
    assert stac_auth.get_token() is None


def test_auth_headers_empty_without_env():
    assert stac_auth.auth_headers() == {}


def test_open_client_unauthenticated_without_env():
    with patch("stac_auth.Client.open") as mock_open:
        stac_auth.open_client("https://stac.example.com")
    mock_open.assert_called_once_with("https://stac.example.com", headers=None)


# --- Env set: token fetched, cached, injected --------------------------------


def test_get_token_fetches_and_caches(monkeypatch):
    _set_env(monkeypatch)
    with patch("stac_auth.httpx.post", return_value=_token_response()) as mock_post:
        assert stac_auth.get_token() == "test-token"
        assert stac_auth.get_token() == "test-token"  # served from cache
    mock_post.assert_called_once()
    # client-credentials grant with the configured client id/secret
    _, kwargs = mock_post.call_args
    assert kwargs["data"]["grant_type"] == "client_credentials"
    assert kwargs["data"]["client_id"] == "stac-writer"
    assert kwargs["data"]["client_secret"] == "s3cr3t"  # noqa: S105


def test_auth_headers_carries_bearer(monkeypatch):
    _set_env(monkeypatch)
    with patch("stac_auth.httpx.post", return_value=_token_response()):
        assert stac_auth.auth_headers() == {"Authorization": "Bearer test-token"}


def test_open_client_injects_bearer(monkeypatch):
    _set_env(monkeypatch)
    with (
        patch("stac_auth.httpx.post", return_value=_token_response()),
        patch("stac_auth.Client.open") as mock_open,
    ):
        stac_auth.open_client("https://stac.example.com")
    mock_open.assert_called_once_with(
        "https://stac.example.com", headers={"Authorization": "Bearer test-token"}
    )


def test_pystac_applies_header_to_session():
    """Locks the plan's verified assumption: headers reach the requests session,
    so raw session.post/delete (upsert_item) inherit Authorization."""
    from pystac_client.stac_api_io import StacApiIO

    io = StacApiIO(headers={"Authorization": "Bearer test-token"})
    assert io.session.headers["Authorization"] == "Bearer test-token"


# --- Cache expiry refetch ----------------------------------------------------


def test_token_refetched_after_expiry(monkeypatch):
    _set_env(monkeypatch)
    clock = {"now": 0.0}
    with (
        patch("stac_auth.time.monotonic", side_effect=lambda: clock["now"]),
        patch("stac_auth.httpx.post", return_value=_token_response(expires_in=300)) as mock_post,
    ):
        assert stac_auth.get_token() == "test-token"  # cached until 300 - margin
        clock["now"] = 280.0  # past the safety-margin expiry
        assert stac_auth.get_token() == "test-token"
    assert mock_post.call_count == 2


# --- Adversarial: never silently unauthenticated -----------------------------


def test_token_endpoint_http_error_raises(monkeypatch):
    _set_env(monkeypatch)
    bad = MagicMock()
    bad.raise_for_status.side_effect = RuntimeError("401 Unauthorized")
    with patch("stac_auth.httpx.post", return_value=bad), pytest.raises(RuntimeError):
        stac_auth.get_token()


def test_token_response_without_access_token_raises(monkeypatch):
    _set_env(monkeypatch)
    empty = MagicMock()
    empty.raise_for_status.return_value = None
    empty.json.return_value = {"expires_in": 300}
    with patch("stac_auth.httpx.post", return_value=empty), pytest.raises(RuntimeError):
        stac_auth.get_token()


def test_partial_env_is_noop(monkeypatch):
    """Missing any one of the three vars → unauthenticated (no half-configured writes)."""
    monkeypatch.setenv("OIDC_TOKEN_URL", OIDC_ENV["OIDC_TOKEN_URL"])
    monkeypatch.setenv("OIDC_CLIENT_ID", OIDC_ENV["OIDC_CLIENT_ID"])
    # OIDC_CLIENT_SECRET intentionally unset
    assert stac_auth.get_token() is None
