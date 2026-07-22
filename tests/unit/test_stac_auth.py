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


_ENV = {
    "OIDC_TOKEN_URL": "https://kc.example.com/token",
    "OIDC_CLIENT_ID": "stac-writer",
    "OIDC_CLIENT_SECRET": "s3cr3t",  # noqa: S105
}


@pytest.mark.parametrize("missing", list(_ENV))
def test_partial_env_is_noop(monkeypatch, missing):
    """Missing ANY one of the three vars → unauthenticated (no half-configured writes)."""
    for key, value in _ENV.items():
        if key != missing:  # the autouse fixture already cleared `missing`
            monkeypatch.setenv(key, value)
    assert stac_auth.get_token() is None


def test_malformed_token_json_raises(oidc_env):
    """Token endpoint returns non-JSON → raise, never a silent unauthenticated write."""
    bad = MagicMock()
    bad.raise_for_status.return_value = None
    bad.json.side_effect = ValueError("not json")
    with patch("stac_auth.httpx.post", return_value=bad), pytest.raises(RuntimeError):
        stac_auth.get_token()


def test_missing_expires_in_defaults(oidc_env):
    """No expires_in in the response → token still returned (defaults to 300s cache)."""
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = {"access_token": "test-token"}  # no expires_in
    with patch("stac_auth.httpx.post", return_value=resp):
        assert stac_auth.get_token() == "test-token"


# --- Fail-closed at the WRITE (not just the helper) --------------------------


def test_write_fails_closed_when_token_endpoint_fails(oidc_env):
    """A configured-but-failing token endpoint must make the actual WRITE raise and never
    send an unauthenticated request (bearer_auth runs in prepare_request, before send)."""
    bad = MagicMock()
    bad.raise_for_status.side_effect = RuntimeError("500 Server Error")

    session = requests.Session()
    session.auth = stac_auth.bearer_auth

    class _NoSend(requests.adapters.HTTPAdapter):
        def send(self, *args, **kwargs):
            raise AssertionError("unauthenticated request must not be sent")

    session.mount("https://", _NoSend())
    with patch("stac_auth.httpx.post", return_value=bad), pytest.raises(RuntimeError):
        session.post("https://stac.example.com/collections", json={})


def test_open_client_session_post_carries_bearer(oidc_env, token_response):
    """Behavioral: a real requests.Session wired by open_client carries the Bearer on POST
    (guards the whole pystac write path, which the per-site delegation tests only mock)."""
    real_session = requests.Session()
    fake_io = MagicMock()
    fake_io.session = real_session
    fake_client = MagicMock()
    fake_client._stac_io = fake_io

    captured: dict[str, str | None] = {}

    class _Capture(requests.adapters.HTTPAdapter):
        def send(self, request, *args, **kwargs):
            captured["auth"] = request.headers.get("Authorization")
            resp = requests.models.Response()
            resp.status_code = 200
            return resp

    real_session.mount("https://", _Capture())
    with (
        patch("stac_auth.Client.open", return_value=fake_client),
        patch("stac_auth.httpx.post", return_value=token_response()),
    ):
        client = stac_auth.open_client("https://stac.example.com")
        client._stac_io.session.post("https://stac.example.com/collections", json={})
    assert captured["auth"] == "Bearer test-token"
