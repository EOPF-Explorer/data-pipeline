"""Shared fixtures for unit tests.

The OIDC-auth scaffolding used by test_stac_auth.py and test_write_sites_authenticated.py
(the shared stac_auth helper's token cache + env), plus the ``no_network`` guard used by the
body-build tests.
"""

import sys
import traceback
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

import stac_auth  # noqa: E402

OIDC_ENV = {
    "OIDC_TOKEN_URL": "https://kc.example.com/realms/eoxhub/protocol/openid-connect/token",
    "OIDC_CLIENT_ID": "stac-writer",
    "OIDC_CLIENT_SECRET": "s3cr3t",  # noqa: S105
}


@pytest.fixture(autouse=True)
def reset_stac_auth_cache(monkeypatch):
    """Clear the stac_auth module-global token cache + OIDC env around every test."""
    for key in OIDC_ENV:
        monkeypatch.delenv(key, raising=False)
    stac_auth._cache.token = None
    stac_auth._cache.expiry = 0.0
    yield
    stac_auth._cache.token = None
    stac_auth._cache.expiry = 0.0


@pytest.fixture
def oidc_env(monkeypatch):
    """Configure the three OIDC env vars as the stac-writer client."""
    for key, value in OIDC_ENV.items():
        monkeypatch.setenv(key, value)
    return OIDC_ENV


NO_NETWORK_SENTINEL = "network I/O during body build"


@contextmanager
def _no_network():
    """Fail ANY outbound network attempt inside the block, with a legible message.

    All three entry points are patched deliberately: urllib3 resolves the host first, so
    watching only ``socket.socket`` would miss a call that dies in ``getaddrinfo``, and a
    regression reaching the network over a pooled keep-alive connection would skip both.

    The sentinel is re-surfaced because pystac swallows the cause: ``link.py`` rewraps every
    resolution failure as ``STACError: HREF ... does not resolve to a STAC object``, which reads
    like a stale fixture URL whose obvious "fix" is to edit the fixture, leaving the bug in
    place. Raising here rather than recording also keeps the diagnosis reachable — an assertion
    placed *after* the call never runs, because the rewrapped error escapes the block first.
    """
    boom = AssertionError(NO_NETWORK_SENTINEL)
    try:
        with (
            patch("socket.socket", side_effect=boom),
            patch("socket.getaddrinfo", side_effect=boom),
            patch("socket.create_connection", side_effect=boom),
        ):
            yield
    except Exception as exc:
        if NO_NETWORK_SENTINEL in "".join(traceback.format_exception(exc)):
            raise AssertionError(
                f"{NO_NETWORK_SENTINEL} (surfaced as {type(exc).__name__})"
            ) from exc
        raise


@pytest.fixture
def no_network():
    """The ``_no_network()`` guard, as a fixture: ``with no_network(): ...``."""
    return _no_network


@pytest.fixture
def token_response():
    """Factory for a mock httpx token-endpoint response (access_token=test-token)."""

    def _make(access_token="test-token", expires_in=300):  # noqa: S107
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        resp.json.return_value = {"access_token": access_token, "expires_in": expires_in}
        return resp

    return _make
