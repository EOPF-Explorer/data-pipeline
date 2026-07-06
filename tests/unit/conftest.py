"""Shared fixtures for unit tests.

Currently just the OIDC-auth scaffolding used by test_stac_auth.py and
test_write_sites_authenticated.py (the shared stac_auth helper's token cache + env).
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock

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
    stac_auth._cached_token = None
    stac_auth._cached_expiry = 0.0
    yield
    stac_auth._cached_token = None
    stac_auth._cached_expiry = 0.0


@pytest.fixture
def oidc_env(monkeypatch):
    """Configure the three OIDC env vars as the stac-writer client."""
    for key, value in OIDC_ENV.items():
        monkeypatch.setenv(key, value)
    return OIDC_ENV


@pytest.fixture
def token_response():
    """Factory for a mock httpx token-endpoint response (access_token=test-token)."""

    def _make(access_token="test-token", expires_in=300):  # noqa: S107
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        resp.json.return_value = {"access_token": access_token, "expires_in": expires_in}
        return resp

    return _make
