"""Shared fixtures for unit tests.

Currently just the OIDC-auth scaffolding used by test_stac_auth.py and
test_write_sites_authenticated.py (the shared stac_auth helper's token cache + env).
"""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

import stac_auth  # noqa: E402

# The dedicated Sentinel-3 OLCI RC image pins a data-model SHA (5ea5662) that predates the
# S1-RTC work, so eopf_geozarr.stac is absent there (see coordination S3-OLCI plan A1).
# Without a guard, the S1-RTC test modules fail at IMPORT and pytest aborts collection for
# the WHOLE suite — CI would run zero tests and go red. Skip only the S1-RTC surface when
# S1 support is missing; this auto-reactivates once the S1+S3 data-model merge restores it.
_S1_SUPPORT = importlib.util.find_spec("eopf_geozarr.stac") is not None

if not _S1_SUPPORT:
    # Paths are relative to this conftest's directory (tests/unit/).
    collect_ignore_glob = [
        "*s1_rtc*",
        "test_trigger_cdse.py",
        "test_s1_store_meta.py",
        "test_register_per_acquisition.py",
    ]

# These two cases in test_write_sites_authenticated.py drive the S1 register paths directly
# (the rest of that module is mission-agnostic and must still run), so skip them by nodeid
# rather than ignoring the whole file.
_S1_ONLY_TESTS = (
    "test_register_per_acquisition_opens_via_helper",
    "test_register_v1_s1_rtc_opens_via_helper",
)


def pytest_collection_modifyitems(config, items):
    """Skip the two S1-only cases in an otherwise-collectable module when S1 is absent."""
    if _S1_SUPPORT:
        return
    skip_s1 = pytest.mark.skip(reason="S1-RTC support absent (Sentinel-3 OLCI RC image pin)")
    for item in items:
        if any(name in item.nodeid for name in _S1_ONLY_TESTS):
            item.add_marker(skip_s1)

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


@pytest.fixture
def token_response():
    """Factory for a mock httpx token-endpoint response (access_token=test-token)."""

    def _make(access_token="test-token", expires_in=300):  # noqa: S107
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        resp.json.return_value = {"access_token": access_token, "expires_in": expires_in}
        return resp

    return _make
