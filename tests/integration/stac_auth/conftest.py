"""Session fixture that brings the auth-chain stack up and waits for readiness.

`uv run pytest tests/integration/stac_auth -v` is self-contained: the fixture runs
`docker compose up -d` and tears down at the end. Set `KEEP_STACK=1` to leave the stack
running for debugging.
"""

from __future__ import annotations

import os
import subprocess
import time
import urllib.request
from pathlib import Path

import pytest

HERE = Path(__file__).parent
COMPOSE = ["docker", "compose", "-f", str(HERE / "docker-compose.yaml")]

KC_BASE = "http://localhost:8066/auth/realms/eoxhub"
DISCOVERY_URL = f"{KC_BASE}/.well-known/openid-configuration"
TOKEN_URL = f"{KC_BASE}/protocol/openid-connect/token"
JWKS_URL = f"{KC_BASE}/protocol/openid-connect/certs"
PROXY_URL = "http://localhost:8067"


def _http_status(url: str, timeout: float = 5.0) -> int:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:  # noqa: S310
            return resp.status
    except urllib.error.HTTPError as exc:
        return exc.code
    except Exception:
        return 0


def _wait_for(url: str, desc: str, tries: int = 90, delay: float = 2.0) -> None:
    for _ in range(tries):
        if _http_status(url) == 200:
            return
        time.sleep(delay)
    raise RuntimeError(f"timed out waiting for {desc} at {url}")


@pytest.fixture(scope="session", autouse=True)
def stack():
    """Bring the compose stack up, wait until the whole chain answers, tear down."""
    subprocess.run([*COMPOSE, "up", "-d"], check=True)  # noqa: S603
    try:
        _wait_for(DISCOVERY_URL, "keycloak realm discovery")
        # A public GET through the proxy proves proxy + stac + pgstac are all live.
        _wait_for(f"{PROXY_URL}/collections", "proxy -> stac -> pgstac public read")
        yield
    finally:
        if os.environ.get("KEEP_STACK") != "1":
            subprocess.run([*COMPOSE, "down", "-v"], check=False)  # noqa: S603
