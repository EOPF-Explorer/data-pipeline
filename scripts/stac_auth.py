"""OIDC client-credentials auth for the STAC Transactions write endpoints.

A no-op when the OIDC env is absent, so local/dev and any unconfigured environment keep
writing unauthenticated. When ``OIDC_TOKEN_URL`` / ``OIDC_CLIENT_ID`` /
``OIDC_CLIENT_SECRET`` are all set, ``get_token`` fetches (and caches) a client-credentials
bearer that ``open_client`` injects into the pystac-client session — the raw
``session.post/delete`` calls used by ``upsert_item`` then carry ``Authorization``
automatically (pystac-client 0.9.0 applies ``Client.open(headers=...)`` to
``_stac_io.session.headers``).

A configured-but-failing token endpoint raises rather than degrading to a silent
unauthenticated write.

See ``claude-docs/plans/stac_transactions_auth.md`` (Task 1).
"""

from __future__ import annotations

import logging
import os
import threading
import time

import httpx
from pystac_client import Client

logger = logging.getLogger(__name__)

# Refetch this many seconds before the token actually expires.
_EXPIRY_MARGIN_S = 30

_lock = threading.Lock()
_cached_token: str | None = None
_cached_expiry: float = 0.0  # time.monotonic() seconds when the cached token expires


def _oidc_env() -> tuple[str, str, str] | None:
    """Return (token_url, client_id, client_secret) if all set, else None."""
    token_url = os.environ.get("OIDC_TOKEN_URL")
    client_id = os.environ.get("OIDC_CLIENT_ID")
    client_secret = os.environ.get("OIDC_CLIENT_SECRET")
    if token_url and client_id and client_secret:
        return token_url, client_id, client_secret
    return None


def get_token() -> str | None:
    """Return a cached client-credentials bearer token, or None when unconfigured.

    None means "write unauthenticated" (preserves local/dev behavior). A configured
    token endpoint that fails raises RuntimeError so a misconfiguration never silently
    degrades to an unauthenticated write.
    """
    env = _oidc_env()
    if env is None:
        return None
    token_url, client_id, client_secret = env

    with _lock:
        global _cached_token, _cached_expiry
        if _cached_token is not None and time.monotonic() < _cached_expiry:
            return _cached_token

        try:
            resp = httpx.post(
                token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": client_id,
                    "client_secret": client_secret,
                },
                timeout=30,
            )
            resp.raise_for_status()
            payload = resp.json()
        except Exception as exc:  # clear, non-silent failure
            raise RuntimeError(f"OIDC token request to {token_url} failed: {exc}") from exc

        access_token = payload.get("access_token")
        if not access_token:
            raise RuntimeError(f"OIDC token response from {token_url} had no access_token")

        token = str(access_token)
        expires_in = float(payload.get("expires_in", 300))
        _cached_token = token
        _cached_expiry = time.monotonic() + max(expires_in - _EXPIRY_MARGIN_S, 0)
        logger.info("Fetched OIDC token for client %s (expires in %ss)", client_id, expires_in)
        return token


def auth_headers() -> dict[str, str]:
    """Return the Authorization header dict, or {} when unauthenticated."""
    token = get_token()
    return {"Authorization": f"Bearer {token}"} if token else {}


def open_client(url: str) -> Client:
    """Open a pystac Client with the Bearer header applied to its session.

    Headers passed to ``Client.open`` land on ``_stac_io.session.headers``, so the raw
    ``session.post/delete`` calls used by ``upsert_item`` inherit ``Authorization``.
    Passes ``headers=None`` when unauthenticated (identical to a bare ``Client.open``).
    """
    return Client.open(url, headers=auth_headers() or None)
