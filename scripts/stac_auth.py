"""OIDC client-credentials auth for the STAC Transactions write endpoints.

A no-op when the OIDC env is absent, so local/dev and any unconfigured environment keep
writing unauthenticated. When ``OIDC_TOKEN_URL`` / ``OIDC_CLIENT_ID`` /
``OIDC_CLIENT_SECRET`` are all set, ``get_token`` fetches (and caches) a client-credentials
bearer that ``open_client`` wires onto the pystac-client session via
``session.auth = bearer_auth``. ``requests`` re-runs the auth hook on every
``session.post/delete`` (used by ``upsert_item``), so each write carries a fresh
``Authorization`` header — even across a batch that outlives the token.

A configured-but-failing token endpoint raises rather than degrading to a silent
unauthenticated write.

Design tracked out-of-repo (session memory + PR description); this is Task 1.
"""

from __future__ import annotations

import logging
import os
import threading
import time

import httpx
import requests
from pystac_client import Client

logger = logging.getLogger(__name__)

# Refetch this many seconds before the token actually expires.
_EXPIRY_MARGIN_S = 30

_lock = threading.Lock()


class _TokenCache:
    """The cached client-credentials token, guarded by ``_lock``."""

    token: str | None = None
    expiry: float = 0.0  # time.monotonic() seconds when the cached token expires


_cache = _TokenCache()


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
        if _cache.token is not None and time.monotonic() < _cache.expiry:
            return _cache.token

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
            # Only the exception TYPE in the message (never interpolate the exception near
            # credential handling); `from exc` keeps the full traceback for debugging.
            raise RuntimeError(
                f"OIDC token request to {token_url} failed: {type(exc).__name__}"
            ) from exc

        access_token = payload.get("access_token")
        if not access_token:
            raise RuntimeError(f"OIDC token response from {token_url} had no access_token")

        token = str(access_token)
        expires_in = float(payload.get("expires_in", 300))
        _cache.token = token
        _cache.expiry = time.monotonic() + max(expires_in - _EXPIRY_MARGIN_S, 0)
        logger.info("Fetched OIDC token for client %s (expires in %ss)", client_id, expires_in)
        return token


def auth_headers() -> dict[str, str]:
    """Return the Authorization header dict, or {} when unauthenticated."""
    token = get_token()
    return {"Authorization": f"Bearer {token}"} if token else {}


def bearer_auth(request: requests.PreparedRequest) -> requests.PreparedRequest:
    """`requests` auth hook — attach a fresh Bearer header to every request.

    Wired onto every requests-backed write session (``open_client`` for pystac, and the
    operator tools' sessions) via ``session.auth = stac_auth.bearer_auth``. Each request
    re-reads the cached token — which ``get_token`` refreshes near expiry — so a batch
    that outlives the token can't send a stale one. A no-op when OIDC env is unset.
    """
    request.headers.update(auth_headers())
    return request


def open_client(url: str) -> Client:
    """Open a pystac Client whose session attaches a fresh Bearer per request.

    pystac-client's ``StacApiIO`` wraps a ``requests.Session``; wiring ``bearer_auth`` onto
    it means the raw ``session.post/delete`` calls used by ``upsert_item`` carry a token
    that stays fresh even across a batch that outlives it. A no-op when OIDC env is unset;
    the landing-page fetch during ``Client.open`` is an unauthenticated public GET.
    """
    client = Client.open(url)
    if client._stac_io is not None:
        client._stac_io.session.auth = bearer_auth
    return client
