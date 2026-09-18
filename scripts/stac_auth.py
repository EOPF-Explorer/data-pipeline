"""OIDC client-credentials auth for the STAC Transactions write endpoints.

A no-op when the OIDC env is absent, so local/dev and any unconfigured environment keep
writing unauthenticated. When ``OIDC_TOKEN_URL`` / ``OIDC_CLIENT_ID`` /
``OIDC_CLIENT_SECRET`` are all set, ``get_token`` fetches (and caches) a client-credentials
bearer that ``open_client`` wires onto the pystac-client session via
``session.auth = bearer_auth``. ``requests`` re-runs the auth hook on every
``session.put/post`` (used by ``upsert_item``), so each write carries a fresh
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
from pystac_client.stac_api_io import StacApiIO
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# Refetch this many seconds before the token actually expires.
_EXPIRY_MARGIN_S = 30

# Per-request read timeout for search pagination; override with STAC_HTTP_TIMEOUT.
_SEARCH_TIMEOUT_S = float(os.getenv("STAC_HTTP_TIMEOUT", "60"))

# See resilient_stac_io() for why 500 is here and when to remove it.
_READ_RETRY_STATUSES = (429, 500, 502, 503, 504)

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


def resilient_stac_io(timeout: float | None = None) -> StacApiIO:
    """A ``StacApiIO`` for READ paths: per-request timeout plus retries on transient 5xx.

    Search pagination is the fragile part of every long run. Without this:

    * no timeout -> a stalled socket hangs the run forever (observed: 4.5 h wall / 26 s
      CPU, never past "Found N items");
    * weak default retries -> one transient failure mid-pagination aborts the whole run
      (observed: a 23k-item staging backfill crashed at ~20%).

    🔴 **``allowed_methods`` MUST include POST.** STAC ``/search`` is a POST, and urllib3's
    default ``Retry`` allows only idempotent methods — so a retry policy that omits it is
    configured, reported as present, and silently never fires on the one call that matters.

    🔴 **500 is in the retry list because of a gateway defect, not because 500 is
    retryable in general.** ``eoapi-stac-auth-proxy`` (v1.1.0) lets an upstream
    ``httpx.ReadTimeout`` escape its ASGI app, so an upstream that exceeds its
    ``UPSTREAM_TIMEOUT`` (15 s in prod) surfaces as **500, not 504**. Proven 2026-09-18:
    six ``historical-cleanup`` ticks failed at 15.013-15.016 s with
    ``APIError: Internal Server Error``, matched one-for-one by ``httpx.ReadTimeout``
    tracebacks in the proxy log for the same second. A correct policy that retries
    502/503/504 and not 500 does not fire here — which is exactly what the previous
    ``_resilient_stac_io`` did. **If the proxy is fixed to return 504, drop 500 from this
    list**: it is a workaround for someone else's status code, and retrying a genuine
    500 elsewhere only delays a real error.

    Safe only on reads. ``/search`` and ``GET`` are idempotent, so a retried request cannot
    duplicate an effect. **Never mount this on a session that carries DELETEs or POSTs of
    items** — retrying those on a 5xx is the non-atomic-write hazard that PUT-instead-of-
    DELETE-then-POST exists to avoid. Write sessions are built separately, on purpose.

    🔴 **Use ``open_resilient_client`` rather than calling this and passing the result to
    ``Client.open`` yourself.** ``Client.from_file`` calls ``stac_io.update(timeout=None)``
    when handed a ``stac_io``, and ``StacApiIO.update`` *assigns* rather than merges — so
    the timeout set here is silently reset to ``None`` and ``STAC_HTTP_TIMEOUT`` becomes a
    no-op. The retries survive (they live on the session's adapters, which ``update`` does
    not touch); only the timeout is lost, which is the half of this that a passing unit
    test cannot see.

    Worst case per request, with the bounds below: the status ladder sleeps
    0+2+4+8+16 = **30 s**, and each of the 6 attempts can spend the full read timeout, so
    **~390 s per page** at the 60 s default. Against a gateway that 500s after its own 15 s
    timeout it is ~120 s. Either way it is far more than one request, so **a caller with a
    runtime budget must check it between pages, not only after discovery** — see
    ``cleanup_expired_items``, which pages explicitly for exactly this reason.
    Override the timeout with ``STAC_HTTP_TIMEOUT``.
    """
    # The categories are bounded separately on purpose. Left at the urllib3 default they
    # are all None, which means each one silently inherits `total` — so a *connection
    # refusal* (the API is down, not blipping) would walk the same 8-step, 246 s ladder as
    # a transient 5xx. That is the wrong trade twice over: it turns "service is down" from
    # a fast, legible failure into a four-minute stall per page, and in a budgeted cron it
    # burns the budget on a request that was never going to succeed.
    #   status  — the case this exists for; a degraded gateway recovers within seconds.
    #   connect — down is down; two tries distinguish a blip from an outage.
    #   read    — a half-open socket; the timeout already bounds each attempt.
    # backoff_max caps each sleep at 20 s (urllib3's own default is 120 s), so the status
    # ladder sleeps 0+2+4+8+16 = 30 s rather than 246 s.
    retry = Retry(
        total=8,
        connect=2,
        read=2,
        status=5,
        backoff_factor=1.0,
        backoff_max=20,
        status_forcelist=_READ_RETRY_STATUSES,
        allowed_methods=frozenset({"GET", "POST"}),
        raise_on_status=False,
    )
    return StacApiIO(
        timeout=timeout if timeout is not None else _SEARCH_TIMEOUT_S, max_retries=retry
    )


def open_resilient_client(url: str) -> Client:
    """Open a read Client that keeps BOTH its retries and its timeout.

    The one correct way to combine ``resilient_stac_io`` with ``Client.open``: the timeout
    must also be passed to ``Client.open``, because ``Client.from_file`` resets the
    ``stac_io``'s timeout to ``None`` otherwise (see ``resilient_stac_io``). Every read path
    should call this rather than assembling the pair by hand, so the trap is sprung once,
    here, instead of at each call site.

    Read paths only — this carries no auth and its retries must never reach a write.
    """
    return Client.open(url, stac_io=resilient_stac_io(), timeout=_SEARCH_TIMEOUT_S)


def open_client(url: str) -> Client:
    """Open a pystac Client whose session attaches a fresh Bearer per request.

    pystac-client's ``StacApiIO`` wraps a ``requests.Session``; wiring ``bearer_auth`` onto
    it means the raw ``session.put/post`` calls used by ``upsert_item`` carry a token
    that stays fresh even across a batch that outlives it. A no-op when OIDC env is unset;
    the landing-page fetch during ``Client.open`` is an unauthenticated public GET.
    """
    client = Client.open(url)
    if client._stac_io is not None:
        client._stac_io.session.auth = bearer_auth
    return client
