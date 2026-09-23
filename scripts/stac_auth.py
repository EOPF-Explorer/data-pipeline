"""STAC client plumbing shared by the crons: OIDC write auth and the resilient read path.

The read half (``open_resilient_client`` and friends) is a retrying client for ``/search``
pagination. It lives here so the tier-down cron does not import the module that deletes S3
objects just to get a flag parser.

The write half is OIDC client-credentials auth for the STAC Transactions endpoints, and a
no-op when the OIDC env is absent, so unconfigured environments keep writing
unauthenticated. With ``OIDC_TOKEN_URL`` / ``OIDC_CLIENT_ID`` / ``OIDC_CLIENT_SECRET`` set,
``get_token`` caches a bearer that ``open_client`` wires on as ``session.auth``; requests
re-runs the hook per call, so every write in a long batch carries a fresh header. A
configured-but-failing token endpoint raises rather than degrading to a silent
unauthenticated write.
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import threading
import time
from types import TracebackType
from typing import Self

import httpx
import requests
from pystac_client import Client
from pystac_client.stac_api_io import StacApiIO
from urllib3 import BaseHTTPResponse
from urllib3.connectionpool import ConnectionPool
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# Refetch this many seconds before the token actually expires.
_EXPIRY_MARGIN_S = 30

# Per-request timeout for search pagination, applied by `requests` to the connect and the
# read half separately. Override with STAC_HTTP_TIMEOUT (parsed lazily, never at import).
_DEFAULT_SEARCH_TIMEOUT_S = 60.0
# A typo fence only, not a deployment's tolerance: a page fetch is uninterruptible for its
# whole ladder, so "6000" typed for 60 outlasts any caller's runtime budget. The real
# bound belongs in each caller's manifest; the sizing rule is in the cleanup README.
_MAX_SEARCH_TIMEOUT_S = 300.0

# See resilient_stac_io() for why 500 is here and when to remove it.
_READ_RETRY_STATUSES = (429, 500, 502, 503, 504)

# Items per /search page. A page, NOT a cap: --max-items / --max-batch-size bound the run,
# this bounds one round trip. Left unset the server picks 10, which turned a 300-item batch
# into 30 POSTs. Whether 100 is the RIGHT page against the real gateway is unproven — the
# tier-down cron died while already passing limit=100, and a bigger page may sit closer to
# the 15 s cliff, not further. The fix is the retry; this is the knob for finding out.
DEFAULT_PAGE_SIZE = 100

# A typo fence, not a survivable size: a page is materialised in memory like a cleanup
# batch, and cleanup_expired_items.MAX_ITEMS_CEILING IS this constant, so raising it also
# raises the cap on an irreversible-delete batch. At ~45 KB/item, 10_000 rows is ~450 MB
# against a 512Mi pod.
MAX_PAGE_SIZE = 10_000


def page_size_arg(raw: str) -> int:
    """argparse ``type`` for ``--page-size``: 1..MAX_PAGE_SIZE, with ``""`` the default.

    ``""`` must be accepted: the Argo templates splice ``value: ""`` into argv for an unset
    optional parameter, and rejecting it fails the pod at parse time on every tick.
    """
    if raw.strip() == "":
        return DEFAULT_PAGE_SIZE
    try:
        value = int(raw)
    except ValueError:
        raise argparse.ArgumentTypeError(f"must be a whole number (got {raw!r})") from None
    if not 1 <= value <= MAX_PAGE_SIZE:
        raise argparse.ArgumentTypeError(f"must be 1..{MAX_PAGE_SIZE} (got {value})")
    return value


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


def _search_timeout_s() -> float:
    """Read ``STAC_HTTP_TIMEOUT`` at call time, not import time.

    Lazily on purpose: parsed at module level, a bad value raises during ``import
    stac_auth``, before argparse and before the caller's guard, so the tick dies with no
    summary at all. ``""`` and unset both mean the default (the Argo unset idiom).

    Bad values raise rather than falling back to the default, and ``0`` is not "no
    timeout" — no timeout is the hang this module exists to prevent.
    """
    raw = os.getenv("STAC_HTTP_TIMEOUT", "").strip()
    if raw == "":
        return _DEFAULT_SEARCH_TIMEOUT_S
    try:
        value = float(raw)
    except ValueError:
        raise ValueError(f"STAC_HTTP_TIMEOUT must be a number of seconds (got {raw!r})") from None
    # `not (0 < x)` rather than `x <= 0`: it also rejects NaN, which float() accepts.
    if not (math.isfinite(value) and 0 < value <= _MAX_SEARCH_TIMEOUT_S):
        raise ValueError(
            f"STAC_HTTP_TIMEOUT must be a number of seconds in (0, "
            f"{_MAX_SEARCH_TIMEOUT_S:g}]; unset it for the default (got {raw!r})"
        )
    return value


class _LoggedRetry(Retry):
    """``Retry`` that says so, at WARNING, every time it fires on a status.

    urllib3 logs a status-forcelist retry at DEBUG and the crons pin that logger to
    WARNING, so the 500 path — the reason this policy exists — would leave no trace in
    production. ``increment`` is the one call every retry goes through, and ``Retry.new``
    rebuilds via ``type(self)``, so the subclass survives the copy-on-increment.
    """

    def increment(
        self,
        method: str | None = None,
        url: str | None = None,
        response: BaseHTTPResponse | None = None,
        error: Exception | None = None,
        _pool: ConnectionPool | None = None,
        _stacktrace: TracebackType | None = None,
    ) -> Self:
        new = super().increment(method, url, response, error, _pool, _stacktrace)
        if response is not None:
            logger.warning(
                "Retrying %s %s after HTTP %s: %s retries left, next sleep %.0f s",
                method,
                url,
                response.status,
                new.total,
                new.get_backoff_time(),
            )
        return new


def resilient_stac_io(timeout: float | None = None) -> StacApiIO:
    """A ``StacApiIO`` for READ paths: per-request timeout plus retries on transient 5xx.

    Four things here are load-bearing and easy to undo by accident:

    🔴 **``allowed_methods`` MUST include POST.** STAC ``/search`` is a POST and urllib3's
    default allows only idempotent methods, so a policy that omits it reports as present
    and never fires on the one call that matters.

    🔴 **500 is retried because of a gateway defect, not on principle.**
    ``eoapi-stac-auth-proxy`` lets an upstream ``httpx.ReadTimeout`` escape its ASGI app,
    so a page past its ``UPSTREAM_TIMEOUT`` surfaces as 500 rather than 504 — which is why
    the previous policy, correct by the book at 502/503/504, never fired.
    **Drop 500 when the proxy is fixed** (developmentseed/stac-auth-proxy#211).

    🔴 **Reads only. Never mount this on a session carrying item DELETEs or POSTs** —
    retrying a non-atomic write on a 5xx is the hazard PUT-instead-of-DELETE-then-POST
    exists to avoid. The cleanup cron's write session carries no retries at all.

    🔴 **Call ``open_resilient_client``, not this plus ``Client.open``.** ``Client.open``
    silently resets the ``stac_io``'s timeout to ``None``; the retries survive, the timeout
    does not, and no unit test on this object can see the difference.

    A page fetch is uninterruptible for its whole ladder, so no caller's runtime budget can
    cut it short — ``scripts/README_cleanup_expired_items.md`` has the rule for sizing an
    outer deadline against that. ``STAC_HTTP_TIMEOUT`` overrides the timeout; an explicit
    ``timeout`` is held to the same range.
    """
    if timeout is not None and not (
        math.isfinite(timeout) and 0 < timeout <= _MAX_SEARCH_TIMEOUT_S
    ):
        raise ValueError(
            f"timeout must be in (0, {_MAX_SEARCH_TIMEOUT_S:g}] seconds (got {timeout!r})"
        )
    # connect/read/status are left at None (inherit `total`) ON PURPOSE: urllib3 files a
    # ConnectionReset under *read*, so `read=2` would cut the budget from 8 to 2 for the
    # exact mid-pagination reset this exists to survive, while still reporting total=8.
    # test_stac_read_retry pins the effective budget per error class.
    #
    # backoff_max caps each sleep at 20 s (default 120), giving 90 s of sleeps per ladder.
    # respect_retry_after_header is off because it bypasses that cap — on 429/503 urllib3
    # would sleep the server's Retry-After, bounded only by retry_after_max (6 h).
    retry = _LoggedRetry(
        total=8,
        backoff_factor=1.0,
        backoff_max=20,
        respect_retry_after_header=False,
        status_forcelist=_READ_RETRY_STATUSES,
        allowed_methods=frozenset({"GET", "POST"}),
        raise_on_status=False,
    )
    return StacApiIO(
        timeout=timeout if timeout is not None else _search_timeout_s(), max_retries=retry
    )


def open_resilient_client(url: str) -> Client:
    """Open a read Client that keeps BOTH its retries and its timeout.

    ``Client.open`` resets the ``stac_io``'s timeout unless it is also passed the timeout
    directly, so every cron read path goes through here and the trap is sprung once.
    ``operator-tools/manage_collections`` is still on bare ``Client.open``; migrating it is
    a follow-up to this PR.

    Read paths only — this carries no auth and its retries must never reach a write.
    """
    timeout = _search_timeout_s()
    return Client.open(url, stac_io=resilient_stac_io(timeout), timeout=timeout)


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
