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
import math
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

# Per-request read timeout for search pagination; override with STAC_HTTP_TIMEOUT
# (parsed lazily by _search_timeout_s, never at import).
_DEFAULT_SEARCH_TIMEOUT_S = 60.0

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


def _search_timeout_s() -> float:
    """Read ``STAC_HTTP_TIMEOUT`` at call time, not import time.

    ``value: ""`` is how this fleet's Argo templates spell an unset optional parameter
    (see ``cleanup_expired_items._budget_seconds``), so ``""`` and unset both mean the
    default. Parsed here rather than at module level on purpose: ``scripts/`` is the
    shipped wheel, and a ``float("")`` raised during ``import stac_auth`` lands before
    argparse and before ``run_cleanup``'s broad guard — the tick dies with no
    ``cleanup_summary`` at all, on every pod that imports this module.

    ``0`` and negatives are rejected rather than mapped to "no timeout": no timeout is
    the 4.5 h hang that ``resilient_stac_io`` exists to prevent, and a typo must not
    silently reintroduce it. Garbage is rejected too, not defaulted: the value the
    operator set is not the value in effect, and a loud failure inside the caller's
    guard (which still emits its summary) beats a silent 60 s.
    """
    raw = os.getenv("STAC_HTTP_TIMEOUT", "").strip()
    if raw == "":
        return _DEFAULT_SEARCH_TIMEOUT_S
    try:
        value = float(raw)
    except ValueError:
        raise ValueError(f"STAC_HTTP_TIMEOUT must be a number of seconds (got {raw!r})") from None
    # `not (x > 0)` rather than `x <= 0`: it also rejects NaN, which float() accepts.
    if not (math.isfinite(value) and value > 0):
        raise ValueError(
            f"STAC_HTTP_TIMEOUT must be a finite number > 0 seconds; unset it for the "
            f"default (got {raw!r})"
        )
    return value


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

    Worst case per request: 9 attempts (``total=8``) with sleeps of
    0+2+4+8+16+20+20+20 = **90 s** between them, and each attempt can spend the full read
    timeout, so **~630 s per page** at the 60 s default. Against a gateway that 500s after
    its own 15 s timeout it is ~225 s. Either way it is far more than one request, so **a
    caller with a runtime budget must check it between pages, not only after discovery**
    — see ``cleanup_expired_items``, which pages explicitly for exactly this reason.
    Override the timeout with ``STAC_HTTP_TIMEOUT`` (``""`` and unset mean the default).
    """
    # One budget, `total=8`, for every error class — connect/read/status are left at
    # urllib3's default (None = inherit `total`) ON PURPOSE. Bounding them lower so that
    # "the API is down" fails fast reads well and is wrong: urllib3 files a ConnectionReset
    # under *read*, not *connect*, so `read=2` silently cuts the budget from 8 to 2 for the
    # transient mid-pagination reset this policy exists to survive (the one that crashed
    # the 23k-item backfill at ~20%), while the object still reports `total=8`.
    # test_stac_read_retry pins the effective budget per error class so that cannot
    # regress unseen. A genuine outage costs at most one page's worst case (above) before
    # the caller's runtime budget or the run's own failure path takes over.
    #
    # backoff_max caps each sleep at 20 s (urllib3's default is 120 s): 90 s of sleeps
    # across the ladder rather than 246 s. respect_retry_after_header is off because it
    # bypasses that cap: on 429/503 urllib3 sleeps the server's Retry-After instead, bounded
    # only by `retry_after_max` (default 21600 s), so one response carrying a large header
    # would stall a page for hours whatever backoff_max says.
    retry = Retry(
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

    The one correct way to combine ``resilient_stac_io`` with ``Client.open``: the timeout
    must also be passed to ``Client.open``, because ``Client.from_file`` resets the
    ``stac_io``'s timeout to ``None`` otherwise (see ``resilient_stac_io``). Every read path
    should call this rather than assembling the pair by hand, so the trap is sprung once,
    here, instead of at each call site.

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
