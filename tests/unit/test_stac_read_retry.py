"""Tests for stac_auth.resilient_stac_io — the read-path retry policy.

Why these are behavioural and not just config assertions: the defect being fixed is a
retry policy that is *present and correct-looking* but never fires. urllib3's default
``Retry`` permits only idempotent methods, so a policy that forgets ``POST`` retries
nothing on STAC ``/search`` — and no assertion about the object's fields would notice the
difference between "retries POST" and "has a retry object". So the central test drives a
real socket and counts the requests the server actually received.
"""

import logging
import os
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest.mock import MagicMock, patch

import pytest
import requests
import stac_auth
from urllib3.exceptions import (
    ConnectTimeoutError,
    MaxRetryError,
    ProtocolError,
    ReadTimeoutError,
)
from urllib3.util.retry import Retry


class _FlakyHandler(BaseHTTPRequestHandler):
    """Fails `fail_times` requests with `fail_status`, then succeeds."""

    fail_times = 0
    fail_status = 500
    seen: list[str] = []

    def _respond(self) -> None:
        type(self).seen.append(self.command)
        if len(type(self).seen) <= type(self).fail_times:
            self.send_response(type(self).fail_status)
            self.end_headers()
            self.wfile.write(b'{"error": "boom"}')
            return
        # "/" must be a STAC landing page so Client.open() can be exercised; every other
        # path answers as a search would.
        if self.path == "/" and self.command == "GET":
            port = str(self.server.server_port).encode()
            body = (
                b'{"type":"Catalog","id":"test","stac_version":"1.0.0",'
                b'"description":"test","conformsTo":['
                b'"https://api.stacspec.org/v1.0.0/core",'
                b'"https://api.stacspec.org/v1.0.0/item-search"],'
                b'"links":[{"rel":"self","href":"http://127.0.0.1:' + port + b'/"}]}'
            )
        else:
            body = b'{"type": "FeatureCollection", "features": [], "links": []}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = _respond
    do_POST = _respond

    def log_message(self, *args: object) -> None:  # silence the test server
        pass


@pytest.fixture
def flaky_server():
    """A local HTTP server whose failure count and status the test sets."""

    def _start(fail_times: int, fail_status: int = 500) -> str:
        _FlakyHandler.fail_times = fail_times
        _FlakyHandler.fail_status = fail_status
        _FlakyHandler.seen = []
        server = HTTPServer(("127.0.0.1", 0), _FlakyHandler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        _start.servers.append(server)
        return f"http://127.0.0.1:{server.server_port}"

    _start.servers = []
    yield _start
    for server in _start.servers:
        server.shutdown()


def _session_from(stac_io) -> requests.Session:
    return stac_io.session


# --- the POST trap -------------------------------------------------------------------


def test_post_is_retried_through_a_real_socket(flaky_server):
    """A POST that 500s twice must be retried and ultimately succeed.

    This is the regression that matters: STAC /search is a POST, and urllib3's default
    Retry would not retry it at all.
    """
    url = flaky_server(fail_times=2)
    session = _session_from(stac_auth.resilient_stac_io(timeout=5))

    resp = session.post(f"{url}/search", json={"collections": ["x"]})

    assert resp.status_code == 200
    assert _FlakyHandler.seen == ["POST", "POST", "POST"], (
        "expected 2 retries then success; urllib3 silently declines to retry POST "
        "unless allowed_methods includes it"
    )


def test_get_is_retried_through_a_real_socket(flaky_server):
    url = flaky_server(fail_times=1)
    session = _session_from(stac_auth.resilient_stac_io(timeout=5))

    resp = session.get(f"{url}/collections")

    assert resp.status_code == 200
    assert _FlakyHandler.seen == ["GET", "GET"]


# --- the status that the gateway actually returns ------------------------------------


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_transient_statuses_are_retried(flaky_server, status):
    """500 included: the prod gateway returns 500 (not 504) on an upstream ReadTimeout."""
    url = flaky_server(fail_times=1, fail_status=status)
    session = _session_from(stac_auth.resilient_stac_io(timeout=5))

    resp = session.post(f"{url}/search", json={})

    assert resp.status_code == 200, f"{status} should have been retried"
    assert len(_FlakyHandler.seen) == 2


def test_500_is_retried_named_explicitly():
    """Guard the specific entry, so removing it is a deliberate act.

    500 is a workaround for eoapi-stac-auth-proxy surfacing httpx.ReadTimeout as 500
    rather than 504. If the proxy is fixed, this entry should be removed on purpose —
    and this test is where that decision surfaces.
    """
    assert 500 in stac_auth._READ_RETRY_STATUSES


def test_404_is_not_retried(flaky_server):
    """A real client error must fail fast, not burn the backoff ladder."""
    url = flaky_server(fail_times=1, fail_status=404)
    session = _session_from(stac_auth.resilient_stac_io(timeout=5))

    resp = session.post(f"{url}/search", json={})

    assert resp.status_code == 404
    assert len(_FlakyHandler.seen) == 1


# --- the boundary this must not cross ------------------------------------------------


def test_write_session_has_no_retries():
    """The item-DELETE/PUT session must NOT carry this policy.

    Retrying a write on a 5xx is the non-atomic-unit hazard; the read policy is mounted
    on the discovery client only. This asserts the two are genuinely different objects.
    """
    import cleanup_expired_items

    write_session = cleanup_expired_items._session("https://stac.example.com")
    read_session = _session_from(stac_auth.resilient_stac_io())

    write_adapter = write_session.get_adapter("https://stac.example.com")
    read_adapter = read_session.get_adapter("https://stac.example.com")

    assert write_adapter.max_retries.total in (
        0,
        None,
    ), "the write session must not retry: a retried DELETE/PUT on a 5xx can tear a non-atomic unit"
    assert read_adapter.max_retries.total == 8


def test_timeout_is_set_and_overridable():
    """No timeout means a stalled socket hangs the run forever (observed: 4.5 h)."""
    assert stac_auth.resilient_stac_io().timeout == stac_auth._search_timeout_s()
    assert stac_auth.resilient_stac_io(timeout=12).timeout == 12


# --- the trap that asserting on the factory alone cannot see -------------------------


def test_timeout_survives_client_open(flaky_server):
    """The timeout must still be set on the client that actually does the searching.

    ``Client.from_file`` calls ``stac_io.update(timeout=None)`` when handed a ``stac_io``,
    and ``StacApiIO.update`` assigns rather than merges — so building the io correctly and
    passing it to ``Client.open`` yourself silently drops the timeout. Asserting the
    factory's attribute (above) passes while production has no timeout at all. This is the
    same class of hole the POST test exists for, one level further out.
    """
    url = flaky_server(fail_times=0)

    client = stac_auth.open_resilient_client(url)

    # Reset to None by Client.open otherwise — open_resilient_client must pass timeout= through.
    assert client._stac_io.timeout == stac_auth._search_timeout_s()


def test_retries_survive_client_open(flaky_server):
    """And the retry policy must survive the same call, on the real client's session."""
    url = flaky_server(fail_times=0)

    client = stac_auth.open_resilient_client(url)
    retry = client._stac_io.session.get_adapter(url).max_retries

    assert retry.total == 8
    assert "POST" in retry.allowed_methods
    assert 500 in retry.status_forcelist


def test_hand_rolled_pairing_loses_the_timeout(flaky_server):
    """Document the trap itself, so a future refactor back to the manual form is caught."""
    from pystac_client import Client

    url = flaky_server(fail_times=0)

    client = Client.open(url, stac_io=stac_auth.resilient_stac_io())

    assert client._stac_io.timeout is None, (
        "pystac-client no longer drops the timeout — open_resilient_client's extra "
        "timeout= argument may now be redundant; re-check before simplifying it away"
    )


# --- the effective budget per error class ---------------------------------------------


def _read_retry() -> Retry:
    return stac_auth.resilient_stac_io(timeout=1).session.get_adapter("https://x").max_retries


def _walk(retry: Retry, **kw) -> tuple[int, float]:
    """Drive Retry.increment() to exhaustion; return (retries granted, total sleep)."""
    granted, slept = 0, 0.0
    while True:
        try:
            retry = retry.increment(method="POST", url="/search", **kw)
        except MaxRetryError:
            return granted, slept
        granted += 1
        slept += retry.get_backoff_time()


class _Status500:
    status = 500

    def get_redirect_location(self) -> None:
        return None


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(
            ProtocolError("Connection aborted.", ConnectionResetError(54)), id="ConnectionReset"
        ),
        pytest.param(ReadTimeoutError(None, "/search", "Read timed out."), id="ReadTimeout"),
        pytest.param(ConnectTimeoutError("connect timed out"), id="ConnectTimeout"),
    ],
)
def test_every_error_class_gets_the_full_budget(error):
    """`total=8` must be the budget that fires, not a number the object merely reports.

    A draft of this policy set `connect=2, read=2, status=5` alongside `total=8` to
    fail fast on outages. urllib3 classifies a ConnectionReset as a *read* error, so
    that cut the budget from 8 to 2 for the mid-pagination reset the policy exists to
    survive — and `retry.total` still said 8. Measured on the two objects: reset/read/
    connect all 8 -> 2. This walks the real `increment()` so the number asserted is
    the one that would fire.
    """
    granted, slept = _walk(_read_retry(), error=error)
    assert granted == 8
    # 0+2+4+8+16+20+20+20 with backoff_max=20: the figure the docstring quotes.
    assert slept == 90.0


def test_status_retries_get_the_full_budget_too():
    granted, slept = _walk(_read_retry(), response=_Status500())
    assert granted == 8
    assert slept == 90.0


def test_retry_after_header_cannot_extend_a_sleep_past_backoff_max():
    """`backoff_max` does not cap `Retry-After`; only ignoring the header does.

    urllib3 sleeps the server's Retry-After on 429/503 when
    `respect_retry_after_header` is on, bounded by `retry_after_max` (default 21600 s),
    not by `backoff_max`. One 503 with a large header would stall a page for hours.
    """
    retry = _read_retry()
    assert retry.respect_retry_after_header is False

    resp = MagicMock()
    resp.status = 503
    resp.headers = {"Retry-After": "3600"}
    resp.get_redirect_location.return_value = None
    # Two increments first: urllib3's backoff is 0 until the history holds two
    # consecutive errors, and a 0 s backoff never reaches time.sleep — so on a
    # history-less Retry the assertion below is vacuous (it was, once).
    retry = retry.increment(method="POST", url="/search", response=resp)
    retry = retry.increment(method="POST", url="/search", response=resp)

    with patch("time.sleep") as sleep:
        retry.sleep(resp)
    sleep.assert_called_once()
    assert sleep.call_args.args[0] == 2.0, "backoff_factor * 2**(2-1), not the header's hour"

    # Control: the same policy with the header honoured sleeps the hour, so the
    # assertion above is on the flag and not on a sleep that never fires.
    with patch("time.sleep") as sleep:
        retry.new(respect_retry_after_header=True).sleep(resp)
    assert sleep.call_args.args[0] == 3600


@pytest.mark.parametrize("method", ["DELETE", "PUT", "PATCH"])
def test_write_methods_are_never_retried_by_the_read_policy(method):
    """The one property that makes "read paths only" true.

    `test_write_session_has_no_retries` shows the write session is a different object;
    this shows that even the read policy itself, mounted on the wrong session by a future
    refactor, would not retry a write on a 5xx.
    """
    retry = _read_retry()
    assert retry.is_retry(method, 500) is False
    assert retry.is_retry("POST", 500) is True
    assert retry.is_retry("GET", 500) is True


def test_each_retry_logs_a_warning(flaky_server, caplog):
    """urllib3 logs a status-forcelist retry at DEBUG only, and both crons pin the
    `urllib3` logger to WARNING — so without this line the 500-retry path, the whole
    point of the policy, leaves no trace in production logs."""
    url = flaky_server(fail_times=2)
    session = _session_from(stac_auth.resilient_stac_io(timeout=5))

    with caplog.at_level(logging.WARNING, logger=stac_auth.logger.name):
        session.post(f"{url}/search", json={})

    lines = [
        r.getMessage()
        for r in caplog.records
        if r.name == stac_auth.logger.name and r.levelno == logging.WARNING
    ]
    assert len(lines) == 2
    assert "POST" in lines[0] and "/search" in lines[0] and "HTTP 500" in lines[0]
    assert "7 retries left" in lines[0]
    assert "6 retries left" in lines[1]


# --- STAC_HTTP_TIMEOUT, parsed at call time --------------------------------------------


@pytest.mark.parametrize("raw", ["", "  "], ids=["empty", "blank"])
def test_timeout_env_empty_means_default(monkeypatch, raw):
    """`value: ""` is this fleet's spelling of an unset optional Argo parameter.

    The module used to compute `float(os.getenv("STAC_HTTP_TIMEOUT", "60"))` at import,
    so `""` raised during `import stac_auth` — before argparse and before the cleanup
    cron's summary guard — on every pod that imports the shipped wheel.
    """
    monkeypatch.setenv("STAC_HTTP_TIMEOUT", raw)
    assert stac_auth._search_timeout_s() == stac_auth._DEFAULT_SEARCH_TIMEOUT_S == 60.0
    assert stac_auth.resilient_stac_io().timeout == 60.0


def test_timeout_env_unset_means_default(monkeypatch):
    monkeypatch.delenv("STAC_HTTP_TIMEOUT", raising=False)
    assert stac_auth._search_timeout_s() == 60.0


def test_timeout_env_valid_value_is_used_by_both_entry_points(monkeypatch, flaky_server):
    monkeypatch.setenv("STAC_HTTP_TIMEOUT", "12.5")
    assert stac_auth.resilient_stac_io().timeout == 12.5
    assert stac_auth.open_resilient_client(flaky_server(fail_times=0))._stac_io.timeout == 12.5


@pytest.mark.parametrize("raw", ["abc", "0", "-5", "nan", "inf"])
def test_timeout_env_garbage_and_non_positive_are_rejected(monkeypatch, raw):
    """A loud ValueError, not a silent default and not "no timeout".

    `0`/negative are rejected rather than read as "no timeout" because no timeout is
    the 4.5 h hang this module exists to prevent. The error is raised at call time,
    inside the caller's guard, so the cleanup cron still emits its summary.
    """
    monkeypatch.setenv("STAC_HTTP_TIMEOUT", raw)
    with pytest.raises(ValueError, match="STAC_HTTP_TIMEOUT"):
        stac_auth._search_timeout_s()


def test_import_survives_an_empty_timeout_env():
    """The trap is import-time evaluation, which no in-process test can see: this module
    is already imported by the time a test sets the variable. So import it fresh."""
    env = {**os.environ, "STAC_HTTP_TIMEOUT": ""}
    # Fixed argv, no shell: nothing here is untrusted input.
    proc = subprocess.run(  # noqa: S603
        [sys.executable, "-c", "import stac_auth; print(stac_auth._search_timeout_s())"],
        cwd=os.path.dirname(stac_auth.__file__),
        env=env,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "60.0"
