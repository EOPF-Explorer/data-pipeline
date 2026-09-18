"""Tests for stac_auth.resilient_stac_io — the read-path retry policy.

Why these are behavioural and not just config assertions: the defect being fixed is a
retry policy that is *present and correct-looking* but never fires. urllib3's default
``Retry`` permits only idempotent methods, so a policy that forgets ``POST`` retries
nothing on STAC ``/search`` — and no assertion about the object's fields would notice the
difference between "retries POST" and "has a retry object". So the central test drives a
real socket and counts the requests the server actually received.
"""

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
import requests
import stac_auth


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
    assert stac_auth.resilient_stac_io().timeout == stac_auth._SEARCH_TIMEOUT_S
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

    assert (
        client._stac_io.timeout == stac_auth._SEARCH_TIMEOUT_S
    ), "timeout was reset by Client.open — open_resilient_client must pass it through"


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
