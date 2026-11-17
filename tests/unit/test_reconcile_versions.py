"""The sentinel-2-l2a-new reconcile: the diff, the bound, the replace's safety and the exit code.

Offline. The read side is an in-memory catalogue that records every search it answers (so
"batched, never per candidate" is a count), the write side a fake session under the real
``create_item`` and ``Run.replace`` (so "no PUT" is a count of ``session.put``), and the
no-retry property of the write session is driven over a real local socket.
"""

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pystac.stac_io
import pytest
import requests
from pystac_client.exceptions import APIError
from reconcile_versions import POLICIES, classify, main, open_write_client

EODC_FIXTURES = Path(__file__).parent.parent / "fixtures/eodc"
FIXTURE = EODC_FIXTURES / "S2C_MSIL2A_20260927T133931_N0513_R067_T28WDE_20260927T170511.json"
OLD_GENERATION_FIXTURE = (
    EODC_FIXTURES / "S2B_MSIL2A_20260907T130029_N0512_R138_T26TLL_20260907T145009.json"
)
NEW = "sentinel-2-l2a-new"
ARCHIVE = "sentinel-2-l2a"
EODC = "https://stac.core.eopf.eodc.eu"
STAC_API = "https://stac.invalid/stac"
RASTER = "https://raster.invalid/raster"
KEY = "EOPF_GEOZARR"
POLICY = POLICIES[NEW]


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    """Any read or write over the wire is a bug in a test (the socket test opts out)."""

    def _explode(*args, **kwargs):
        raise AssertionError("tests must not reach the network")

    monkeypatch.setattr(pystac.stac_io.DefaultStacIO, "read_text", _explode)
    monkeypatch.setattr("stac_auth.open_client", _explode)
    monkeypatch.setattr("stac_auth.open_resilient_client", _explode)
    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", _explode)


def props(version, dt="2026-11-01T10:00:00Z", *, as_string=False, cloud=12.5):
    """Item properties carrying one EOPF_GEOZARR value (None: no key) and a cloud cover."""
    software = {"EOPF_CPM": "3.0.0"} if version is None else {"EOPF_CPM": "3.0.0", KEY: version}
    return {"datetime": dt, "processing:software": json.dumps(software) if as_string else software,
            "eo:cloud_cover": cloud}  # fmt: skip


# --- the verdict table ---


@pytest.mark.parametrize(
    ("source", "target", "verdict"),
    [
        (None, None, "skipped_no_key"),
        ("0.12.0", None, "skipped_below_floor"),
        ("0.13.0rc1", None, "skipped_below_floor"),
        ("v0.13.1-0-g1a2b3c4", None, "skipped_unreadable"),
        ("1!0.1", None, "skipped_unreadable"),  # an epoch would replace every final target
        ("0.14.0rc1", None, "skipped_prerelease"),
        ("0.13.2.dev3", None, "skipped_prerelease"),
        ("0.13.0+eodc.1", None, "skipped_prerelease"),
        ("0.13.0", None, "create"),
        ("0.13.1", "0.13.0", "replace"),
        ("0.13.1", "no-key", "replace"),
        ("0.14.0", "0.14.0rc1", "replace"),
        # Review finding 7: a non-final target goes back to a release, even a lower one.
        ("0.13.1", "0.14.0rc1", "replace"),
        ("0.13.0", "0.13.2.dev3", "replace"),
        ("0.13.1", "0.13.1+eodc.1", "replace"),
        ("0.13.1", "0.13.1", "skipped_not_newer"),
        ("0.13.0", "0.13.1", "skipped_not_newer"),  # never a downgrade of a final release
        ("0.13.1", "0.13.1.post1", "skipped_not_newer"),  # a post-release is final
        ("0.13.1", "v0.13.1-0-g1a2b3c4", "refused_target_unreadable"),
        ("0.13.1", "1!0.1", "refused_target_unreadable"),
    ],
)
def test_the_verdict_for_each_source_and_target_version(source, target, verdict):
    target_props = None if target is None else props(None if target == "no-key" else target)
    assert classify(props(source), target_props, POLICY) == verdict


@pytest.mark.parametrize(
    ("cloud", "verdict"),
    [(89.9, "create"), (0, "create"), (90.0, "skipped_cloud_cover"),
     (None, "skipped_cloud_cover"), ("12.5", "skipped_cloud_cover"),
     (float("nan"), "skipped_cloud_cover"), (False, "skipped_cloud_cover"),
     (True, "skipped_cloud_cover")],
)  # fmt: skip
def test_cloud_cover_is_checked_here_not_only_by_the_search_filter(cloud, verdict):
    """Review finding 8: a missing or cloudy eo:cloud_cover is a counted skip, never a create."""
    assert classify(props("0.13.1", cloud=cloud), None, POLICY) == verdict


def test_the_policy_is_fixed_per_target_not_by_flags():
    assert set(POLICIES) == {NEW}
    assert (POLICY.key, str(POLICY.floor), POLICY.exclude_collection) == (KEY, "0.13.0", ARCHIVE)
    assert POLICY.acquired_from.isoformat() == "2026-09-29T00:00:00+00:00"
    assert (POLICY.source_stac_api, POLICY.source_collection) == (EODC, "sentinel-2-l2a-zarr3")


# --- fakes ---


class FakeSearch:
    def __init__(self, pages):
        self.pages = pages

    def pages_as_dicts(self):
        yield from self.pages


class FakeCatalogue:
    """A read Client over {collection: {id: properties} or a callable returning one}.

    It records every search it answers.
    """

    def __init__(self, collections):
        self.collections = collections
        self.searches = []

    def search(self, *, collections, limit, fields, ids=None, **kwargs):
        self.searches.append({"collections": collections, "ids": ids, "limit": limit,
                              "fields": fields, **kwargs})  # fmt: skip
        (collection,) = collections
        items = self.collections.get(collection, {})
        items = items() if callable(items) else items
        hits = [(i, p) for i, p in items.items() if ids is None or i in ids]
        return FakeSearch(
            [{"features": [{"id": i, "properties": p} for i, p in hits[s : s + limit]]}
             for s in range(0, len(hits), limit)]
        )  # fmt: skip


def answer(status, body=None):
    resp = requests.Response()
    resp.status_code = status
    resp._content = json.dumps(body).encode() if body is not None else b""
    return resp


class FakeTarget:
    """The target behind the write client: GET/POST/PUT on an in-memory collection.

    ``overrides`` maps (method, id) to a status, an exception, or for GET a raw body.
    """

    def __init__(self, items, overrides=None):
        self.items = {i: self.item(i, p) for i, p in items.items()}
        self.overrides = overrides or {}
        self.client = MagicMock(self_href=STAC_API)
        self.session = self.client._stac_io.session
        self.session.get.side_effect = self.get
        self.session.post.side_effect = self.post
        self.session.put.side_effect = self.put
        self.put_hook = None

    @staticmethod
    def item(item_id, properties):
        return {"type": "Feature", "stac_version": "1.0.0", "id": item_id, "collection": NEW,
                "properties": properties, "links": [], "assets": {}}  # fmt: skip

    def _override(self, method, item_id):
        got = self.overrides.get((method, item_id))
        if isinstance(got, Exception):
            raise got
        return got

    def get(self, url, *, timeout, allow_redirects):
        assert timeout and allow_redirects is False
        item_id = url.rsplit("/", 1)[1]
        assert url == f"{STAC_API}/collections/{NEW}/items/{item_id}"
        got = self._override("GET", item_id)
        if isinstance(got, int):
            return answer(got)
        if got is not None:
            return answer(200, got)
        return answer(200, self.items[item_id]) if item_id in self.items else answer(404)

    def post(self, url, json, **kwargs):
        assert url == f"{STAC_API}/collections/{NEW}/items" and kwargs["timeout"]
        status = self._override("POST", json["id"])
        if status:
            return answer(status)
        if json["id"] in self.items:
            return answer(409)
        self.items[json["id"]] = json
        return answer(201)

    def put(self, url, *, json, headers, timeout, allow_redirects):
        assert timeout and allow_redirects is False
        assert url == f"{STAC_API}/collections/{NEW}/items/{json['id']}"
        if self.put_hook:
            self.put_hook(json["id"])
        status = self._override("PUT", json["id"])
        if status:
            return answer(status)
        if json["id"] not in self.items:
            return answer(404)  # pgstac's update is delete + create: it never creates
        self.items[json["id"]] = json
        return answer(200)

    def writes(self):
        return [(c[0], (c.kwargs.get("json") or c.args[1])["id"]) for c in self.session.method_calls
                if c[0] in ("post", "put")]  # fmt: skip


class World:
    """EODC, the target and the archive for one test, and main() run against them."""

    def __init__(self, source, target=(), archive=(), fetched=None, overrides=None):
        # source/target: {id: version or None}; a source id's datetime orders it, newest first
        self.source = {i: props(v, f"2026-11-{10 + n:02d}T10:00:00Z", as_string=True)
                       for n, (i, v) in enumerate(source.items())}  # fmt: skip
        self.target = FakeTarget({i: props(v) for i, v in dict(target).items()}, overrides)
        self.eodc = FakeCatalogue({POLICY.source_collection: self.source})
        # The target as a search sees it now: what the write side holds, so re-runs converge.
        live = lambda: {i: item["properties"] for i, item in self.target.items.items()}  # noqa: E731
        self.explorer = FakeCatalogue({NEW: live, ARCHIVE: dict.fromkeys(archive, {})})
        self.fetched = dict(source) | dict(fetched or {})
        self.base = json.loads(FIXTURE.read_text())
        self.fetch = MagicMock(side_effect=self._fetch)

    def _fetch(self, api, collection, item_id):
        """The fetched item at ``fetched[item_id]``: a version, or "old" / "unbuildable"."""
        assert (api, collection) == (EODC, POLICY.source_collection)
        version = self.fetched[item_id]
        base = json.loads(OLD_GENERATION_FIXTURE.read_text()) if version == "old" else self.base
        software = {"EOPF_CPM": "3.0.0", KEY: version}
        if version in ("old", "unbuildable"):
            software[KEY] = "0.13.1"
        properties = {**base["properties"], "processing:software": software}
        if version == "unbuildable":
            del properties["proj:code"]  # build_proxy_item refuses it (ValueError)
        return {**base, "id": item_id, "properties": properties}

    def after_the_diff(self, change):
        """Apply ``change`` as each id is fetched: after the diff, before its write."""
        fetch = self._fetch

        def fetch_then_change(*args):
            change()
            return fetch(*args)

        self.fetch.side_effect = fetch_then_change

    def run(self, tmp_path, *extra, max_writes="10", dry_run=False):
        args = ["--collection", NEW, "--stac-api-url", STAC_API, "--raster-api-url", RASTER,
                *extra]  # fmt: skip
        if max_writes is not None:
            args += ["--max-writes", max_writes]
        if dry_run:
            args.append("--dry-run")
        else:
            args += ["--created-ids", str(tmp_path / "created.jsonl"),
                     "--replaced-ids", str(tmp_path / "replaced.jsonl"), "--run-id", "wf-1"]  # fmt: skip
        clients = {EODC: self.eodc, STAC_API: self.explorer}
        with (
            patch("reconcile_versions.stac_auth.open_resilient_client", side_effect=clients.get),
            patch("reconcile_versions.open_write_client", return_value=self.target.client),
            patch("reconcile_versions.fetch_source_item", self.fetch),
        ):
            return main(args)


def lines(path):
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def summary(caplog):
    (line,) = [r.getMessage() for r in caplog.records if r.getMessage().startswith("Summary for")]
    pairs = dict(field.split("=") for field in line.split(": ", 1)[1].split())
    return {key: int(value) for key, value in pairs.items()}


@pytest.fixture(autouse=True)
def info_logs(caplog):
    caplog.set_level("INFO", logger="reconcile_versions")


# --- the diff, end to end ---


def test_each_source_id_gets_one_verdict_and_the_counts_add_up(tmp_path, caplog):
    world = World(
        source={"nokey": None, "old": "0.12.0", "git": "v0.13.1-0-g1a2b3c4", "rc": "0.14.0rc1",
                "arch": "0.13.1", "new": "0.13.1", "same": "0.13.1", "down": "0.13.0",
                "up": "0.13.1", "bad": "0.13.1"},
        target={"same": "0.13.1", "down": "0.13.1", "up": "0.13.0", "bad": "v0.13.1-0-g1"},
        archive={"arch", "unrelated"},
    )  # fmt: skip
    assert world.run(tmp_path) == 0, "#522 review 1: an unreadable target is counted, loudly"
    assert "Look: refused_target_unreadable=1, e.g. bad" in caplog.text
    counts = summary(caplog)
    assert counts["source"] == 10
    assert sum(v for k, v in counts.items() if k != "source") == 10
    assert {k: v for k, v in counts.items() if v and k != "source"} == {
        "created": 1, "replaced": 1, "skipped_no_key": 1, "skipped_below_floor": 1,
        "skipped_unreadable": 1, "skipped_prerelease": 1, "skipped_in_archive": 1,
        "skipped_not_newer": 2, "refused_target_unreadable": 1,
    }  # fmt: skip
    assert sorted(world.target.writes()) == [("post", "new"), ("put", "up")]


def test_the_source_search_is_the_policy_range_with_the_registers_cloud_filter(tmp_path):
    world = World(source={"A": "0.13.1"})
    assert world.run(tmp_path, dry_run=True) == 0
    (search,) = world.eodc.searches
    assert search["collections"] == ["sentinel-2-l2a-zarr3"]
    assert search["datetime"] == "2026-09-29T00:00:00Z/.."
    assert search["filter"] == {"op": "<", "args": [{"property": "eo:cloud_cover"}, 90.0]}
    assert search["filter_lang"] == "cql2-json"


def test_the_diff_is_batched_never_one_search_per_candidate(tmp_path):
    """34.7k target items today: one search per candidate crossed the 1800 s deadline."""
    ids = [f"S{n:03d}" for n in range(250)]
    world = World(source=dict.fromkeys(ids, "0.13.1"), target=dict.fromkeys(ids[:40], "0.13.0"))
    assert world.run(tmp_path, dry_run=True) == 0
    assert len(world.eodc.searches) == 1
    targets = [s for s in world.explorer.searches if s["collections"] == [NEW]]
    archive = [s for s in world.explorer.searches if s["collections"] == [ARCHIVE]]
    assert len(targets) == 1 and targets[0]["ids"] is None
    assert [len(s["ids"]) for s in archive] == [100, 100, 10], "only the 210 create candidates"
    for search in world.eodc.searches + world.explorer.searches:
        assert search["limit"] == 1000
        assert search["fields"] == {"include": ["id", "properties.datetime",
                                                "properties.processing:software",
                                                "properties.eo:cloud_cover"]}  # fmt: skip


def test_an_id_in_the_converted_archive_is_never_created(tmp_path, caplog):
    """EODC's 30 Sep burst: ~800-1,000 scenes already in sentinel-2-l2a under the same ids."""
    world = World(source={"A": "0.13.1", "B": "0.13.1"}, archive={"A"})
    assert world.run(tmp_path) == 0
    assert world.target.writes() == [("post", "B")]
    assert summary(caplog)["skipped_in_archive"] == 1
    assert "   skipped_in_archive: 1, e.g. A\n" in caplog.text
    assert [line["id"] for line in lines(tmp_path / "created.jsonl")] == ["B"]
    world.fetch.assert_called_once()


def test_a_source_the_cloud_filter_should_have_dropped_is_never_written(tmp_path, caplog):
    """The fake catalogue ignores CQL2 `filter`, as EODC could: the policy must still hold."""
    world = World(source={"A": "0.13.1", "B": "0.13.1", "C": "0.13.1"})
    world.source["A"]["eo:cloud_cover"] = 96.7
    del world.source["B"]["eo:cloud_cover"]
    assert world.run(tmp_path) == 0
    assert world.target.writes() == [("post", "C")]
    assert summary(caplog)["skipped_cloud_cover"] == 2
    caplog.clear()
    world.source["C"]["eo:cloud_cover"] = 90.0
    assert world.run(tmp_path) == 1, "no usable source: e.g. EODC dropped the field"
    assert "has a usable EOPF_GEOZARR" in caplog.text


def test_an_ids_search_that_ignores_ids_is_refused_before_any_write(tmp_path, caplog):
    world = World(source={"A": "0.13.1"})
    unfiltered = world.explorer.search
    world.explorer.search = lambda **kw: unfiltered(
        **({**kw, "ids": None} if kw.get("ids") else kw)
    )
    world.explorer.collections[ARCHIVE] = {"X": {}}
    assert world.run(tmp_path) == 1
    assert world.target.writes() == []
    assert "it was not asked for" in caplog.text


@pytest.mark.parametrize(
    "source",
    [
        {"A": None, "B": None},
        {"A": "0.12.0", "B": None},
        {"A": "v0.13.1-0-g1a2b3c4", "B": "0.12.0"},
        {"A": "0.14.0rc1", "B": None},
        {},
    ],
    ids=["renamed-key", "below-floor", "new-stamp-format", "pre-releases", "empty"],
)
def test_a_source_with_nothing_usable_fails_loudly_and_writes_nothing(tmp_path, caplog, source):
    """A renamed key or data-model's git-describe stamp would otherwise be a green no-op."""
    world = World(source=source)
    assert world.run(tmp_path) == 1
    assert world.target.writes() == [] and not world.fetch.called
    assert "has a usable EOPF_GEOZARR >= 0.13.0" in caplog.text
    assert summary(caplog)["source"] == len(source), "the counts print before the refusal"


def test_one_usable_source_is_enough(tmp_path):
    world = World(
        source={"A": "v0.13.1-0-g1a2b3c4", "B": None, "C": "0.13.1"}, target={"C": "0.13.1"}
    )
    assert world.run(tmp_path) == 0


def test_a_converged_run_opens_no_write_client(tmp_path, caplog):
    """#522 review 8: with no action, the run reports and returns before building its Run."""
    world = World(source={"A": "0.13.1"}, target={"A": "0.13.1"})
    with patch("reconcile_versions.Run", side_effect=AssertionError("built a Run")):
        assert world.run(tmp_path) == 0
    assert summary(caplog)["skipped_not_newer"] == 1 and summary(caplog)["remaining"] == 0


def test_a_read_failure_writes_nothing(tmp_path, caplog):
    world = World(source={"A": "0.13.1"})
    world.explorer.search = MagicMock(side_effect=requests.ConnectionError("down"))
    assert world.run(tmp_path) == 1
    assert world.target.writes() == []
    assert "nothing written" in caplog.text


# --- the bound ---


def test_max_writes_takes_the_newest_n_and_reports_the_rest_then_converges(tmp_path, caplog):
    world = World(source={"A": "0.13.1", "B": "0.13.1", "C": "0.13.1", "D": "0.13.1", "E": "0.13.1"},
                  target={"B": "0.13.0", "E": "0.13.0"})  # fmt: skip
    assert world.run(tmp_path, max_writes="2") == 0
    assert world.target.writes() == [("put", "E"), ("post", "D")], "newest acquisition first"
    assert summary(caplog)["remaining"] == 3
    assert "remaining: 3 id(s) not reached" in caplog.text
    caplog.clear()
    assert world.run(tmp_path, max_writes="2") == 0
    assert summary(caplog)["remaining"] == 1
    caplog.clear()
    assert world.run(tmp_path, max_writes="2") == 0
    caplog.clear()
    world.target.session.reset_mock()
    assert world.run(tmp_path, max_writes="2") == 0
    assert world.target.writes() == []
    assert summary(caplog)["remaining"] == 0 and summary(caplog)["skipped_not_newer"] == 5


@pytest.mark.parametrize("bad", ["unbuildable", "old", "0.12.0", "gone", "unreadable"])
def test_ids_that_end_before_their_write_never_hold_up_the_rest(tmp_path, caplog, bad):
    """Review finding 2: such ids end the same way on every run. Eleven of them at the head
    of the order used to fill every run's bound, or trip the 10-failures stop, forever.
    "unreadable" (#522 review 2): readable in the diff, not when the replace GETs it."""
    head = [f"S{n:02d}" for n in range(11)]
    source = dict.fromkeys(["G1", "G2", *head], "0.13.1")  # the head ids are the newest
    if bad in ("gone", "unreadable"):  # the target search lists them; their GET does not
        world = World(source, target=dict.fromkeys(head, "0.13.0"),
                      overrides={("GET", i): 404 if bad == "gone"
                                 else FakeTarget.item(i, props("v0.13.1-0-g1")) for i in head})  # fmt: skip
    else:
        world = World(source, fetched=dict.fromkeys(head, bad))
    key = {"unbuildable": "skipped_unbuildable", "old": "skipped_old_generation",
           "0.12.0": "skipped_source_changed", "gone": "gone",
           "unreadable": "refused_target_unreadable"}[bad]  # fmt: skip
    for created, remaining in [(["G2"], 1), (["G2", "G1"], 0)]:  # two runs, one write each
        caplog.clear()
        code = world.run(tmp_path, max_writes="1")
        assert world.target.writes() == [("post", i) for i in created]
        counts = summary(caplog)
        assert (counts[key], counts["failed"], counts["remaining"]) == (11, 0, remaining)
        assert code == 0, "#522 review 1: else the run that converges fails too"


def test_a_write_that_fails_counts_toward_the_bound(tmp_path, caplog):
    """A POST or PUT that fails may have landed: --max-writes caps what is SENT."""
    world = World(source=dict.fromkeys("ABC", "0.13.1"), target={"C": "0.13.0"},
                  overrides={("POST", "B"): 502, ("PUT", "C"): requests.ReadTimeout("slow")})  # fmt: skip
    assert world.run(tmp_path, max_writes="2") == 1
    assert world.target.writes() == [("put", "C"), ("post", "B")]
    assert (summary(caplog)["failed"], summary(caplog)["remaining"]) == (2, 1)


@pytest.mark.parametrize("value", ["0", "-1", "", "ten", "1.5"])
def test_max_writes_is_a_positive_whole_number(tmp_path, value):
    with pytest.raises(SystemExit) as exc:
        World(source={}).run(tmp_path, max_writes=value)
    assert exc.value.code == 2


@pytest.mark.parametrize("dry_run", [True, False], ids=["dry-run", "real-run"])
def test_every_run_needs_max_writes(tmp_path, dry_run):
    """Review finding 6: a dry run without it had a bound of 0, so it fetched and checked
    nothing and still looked green."""
    world = World(source={"A": "0.13.1"})
    with pytest.raises(SystemExit) as exc:
        world.run(tmp_path, max_writes=None, dry_run=dry_run)
    assert exc.value.code == 2
    assert not world.fetch.called and world.eodc.searches == []


@pytest.mark.parametrize("drop", ["--created-ids", "--replaced-ids", "--run-id"])
def test_a_real_run_needs_both_lists_and_a_run_id(tmp_path, drop, caplog):
    args = {"--max-writes": "10", "--created-ids": str(tmp_path / "c"),
            "--replaced-ids": str(tmp_path / "r"), "--run-id": "wf-1"}  # fmt: skip
    del args[drop]
    argv = ["--collection", NEW, "--stac-api-url", STAC_API, "--raster-api-url", RASTER]
    assert main(argv + [x for pair in args.items() for x in pair]) == 1
    assert drop in caplog.text


@pytest.mark.parametrize("collection", [ARCHIVE, "sentinel-2-l2a-staging", "sentinel-2-l2a-new "])
def test_any_other_target_is_refused_before_any_network_call(caplog, collection):
    argv = ["--collection", collection, "--stac-api-url", STAC_API, "--raster-api-url", RASTER,
            "--max-writes", "10", "--dry-run"]  # fmt: skip
    assert main(argv) == 1
    assert f"Refusing --collection {collection!r}" in caplog.text, "refused, not failed later"


@pytest.mark.parametrize("flag", ["--stac-api-url", "--raster-api-url"])
def test_every_url_must_be_https(flag):
    argv = {"--collection": NEW, "--stac-api-url": STAC_API, "--raster-api-url": RASTER,
            "--max-writes": "1"}  # fmt: skip
    argv[flag] = "http://stac.invalid"
    assert main([x for pair in argv.items() for x in pair] + ["--dry-run"]) == 1


def test_a_dry_run_checks_the_first_n_and_writes_nothing(tmp_path, caplog):
    world = World(source={"A": "0.13.1", "B": "0.13.1", "C": "0.13.1"}, target={"A": "0.13.0"})
    assert world.run(tmp_path, max_writes="2", dry_run=True) == 0
    assert world.target.writes() == []
    assert not (tmp_path / "created.jsonl").exists() and not (tmp_path / "replaced.jsonl").exists()
    counts = summary(caplog)
    assert (counts["would_create"], counts["would_replace"], counts["remaining"]) == (2, 0, 1)
    assert [c.args[2] for c in world.fetch.call_args_list] == ["C", "B"]


def test_a_dry_run_reaches_the_replace_check_before_stopping(tmp_path, caplog):
    world = World(source={"A": "0.13.1"}, target={"A": "0.13.0"})
    assert world.run(tmp_path, dry_run=True) == 0
    world.target.session.get.assert_called_once()
    world.target.session.put.assert_not_called()
    assert "would replace A (EOPF_GEOZARR 0.13.0 -> 0.13.1)" in caplog.text


# --- create ---


def test_a_create_is_recorded_as_the_register_records_it(tmp_path):
    """rollback_created_items.py reads these lines: id + collection, nothing else required."""
    world = World(source={"A": "0.13.1"})
    assert world.run(tmp_path) == 0
    (line,) = lines(tmp_path / "created.jsonl")
    assert set(line) == {"id", "collection", "ts"} and (line["id"], line["collection"]) == (
        "A",
        NEW,
    )
    created = world.target.items["A"]
    assert created["collection"] == NEW and "expires" not in created["properties"]
    assert json.loads(world.source["A"]["processing:software"])[KEY] == "0.13.1"
    assert created["properties"]["processing:software"][KEY] == "0.13.1"


def test_create_answers_follow_the_registers_rules(tmp_path, caplog):
    fail = {("POST", "C"): 502, ("POST", "D"): requests.ReadTimeout("slow"), ("POST", "E"): 400}
    world = World(source=dict.fromkeys("ABCDE", "0.13.1"), overrides=fail)
    # B is created by someone else after the diff: its POST answers 409.
    world.after_the_diff(lambda: world.target.items.setdefault("B", {"properties": {}}))
    assert world.run(tmp_path) == 1
    recorded = [
        (line["id"], line.get("uncertain", False)) for line in lines(tmp_path / "created.jsonl")
    ]
    assert recorded == [("D", True), ("C", True), ("A", False)]
    counts = summary(caplog)
    assert (counts["created"], counts["exists"], counts["failed"]) == (1, 1, 3)
    world.target.session.put.assert_not_called()


# --- replace ---


def test_a_replace_records_the_pre_image_before_the_put_and_the_outcome_after(tmp_path):
    world = World(source={"A": "0.13.1"}, target={"A": "0.13.0"})
    pre_image = json.loads(json.dumps(world.target.items["A"]))
    seen_at_put = []
    world.target.put_hook = lambda _id: seen_at_put.extend(lines(tmp_path / "replaced.jsonl"))
    assert world.run(tmp_path) == 0
    (before,) = seen_at_put
    assert before == {**before, "item_id": "A", "collection": NEW, "run_id": "wf-1",
                      "phase": "before_put", "old_version": "0.13.0", "new_version": "0.13.1",
                      "pre_image": pre_image}  # fmt: skip
    after = lines(tmp_path / "replaced.jsonl")[1]
    assert (after["phase"], after["outcome"], after["status"]) == ("after_put", "replaced", 200)
    assert world.target.items["A"]["properties"]["processing:software"][KEY] == "0.13.1"
    assert lines(tmp_path / "created.jsonl") == []
    # rollback_created_items.read_created_ids refuses a line without a string "id": fed this
    # list by mistake, it stops instead of deleting items that existed before the run.
    assert not any("id" in line for line in lines(tmp_path / "replaced.jsonl"))


@pytest.mark.parametrize(
    "body",
    [
        {"id": "A"},
        {"type": "Feature", "id": "A", "collection": NEW, "properties": None},
        {"type": "FeatureCollection", "features": []},
        {"type": "Collection", "id": "A", "collection": NEW, "properties": {}},
        {"type": "Feature", "id": "B", "collection": NEW, "properties": {}},
        {"type": "Feature", "id": "A", "collection": ARCHIVE, "properties": {}},
        {"type": "Feature", "id": "A", "collection": NEW, "properties": props("v0.13.1-0-g1")},
        302,
        500,
    ],
    ids=["id-only", "null-properties", "feature-collection", "not-a-feature", "other-id",
         "other-collection", "unreadable", "redirect", "server-error"],
)  # fmt: skip
def test_a_get_that_is_not_the_item_refuses_the_put(tmp_path, caplog, body):
    """A body that is not the item is refused as in the diff (#522 review 2): counted, not a
    failure. A status other than 200 or 404 is a failure."""
    world = World(source={"A": "0.13.1", "B": "0.13.1"}, target={"A": "0.13.0", "B": "0.13.0"},
                  overrides={("GET", "A"): body})  # fmt: skip
    failed = isinstance(body, int)
    assert world.run(tmp_path) == failed
    assert world.target.writes() == [("put", "B")], "A refused, B still replaced"
    assert [line["item_id"] for line in lines(tmp_path / "replaced.jsonl")] == ["B", "B"]
    counts = summary(caplog)
    assert (counts["failed"], counts["refused_target_unreadable"]) == (failed, not failed)
    assert "refused" in caplog.text


def test_a_body_that_is_not_json_refuses_the_put(tmp_path):
    world = World(source={"A": "0.13.1"}, target={"A": "0.13.0"})
    world.target.session.get.side_effect = lambda *a, **k: answer(200)
    assert world.run(tmp_path) == 1
    world.target.session.put.assert_not_called()


def test_an_item_gone_before_the_get_is_skipped_never_created(tmp_path, caplog):
    """The clean may delete it mid-run; a PUT would 404 and the replace path never POSTs."""
    world = World(source={"A": "0.13.1"}, target={"A": "0.13.0"}, overrides={("GET", "A"): 404})
    assert world.run(tmp_path) == 0
    assert world.target.writes() == []
    assert not world.fetch.called, "review finding 10: the GET comes before the source fetch"
    assert summary(caplog)["gone"] == 1
    assert lines(tmp_path / "created.jsonl") == lines(tmp_path / "replaced.jsonl") == []


def test_an_item_gone_between_the_get_and_the_put_is_gone(tmp_path, caplog):
    world = World(source={"A": "0.13.1"}, target={"A": "0.13.0"}, overrides={("PUT", "A"): 404})
    assert world.run(tmp_path) == 0
    assert lines(tmp_path / "replaced.jsonl")[1]["outcome"] == "gone"
    assert summary(caplog)["gone"] == 1


def test_a_target_caught_up_since_the_diff_costs_no_fetch_and_no_put(tmp_path, caplog):
    """The GET is re-compared, so a newer target is never downgraded; and it comes first
    (review finding 10), so the EODC fetch and build are skipped too."""
    world = World(source={"A": "0.13.1"}, target={"A": "0.13.0"},
                  overrides={("GET", "A"): FakeTarget.item("A", props("0.13.1"))})  # fmt: skip
    assert world.run(tmp_path) == 0
    world.target.session.put.assert_not_called()
    assert not world.fetch.called
    assert summary(caplog)["skipped_not_newer"] == 1


@pytest.mark.parametrize("failure", [requests.ReadTimeout("slow"), 502, 422])
def test_a_failed_put_fails_the_run_and_never_lands_in_created_ids(tmp_path, caplog, failure):
    """Review finding 1: an uncertain write over an existing id must not be rollback input."""
    world = World(source={"A": "0.13.1", "B": "0.13.1"}, target={"A": "0.13.0", "B": "0.13.0"},
                  overrides={("PUT", "A"): failure})  # fmt: skip
    assert world.run(tmp_path) == 1
    assert lines(tmp_path / "created.jsonl") == []
    outcomes = {line["item_id"]: line.get("outcome") for line in lines(tmp_path / "replaced.jsonl")
                if line["phase"] == "after_put"}  # fmt: skip
    assert outcomes == {"A": "refused" if failure == 422 else "uncertain", "B": "replaced"}
    counts = summary(caplog)
    assert (counts["replaced"], counts["failed"]) == (1, 1)


def test_a_source_that_changed_since_the_diff_is_not_written(tmp_path, caplog):
    """C: the fetched source is no newer than the target as just read, so no PUT."""
    world = World(source={"A": "0.13.1", "B": "0.13.1", "C": "0.13.1"},
                  target={"A": "0.13.0", "C": "0.13.0"},
                  fetched={"A": "0.12.0", "B": "old", "C": "0.13.0"})  # fmt: skip
    assert world.run(tmp_path) == 0
    assert world.target.writes() == []
    counts = summary(caplog)
    assert (counts["skipped_source_changed"], counts["skipped_old_generation"]) == (2, 1)


def test_a_pre_release_target_is_brought_back_to_a_release_once(tmp_path, caplog):
    world = World(source={"A": "0.13.1"}, target={"A": "0.14.0rc1"})
    assert world.run(tmp_path) == 0
    assert world.target.writes() == [("put", "A")]
    assert lines(tmp_path / "replaced.jsonl")[0]["old_version"] == "0.14.0rc1"
    caplog.clear()
    assert world.run(tmp_path) == 0
    assert world.target.writes() == [("put", "A")], "converged: 0.13.1 is final"
    assert summary(caplog)["skipped_not_newer"] == 1


def test_the_fetched_version_is_the_one_written_and_compared(tmp_path, caplog):
    world = World(source={"A": "0.13.1"}, target={"A": "0.13.0"}, fetched={"A": "0.13.2"})
    assert world.run(tmp_path) == 0
    assert lines(tmp_path / "replaced.jsonl")[0]["new_version"] == "0.13.2"


# --- failures and stops ---


def test_ends_that_recur_on_every_run_exit_0_and_name_sample_ids(tmp_path, caplog):
    """#522 review 1: an unbuildable source or an unreadable target ends the same way on every
    run, the one that converges included. Counted and named, never a failed run."""
    world = World(source={"A": "0.13.1", "B": "0.13.1", "C": "0.13.1", "D": "0.13.1"},
                  target={"C": "v0.13.1-0-g1", "D": "0.13.0"}, fetched={"A": "unbuildable"},
                  overrides={("GET", "D"): FakeTarget.item("D", props("1!0.1"))})  # fmt: skip
    assert world.run(tmp_path) == 0
    assert world.target.writes() == [("post", "B")]
    counts = summary(caplog)
    assert (counts["skipped_unbuildable"], counts["refused_target_unreadable"]) == (1, 2)
    assert "Look: skipped_unbuildable=1, e.g. A\n" in caplog.text
    assert "Look: refused_target_unreadable=2, e.g. C, D\n" in caplog.text


@pytest.mark.parametrize("failure", [requests.ConnectionError("reset"), requests.HTTPError("404")])
def test_a_source_fetch_that_fails_fails_the_run(tmp_path, caplog, failure):
    """Transient, or a 404 an EODC change could turn every id into: never a converged run."""
    world = World(source={"A": "0.13.1", "B": "0.13.1"})

    def fetch(api, collection, item_id):
        if item_id == "B":
            raise failure
        return world._fetch(api, collection, item_id)

    world.fetch.side_effect = fetch
    assert world.run(tmp_path) == 1
    assert world.target.writes() == [("post", "A")]
    assert summary(caplog)["failed"] == 1


def test_one_failing_write_does_not_stop_the_batch_and_fails_the_run(tmp_path, caplog):
    world = World(source=dict.fromkeys("ABC", "0.13.1"), overrides={("POST", "B"): 400})
    assert world.run(tmp_path) == 1
    assert sorted(world.target.writes()) == [("post", "A"), ("post", "B"), ("post", "C")]
    counts = summary(caplog)
    assert (counts["created"], counts["failed"]) == (2, 1)


def test_an_unwritable_replaced_ids_list_stops_the_run_before_the_put(tmp_path, caplog):
    world = World(source={"A": "0.13.1", "B": "0.13.1"}, target={"A": "0.13.0", "B": "0.13.0"})
    world.after_the_diff(lambda: (tmp_path / "replaced.jsonl").chmod(0o444))
    assert world.run(tmp_path) == 1
    world.target.session.put.assert_not_called()
    assert (summary(caplog)["failed"], summary(caplog)["remaining"]) == (1, 1)
    assert "Stopping: cannot record B" in caplog.text


def test_ten_failures_in_a_row_stop_the_run(tmp_path, caplog):
    ids = [f"S{n:02d}" for n in range(12)]
    world = World(source=dict.fromkeys(ids, "0.13.1"),
                  overrides={("POST", i): 503 for i in ids})  # fmt: skip
    assert world.run(tmp_path, max_writes="20") == 1
    assert len(world.target.writes()) == 10
    assert (summary(caplog)["failed"], summary(caplog)["remaining"]) == (10, 2)


# --- the write session, over a real socket ---


class _CountingHandler(BaseHTTPRequestHandler):
    """A STAC landing page at /, and 503 + Retry-After (or a slow answer) on item paths."""

    seen: list[tuple[str, str, str | None]] = []
    delay = 0.0
    landing_delay = 0.0

    def _respond(self):
        type(self).seen.append((self.command, self.path, self.headers.get("Authorization")))
        if self.path == "/" and self.command == "GET":
            time.sleep(type(self).landing_delay)
            base = f"http://127.0.0.1:{self.server.server_port}"
            body = json.dumps({"type": "Catalog", "id": "t", "stac_version": "1.0.0",
                               "description": "t",
                               "conformsTo": ["https://api.stacspec.org/v1.0.0/core"],
                               "links": [{"rel": "self", "href": f"{base}/"},
                                         {"rel": "root", "href": f"{base}/"}]}).encode()  # fmt: skip
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.command == "PUT":
            self.rfile.read(int(self.headers["Content-Length"]))
        time.sleep(type(self).delay)
        self.send_response(503)
        self.send_header("Retry-After", "0")
        self.send_header("Content-Length", "0")
        self.end_headers()

    do_GET = do_PUT = _respond

    def log_message(self, *args):
        pass


@pytest.fixture
def counting_server():
    _CountingHandler.seen = []
    _CountingHandler.delay = _CountingHandler.landing_delay = 0.0
    server = HTTPServer(("127.0.0.1", 0), _CountingHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()


def test_the_write_session_sends_each_get_and_put_once(counting_server):
    """Review finding 6: pystac-client's default Retry(total=5) re-sent a PUT six times on a
    503 + Retry-After. The reconcile's session must send every request exactly once."""
    session = open_write_client(counting_server)._stac_io.session
    url = f"{counting_server}/collections/{NEW}/items/A"
    assert session.get(url, timeout=5, allow_redirects=False).status_code == 503
    assert session.put(url, json={"id": "A"}, timeout=5, allow_redirects=False).status_code == 503
    assert [(m, p) for m, p, _ in _CountingHandler.seen if p != "/"] == [
        ("GET", f"/collections/{NEW}/items/A"),
        ("PUT", f"/collections/{NEW}/items/A"),
    ]


def test_a_put_that_times_out_is_not_re_sent(counting_server):
    _CountingHandler.delay = 1.0
    session = open_write_client(counting_server)._stac_io.session
    with pytest.raises(requests.ReadTimeout):
        session.put(f"{counting_server}/collections/{NEW}/items/A", json={"id": "A"}, timeout=0.2)
    time.sleep(1.5)  # a retry would be queued behind the slow answer, then recorded
    assert [m for m, p, _ in _CountingHandler.seen if p != "/"] == ["PUT"]


def test_opening_the_write_client_times_out(counting_server, monkeypatch):
    """Review finding 3: a landing page that never answers must not hang the run while it
    holds the register's semaphore."""
    monkeypatch.setattr("reconcile_versions.WRITE_TIMEOUT_S", 0.2)
    _CountingHandler.landing_delay = 1.0
    started = time.monotonic()
    with pytest.raises(APIError, match="timed out"):
        open_write_client(counting_server)
    assert time.monotonic() - started < 0.9


def test_the_write_session_carries_the_bearer(counting_server, oidc_env, token_response):
    with patch("stac_auth.httpx.post", return_value=token_response()):
        session = open_write_client(counting_server)._stac_io.session
        session.put(f"{counting_server}/collections/{NEW}/items/A", json={"id": "A"}, timeout=5)
    assert _CountingHandler.seen[-1] == ("PUT", f"/collections/{NEW}/items/A", "Bearer test-token")
