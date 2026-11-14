"""Rewrite catalogue items' titiler links to the titiler-eopf 0.12 item-route form.

When prod /raster moves from titiler-eopf 0.11 to 0.12, every link the catalogue holds in the 0.11
form breaks: ``variables=`` and path-style ``expression=`` return 422, and the bare item ``/viewer``
returns 404. This tool rewrites them in place to what ``scripts/raster_links.py`` builds, the builder
register uses, so a migrated item and a newly registered one carry identical links. It touches only
the ``viewer``, ``xyz`` and ``tilejson`` links and the ``thumbnail`` asset.

Each collection has an entry in ``PASSES``: a precondition (what the item's data must hold for
the target form to render), the forms its items may hold today, and the form to write; all three
see the item (orbit, datetime, pinned slice, assets). An item is written only when it meets the
precondition and its links match one of those forms; an item already in the target form is
skipped, and anything else is reported as unrecognised, with the reason, and left alone. S2 L2A,
S1 RTC (cubes and acquisitions) and OLCI have entries. L1C has none, because no 0.12 route renders
it as stored, so the tool refuses it; ``--mapping`` only lets a staging or scratch copy (an id
that starts with the entry's name) use an entry.

Safety properties (as in ``repair_stac_raster_links.py`` and ``fill_coordinate_holes.py``):
- dry run by default; ``--apply`` needs ``--backup-dir`` and the OIDC writer env
- ``--max-writes`` is a hard bound on PUTs, checked before each one, so a run stops at an item
  boundary. Never bound a run with a kill or a timeout
- each item is re-read just before its PUT, and the body, the pre-image and the form check all use
  that fresh copy, so a write that landed since the page read is kept, not reverted (the API has
  no If-Match). A dry run makes the same GETs, so its timing includes them
- the fresh copy, with the links its PUT writes, is appended to a pre-image JSONL and fsync'd
  BEFORE the PUT; a PUT that fails leaves its pre-image too
- updates only: a PUT, never a POST. An item that is gone (GET 404, or PUT 404 then GET 404: e.g.
  the hourly historical-cleanup deleted it) is logged and skipped, and is not a failure. A PUT 404
  on an item that is still there is a concurrent writer, and a failure
- every other error, whatever raises it, is a failure; ``MAX_CONSECUTIVE_FAILURES`` in a row, on
  the write path or on the page (two streaks, so page skips never hide failing writes), stop the
  run at the next item boundary, as does a page read that fails. The PUTs in flight finish and
  are tallied, and the summary always prints
- ``--restore`` puts each pre-image's links back (only those: anything written since is kept)
  where the item's fresh links are still what this tool wrote, and skips and reports the rest as
  stale. It honours ``--max-writes`` and dry run, and writes its own pre-image, so a restore can
  be undone

Not a ``migrate_catalog`` migration: that runner writes by default, records only the post-image,
counts a 404 as a write failure and has no restore, and changing those defaults would change every
migration. This tool reuses its write-body builder, the shared read client and the write auth.
"""

import argparse
import copy
import fcntl
import json
import logging
import math
import os
import re
import sys
import threading
import time
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from datetime import datetime
from pathlib import Path
from time import monotonic
from typing import Any, NamedTuple, TextIO
from urllib.parse import parse_qsl, quote, urlsplit

import requests

scripts_dir = Path(__file__).parent.parent / "scripts"
if str(scripts_dir) not in sys.path:
    sys.path.insert(0, str(scripts_dir))

import stac_auth  # noqa: E402
from _migrate_catalog.runner import _transaction_body  # noqa: E402
from raster_links import (  # noqa: E402
    OLCI_ASSET,
    OLCI_RGB,
    S1_ACQUISITION_THUMBNAIL_TITLE,
    S1_ORBIT_ASSETS,
    S1_TITLE,
    S2_COLOR_FORMULA,
    S2_LEGACY_QUERY,
    missing_rgb_bands,
    olci_links,
    olci_thumbnail,
    s1_rtc_acquisition_links,
    s1_rtc_acquisition_thumbnail,
    s1_rtc_cube_links,
    s1_rtc_cube_thumbnail,
    s2_true_colour_links,
    s2_true_colour_thumbnail,
)

logger = logging.getLogger(__name__)

RASTER = "https://api.explorer.eopf.copernicus.eu/raster"
RSTAGING = "https://api.explorer.eopf.copernicus.eu/rstaging"
PREIMAGE_FORMAT = "item-api-links-preimage/1"
VIZ_RELS = frozenset({"viewer", "xyz", "tilejson"})
# As register_proxy: a streak, reset by an item that gets through on the same path (see
# LinkRun.write_streak), so the API refusing writes stops a run, a handful of bad items does not.
MAX_CONSECUTIVE_FAILURES = 10
WRITE_TIMEOUT_S = 30
PAGE_SIZE = stac_auth.DEFAULT_PAGE_SIZE
# Sequential PUT latency against prod (README_MIGRATIONS, "Concurrency"): the dry-run projection's
# assumption unless --put-seconds gives the mean PUT an --apply drill measured.
ASSUMED_PUT_SECONDS = 0.412

# An item's managed parts: its viewer/xyz/tilejson links, in order, and its thumbnail asset.
Links = tuple[list[dict[str, Any]], dict[str, Any] | None]


def legacy_s2(collection: str, item_id: str) -> Links:
    """The 0.11 sentinel-2 links register_v1 wrote up to v1.21.x: bare ``/viewer``, ``variables=``
    + ``bidx=1``, a PNG preview. The only form in all 173,720 sentinel-2-l2a items backed up on
    2026-09-23 (restamp_expires recovery files)."""
    base = f"{RASTER}/collections/{collection}/items/{item_id}"
    query = S2_LEGACY_QUERY
    links = [
        {
            "rel": "viewer",
            "href": f"{base}/viewer",
            "type": "text/html",
            "title": f"Viewer for {item_id}",
        },
        {
            "rel": "xyz",
            "href": f"{base}/tiles/WebMercatorQuad/{{z}}/{{x}}/{{y}}.png?{query}",
            "type": "image/png",
            "title": "Sentinel-2 L2A True Color",
        },
        {
            "rel": "tilejson",
            "href": f"{base}/WebMercatorQuad/tilejson.json?{query}",
            "type": "application/json",
            "title": f"TileJSON for {item_id}",
        },
    ]
    thumbnail = {
        "href": f"{base}/preview?format=png&{query}",
        "type": "image/png",
        "title": "Sentinel-2 L2A True Color Preview",
        "roles": ["thumbnail"],
    }
    return links, thumbnail


def s2_target(raster_api_url: str, collection: str, item_id: str) -> Links:
    return (
        s2_true_colour_links(raster_api_url, collection, item_id),
        s2_true_colour_thumbnail(raster_api_url, collection, item_id),
    )


def _s2_problem(item: dict[str, Any]) -> str | None:
    if missing := missing_rgb_bands(item.get("assets", {}).get("reflectance")):
        return f"reflectance has no {missing} band"
    return None


# --- S1 RTC ---
# Every acquisition's 0.11 links point at its cube, on this collection; its 0.12 links do not.
S1_CUBE_COLLECTION = "sentinel-1-grd-rtc-staging"
_SLICE = re.compile(r"time=(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)")


def _hrefless(item: dict[str, Any]) -> list[str]:
    assets = item.get("assets", {})
    return sorted(k for k, a in assets.items() if not (isinstance(a, dict) and a.get("href")))


def _s1_orbit(item: dict[str, Any]) -> str:
    return str(item.get("properties", {}).get("sat:orbit_state", ""))


def _s1_pin(item: dict[str, Any]) -> str | None:
    """A cube's pinned slice: the sel time on its xyz link, at top level (0.11) or inside
    ``assets`` (0.12). None without one: a cube link without a slice renders the wrong picture."""
    xyz = next((x.get("href", "") for x in item.get("links", []) if x.get("rel") == "xyz"), "")
    params = dict(parse_qsl(urlsplit(xyz).query))
    options = params.get("assets", "").split("|")
    sel = params.get("sel", "") or next((o[4:] for o in options if o.startswith("sel=")), "")
    return m.group(1) if (m := _SLICE.fullmatch(sel)) else None


def _s1_acquisition_time(item: dict[str, Any]) -> str | None:
    """The acquisition's datetime to the second: the slice its links select."""
    try:
        return datetime.fromisoformat(item["properties"]["datetime"]).strftime("%Y-%m-%dT%H:%M:%S")
    except (KeyError, TypeError, ValueError):
        return None


def _legacy_s1(base: str, item_id: str, orbit: str, sel_time: str, thumbnail_title: str) -> Links:
    """The 0.11 S1 form register_v1's render path (cubes) and register_per_acquisition write:
    path-style expression, bidx=1, a top-level sel, tilesize=256 on the links, a PNG preview."""
    expression = f"/{orbit}:vv;/{orbit}:vh;(/{orbit}:vv)/(/{orbit}:vh)"
    query = (
        f"expression={quote(expression, safe='')}"
        "&rescale=0.0%2C0.4&rescale=0.0%2C0.1&rescale=1.0%2C15.0&bidx=1"
    )
    sel = f"sel=time={quote(sel_time, safe='')}"
    linked = f"{query}&tilesize=256&{sel}"
    links = [
        {
            "rel": "viewer",
            "href": f"{base}/WebMercatorQuad/map.html?{linked}",
            "type": "text/html",
            "title": S1_TITLE,
        },
        {
            "rel": "tilejson",
            "href": f"{base}/WebMercatorQuad/tilejson.json?{linked}",
            "type": "application/json",
            "title": f"TileJSON for {item_id}",
        },
        {
            "rel": "xyz",
            "href": f"{base}/tiles/WebMercatorQuad/{{z}}/{{x}}/{{y}}.png?{linked}",
            "type": "image/png",
            "title": S1_TITLE,
        },
    ]
    thumbnail = {
        "href": f"{base}/preview?format=png&{query}&{sel}",
        "type": "image/png",
        "title": thumbnail_title,
        "roles": ["thumbnail"],
    }
    return links, thumbnail


def _s1_cube_problem(item: dict[str, Any]) -> str | None:
    # The six single-orbit cubes list the other orbit's assets without an href, and 0.12 then
    # fails on any route: report them until those assets are dropped or titiler-eopf is fixed.
    if hrefless := _hrefless(item):
        return f"asset without href: {hrefless}"
    if (orbit := _s1_orbit(item)) not in S1_ORBIT_ASSETS:
        return f"sat:orbit_state is {orbit!r}"
    if not (item.get("assets", {}).get(S1_ORBIT_ASSETS[orbit]) or {}).get("href"):
        return f"no {S1_ORBIT_ASSETS[orbit]} asset with an href"
    if _s1_pin(item) is None:
        return "no pinned slice (sel=time=…) on its xyz link"
    return None


def legacy_s1_cube(collection: str, item: dict[str, Any]) -> Links:
    base = f"{RASTER}/collections/{collection}/items/{item['id']}"
    return _legacy_s1(base, item["id"], _s1_orbit(item), _s1_pin(item) or "", S1_TITLE)


def s1_cube_target(raster_api_url: str, collection: str, item: dict[str, Any]) -> Links:
    orbit, pin = _s1_orbit(item), _s1_pin(item) or ""
    return (
        s1_rtc_cube_links(raster_api_url, collection, item["id"], orbit, pin),
        s1_rtc_cube_thumbnail(raster_api_url, collection, item["id"], orbit, pin),
    )


def _s1_acquisition_problem(item: dict[str, Any]) -> str | None:
    if hrefless := _hrefless(item):
        return f"asset without href: {hrefless}"
    if (orbit := _s1_orbit(item)) not in S1_ORBIT_ASSETS:
        return f"sat:orbit_state is {orbit!r}"
    asset = item.get("assets", {}).get(S1_ORBIT_ASSETS[orbit]) or {}
    bands = {str(b.get("name", "")) for b in asset.get("bands") or [] if isinstance(b, dict)}
    if not {"vv", "vh"} <= bands:
        return f"no {S1_ORBIT_ASSETS[orbit]} asset with bands vv,vh"
    if _s1_acquisition_time(item) is None:
        return "no datetime"
    return None


def legacy_s1_acquisition(item: dict[str, Any]) -> Links:
    cube_id = item["id"].rsplit("-", 1)[0]  # s1-rtc-{tile}-{date}t{time} -> s1-rtc-{tile}
    base = f"{RASTER}/collections/{S1_CUBE_COLLECTION}/items/{cube_id}"
    sel_time = _s1_acquisition_time(item) or ""
    return _legacy_s1(base, item["id"], _s1_orbit(item), sel_time, S1_ACQUISITION_THUMBNAIL_TITLE)


def s1_acquisition_target(raster_api_url: str, collection: str, item: dict[str, Any]) -> Links:
    orbit, sel_time = _s1_orbit(item), _s1_acquisition_time(item) or ""
    return (
        s1_rtc_acquisition_links(raster_api_url, collection, item["id"], orbit, sel_time),
        s1_rtc_acquisition_thumbnail(raster_api_url, collection, item["id"], orbit, sel_time),
    )


# --- OLCI ---
_OLCI_LEGACY_QUERY = "&".join(
    [f"variables={quote('/measurements/r0:' + band, safe='')}" for band, _ in OLCI_RGB]
    + [f"rescale={quote(r, safe='')}" for _, r in OLCI_RGB]
    + [f"color_formula={quote(S2_COLOR_FORMULA, safe='')}", "bidx=1"]
)


def _olci_problem(item: dict[str, Any]) -> str | None:
    # The bare variable names (oa08_radiance) resolve only in the measurements group.
    href = str((item.get("assets", {}).get(OLCI_ASSET) or {}).get("href") or "")
    if not href.rstrip("/").endswith("/measurements"):
        return f"no {OLCI_ASSET} asset with an href to the measurements group"
    return None


def legacy_olci(collection: str, item_id: str) -> Links:
    """The 0.11 form register_v1 (rc8, S3_VIZ_ENABLED) wrote: item route, full-path variables."""
    base = f"{RASTER}/collections/{collection}/items/{item_id}"
    query = _OLCI_LEGACY_QUERY
    links = [
        {
            "rel": "viewer",
            "href": f"{base}/viewer",
            "type": "text/html",
            "title": f"Viewer for {item_id}",
        },
        {
            "rel": "xyz",
            "href": f"{base}/tiles/WebMercatorQuad/{{z}}/{{x}}/{{y}}.png?{query}",
            "type": "image/png",
            "title": "Sentinel-3 OLCI False Color",
        },
        {
            "rel": "tilejson",
            "href": f"{base}/WebMercatorQuad/tilejson.json?{query}&minzoom=0&maxzoom=11",
            "type": "application/json",
            "title": f"TileJSON for {item_id}",
        },
    ]
    thumbnail = {
        "href": f"{base}/preview?format=png&{query}",
        "type": "image/png",
        "title": "Sentinel-3 OLCI False Color Preview",
        "roles": ["thumbnail"],
    }
    return links, thumbnail


class Pass(NamedTuple):
    """How one collection's items move to the 0.12 links. Each part sees the item."""

    problem: Callable[[dict[str, Any]], str | None]  # why it can't take the target; None if it can
    sources: Callable[[str, dict[str, Any]], list[Links]]  # (collection, item): forms it may hold
    target: Callable[[str, str, dict[str, Any]], Links]  # (raster host, collection, item)


def _by_id(build: Callable[[str, str, str], Links]) -> Callable[[str, str, dict[str, Any]], Links]:
    return lambda raster, collection, item: build(raster, collection, item["id"])


# Collection -> its Pass. Adding a collection is one entry here. L1C has none: no titiler-eopf
# 0.12 route renders it as stored (link-forms_2026-11-04).
PASSES: dict[str, Pass] = {
    "sentinel-2-l2a": Pass(_s2_problem, lambda c, it: [legacy_s2(c, it["id"])], _by_id(s2_target)),
    # register_proxy writes the 0.12 form: only the host moves, either way (/rstaging -> /raster
    # at the flip, and back on a rollback).
    "sentinel-2-l2a-new": Pass(
        _s2_problem,
        lambda c, it: [s2_target(h, c, it["id"]) for h in (RSTAGING, RASTER)],
        _by_id(s2_target),
    ),
    "sentinel-1-grd-rtc-staging": Pass(
        _s1_cube_problem, lambda c, it: [legacy_s1_cube(c, it)], s1_cube_target
    ),
    "sentinel-1-grd-rtc-acquisitions-staging": Pass(
        _s1_acquisition_problem, lambda _, it: [legacy_s1_acquisition(it)], s1_acquisition_target
    ),
    "sentinel-3-olci-l1-efr-staging": Pass(
        _olci_problem,
        lambda c, it: [legacy_olci(c, it["id"])],
        _by_id(lambda r, c, i: (olci_links(r, c, i), olci_thumbnail(r, c, i))),
    ),
}


def managed(item: dict[str, Any]) -> Links:
    links = [link for link in item.get("links", []) if link.get("rel") in VIZ_RELS]
    return links, item.get("assets", {}).get("thumbnail")


def with_managed(item: dict[str, Any], new: Links) -> dict[str, Any]:
    """A copy of ``item`` with ``new`` in place of its managed parts; everything else is kept."""
    links, thumbnail = copy.deepcopy(new)
    out = copy.deepcopy(item)
    old = out.get("links", [])
    at = next((i for i, link in enumerate(old) if link.get("rel") in VIZ_RELS), len(old))
    kept = [link for link in old if link.get("rel") not in VIZ_RELS]
    out["links"] = kept[:at] + links + kept[at:]
    if thumbnail is None:
        out.get("assets", {}).pop("thumbnail", None)
    else:
        out.setdefault("assets", {})["thumbnail"] = thumbnail
    return out


def _norm_href(
    href: str, any_collection: bool
) -> tuple[str, str, str, tuple[tuple[str, str], ...]]:
    """An href by meaning: the query as decoded parameters (each name's values keep their order).
    With ``any_collection`` the collection in an item path is ignored too, because a scratch copy
    made with ``migrate_catalog clone`` keeps its source collection's links."""
    url = urlsplit(href)
    path = url.path
    head, sep, rest = path.partition("/collections/")
    if any_collection and sep and "/items/" in rest:
        path = f"{head}/collections/*/items/{rest.split('/items/', 1)[1]}"
    query = sorted(parse_qsl(url.query, keep_blank_values=True), key=lambda kv: kv[0])
    return url.scheme, url.netloc, path, tuple(query)


def _shape(links: Links, any_collection: bool) -> Any:
    """``links`` as compared: hrefs by meaning (see ``_norm_href``), every other field exactly."""

    def norm(d: dict[str, Any]) -> dict[str, Any]:
        return {**d, "href": _norm_href(d.get("href", ""), any_collection)}

    viz, thumbnail = links
    return [norm(link) for link in viz], (None if thumbnail is None else norm(thumbnail))


def _why(current: Links, expected: Links, any_collection: bool) -> str:
    """The first field where ``current`` differs from ``expected``, for the UNRECOGNISED log."""
    (cur, cur_thumb), (exp, exp_thumb) = (
        _shape(current, any_collection),
        _shape(expected, any_collection),
    )
    rels = [link.get("rel") for link in current[0]]
    if rels != [link.get("rel") for link in expected[0]]:
        return f"links are {rels}"
    for c, e, raw in zip(cur, exp, current[0], strict=True):
        for key in sorted(c.keys() | e.keys()):
            if c.get(key) != e.get(key):
                return f"{raw['rel']} {key} is {raw.get(key)!r}"
    if cur_thumb is None:
        return "no thumbnail asset"
    for key in sorted(cur_thumb.keys() | (exp_thumb or {}).keys()):
        if cur_thumb.get(key) != (exp_thumb or {}).get(key):
            return f"thumbnail {key} is {(current[1] or {}).get(key)!r}"
    return "links differ"


def _full_fsync(fd: int) -> None:
    """fsync, and on macOS also flush the drive's write cache (as in fill_coordinate_holes)."""
    if hasattr(fcntl, "F_FULLFSYNC"):
        try:
            fcntl.fcntl(fd, fcntl.F_FULLFSYNC)
            return
        except OSError:
            pass
    os.fsync(fd)


def write_rate(concurrency: int, put_seconds: float | None) -> float:
    """PUTs/s for the dry-run projection: ``concurrency`` PUTs in flight, each taking the mean an
    --apply drill measured at that concurrency, else ``ASSUMED_PUT_SECONDS``."""
    return concurrency / (put_seconds or ASSUMED_PUT_SECONDS)


def _preimage_entry(line: str, collection: str) -> dict[str, Any] | None:
    """A pre-image line of ``collection``, or None for anything else (torn, foreign, malformed)."""
    try:
        entry = json.loads(line)
    except json.JSONDecodeError:
        return None
    if not (
        isinstance(entry, dict)
        and entry.get("format") == PREIMAGE_FORMAT
        and entry.get("collection") == collection
        and isinstance(entry.get("id"), str)
        and isinstance(entry.get("item"), dict)
        and isinstance(entry.get("written"), list)
        and len(entry["written"]) == 2
        and isinstance(entry["written"][0], list)
    ):
        return None
    return entry


Fetch = Callable[[list[str] | None], Iterable[dict[str, Any]]]
# What to do with an item, decided on its fresh copy: (outcome, item to PUT, why).
Plan = Callable[[dict[str, Any]], tuple[str, dict[str, Any] | None, str]]
Task = tuple[str, Plan]


class Result(NamedTuple):
    """One item's outcome: ok | due (dry run) | gone | failed | already | unrecognised | stale."""

    item_id: str
    outcome: str
    detail: str = ""
    put_sent: bool = False
    get_s: float = 0.0
    put_s: float = 0.0


class LinkRun:
    """One bounded migration (or restore) of one collection's links."""

    def __init__(
        self,
        *,
        api_url: str,
        collection: str,
        fetch: Fetch,
        make_session: Callable[[], requests.Session],
        max_writes: int,
        apply: bool,
        backup_dir: Path | None,
        concurrency: int = 1,
        any_collection: bool = False,
    ) -> None:
        if max_writes < 1:
            raise ValueError(f"max_writes must be >= 1, got {max_writes}")
        if apply and backup_dir is None:
            raise ValueError("a real run needs a backup_dir")
        self.api_url = api_url.rstrip("/")
        self.collection = collection
        self.fetch = fetch
        self.make_session = make_session
        self.max_writes = max_writes
        self.apply = apply
        self.backup_dir = backup_dir
        self.concurrency = concurrency
        self.any_collection = any_collection
        self.backup_path: Path | None = None
        self._backup_fh: TextIO | None = None
        self._backup_lock = threading.Lock()
        self._local = threading.local()
        self.started = monotonic()
        self.get_seconds = self.put_seconds = 0.0
        self.gets = 0
        # writes: PUTs sent (in a dry run, PUTs due). Every scanned item ends in one bucket.
        self.scanned = self.writes = 0
        self.ok = self.due = self.gone = self.failed = 0
        self.already = self.unrecognised = self.stale = 0
        # Failures in a row: on the write path (worker outcomes, decided on the fresh copy) and on
        # the page (items that never reach a worker). Separate, so page skips never hide failing writes.
        self.write_streak = self.page_streak = 0
        self.truncated = self.aborted = False

    def _shape(self, links: Links) -> Any:
        return _shape(links, self.any_collection)

    # ---------- the two modes ----------

    def migrate(self, pass_: Pass, raster_api_url: str, ids: list[str] | None = None) -> None:
        def plan(item: dict[str, Any]) -> tuple[str, dict[str, Any] | None, str]:
            # "already" first, so a migrated item is never re-judged by the precondition.
            try:
                new: Links | None = pass_.target(raster_api_url, self.collection, item)
            except ValueError:  # it lacks what the target needs (an S1 orbit or slice)
                new = None
            # Exactly this collection's links: a scratch copy still pointing at its source
            # collection is rewritten. Only the source-form match below may ignore the collection.
            if new is not None and _shape(managed(item), False) == _shape(new, False):
                return "already", None, ""
            if why := pass_.problem(item):
                return "unrecognised", None, why
            if new is None:  # the precondition missed what the target needs: raise, count it failed
                new = pass_.target(raster_api_url, self.collection, item)
            current = self._shape(managed(item))
            forms = pass_.sources(self.collection, item)
            if any(current == self._shape(form) for form in forms):
                return "write", with_managed(item, new), ""
            return "unrecognised", None, _why(managed(item), forms[0], self.any_collection)

        def tasks() -> Iterator[Task]:
            for item in self.fetch(ids):
                self._scanned()
                try:
                    outcome, _, why = plan(item)  # from the page; re-planned on a fresh copy
                except Exception as exc:  # noqa: BLE001 — a malformed item is this item's failure
                    outcome, why = "failed", f"{type(exc).__name__}: {exc}"
                if outcome == "write":
                    yield item["id"], plan
                    continue
                self._settle(str(item.get("id")), outcome, why, worker=False)
                if self._out_of_budget():
                    return

        self._drive(tasks())

    def restore(self, preimage_file: Path) -> None:
        def plan_for(entry: dict[str, Any]) -> Plan:
            def plan(item: dict[str, Any]) -> tuple[str, dict[str, Any] | None, str]:
                before = managed(entry["item"])
                now = self._shape(managed(item))
                if now == self._shape(before):
                    return "already", None, ""
                if now != self._shape((entry["written"][0], entry["written"][1])):
                    return "stale", None, "its links changed since this tool wrote them"
                return "write", with_managed(item, before), ""

            return plan

        def tasks() -> Iterator[Task]:
            with open(preimage_file, encoding="utf-8") as fh:
                for n, line in enumerate(fh, 1):
                    if not line.strip():
                        continue
                    self._scanned()
                    entry = _preimage_entry(line, self.collection)
                    if entry is None:
                        why = "not a pre-image line of this collection"
                        self._settle(f"{preimage_file}:{n}", "failed", why, worker=False)
                        if self._out_of_budget():
                            return
                        continue
                    yield entry["id"], plan_for(entry)

        logger.warning("RESTORE from %s: links go back to their pre-image", preimage_file)
        self._drive(tasks())

    # ---------- writing ----------

    def _drive(self, tasks: Iterator[Task]) -> None:
        """For each task: check the bounds, then hand it to a worker (``_write``). At most
        ``concurrency`` are in flight, and however the loop ends they finish and are tallied."""
        with ThreadPoolExecutor(max_workers=self.concurrency) as pool:
            in_flight: set[Future[Result]] = set()
            try:
                while True:
                    try:
                        item_id, plan = next(tasks)
                    except StopIteration:
                        break
                    except Exception as exc:  # e.g. a search page that failed all its retries
                        self.aborted = True
                        logger.error(
                            "Stopping: reading items failed: %s: %s", type(exc).__name__, exc
                        )
                        break
                    if len(in_flight) >= self.concurrency:
                        done, in_flight = wait(in_flight, return_when=FIRST_COMPLETED)
                        self._tally(done)
                    if self._out_of_budget():
                        self.scanned -= 1  # this item was not processed
                        break
                    if self.writes >= self.max_writes:
                        self.truncated = True
                        self.scanned -= 1
                        logger.warning("--max-writes %d reached; items remain", self.max_writes)
                        break
                    self.writes += 1  # reserved; released when the fresh copy needs no PUT
                    in_flight.add(pool.submit(self._write, item_id, plan))
            finally:
                self._tally(wait(in_flight).done)

    def _write(self, item_id: str, plan: Plan) -> Result:
        """Re-GET the item, re-plan it on that fresh copy, then pre-image and PUT. Never raises:
        whatever goes wrong is this item's failure."""
        get_s = put_s = 0.0
        sent = False
        try:
            session: requests.Session | None = getattr(self._local, "session", None)
            if session is None:
                session = self._local.session = self.make_session()
            url = f"{self.api_url}/collections/{self.collection}/items/{item_id}"
            start = monotonic()
            resp = session.get(url, timeout=WRITE_TIMEOUT_S)
            get_s = monotonic() - start
            if resp.status_code == 404:
                return Result(item_id, "gone", "GET 404", get_s=get_s)
            if not 200 <= resp.status_code < 300:
                return Result(item_id, "failed", f"GET {resp.status_code}", get_s=get_s)
            fresh = resp.json()
            outcome, new, why = plan(fresh)
            if outcome != "write" or new is None:
                return Result(item_id, outcome, why, get_s=get_s)
            body = _transaction_body(new)
            if not self.apply:
                return Result(item_id, "due", get_s=get_s)
            self._backup(fresh, body)
            start, sent = monotonic(), True
            resp = session.put(url, json=body, timeout=WRITE_TIMEOUT_S)
            put_s = monotonic() - start
            if resp.status_code == 404:
                # pgstac's update deletes then inserts, so a concurrent PUT of the same item also
                # 404s: the item is gone only if it is really not there.
                check = session.get(url, timeout=WRITE_TIMEOUT_S).status_code
                outcome = "gone" if check == 404 else "failed"
                return Result(item_id, outcome, f"PUT 404, then GET {check}", sent, get_s, put_s)
            if not 200 <= resp.status_code < 300:
                why = f"PUT {resp.status_code}: {resp.text[:200]}"
                return Result(item_id, "failed", why, sent, get_s, put_s)
            return Result(item_id, "ok", "", sent, get_s, put_s)
        except Exception as exc:  # noqa: BLE001 — e.g. a token refresh that raises RuntimeError
            return Result(item_id, "failed", f"{type(exc).__name__}: {exc}", sent, get_s, put_s)

    def _tally(self, done: Iterable[Future[Result]]) -> None:
        for future in done:
            result = future.result()
            self.gets += 1
            self.get_seconds += result.get_s
            self.put_seconds += result.put_s
            if not result.put_sent and result.outcome != "due":
                self.writes -= 1
            self._settle(result.item_id, result.outcome, result.detail, worker=True)

    def _backup(self, current: dict[str, Any], body: dict[str, Any]) -> None:
        """Append the item as it is now, with the links its PUT writes, and fsync, BEFORE the PUT.
        The written links are what --restore's staleness guard compares with."""
        with self._backup_lock:
            if self._backup_fh is None:
                if self.backup_dir is None:
                    raise RuntimeError("no backup_dir: refusing to write without a pre-image")
                self.backup_dir.mkdir(parents=True, exist_ok=True)
                stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
                name = f"item-api-links-{self.collection}-{stamp}-{time.time_ns()}.jsonl"
                self.backup_path = self.backup_dir / name
                # "x": a new file per run, so a restore never appends to the file it reads.
                self._backup_fh = open(self.backup_path, "x", encoding="utf-8")  # noqa: SIM115 — held across items
                dir_fd = os.open(self.backup_dir, os.O_RDONLY)  # make the file's name durable too
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
                logger.info("Pre-images: %s", self.backup_path.resolve())
            record = {
                "format": PREIMAGE_FORMAT,
                "collection": self.collection,
                "id": current["id"],
                "item": current,
                "written": managed(body),
            }
            self._backup_fh.write(json.dumps(record) + "\n")
            self._backup_fh.flush()
            _full_fsync(self._backup_fh.fileno())

    # ---------- reporting ----------

    def _settle(self, what: str, outcome: str, detail: str, *, worker: bool) -> None:
        """Count one item's outcome. A failure extends the streak of its path (``worker``: the
        write path; else the page) and any other outcome on that path resets it."""
        failed = outcome == "failed"
        if worker:
            self.write_streak = self.write_streak + 1 if failed else 0
        else:
            self.page_streak = self.page_streak + 1 if failed else 0
        self._count(what, outcome, detail)

    def _out_of_budget(self) -> bool:
        """True, and the run marked aborted, once MAX_CONSECUTIVE_FAILURES failed in a row."""
        streak = max(self.write_streak, self.page_streak)
        if streak < MAX_CONSECUTIVE_FAILURES:
            return False
        self.aborted = True
        logger.error("Aborting: %d failures in a row", streak)
        return True

    def _count(self, what: str, outcome: str, detail: str = "") -> None:
        setattr(self, outcome, getattr(self, outcome) + 1)
        if outcome == "failed":
            logger.error("FAILED %s: %s", what, detail)
        elif outcome in ("gone", "unrecognised", "stale"):
            logger.warning("%s %s: %s", outcome.upper(), what, detail)

    def _scanned(self) -> None:
        self.scanned += 1
        if self.scanned % 1000 == 0:
            logger.info("progress: %s", self.counts())

    def counts(self) -> str:
        return (
            f"scanned={self.scanned} writes={self.writes} ok={self.ok} due={self.due} "
            f"already={self.already} unrecognised={self.unrecognised} stale={self.stale} "
            f"gone={self.gone} failed={self.failed} truncated={self.truncated} "
            f"aborted={self.aborted}"
        )

    def summary(self, put_seconds: float | None = None) -> str:
        elapsed = monotonic() - self.started
        per_s = self.scanned / elapsed if elapsed else 0
        mean_get = self.get_seconds / self.gets if self.gets else 0
        lines = [
            f"[{'APPLY' if self.apply else 'DRY-RUN'}] {self.collection}: {self.counts()}",
            f"timing: {elapsed:.1f} s, {per_s:.1f} items/s, mean GET {mean_get:.3f} s",
        ]
        if self.apply:
            mean_put = self.put_seconds / self.writes if self.writes else 0
            lines[1] += (
                f", {self.writes / elapsed if elapsed else 0:.2f} writes/s, mean PUT {mean_put:.3f} s "
                f"at --concurrency {self.concurrency} (a dry run's --put-seconds)"
            )
        else:
            # The same run with --apply: what was timed here (pages, and the GET each item to
            # write gets just before its PUT), plus the PUTs at `rate`.
            rate = write_rate(self.concurrency, put_seconds)
            total = elapsed + self.writes / rate
            how = (
                f"--put-seconds {put_seconds} at --concurrency {self.concurrency}"
                if put_seconds
                else f"ASSUMED: {ASSUMED_PUT_SECONDS} s per PUT, linear in --concurrency "
                f"{self.concurrency}; README_MIGRATIONS measured 6.25x, not 8x, at 8 workers. "
                "Time an --apply drill at this concurrency and pass its mean PUT as --put-seconds"
            )
            lines.append(
                f"projected --apply: {elapsed:.0f} s measured (pages and per-item GETs) + "
                f"{self.writes} PUTs at {rate:.2f}/s ({how}) = {total:.0f} s ({total / 3600:.1f} h)"
            )
        if self.backup_path is not None:
            lines.append(f"pre-images: {self.backup_path}")
        return "\n".join(lines)


def make_session() -> requests.Session:
    """A write session whose every request carries a fresh bearer (no-op without OIDC env)."""
    session = requests.Session()
    session.auth = stac_auth.bearer_auth
    return session


def make_fetch(api_url: str, collection: str) -> Fetch:
    """Items of ``collection`` (all, or the given ids), page by page, on the shared read client."""
    client = stac_auth.open_resilient_client(api_url)

    def fetch(ids: list[str] | None) -> Iterator[dict[str, Any]]:
        chunks: list[list[str] | None] = (
            [None]
            if ids is None
            else [ids[i : i + PAGE_SIZE] for i in range(0, len(ids), PAGE_SIZE)]
        )
        for chunk in chunks:
            search = client.search(
                collections=[collection],
                ids=chunk,
                max_items=None,
                limit=PAGE_SIZE,
            )
            for page in search.pages_as_dicts():
                yield from page.get("features", [])

    return fetch


def _path(value: str) -> Path:
    return Path(value).expanduser()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--stac-api-url", required=True, help="STAC API (no default: name it)")
    parser.add_argument("--collection", required=True)
    parser.add_argument(
        "--max-writes", required=True, type=int, help="hard bound on PUTs, enforced in code"
    )
    parser.add_argument("--apply", action="store_true", help="actually write (default: dry run)")
    parser.add_argument("--backup-dir", type=_path, help="where the pre-image JSONL goes")
    parser.add_argument("--concurrency", type=int, default=1, help="items in flight (default 1)")
    parser.add_argument("--raster-api-url", help="titiler host the new links point at")
    parser.add_argument(
        "--mapping",
        help="PASSES entry for a staging/scratch copy whose id starts with it (default: --collection)",
    )
    parser.add_argument("--ids-file", type=_path, help="only these ids, one per line (# comments)")
    parser.add_argument("--restore", type=_path, help="put back the links of a pre-image JSONL")
    parser.add_argument(
        "--put-seconds",
        type=float,
        help="dry-run projection: the mean PUT an --apply drill measured at this --concurrency",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if args.max_writes < 1:
        parser.error("--max-writes must be a positive integer")
    if not 1 <= args.concurrency <= 64:
        parser.error("--concurrency must be between 1 and 64")
    if args.put_seconds is not None and not (
        math.isfinite(args.put_seconds) and args.put_seconds > 0
    ):
        parser.error("--put-seconds must be a positive number of seconds")
    for url, name in (
        (args.stac_api_url, "--stac-api-url"),
        (args.raster_api_url, "--raster-api-url"),
    ):
        if url is not None and (urlsplit(url).scheme != "https" or not urlsplit(url).netloc):
            parser.error(f"{name} must be an absolute https URL, got {url!r}")
    if args.apply and args.backup_dir is None:
        parser.error("--apply needs --backup-dir (the pre-images go there)")
    if args.apply and stac_auth._oidc_env() is None:
        parser.error("--apply needs the OIDC writer env: OIDC_TOKEN_URL, OIDC_CLIENT_ID, ...SECRET")
    mapping = args.mapping or args.collection
    if args.restore:
        if args.raster_api_url or args.mapping or args.ids_file:
            parser.error("--restore takes no --raster-api-url, --mapping or --ids-file")
    else:
        if mapping not in PASSES or not args.collection.startswith(mapping):
            parser.error(
                f"no link mapping for {args.collection!r}; this tool covers {', '.join(PASSES)} "
                "and, with --mapping, staging or scratch copies whose id starts with one. L1C is "
                "refused: no titiler-eopf 0.12 route renders it as stored; adding a collection "
                "is a PASSES entry"
            )
        if not args.raster_api_url:
            parser.error("--raster-api-url is required (the host the new links point at)")
    ids = None
    if args.ids_file:
        lines = args.ids_file.read_text().splitlines()
        ids = [i for line in lines if (i := line.split("#", 1)[0].strip())]
        if not ids:
            parser.error(f"--ids-file {args.ids_file} lists no ids")

    logger.info(
        "%s %s collection=%s max-writes=%d concurrency=%d",
        "APPLY" if args.apply else "DRY-RUN",
        args.stac_api_url,
        args.collection,
        args.max_writes,
        args.concurrency,
    )
    run = LinkRun(
        api_url=args.stac_api_url,
        collection=args.collection,
        fetch=make_fetch(args.stac_api_url, args.collection),
        make_session=make_session,
        max_writes=args.max_writes,
        apply=args.apply,
        backup_dir=args.backup_dir,
        concurrency=args.concurrency,
        any_collection=mapping != args.collection,
    )
    code = 0
    try:
        if args.restore:
            run.restore(args.restore)
        else:
            logger.info("mapping=%s raster=%s", mapping, args.raster_api_url)
            run.migrate(PASSES[mapping], args.raster_api_url, ids)
            if ids is not None and not (run.truncated or run.aborted):
                logger.info("%d of %d requested ids found", run.scanned, len(ids))
    except KeyboardInterrupt:
        logger.warning("Interrupted: the PUTs in flight finished; every PUT sent has a pre-image")
        code = 130
    finally:
        print(run.summary(args.put_seconds))
    return code or (1 if run.failed or run.aborted else 0)


if __name__ == "__main__":
    sys.exit(main())
