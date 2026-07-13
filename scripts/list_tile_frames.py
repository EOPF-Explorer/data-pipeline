#!/usr/bin/env python3
"""List the S1 GRD frames overlapping an MGRS tile in a date window (frame-cache pull input).

The frame-cache pull pre-step (Argo `eopf-explorer-s1tiling` template, S1 caching T7) needs the
product ids of the GRD frames S1Tiling will search for a tile+orbit+window, so it can restore the
cache-present ones into ``data_raw`` before ``S1Processor`` runs (its disk scan then skips those
CDSE downloads). ``cache_frames.py pull`` takes an explicit id list but computes none; the cron's
``discover`` collapses same-pass frames to ONE representative (``collapse_same_pass``) whereas a tile
is covered by several adjacent frames of a pass. This enumerates them via a CDSE STAC query over the
tile bbox — the uncollapsed, platform-scoped list.

Parity note (why an approximate list is safe): S1Tiling only reuses a pre-placed SAFE whose product
id is in ITS OWN search results, so a frame listed here but not needed is harmless waste, and a frame
missed here is just downloaded from CDSE as normal. So this only moves the cache HIT-RATE, never the
output — the pull step degrades to a no-op cache-miss on any discrepancy.
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import sys

from pystac_client import Client
from watch_cdse_and_process import CDSE_COLLECTION, CDSE_STAC_URL, ORBIT_STATE_PROPERTY, tile_bbox

log = logging.getLogger("list_tile_frames")


def _platform_of(product_id: str) -> str:
    """Mission prefix of a CDSE product id, e.g. ``S1A_IW_GRDH_...`` -> ``S1A``."""
    return product_id.split("_", 1)[0].upper()


def list_tile_frames(
    stac_url: str,
    tile_id: str,
    orbit_direction: str,
    date_start: str,
    date_end: str,
    platform: str,
) -> list[str]:
    """Product ids of the GRD frames overlapping ``tile_id`` in ``[date_start, date_end]``.

    Scoped to ``platform`` (S1Tiling runs one platform per call), uncollapsed (every overlapping
    frame of a pass), de-duplicated, in CDSE search order. ``date_end`` is inclusive: the query
    window is ``[date_start 00:00, date_end + 1 day 00:00)`` so the whole ``date_end`` day is
    covered. Raises ``ValueError`` for a malformed/unknown tile id (via ``tile_bbox``).
    """
    bbox = tile_bbox(tile_id)
    start = dt.datetime.fromisoformat(date_start).replace(tzinfo=dt.UTC)
    end = dt.datetime.fromisoformat(date_end).replace(tzinfo=dt.UTC) + dt.timedelta(days=1)
    want = platform.upper()
    search = Client.open(stac_url).search(
        collections=[CDSE_COLLECTION],
        bbox=bbox,
        datetime=f"{start.isoformat()}/{end.isoformat()}",
        query={ORBIT_STATE_PROPERTY: {"eq": orbit_direction}},
    )
    frames: list[str] = []
    seen: set[str] = set()
    for item in search.items():
        pid = item.id
        if _platform_of(pid) != want or pid in seen:
            continue
        seen.add(pid)
        frames.append(pid)
    return frames


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--tile-id", required=True, help="MGRS 100 km tile id, e.g. 31TCH")
    parser.add_argument("--orbit-direction", required=True, choices=("ascending", "descending"))
    parser.add_argument("--date-start", required=True, help="inclusive, YYYY-MM-DD")
    parser.add_argument("--date-end", required=True, help="inclusive, YYYY-MM-DD")
    parser.add_argument("--platform", required=True, help="S1A | S1C (one per call, like S1Tiling)")
    parser.add_argument("--stac-url", default=CDSE_STAC_URL)
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s", stream=sys.stderr
    )
    args = build_parser().parse_args(argv)
    frames = list_tile_frames(
        args.stac_url,
        args.tile_id,
        args.orbit_direction,
        args.date_start,
        args.date_end,
        args.platform,
    )
    log.info(
        "tile %s / %s / %s..%s / %s: %d frame(s)",
        args.tile_id,
        args.orbit_direction,
        args.date_start,
        args.date_end,
        args.platform,
        len(frames),
    )
    for (
        pid
    ) in frames:  # stdout = the frame id list (pipe into cache_frames.py pull --frames-file -)
        print(pid)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
