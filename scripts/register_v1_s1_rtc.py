"""Register one S1 GRD RTC Zarr store as a STAC item.

Builds a STAC item from the Zarr store metadata, augments it with
visualization links and alternate S3 assets, then upserts it to the
staging STAC API.

Exit codes:
    0 -- success
    1 -- failure (item build error or API error)
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import sys
from pathlib import Path
from typing import NamedTuple
from urllib.parse import urlparse

import numpy as np
import zarr

sys.path.insert(0, str(Path(__file__).parent))

from eopf_geozarr.stac.s1_rtc import build_s1_rtc_stac_item
from pystac import Item
from pystac_client import Client
from register_per_acquisition import _reorient_item_to_orbit
from register_v1 import (
    add_alternate_s3_assets,
    add_store_link,
    add_thumbnail_asset,
    add_visualization_links,
    s3_to_https,
    upsert_item,
    warm_thumbnail_cache,
)
from run_ingest_register import check_env_consistency

log = logging.getLogger(__name__)

# The cube preview defaults to the most recent acquisition that covers most of the tile, so the browser
# shows fresh, near-full data rather than the (default-rendered) oldest slice.
COVERAGE_THRESHOLD = 0.80


class Slice(NamedTuple):
    """One cube time slice: which orbit group it lives in, its acquisition instant, and the fraction of
    the tile it covers (0..1)."""

    orbit: str
    dt: dt.datetime
    coverage: float


def pick_slice(slices: list[Slice]) -> Slice | None:
    """Choose the slice the cube preview should default to.

    The most recent acquisition with coverage strictly above ``COVERAGE_THRESHOLD``; if none clears it,
    the highest-coverage slice (ties broken by most recent). Spans both orbit groups. Returns ``None``
    for an empty cube.
    """
    if not slices:
        return None
    good = [s for s in slices if s.coverage > COVERAGE_THRESHOLD]
    if good:
        return max(good, key=lambda s: s.dt)
    return max(slices, key=lambda s: (s.coverage, s.dt))


_ORBITS = ("ascending", "descending")


def _open_cube_root(store: str) -> zarr.Group:
    """Open the cube root, mirroring build_s1_rtc_stac_item: prefer consolidated metadata, fall back to a
    plain group open (an appended same-orbit cube can lack root consolidated metadata)."""
    try:
        return zarr.open_consolidated(store, zarr_format=3)
    except Exception as exc:  # noqa: BLE001 — only the consolidated-metadata absence is expected
        if "consolidated metadata" not in str(exc).lower():
            raise
        return zarr.open_group(store, mode="r", zarr_format=3)


def slice_coverages(store: str) -> list[Slice]:
    """Per-slice tile coverage from the cube, both orbit groups.

    Reads the ``border_mask`` at the cheap ``r720m`` level only (~150x150). Coverage is the fraction of
    **valid** pixels; the S1Tiling ``_BorderMask`` is stored with ``fill_value=0`` for the border, so
    valid = non-zero. ``time`` is raw int64 ns (as build_s1_rtc_stac_item reads it) -> UTC datetime.
    """
    root = _open_cube_root(store)
    out: list[Slice] = []
    for orbit in _ORBITS:
        if orbit not in root:
            continue
        level = root[orbit]["r720m"]
        mask = np.asarray(level["border_mask"])  # (time, y, x), uint8
        times_ns = np.asarray(level["time"]).tolist()  # int64 ns since epoch
        for i, t_ns in enumerate(times_ns):
            sl = mask[i]
            coverage = float(np.count_nonzero(sl) / sl.size)
            out.append(Slice(orbit, dt.datetime.fromtimestamp(t_ns / 1e9, tz=dt.UTC), coverage))
    return out


def _pin_preview_to_best_recent(item: Item, store: str) -> tuple[Item, str | None]:
    """Reorient the cube item to its best-recent slice's orbit and return that slice's ``sel=time`` value.

    Picks the slice the preview should default to (``pick_slice`` over ``slice_coverages``), then reuses
    ``_reorient_item_to_orbit`` so the render expression / orbit metadata target the chosen slice's orbit.
    Best-effort: any coverage-read failure (or an empty cube) leaves the item unchanged and returns
    ``None`` → the preview falls back to the default slice rather than failing registration.
    """
    try:
        chosen = pick_slice(slice_coverages(store))
    except Exception:
        log.warning(
            "Could not read slice coverage from %s; preview uses default slice",
            store,
            exc_info=True,
        )
        return item, None
    if chosen is None:
        return item, None
    item_dict = item.to_dict()
    _reorient_item_to_orbit(item_dict, chosen.orbit)
    return Item.from_dict(item_dict), chosen.dt.strftime("%Y-%m-%dT%H:%M:%S")


def register(
    store: str,
    collection: str,
    stac_api_url: str,
    raster_api_url: str,
    s3_endpoint: str,
) -> int:
    """Build and register one S1 RTC STAC item.

    Returns exit code: 0 = success, 1 = failure.
    """
    # Fail fast on a per-env bucket/collection mismatch (the 32TLR footgun): the standalone
    # register path takes a hand-typed --store + --collection, so it needs the same guard as
    # run_ingest_register. Only s3:// stores carry an identifiable bucket in the netloc.
    parsed = urlparse(store)
    if parsed.scheme == "s3":
        check_env_consistency(collection, parsed.netloc)

    try:
        item = build_s1_rtc_stac_item(store, collection)
    except Exception:
        log.exception("Failed to build STAC item from %s", store)
        return 1

    # Default the cube preview to the best-recent acquisition (most recent >80% coverage, else max
    # coverage) and reorient the item to that slice's orbit so the render targets the right group.
    item, sel_time = _pin_preview_to_best_recent(item, store)

    # build_s1_rtc_stac_item returns s3:// hrefs; TiTiler needs https:// via the gateway
    for asset in item.assets.values():
        if asset.href and asset.href.startswith("s3://"):
            asset.href = s3_to_https(asset.href)

    add_store_link(item, store)
    add_alternate_s3_assets(item, s3_endpoint)
    add_visualization_links(item, raster_api_url, collection, sel_time=sel_time)
    add_thumbnail_asset(item, raster_api_url, collection, sel_time=sel_time)
    warm_thumbnail_cache(item)

    try:
        client = Client.open(stac_api_url)
        upsert_item(client, collection, item)
    except Exception:
        log.exception("Failed to upsert item %s to %s", item.id, stac_api_url)
        return 1

    # TEMPORARY (#246): no render-copy needed — run_ingest_register writes the cube directly at
    # titiler's reconstructed path (tests-output/{collection}/s1-rtc-{tile}.zarr). Revert when
    # titiler-eopf#108 lands.
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--store", required=True, help="S3 URI of the GeoZarr V3 store")
    parser.add_argument("--collection", required=True, help="Target STAC collection ID")
    parser.add_argument("--stac-api-url", required=True, help="STAC API base URL")
    parser.add_argument("--raster-api-url", required=True, help="TiTiler raster API base URL")
    parser.add_argument("--s3-endpoint", required=True, help="S3 endpoint URL")
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    args = _build_parser().parse_args()
    sys.exit(
        register(
            store=args.store,
            collection=args.collection,
            stac_api_url=args.stac_api_url,
            raster_api_url=args.raster_api_url,
            s3_endpoint=args.s3_endpoint,
        )
    )


if __name__ == "__main__":
    main()
