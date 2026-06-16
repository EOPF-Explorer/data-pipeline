"""Data-driven CDSE trigger for the S1 GRD RTC pipeline (Phase-6 Task 6).

Queries CDSE for new S1 GRD products over a tile+window, keeps only ``{S1A, S1C}`` (S1D skipped,
logged), drops acquisitions whose **per-acquisition STAC item already exists** in the env-split
per-acquisition collection (``--acq-collection``; cron passes ``…-staging``), and emits the remaining
NEW products as a JSON array (to
``--output`` or stdout). The Argo CronWorkflow consumes that array to fan out one child Workflow per
product (decision B); the **cube-time-present** dedup arm runs downstream in ingest (T4). This is a
pure query -> filter -> dedup -> emit step: no subprocess, no S3, no submission.

Each emitted record is ``{tile, orbit, date_start, date_end, date, product_id, platform}``; the
CronWorkflow's ``withParam`` consumes the controlled ``tile/orbit/date_start/date_end`` (the s1tiling
window = acquisition day ∓1), never the raw ``product_id``.

Usage:
    uv run python scripts/trigger_cdse.py --tiles 31TCH --orbit-direction descending \
      --lookback-days 7 --stac-api-url <eopf-stac> [--output new_products.json]
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sys
from pathlib import Path
from typing import TypedDict

import mgrs
from pystac_client import Client
from register_per_acquisition import DEFAULT_ACQ_COLLECTION, acquisition_id
from shapely.geometry import Polygon, shape

# Reuse the watcher's tile geometry + CDSE query constants (single source of truth); query_cdse
# itself is *not* reused -- it drops the datetime/platform this trigger needs (see query_products).
from watch_cdse_and_process import (
    CDSE_COLLECTION,
    CDSE_STAC_URL,
    ORBIT_STATE_PROPERTY,
    tile_bbox,
)

log = logging.getLogger(__name__)

# Phase-6 platform allowlist (spec §2): S1A + S1C only. Allowlist (not an S1D denylist) so a future
# S1B/S1E can't leak through until s1tiling support is confirmed.
ENABLED_PLATFORMS = {"S1A", "S1C"}

# Minimum fraction of the MGRS tile a product footprint must cover to be worth processing. The CDSE
# query is a *bbox* intersection, so a slanted S1 swath that only grazes a tile corner still matches
# (S1A orbit 30 covered 1.5% of 30TWN on 2026-06-06) and s1tiling then emits an all-nodata tile. A
# tile an orbit genuinely images is ~fully covered (after same-pass frame mosaic); low partial
# coverage means a *different* orbit covers it properly, so requiring 20% drops the empty-tile slivers
# without dropping legitimate swath-edge tiles. Overridable via --min-coverage.
MIN_TILE_COVERAGE = 0.20


class Product(TypedDict):
    """A CDSE product as the trigger carries it internally: query metadata + tile coverage. The
    emitted record (``select_new_products``) is a *different*, all-string shape — ``coverage`` is an
    internal gate signal, not part of the JSON the CronWorkflow consumes."""

    product_id: str
    platform: str
    datetime: str
    date: str
    coverage: float


def platform_of(product_id: str) -> str:
    """Mission prefix of a CDSE product id, e.g. ``S1A_IW_GRDH_...`` -> ``S1A``."""
    return product_id.split("_", 1)[0].upper()


def is_enabled_platform(platform: str) -> bool:
    return platform in ENABLED_PLATFORMS


def _item_datetime(item: object) -> dt.datetime | None:
    """Acquisition instant of a CDSE item, or None if it carries neither datetime nor start."""
    when = getattr(item, "datetime", None)
    if isinstance(when, dt.datetime):
        return when
    start = getattr(item, "properties", {}).get("start_datetime")
    if isinstance(start, str):
        return dt.datetime.fromisoformat(start.replace("Z", "+00:00"))
    return None


def tile_polygon(tile_id: str) -> Polygon:
    """The MGRS 100 km tile as a WGS84 polygon (true square corners, not the axis-aligned bbox).

    Coverage must be measured against the real square: ``tile_bbox`` over-states the footprint (it is
    the min/max over the four non-axis-aligned corners), which would mask the corner-graze it exists to
    catch. Raises ``ValueError`` for a malformed/unknown tile id (mirrors ``tile_bbox``).
    """
    m = mgrs.MGRS()
    corners = [
        "0000000000",
        "9999900000",
        "9999999999",
        "0000099999",
    ]  # SW, SE, NE, NW (ring order)
    try:
        latlon = [m.toLatLon(f"{tile_id}{c}") for c in corners]
    except mgrs.core.MGRSError as exc:
        raise ValueError(f"invalid MGRS tile id: {tile_id!r}") from exc
    return Polygon([(lon, lat) for lat, lon in latlon])


def tile_coverage(geometry: dict | None, tile_poly: Polygon) -> float:
    """Fraction (0..1) of ``tile_poly`` covered by a product footprint ``geometry`` (GeoJSON).

    A swath grazing a tile corner scores near 0; a pass imaging the whole tile ~1. Missing geometry
    can't be verified to cover anything, so it scores 0 (dropped by the gate).
    """
    if not geometry or tile_poly.area == 0:
        return 0.0
    return float(shape(geometry).intersection(tile_poly).area / tile_poly.area)


def query_products(
    stac_url: str, bbox: list[float], orbit_direction: str, lookback_days: int, tile_poly: Polygon
) -> list[Product]:
    """Query CDSE for S1 GRD products over `bbox` in the last `lookback_days`.

    Returns ``[{"product_id", "platform", "datetime"(ISO, to seconds), "date", "coverage"}, ...]``.
    Mirrors ``watch_cdse_and_process.query_cdse``'s filter but keeps the per-second ``datetime``
    (needed for the per-acquisition item-id dedup), the parsed ``platform``, and the ``coverage``
    fraction of ``tile_poly`` (for the empty-tile gate — the CDSE bbox match alone admits corner
    grazes). Items without a datetime are skipped (logged).
    """
    now = dt.datetime.now(dt.UTC)
    start = now - dt.timedelta(days=lookback_days)
    search = Client.open(stac_url).search(
        collections=[CDSE_COLLECTION],
        bbox=bbox,
        datetime=f"{start.isoformat()}/{now.isoformat()}",
        query={ORBIT_STATE_PROPERTY: {"eq": orbit_direction}},
    )
    products: list[Product] = []
    for item in search.items():
        when = _item_datetime(item)
        if when is None:
            log.warning("skipping %s: no datetime", item.id)
            continue
        products.append(
            {
                "product_id": item.id,
                "platform": platform_of(item.id),
                "datetime": when.isoformat(),
                "date": when.date().isoformat(),
                "coverage": tile_coverage(item.geometry, tile_poly),
            }
        )
    return products


def expected_item_id(tile_id: str, when: dt.datetime) -> str:
    """Per-acquisition STAC item id this acquisition would register as (the dedup key)."""
    return acquisition_id(tile_id, when)


def item_exists(stac_api_url: str, acq_collection: str, item_id: str) -> bool:
    """True if `item_id` already exists in `acq_collection` on the target STAC API.

    Uses an id-scoped search (any hit ⇒ exists). If a deployment's ``ids`` filter proves unreliable,
    this is the seam to swap for a direct ``GET /collections/{c}/items/{id}`` (200/404), mirroring
    ``register_per_acquisition._upsert_items``.
    """
    search = Client.open(stac_api_url).search(collections=[acq_collection], ids=[item_id])
    return next(iter(search.items()), None) is not None


def collapse_same_pass(products: list[Product]) -> list[Product]:
    """Collapse adjacent frames of one satellite pass to a single representative product.

    CDSE returns one product per *frame*; a tile is typically covered by 2+ adjacent frames of the same
    pass (same relative orbit + datatake), which s1tiling mosaics into ONE tile ``time``. Emitting per
    frame would spawn redundant pipelines and register only one of the N per-acquisition ids — a re-run
    then re-emits the rest (the dedup loop never closes). A pass images a tile once per date+platform for
    a fixed orbit direction, so group by ``(date, platform)`` and keep the earliest-datetime frame.
    """
    # Keep one product per (date, platform); the dict preserves first-seen pass order, and within a
    # pass the earliest-datetime frame wins (the representative). No global re-sort — that would reorder
    # distinct acquisitions relative to the CDSE query order.
    by_pass: dict[tuple[str, str], Product] = {}
    for product in products:
        key = (product["date"], product["platform"])
        current = by_pass.get(key)
        if current is None or product["datetime"] < current["datetime"]:
            by_pass[key] = product
    return list(by_pass.values())


def drop_low_coverage(products: list[Product], min_coverage: float) -> list[Product]:
    """Drop products whose footprint covers less than ``min_coverage`` of the tile (logged).

    Runs **before** ``collapse_same_pass`` so a corner-grazing frame can't become a pass's
    representative; a pass whose every frame is sub-threshold is dropped entirely (no s1tiling run).
    """
    kept: list[Product] = []
    for product in products:
        coverage = product["coverage"]
        if coverage >= min_coverage:
            kept.append(product)
        else:
            log.info(
                "skip %s: covers %.1f%% of tile (< %.0f%% min)",
                product["product_id"],
                coverage * 100,
                min_coverage * 100,
            )
    return kept


def select_new_products(args: argparse.Namespace) -> list[dict[str, str]]:
    """Per tile: query CDSE, drop low-coverage grazes (logged), collapse same-pass frames, drop
    non-{S1A,S1C} (logged) and already-registered acquisitions (logged), and return the new products
    as ``{tile, orbit, product_id, datetime, date, platform, date_start, date_end}``."""
    tiles = [t.strip() for t in args.tiles.split(",") if t.strip()]
    # `both` discovers ascending + descending passes (asc+desc AOI); each is queried separately so
    # same-pass collapse never merges across directions, and each product carries its own orbit.
    orbits = (
        ["ascending", "descending"] if args.orbit_direction == "both" else [args.orbit_direction]
    )
    new_products: list[dict[str, str]] = []
    for tile in tiles:
        bbox = tile_bbox(tile)
        poly = tile_polygon(tile)
        for orbit in orbits:
            products = collapse_same_pass(
                drop_low_coverage(
                    query_products(CDSE_STAC_URL, bbox, orbit, args.lookback_days, poly),
                    args.min_coverage,
                )
            )
            for product in products:
                platform = product["platform"]
                if not is_enabled_platform(platform):
                    log.info("skip %s: platform %s not enabled", product["product_id"], platform)
                    continue
                when = dt.datetime.fromisoformat(product["datetime"])
                item_id = expected_item_id(tile, when)
                if item_exists(args.stac_api_url, args.acq_collection, item_id):
                    log.info("skip %s: item %s already registered", product["product_id"], item_id)
                    continue
                # s1tiling window brackets the acquisition day (date∓1, matching the local watcher),
                # so the CronWorkflow fans out child pipelines with no per-product date-math step.
                acq_date = dt.date.fromisoformat(product["date"])
                new_products.append(
                    {
                        "tile": tile,
                        "orbit": orbit,
                        "product_id": product["product_id"],
                        "datetime": product["datetime"],
                        "date": product["date"],
                        "date_start": (acq_date - dt.timedelta(days=1)).isoformat(),
                        "date_end": (acq_date + dt.timedelta(days=1)).isoformat(),
                        "platform": platform,
                    }
                )
    return new_products


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tiles", required=True, help="Comma-separated MGRS tile IDs (e.g. 31TCH)")
    parser.add_argument(
        "--orbit-direction", required=True, choices=["ascending", "descending", "both"],
        help="single direction, or 'both' to discover ascending + descending passes (asc+desc AOI)",
    )  # fmt: skip
    parser.add_argument("--lookback-days", required=True, type=int, help="CDSE query window (days)")
    parser.add_argument(
        "--min-coverage",
        type=float,
        default=MIN_TILE_COVERAGE,
        help=f"min fraction of the tile a footprint must cover (default: {MIN_TILE_COVERAGE})",
    )
    parser.add_argument(
        "--stac-api-url", required=True, help="EOPF target STAC API (per-acquisition dedup)"
    )
    parser.add_argument(
        "--acq-collection",
        default=DEFAULT_ACQ_COLLECTION,
        help=f"Per-acquisition collection to dedup against (default: {DEFAULT_ACQ_COLLECTION})",
    )
    parser.add_argument("--output", help="Write the JSON array here (default: stdout)")
    return parser


def main() -> None:
    # Logs on stderr so stdout stays a clean JSON array (Argo can capture either stdout or --output).
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s %(message)s", stream=sys.stderr
    )
    args = build_parser().parse_args()
    products = select_new_products(args)
    payload = json.dumps(products, indent=2)
    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(payload)
    else:
        print(payload)
    log.info("emitted %d new product(s)", len(products))


if __name__ == "__main__":
    main()
