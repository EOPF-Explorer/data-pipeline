"""Data-driven CDSE trigger for the S1 GRD RTC pipeline (Phase-6 Task 6).

Queries CDSE for new S1 GRD products over a tile+window, keeps only ``{S1A, S1C}`` (S1D skipped,
logged), drops acquisitions whose **per-acquisition STAC item already exists** in
``sentinel-1-grd-rtc-acquisitions``, and emits the remaining NEW products as a JSON array (to
``--output`` or stdout). The Argo CronWorkflow consumes that array to fan out one child Workflow per
product (decision B); the **cube-time-present** dedup arm runs downstream in ingest (T4). This is a
pure query -> filter -> dedup -> emit step: no subprocess, no S3, no submission.

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

from pystac_client import Client
from register_per_acquisition import DEFAULT_ACQ_COLLECTION, acquisition_id

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


def query_products(
    stac_url: str, bbox: list[float], orbit_direction: str, lookback_days: int
) -> list[dict[str, str]]:
    """Query CDSE for S1 GRD products over `bbox` in the last `lookback_days`.

    Returns ``[{"product_id", "platform", "datetime"(ISO, to seconds), "date"}, ...]``. Mirrors
    ``watch_cdse_and_process.query_cdse``'s filter but keeps the per-second ``datetime`` (needed for
    the per-acquisition item-id dedup) and the parsed ``platform``. Items without a datetime are
    skipped (logged).
    """
    now = dt.datetime.now(dt.UTC)
    start = now - dt.timedelta(days=lookback_days)
    search = Client.open(stac_url).search(
        collections=[CDSE_COLLECTION],
        bbox=bbox,
        datetime=f"{start.isoformat()}/{now.isoformat()}",
        query={ORBIT_STATE_PROPERTY: {"eq": orbit_direction}},
    )
    products: list[dict[str, str]] = []
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


def select_new_products(args: argparse.Namespace) -> list[dict[str, str]]:
    """Per tile: query CDSE, drop non-{S1A,S1C} (logged) and already-registered acquisitions (logged),
    and return the new products to submit as ``{tile, orbit, product_id, datetime, date, platform}``."""
    tiles = [t.strip() for t in args.tiles.split(",") if t.strip()]
    new_products: list[dict[str, str]] = []
    for tile in tiles:
        products = query_products(
            CDSE_STAC_URL, tile_bbox(tile), args.orbit_direction, args.lookback_days
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
            new_products.append(
                {
                    "tile": tile,
                    "orbit": args.orbit_direction,
                    "product_id": product["product_id"],
                    "datetime": product["datetime"],
                    "date": product["date"],
                    "platform": platform,
                }
            )
    return new_products


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tiles", required=True, help="Comma-separated MGRS tile IDs (e.g. 31TCH)")
    parser.add_argument("--orbit-direction", required=True, choices=["ascending", "descending"])
    parser.add_argument("--lookback-days", required=True, type=int, help="CDSE query window (days)")
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
