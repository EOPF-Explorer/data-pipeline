"""Watch CDSE for new S1 GRD products and drive Script A -> Script B for each unseen one.

Local stand-in for the future Argo CronWorkflow (sub-issue 8). Pure orchestration: query CDSE,
dedupe via a state file, run run_s1tiling.py then run_ingest_register.py per new product, report.

CLI aligns to the *real* run_ingest_register.py interface: the Zarr store path is derived from
``--collection`` (``s3://{s3_zarr_bucket}/{collection}/s1-grd-rtc-{tile}.zarr``), so there is no
``--s3-zarr-prefix`` argument.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import math
import os
import subprocess
from pathlib import Path

import mgrs
from pystac_client import Client
from run_ingest_register import check_env_consistency

log = logging.getLogger(__name__)

# Source catalogue queried for new acquisitions (distinct from the EOPF target STAC passed to
# Script B via --stac-api-url). Verified live in Task 6.
CDSE_STAC_URL = "https://catalogue.dataspace.copernicus.eu/stac"
# Verified live 2026-06-05: id is lowercase (SENTINEL-1-GRD returns 0); sat:orbit_state is lowercase
# ('descending'/'ascending') and the `query` extension filters correctly.
CDSE_COLLECTION = "sentinel-1-grd"
ORBIT_STATE_PROPERTY = "sat:orbit_state"

# Local S1Tiling workdir (matches the Sub-issue 4 layout); used to default Script A's local args.
S1T_WORKDIR = os.environ.get("S1T_WORKDIR", os.path.expanduser("~/s1tiling"))

# Local idempotency state (gitignored).
STATE_FILE = Path("data/.processed_products.json")

# Processed-product state: {tile: {orbit: [{"product_id", "date"}]}}
State = dict[str, dict[str, list[dict[str, str]]]]


# An S2 tile is a 109.8 km square anchored at the NW corner of its MGRS 100 km square, so it
# extends 9.8 km beyond the square's east and south edges. `mgrs` only knows the 100 km square;
# the un-padded bbox starved AgglomerateDEM of the easternmost DEM cell on 32TMT (exit 66,
# 2026-07-02 — see claude-docs/plans/s1_ensure_dem_true_tile_bbox.md).
S2_TILE_OVERHANG_M = 109_800.0 - 100_000.0
# The true tile also pokes past the square-corner bbox on the west/north sides: grid convergence
# tilts the edges (~9800·tan γ, ≤ ~0.7 km at the 84° MGRS limit) and parallel curvature bows the
# N/S edges between the sampled corners (sagitta ≈ 236·tan(lat) m, ≤ ~2.3 km at 84°). A flat 3 km
# tolerance bounds both anywhere the grid exists; measured worst case in the AOI+33H sweep test is
# ~0.42 km. Cost: the DEM window gains a 1° cell only within 3 km of a degree boundary.
BBOX_TOLERANCE_M = 3_000.0
METERS_PER_DEG_LAT = 111_320.0


def tile_bbox(tile_id: str) -> list[float]:
    """Return the WGS84 bbox [lon_min, lat_min, lon_max, lat_max] of the true S2 tile extent.

    The MGRS square's corners are not axis-aligned in lat/lon, so start from the min/max over
    all four corners, then pad east and south to the 109.8 km S2 extent plus the projection
    tolerance on every side (the pole-most latitude gives the widest east pad, keeping the bbox
    conservative). Raises ``ValueError`` for a malformed/unknown tile id.
    """
    m = mgrs.MGRS()
    corners = ["0000000000", "9999900000", "0000099999", "9999999999"]  # SW, SE, NW, NE
    try:
        latlon = [m.toLatLon(f"{tile_id}{c}") for c in corners]
    except mgrs.core.MGRSError as exc:
        raise ValueError(f"invalid MGRS tile id: {tile_id!r}") from exc
    lats = [lat for lat, _ in latlon]
    lons = [lon for _, lon in latlon]
    meters_per_deg_lon = METERS_PER_DEG_LAT * math.cos(
        math.radians(max(abs(lat) for lat in lats))  # pole-most latitude ⇒ widest, conservative
    )
    dlat = (S2_TILE_OVERHANG_M + BBOX_TOLERANCE_M) / METERS_PER_DEG_LAT
    dlon = (S2_TILE_OVERHANG_M + BBOX_TOLERANCE_M) / meters_per_deg_lon
    tol_lat = BBOX_TOLERANCE_M / METERS_PER_DEG_LAT
    tol_lon = BBOX_TOLERANCE_M / meters_per_deg_lon
    return [min(lons) - tol_lon, min(lats) - dlat, max(lons) + dlon, max(lats) + tol_lat]


def _item_date(item: object) -> str | None:
    """Return the acquisition date as YYYY-MM-DD, or None if the item carries no datetime."""
    when = getattr(item, "datetime", None)
    if when is not None:
        return str(when.date().isoformat())
    start = getattr(item, "properties", {}).get("start_datetime")
    return start[:10] if isinstance(start, str) else None


def query_cdse(
    stac_url: str, bbox: list[float], orbit_direction: str, lookback_days: int
) -> list[dict[str, str]]:
    """Query the CDSE STAC API for S1 GRD products over `bbox` in the last `lookback_days`.

    Returns ``[{"product_id", "date": "YYYY-MM-DD"}, ...]``. Items without a datetime are skipped.
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
        date = _item_date(item)
        if date is None:
            log.warning("skipping %s: no datetime", item.id)
            continue
        products.append({"product_id": item.id, "date": date})
    return products


def load_processed(path: str | Path) -> State:
    """Load the processed-product state; a missing or unreadable file yields empty state."""
    p = Path(path)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text())
    except (json.JSONDecodeError, OSError):
        log.warning("state file %s unreadable; treating as empty", p)
        return {}
    return data if isinstance(data, dict) else {}


def is_processed(state: State, tile: str, orbit: str, product_id: str) -> bool:
    return any(e["product_id"] == product_id for e in state.get(tile, {}).get(orbit, []))


def mark_processed(state: State, tile: str, orbit: str, product_id: str, date: str) -> None:
    entries = state.setdefault(tile, {}).setdefault(orbit, [])
    entries.append({"product_id": product_id, "date": date})


def save_processed(path: str | Path, state: State) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(state, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tiles", required=True, help="Comma-separated MGRS tile IDs (e.g. 31TCH)")
    parser.add_argument("--orbit-direction", required=True, choices=["ascending", "descending"])
    parser.add_argument("--lookback-days", required=True, type=int, help="CDSE query window (days)")
    parser.add_argument(
        "--s3-bucket", required=True, help="Bucket for S1Tiling GeoTIFFs (Script A)"
    )
    parser.add_argument("--s3-prefix", required=True, help="Key prefix for GeoTIFFs (Script A)")
    parser.add_argument(
        "--s3-zarr-bucket", required=True, help="Bucket for the Zarr store (Script B output)"
    )
    parser.add_argument("--s3-endpoint", required=True, help="S3 endpoint URL")
    parser.add_argument("--collection", required=True, help="Target STAC collection ID")
    parser.add_argument("--stac-api-url", required=True, help="STAC API base URL")
    parser.add_argument("--raster-api-url", required=True, help="TiTiler raster API base URL")
    parser.add_argument("--dry-run", action="store_true")
    # Local-only Script A args (no Argo equivalent); default to the $S1T_WORKDIR layout.
    parser.add_argument("--eodag-cfg", default=f"{S1T_WORKDIR}/config/eodag.yml")
    parser.add_argument("--dem-dir", default=f"{S1T_WORKDIR}/DEM/COP_DEM_GLO30")
    parser.add_argument("--data-dir", default=S1T_WORKDIR)
    parser.add_argument("--cfg", default="config/S1GRD_RTC.cfg")
    return parser


def _script_a_cmd(args: argparse.Namespace, tile: str, date_start: str, date_end: str) -> list[str]:
    return [
        "uv", "run", "python", "scripts/run_s1tiling.py",
        "--tile-id", tile,
        "--orbit-direction", args.orbit_direction,
        "--date-start", date_start,
        "--date-end", date_end,
        "--s3-bucket", args.s3_bucket,
        "--s3-prefix", args.s3_prefix,
        "--s3-endpoint", args.s3_endpoint,
        "--eodag-cfg", args.eodag_cfg,
        "--dem-dir", args.dem_dir,
        "--data-dir", args.data_dir,
        "--cfg", args.cfg,
    ]  # fmt: skip


def _script_b_cmd(args: argparse.Namespace, tile: str, geotiff_prefix: str) -> list[str]:
    # Aligned to the real run_ingest_register.py: Zarr path derives from --collection,
    # so the watcher's --s3-zarr-bucket maps to Script B's --s3-output-bucket (no output prefix).
    return [
        "uv", "run", "python", "scripts/run_ingest_register.py",
        "--s3-geotiff-prefix", geotiff_prefix,
        "--tile-id", tile,
        "--orbit-direction", args.orbit_direction,
        "--collection", args.collection,
        "--s3-output-bucket", args.s3_zarr_bucket,
        "--s3-endpoint", args.s3_endpoint,
        "--stac-api-url", args.stac_api_url,
        "--raster-api-url", args.raster_api_url,
    ]  # fmt: skip


def process_product(args: argparse.Namespace, product: dict[str, str], tile: str) -> bool:
    """Run Script A then Script B for one product. Returns True on success, False on any failure.

    The S3 GeoTIFF prefix is reconstructed from Script A's own output formula (rather than captured
    from its stdout) so Script A's long docker logs can stream straight to the terminal.
    """
    date = dt.date.fromisoformat(product["date"])
    date_start = (date - dt.timedelta(days=1)).isoformat()
    date_end = (date + dt.timedelta(days=1)).isoformat()
    # Mirrors run_s1tiling.py: s3://{bucket}/{prefix}/{tile}/{orbit}/{date_start}/
    geotiff_prefix = (
        f"s3://{args.s3_bucket}/{args.s3_prefix}/{tile}/{args.orbit_direction}/{date_start}/"
    )

    a_cmd = _script_a_cmd(args, tile, date_start, date_end)
    b_cmd = _script_b_cmd(args, tile, geotiff_prefix)

    if args.dry_run:
        log.info(
            "[dry-run] %s -> would run:\n  A: %s\n  B: %s",
            product["product_id"],
            " ".join(a_cmd),
            " ".join(b_cmd),
        )
        return True

    if subprocess.run(a_cmd).returncode != 0:  # noqa: S603 -- Script A streams to terminal (not captured)
        log.error("Script A (s1tiling) failed for %s", product["product_id"])
        return False
    if subprocess.run(b_cmd).returncode != 0:  # noqa: S603
        log.error("Script B (ingest/register) failed for %s", product["product_id"])
        return False
    return True


def run_watch(args: argparse.Namespace) -> dict[str, int]:
    """Query each tile, run Script A -> B for unseen products, persist state, return run counts."""
    # Fail fast on a cross-env bucket/collection pair, before the CDSE query or any s1tiling run --
    # Script B (run_ingest_register) enforces the same invariant, but only after orthorectification.
    check_env_consistency(args.collection, args.s3_zarr_bucket)
    tiles = [t.strip() for t in args.tiles.split(",") if t.strip()]
    state = load_processed(STATE_FILE)
    counts = {"found": 0, "new": 0, "processed": 0, "failed": 0}

    for tile in tiles:
        products = query_cdse(
            CDSE_STAC_URL, tile_bbox(tile), args.orbit_direction, args.lookback_days
        )
        counts["found"] += len(products)
        for product in products:
            if is_processed(state, tile, args.orbit_direction, product["product_id"]):
                log.info("skipping %s (already processed)", product["product_id"])
                continue
            counts["new"] += 1
            if process_product(args, product, tile):
                counts["processed"] += 1
                if not args.dry_run:
                    mark_processed(
                        state, tile, args.orbit_direction, product["product_id"], product["date"]
                    )
                    save_processed(STATE_FILE, state)
            else:
                counts["failed"] += 1

    log.info(
        "Summary: %d found, %d new, %d processed, %d failed",
        counts["found"],
        counts["new"],
        counts["processed"],
        counts["failed"],
    )
    return counts


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    run_watch(build_parser().parse_args())


if __name__ == "__main__":
    main()
