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
import logging

import mgrs
from pystac_client import Client

log = logging.getLogger(__name__)

CDSE_COLLECTION = "SENTINEL-1-GRD"
# Orbit-state filter is isolated here so Task 6 can pin casing/mechanism against the live API.
ORBIT_STATE_PROPERTY = "sat:orbit_state"


def tile_bbox(tile_id: str) -> list[float]:
    """Return the WGS84 bbox [lon_min, lat_min, lon_max, lat_max] of an MGRS 100 km square.

    The square's corners are not axis-aligned in lat/lon, so the bbox is the min/max over all four
    corners. Raises ``ValueError`` for a malformed/unknown tile id.
    """
    m = mgrs.MGRS()
    corners = ["0000000000", "9999900000", "0000099999", "9999999999"]  # SW, SE, NW, NE
    try:
        latlon = [m.toLatLon(f"{tile_id}{c}") for c in corners]
    except mgrs.core.MGRSError as exc:
        raise ValueError(f"invalid MGRS tile id: {tile_id!r}") from exc
    lats = [lat for lat, _ in latlon]
    lons = [lon for _, lon in latlon]
    return [min(lons), min(lats), max(lons), max(lats)]


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
    return parser


def main() -> None:
    build_parser().parse_args()
    # Tasks 2-5 wire query_cdse -> dedupe -> process_product here.
    raise SystemExit("watch_cdse_and_process: orchestration not yet implemented (Task 1 skeleton)")


if __name__ == "__main__":
    main()
