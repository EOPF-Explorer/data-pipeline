"""Orchestrate ingest then register for one S1 GRD RTC tile.

Derives the Zarr store path from bucket/prefix/tile-id, calls ingest_v1_s1_rtc.py,
and — unless ingest reports no acquisitions (exit 2) — calls register_v1_s1_rtc.py.

Exit codes:
    0 -- success, or ingest found no acquisitions (register skipped)
    1 -- ingest or register failure
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys

log = logging.getLogger(__name__)


def run_pipeline(
    s3_geotiff_prefix: str,
    tile_id: str,
    orbit_direction: str,
    collection: str,
    s3_output_bucket: str,
    s3_output_prefix: str,
    s3_endpoint: str,
    stac_api_url: str,
    raster_api_url: str,
) -> int:
    zarr_store = f"s3://{s3_output_bucket}/{s3_output_prefix}/s1-grd-rtc-{tile_id}.zarr"

    # Step 1 — ingest
    ingest_cmd = [  # noqa: S607
        "uv",
        "run",
        "python",
        "scripts/ingest_v1_s1_rtc.py",
        "--s3-geotiff-prefix",
        s3_geotiff_prefix,
        "--s3-zarr-store",
        zarr_store,
        "--tile-id",
        tile_id,
        "--orbit-direction",
        orbit_direction,
    ]
    result = subprocess.run(ingest_cmd)  # noqa: S603
    if result.returncode == 2:
        log.info("no acquisitions found — skipping register")
        return 0
    if result.returncode != 0:
        return result.returncode

    # Step 2 — register (only reached when ingest exited 0)
    register_cmd = [  # noqa: S607
        "uv",
        "run",
        "python",
        "scripts/register_v1_s1_rtc.py",
        "--store",
        zarr_store,
        "--collection",
        collection,
        "--stac-api-url",
        stac_api_url,
        "--raster-api-url",
        raster_api_url,
        "--s3-endpoint",
        s3_endpoint,
        "--s3-output-bucket",
        s3_output_bucket,
        "--s3-output-prefix",
        s3_output_prefix,
    ]
    result = subprocess.run(register_cmd)  # noqa: S603
    return result.returncode


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--s3-geotiff-prefix", required=True, help="S3 prefix with S1Tiling GeoTIFFs"
    )
    parser.add_argument("--tile-id", required=True, help="MGRS tile ID (e.g. 31TCH)")
    parser.add_argument("--orbit-direction", required=True, choices=["ascending", "descending"])
    parser.add_argument("--collection", required=True, help="Target STAC collection ID")
    parser.add_argument("--s3-output-bucket", required=True, help="S3 bucket for Zarr output")
    parser.add_argument("--s3-output-prefix", required=True, help="S3 prefix for Zarr output")
    parser.add_argument("--s3-endpoint", required=True, help="S3 endpoint URL")
    parser.add_argument("--stac-api-url", required=True, help="STAC API base URL")
    parser.add_argument("--raster-api-url", required=True, help="TiTiler raster API base URL")
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    args = _build_parser().parse_args()
    sys.exit(
        run_pipeline(
            s3_geotiff_prefix=args.s3_geotiff_prefix,
            tile_id=args.tile_id,
            orbit_direction=args.orbit_direction,
            collection=args.collection,
            s3_output_bucket=args.s3_output_bucket,
            s3_output_prefix=args.s3_output_prefix,
            s3_endpoint=args.s3_endpoint,
            stac_api_url=args.stac_api_url,
            raster_api_url=args.raster_api_url,
        )
    )


if __name__ == "__main__":
    main()
