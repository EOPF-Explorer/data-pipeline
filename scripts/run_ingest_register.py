"""Orchestrate ingest then register for one S1 GRD RTC tile.

Derives the Zarr store path from bucket/collection/tile-id, calls ingest_v1_s1_rtc.py,
and — unless ingest reports no acquisitions (exit 2) — calls register_v1_s1_rtc.py.

S3 endpoint/credentials are configured two ways and must agree: the ingest subprocess
reads GeoTIFFs and writes the local store via s3fs/rasterio, which use the ambient
``AWS_ENDPOINT_URL`` / ``AWS_PROFILE`` (or ``AWS_ACCESS_KEY_ID``/``AWS_SECRET_ACCESS_KEY``);
the ``aws s3 sync`` upload and register step instead use the ``--s3-endpoint`` arg. Set
``AWS_ENDPOINT_URL`` to the same endpoint passed as ``--s3-endpoint`` (with read+write creds
for the output bucket) or the store will be read from one endpoint and written to another.

Exit codes:
    0 -- success, or ingest found no acquisitions (register skipped)
    1 -- ingest or register failure
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import subprocess
import sys
import tempfile

log = logging.getLogger(__name__)


def run_pipeline(
    s3_geotiff_prefix: str,
    tile_id: str,
    orbit_direction: str,
    collection: str,
    s3_output_bucket: str,
    s3_endpoint: str,
    stac_api_url: str,
    raster_api_url: str,
) -> int:
    # Store key prefix is the STAC collection, so per-mission/per-env buckets stay
    # self-describing: s3://{bucket}/{collection}/s1-grd-rtc-{tile}.zarr
    if not collection or "/" in collection:
        raise ValueError(
            f"collection must be a non-empty single path segment (no '/'), got: {collection!r}"
        )
    s3_zarr = f"s3://{s3_output_bucket}/{collection}/s1-grd-rtc-{tile_id}.zarr"
    # eopf_geozarr uses pathlib.Path internally, which collapses s3:// to s3:/ and
    # writes the zarr to a local directory. Use a local temp path for ingest, then
    # sync the result to S3 before registering.
    local_zarr = os.path.join(tempfile.gettempdir(), f"s1-grd-rtc-{tile_id}.zarr")
    if os.path.exists(local_zarr):
        shutil.rmtree(local_zarr)

    # Step 1 — ingest (writes to local temp dir)
    ingest_cmd = [  # noqa: S607
        "uv",
        "run",
        "python",
        "scripts/ingest_v1_s1_rtc.py",
        "--s3-geotiff-prefix",
        s3_geotiff_prefix,
        "--s3-zarr-store",
        local_zarr,
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

    # Step 2 — upload local zarr to S3
    upload_cmd = [  # noqa: S607
        "aws",
        "s3",
        "sync",
        local_zarr,
        s3_zarr,
        "--endpoint-url",
        s3_endpoint,
    ]
    result = subprocess.run(upload_cmd)  # noqa: S603
    if result.returncode != 0:
        log.error("zarr upload to %s failed (exit %d)", s3_zarr, result.returncode)
        return result.returncode

    # Step 3 — register (only reached when upload exited 0)
    register_cmd = [  # noqa: S607
        "uv",
        "run",
        "python",
        "scripts/register_v1_s1_rtc.py",
        "--store",
        s3_zarr,
        "--collection",
        collection,
        "--stac-api-url",
        stac_api_url,
        "--raster-api-url",
        raster_api_url,
        "--s3-endpoint",
        s3_endpoint,
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
            s3_endpoint=args.s3_endpoint,
            stac_api_url=args.stac_api_url,
            raster_api_url=args.raster_api_url,
        )
    )


if __name__ == "__main__":
    main()
