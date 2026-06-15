"""Orchestrate ingest then register for one S1 GRD RTC tile.

Derives the Zarr store path from bucket/collection/tile-id, calls ingest_v1_s1_rtc.py,
and — unless ingest reports no acquisitions (exit 2) — calls register_v1_s1_rtc.py.

Ingest writes the ``s3://`` cube directly via ``ingest_v1_s1_rtc.run_ingest``, which fetches any
existing per-tile cube, **appends** the new scene as a ``time`` slice (T4), and uploads with s3fs —
so the cube accumulates across runs instead of being overwritten.

S3 endpoint/credentials are configured two ways and must agree: the ingest subprocess reads the
GeoTIFFs and fetches/appends/uploads the ``s3://`` cube via s3fs/rasterio, which use the ambient
``AWS_ENDPOINT_URL`` / ``AWS_PROFILE`` (or ``AWS_ACCESS_KEY_ID``/``AWS_SECRET_ACCESS_KEY``); the
register step instead uses the ``--s3-endpoint`` arg. Set ``AWS_ENDPOINT_URL`` to the same endpoint
passed as ``--s3-endpoint`` (with read+write creds for the output bucket) or the cube will be read
from one endpoint and written to another.

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

# Per-environment S3 buckets and STAC collections are matched pairs: a tile ingested for
# `staging` must land in BOTH the staging bucket and the staging collection. Crossing them
# (e.g. the `-tests` bucket with the `-staging` collection) produces an item whose Zarr lives
# where the gateway/TiTiler won't serve it for that collection -- the 32TLR footgun, where a
# staging-collection item was written to the local tests bucket and rendered broken. We can only
# judge consistency when BOTH names are recognized per-env values; ad-hoc names (CI fixtures,
# one-off buckets, the cross-env `-acquisitions` collection) are unrecognized and pass through.
_BUCKET_ENV = {
    "esa-zarr-sentinel-explorer-tests": "test",
    "esa-zarr-sentinel-explorer-s1-l1grd-staging": "staging",
    "esa-zarr-sentinel-explorer-s1-l1grd-prod": "prod",
}
_COLLECTION_ENV = {
    "sentinel-1-grd-rtc-tests": "test",
    "sentinel-1-grd-rtc-staging": "staging",
    "sentinel-1-grd-rtc-prod": "prod",
}


def check_env_consistency(collection: str, s3_output_bucket: str) -> None:
    """Reject a collection/bucket pair that mixes environments.

    Raises ``ValueError`` when both names are recognized per-env values but disagree on the
    environment. Unrecognized names pass through unchecked (their environment is unknown).
    """
    bucket_env = _BUCKET_ENV.get(s3_output_bucket)
    collection_env = _COLLECTION_ENV.get(collection)
    if bucket_env and collection_env and bucket_env != collection_env:
        raise ValueError(
            f"environment mismatch: bucket {s3_output_bucket!r} is '{bucket_env}' but collection "
            f"{collection!r} is '{collection_env}'. Per-env buckets and collections must match "
            f"(the 32TLR footgun: a staging item written to the tests bucket renders broken). "
            f"Use a '{collection_env}' bucket, or the collection matching this bucket's environment."
        )


def run_pipeline(
    s3_geotiff_prefix: str,
    tile_id: str,
    orbit_direction: str,
    collection: str,
    s3_output_bucket: str,
    s3_endpoint: str,
    stac_api_url: str,
    raster_api_url: str,
    acquisitions_collection: str = "sentinel-1-grd-rtc-acquisitions",
) -> int:
    if not collection or "/" in collection:
        raise ValueError(
            f"collection must be a non-empty single path segment (no '/'), got: {collection!r}"
        )
    check_env_consistency(collection, s3_output_bucket)
    # TEMPORARY (#246): write the cube directly at titiler-eopf's reconstructed render path
    # — s3://{bucket}/tests-output/{collection}/{item_id}.zarr where item_id == s1-rtc-{tile} —
    # so new tiles preview without a copy (titiler ignores the asset href). Replaces the #250
    # auto-copy. Revert to s3://{bucket}/{collection}/s1-grd-rtc-{tile}.zarr when titiler-eopf#108
    # (resolve store from href) lands.
    s3_zarr = f"s3://{s3_output_bucket}/tests-output/{collection}/s1-rtc-{tile_id}.zarr"

    # Step 1 — ingest directly into the s3:// cube. run_ingest fetches any existing cube, appends the
    # new scene as a time slice (T4), and uploads via s3fs — so the per-tile cube accumulates across
    # runs instead of being overwritten. Endpoint/creds come from the ambient AWS_ENDPOINT_URL/AWS_*
    # (see the module docstring); no separate `aws s3 sync` step.
    ingest_cmd = [  # noqa: S607
        "uv",
        "run",
        "python",
        "scripts/ingest_v1_s1_rtc.py",
        "--s3-geotiff-prefix",
        s3_geotiff_prefix,
        "--s3-zarr-store",
        s3_zarr,
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
    if result.returncode != 0:
        return result.returncode

    # Step 3 — register the per-acquisition items (the -acquisitions collection). The data model
    # has TWO registrations — the per-tile cube item (Step 2, -staging) AND one item per acquisition
    # (here). Running both from this single stage keeps the collections in sync; skipping either is
    # the kind of gap that leaves a tile invisible in one collection.
    peracq_cmd = [  # noqa: S607
        "uv",
        "run",
        "python",
        "scripts/register_per_acquisition.py",
        "--store",
        s3_zarr,
        "--tile-id",
        tile_id,
        "--orbit-direction",
        orbit_direction,
        "--collection",
        acquisitions_collection,
        "--stac-api-url",
        stac_api_url,
        "--raster-api-url",
        raster_api_url,
    ]
    result = subprocess.run(peracq_cmd)  # noqa: S603
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
    parser.add_argument(
        "--acquisitions-collection",
        default="sentinel-1-grd-rtc-acquisitions",
        help="STAC collection for the per-acquisition items (registered alongside the cube item)",
    )
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
            acquisitions_collection=args.acquisitions_collection,
        )
    )


if __name__ == "__main__":
    main()
