#!/usr/bin/env python3
"""GeoZarr conversion entry point - orchestrates conversion workflow."""

from __future__ import annotations

import argparse
import logging
import os
from urllib.parse import urlparse

import fsspec
import httpx
import xarray as xr
from eopf_geozarr import create_geozarr_dataset
from eopf_geozarr.conversion.fs_utils import get_storage_options

# Configure logging (set LOG_LEVEL=DEBUG for verbose output)
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)
for lib in ["botocore", "s3fs", "aiobotocore", "urllib3"]:
    logging.getLogger(lib).setLevel(logging.WARNING)


# === Conversion Parameters ===

# Conversion parameters by mission (defaults to Sentinel-2 if unrecognized)
CONFIGS: dict[str, dict] = {
    "sentinel-1": {
        "groups": ["/measurements"],
        "crs_groups": ["/conditions/gcp"],
        "spatial_chunk": 4096,
        "tile_width": 512,
        "enable_sharding": False,
    },
    "sentinel-2": {
        "groups": [
            "/measurements/reflectance/r10m",
            "/measurements/reflectance/r20m",
            "/measurements/reflectance/r60m",
            "/quality/l2a_quicklook/r10m",
        ],
        "crs_groups": ["/conditions/geometry"],
        "spatial_chunk": 1024,
        "tile_width": 256,
        "enable_sharding": True,
    },
}


def get_config(collection_id: str) -> dict:
    """Get conversion config for collection (defaults to Sentinel-2)."""
    prefix = "-".join(collection_id.lower().split("-")[:2])
    return CONFIGS.get(prefix, CONFIGS["sentinel-2"]).copy()


def get_zarr_url(stac_item_url: str) -> str:
    """Get Zarr asset URL from STAC item (priority: product, zarr, any .zarr)."""
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        assets = client.get(stac_item_url).raise_for_status().json().get("assets", {})

    # Try priority assets first
    for key in ["product", "zarr"]:
        if key in assets and (href := assets[key].get("href")):
            return str(href)

    # Fallback: any asset with .zarr in href
    for asset in assets.values():
        if ".zarr" in asset.get("href", ""):
            return str(asset["href"])

    raise RuntimeError("No Zarr asset found in STAC item")


# === Conversion Workflow ===


def run_conversion(
    source_url: str,
    collection: str,
    s3_output_bucket: str,
    s3_output_prefix: str,
    groups: str | None = None,
    spatial_chunk: int | None = None,
    tile_width: int | None = None,
    enable_sharding: bool | None = None,
) -> str:
    """Run GeoZarr conversion workflow.

    Args:
        source_url: Source STAC item URL or direct Zarr URL
        collection: Collection ID for parameter lookup
        s3_output_bucket: S3 bucket for output
        s3_output_prefix: S3 prefix for output
        groups: Override groups (comma-separated if multiple)
        spatial_chunk: Override spatial chunk size
        tile_width: Override tile width
        enable_sharding: Override sharding flag

    Returns:
        Output Zarr URL (s3://...)
    """
    item_id = urlparse(source_url).path.rstrip("/").split("/")[-1]
    logger.info(f"🔄 Converting: {item_id}")
    logger.info(f"   Collection: {collection}")

    # Resolve source: STAC item or direct Zarr URL
    zarr_url = get_zarr_url(source_url) if "/items/" in source_url else source_url
    logger.info(f"   Source: {zarr_url}")

    # Get config and apply overrides
    config = get_config(collection)
    if groups:
        config["groups"] = groups.split(",")
    if spatial_chunk is not None:
        config["spatial_chunk"] = spatial_chunk
    if tile_width is not None:
        config["tile_width"] = tile_width
    if enable_sharding is not None:
        config["enable_sharding"] = enable_sharding

    logger.info(
        f"   Parameters: chunk={config['spatial_chunk']}, tile={config['tile_width']}, sharding={config['enable_sharding']}"
    )

    # Construct output path and clean existing
    output_url = f"s3://{s3_output_bucket}/{s3_output_prefix}/{collection}/{item_id}.zarr"
    logger.info(f"   Output: {output_url}")

    try:
        fs = fsspec.filesystem("s3", client_kwargs={"endpoint_url": os.getenv("AWS_ENDPOINT_URL")})
        fs.rm(output_url, recursive=True)
        logger.info("   🧹 Cleaned existing output")
    except FileNotFoundError:
        pass
    except Exception as e:
        logger.warning(f"   ⚠️  Cleanup warning: {e}")

    # Load and convert
    storage_options = get_storage_options(zarr_url)
    dt = xr.open_datatree(zarr_url, engine="zarr", chunks="auto", storage_options=storage_options)
    logger.info(f"   📂 Loaded {len(dt.children)} groups from source")

    create_geozarr_dataset(
        dt_input=dt,
        groups=config["groups"],
        output_path=output_url,
        spatial_chunk=config["spatial_chunk"],
        tile_width=config["tile_width"],
        crs_groups=config.get("crs_groups"),
        enable_sharding=config["enable_sharding"],
    )

    logger.info(f"✅ Conversion complete → {output_url}")
    return output_url


def main() -> None:
    """CLI entry point for GeoZarr conversion."""
    parser = argparse.ArgumentParser(description="Convert EOPF Zarr to GeoZarr format")
    parser.add_argument("--source-url", required=True, help="Source STAC item or Zarr URL")
    parser.add_argument("--collection", required=True, help="Collection ID")
    parser.add_argument("--s3-output-bucket", required=True, help="S3 bucket")
    parser.add_argument("--s3-output-prefix", required=True, help="S3 prefix")
    parser.add_argument("--groups", help="Override groups (comma-separated)")
    parser.add_argument("--spatial-chunk", type=int, help="Override spatial chunk size")
    parser.add_argument("--tile-width", type=int, help="Override tile width")
    parser.add_argument("--enable-sharding", action="store_true", help="Enable sharding")
    args = parser.parse_args()

    run_conversion(
        source_url=args.source_url,
        collection=args.collection,
        s3_output_bucket=args.s3_output_bucket,
        s3_output_prefix=args.s3_output_prefix,
        groups=args.groups,
        spatial_chunk=args.spatial_chunk,
        tile_width=args.tile_width,
        enable_sharding=args.enable_sharding,
    )


if __name__ == "__main__":
    main()
