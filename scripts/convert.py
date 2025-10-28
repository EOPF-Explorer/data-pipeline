#!/usr/bin/env python3
"""GeoZarr conversion entry point - orchestrates conversion workflow."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Any
from urllib.parse import urlparse

import fsspec
import httpx
import xarray as xr
from eopf_geozarr import create_geozarr_dataset
from eopf_geozarr.conversion.fs_utils import get_storage_options
from get_conversion_params import get_conversion_params

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_zarr_url(stac_item_url: str) -> str:
    """Get Zarr asset URL from STAC item."""
    r = httpx.get(stac_item_url, timeout=30.0, follow_redirects=True)
    r.raise_for_status()
    assets = r.json().get("assets", {})

    # Priority: product, zarr, then any .zarr asset
    for key in ["product", "zarr"]:
        if key in assets and (href := assets[key].get("href")):
            return str(href)

    # Fallback: any asset with .zarr in href
    for asset in assets.values():
        if ".zarr" in asset.get("href", ""):
            return str(asset["href"])

    raise RuntimeError("No Zarr asset found in STAC item")


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
        groups: Override groups parameter (comma-separated if multiple)
        spatial_chunk: Override spatial chunk size
        tile_width: Override tile width
        enable_sharding: Override sharding enable flag

    Returns:
        Output Zarr URL (s3://...)

    Raises:
        RuntimeError: If conversion fails
    """
    # Extract item ID from URL
    item_id = urlparse(source_url).path.rstrip("/").split("/")[-1]
    logger.info(f"Starting GeoZarr conversion for {item_id}")

    # Resolve source: STAC item or direct Zarr URL
    if "/items/" in source_url:
        logger.info("Extracting Zarr URL from STAC item...")
        zarr_url = get_zarr_url(source_url)
        logger.info(f"Zarr URL: {zarr_url}")
    else:
        zarr_url = source_url
        logger.info(f"Direct Zarr URL: {zarr_url}")

    # Get conversion parameters (with optional overrides)
    params = get_conversion_params(collection)
    overrides = {
        "groups": groups.split(",") if groups and "," in groups else groups or None,
        "spatial_chunk": spatial_chunk,
        "tile_width": tile_width,
        "enable_sharding": enable_sharding,
    }
    params.update({k: v for k, v in overrides.items() if v is not None})

    logger.info(f"Conversion params: {params}")  # Construct output path
    output_url = f"s3://{s3_output_bucket}/{s3_output_prefix}/{collection}/{item_id}.zarr"

    # Clean up existing output to avoid base array artifacts
    logger.info(f"🧹 Cleaning up existing output at: {output_url}")
    try:
        fs = fsspec.filesystem("s3", client_kwargs={"endpoint_url": os.getenv("AWS_ENDPOINT_URL")})
        fs.rm(output_url, recursive=True)
        logger.info("✅ Cleanup completed")
    except Exception as e:
        logger.info(f"ℹ️  No existing output to clean (or cleanup failed): {e}")

    logger.info("Starting GeoZarr conversion...")
    logger.info(f"  Source:      {zarr_url}")
    logger.info(f"  Destination: {output_url}")

    # Optional: Set up Dask cluster if enabled via environment variable
    # Note: eopf-geozarr handles its own Dask setup when using create_geozarr_dataset
    # This is here only for future compatibility if we need external cluster management
    use_dask = os.getenv("ENABLE_DASK_CLUSTER", "").lower() in ("true", "1", "yes")
    if use_dask:
        logger.info("🚀 Dask cluster enabled via ENABLE_DASK_CLUSTER env var")
        # Future: Could connect to external cluster here if needed
        # from dask.distributed import Client
        # dask_address = os.getenv("DASK_SCHEDULER_ADDRESS")
        # client = Client(dask_address) if dask_address else Client()

    # Load source dataset
    logger.info("Loading source dataset...")
    storage_options = get_storage_options(zarr_url)
    dt = xr.open_datatree(
        zarr_url,
        engine="zarr",
        chunks="auto",
        storage_options=storage_options,
    )
    logger.info(f"Loaded DataTree with {len(dt.children)} groups")

    # Convert to GeoZarr
    logger.info("Converting to GeoZarr format...")

    # Parse extra flags for optional parameters
    kwargs: dict[str, Any] = {}
    if params["extra_flags"] and "--crs-groups" in params["extra_flags"]:
        crs_groups_str = params["extra_flags"].split("--crs-groups")[1].strip().split()[0]
        kwargs["crs_groups"] = [crs_groups_str]

    # Add sharding if enabled
    if params.get("enable_sharding", False):
        kwargs["enable_sharding"] = True

    # groups parameter must be a list
    groups_param = params["groups"]
    if isinstance(groups_param, str):
        groups_list: list[str] = [groups_param]
    else:
        # groups_param is list[str] in mission configs
        groups_list = list(groups_param) if groups_param else []

    create_geozarr_dataset(
        dt_input=dt,
        groups=groups_list,
        output_path=output_url,
        spatial_chunk=params["spatial_chunk"],
        tile_width=params["tile_width"],
        **kwargs,
    )

    logger.info("✅ Conversion completed successfully!")
    logger.info(f"Output: {output_url}")

    return output_url


def main(argv: list[str] | None = None) -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Run GeoZarr conversion workflow")
    parser.add_argument("--source-url", required=True, help="Source STAC item or Zarr URL")
    parser.add_argument("--collection", required=True, help="Collection ID")
    parser.add_argument("--s3-output-bucket", required=True, help="S3 output bucket")
    parser.add_argument("--s3-output-prefix", required=True, help="S3 output prefix")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    # Optional parameter overrides
    parser.add_argument("--groups", help="Override groups (comma-separated)")
    parser.add_argument("--spatial-chunk", help="Override spatial chunk size")
    parser.add_argument("--tile-width", help="Override tile width")
    parser.add_argument("--enable-sharding", help="Override sharding (true/false)")

    args = parser.parse_args(argv)
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Parse override args (empty string → None)
    groups = args.groups or None
    spatial_chunk = int(args.spatial_chunk) if args.spatial_chunk else None
    tile_width = int(args.tile_width) if args.tile_width else None
    enable_sharding = args.enable_sharding.lower() == "true" if args.enable_sharding else None

    try:
        output_url = run_conversion(
            args.source_url,
            args.collection,
            args.s3_output_bucket,
            args.s3_output_prefix,
            groups,
            spatial_chunk,
            tile_width,
            enable_sharding,
        )
        logger.info(f"Success: {output_url}")
        return 0
    except Exception as e:
        logger.error(f"Conversion failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
