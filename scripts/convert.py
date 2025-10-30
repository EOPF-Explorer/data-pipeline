#!/usr/bin/env python3
"""GeoZarr conversion entry point - orchestrates conversion workflow."""

from __future__ import annotations

import argparse
import logging
import sys
from urllib.parse import urlparse

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
) -> str:
    """Run GeoZarr conversion workflow.

    Args:
        source_url: Source STAC item URL or direct Zarr URL
        collection: Collection ID for parameter lookup
        s3_output_bucket: S3 bucket for output
        s3_output_prefix: S3 prefix for output

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

    # Get conversion parameters from collection config
    logger.debug(f"Getting conversion parameters for {collection}...")
    params = get_conversion_params(collection)
    logger.debug(f"  Groups:      {params['groups']}")
    logger.debug(f"  Chunk:       {params['spatial_chunk']}")
    logger.debug(f"  Tile width:  {params['tile_width']}")
    logger.debug(f"  Extra flags: {params['extra_flags']}")

    # Construct output path
    output_url = f"s3://{s3_output_bucket}/{s3_output_prefix}/{collection}/{item_id}.zarr"

    logger.info("Starting GeoZarr conversion...")
    logger.info(f"  Source:      {zarr_url}")
    logger.info(f"  Destination: {output_url}")

    # Set up Dask cluster for parallel processing
    from dask.distributed import Client

    with Client() as client:
        logger.info(f"🚀 Dask cluster started: {client.dashboard_link}")

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
        kwargs = {}
        if params["extra_flags"] and "--crs-groups" in params["extra_flags"]:
            crs_groups_str = params["extra_flags"].split("--crs-groups")[1].strip().split()[0]
            kwargs["crs_groups"] = [crs_groups_str]

        create_geozarr_dataset(
            dt_input=dt,
            groups=params["groups"],
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

    args = parser.parse_args(argv)

    try:
        output_url = run_conversion(
            args.source_url,
            args.collection,
            args.s3_output_bucket,
            args.s3_output_prefix,
        )
        logger.info(f"Success: {output_url}")
        return 0
    except Exception as e:
        logger.error(f"Conversion failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
