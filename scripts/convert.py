#!/usr/bin/env python3
"""GeoZarr conversion entry point - orchestrates conversion workflow."""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys

from get_conversion_params import get_conversion_params
from utils import extract_item_id, get_zarr_url

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def run_conversion(
    source_url: str,
    collection: str,
    s3_output_bucket: str,
    s3_output_prefix: str,
    verbose: bool = False,
) -> str:
    """Run GeoZarr conversion workflow.

    Args:
        source_url: Source STAC item URL or direct Zarr URL
        collection: Collection ID for parameter lookup
        s3_output_bucket: S3 bucket for output
        s3_output_prefix: S3 prefix for output
        verbose: Enable verbose logging

    Returns:
        Output Zarr URL (s3://...)

    Raises:
        RuntimeError: If conversion fails
    """
    logger.info("=" * 78)
    logger.info("  STEP 1/2: GEOZARR CONVERSION")
    logger.info("=" * 78)

    # Extract item ID from URL
    item_id = extract_item_id(source_url)
    logger.info(f"Item ID: {item_id}")

    # Resolve source: STAC item or direct Zarr URL
    if "/items/" in source_url:
        logger.info("Extracting Zarr URL from STAC item...")
        zarr_url = get_zarr_url(source_url)
        logger.info(f"Zarr URL: {zarr_url}")
    else:
        zarr_url = source_url
        logger.info(f"Direct Zarr URL: {zarr_url}")

    # Get conversion parameters from collection config
    logger.info(f"Getting conversion parameters for {collection}...")
    params = get_conversion_params(collection)
    logger.info(f"  Groups:      {params['groups']}")
    logger.info(f"  Chunk:       {params['spatial_chunk']}")
    logger.info(f"  Tile width:  {params['tile_width']}")
    logger.info(f"  Extra flags: {params['extra_flags']}")

    # Construct output path
    output_url = f"s3://{s3_output_bucket}/{s3_output_prefix}/{collection}/{item_id}.zarr"

    # Build conversion command
    cmd = [
        "eopf-geozarr",
        "convert",
        zarr_url,
        output_url,
        "--groups",
        params["groups"],
        "--spatial-chunk",
        str(params["spatial_chunk"]),
        "--tile-width",
        str(params["tile_width"]),
        "--dask-cluster",
    ]

    # Add extra flags if present
    if params.get("extra_flags"):
        # Split extra_flags string into individual args
        extra_args = params["extra_flags"].split()
        cmd.extend(extra_args)

    if verbose:
        cmd.append("--verbose")

    logger.info("Starting GeoZarr conversion...")
    logger.info(f"  Source:      {zarr_url}")
    logger.info(f"  Destination: {output_url}")
    logger.info("-" * 78)
    logger.info("  CONVERSION LOGS (parallel processing with local Dask cluster)")
    logger.info("-" * 78)

    # Run conversion
    result = subprocess.run(cmd, check=False)

    if result.returncode != 0:
        logger.error(f"Conversion failed with exit code {result.returncode}")
        raise RuntimeError(f"eopf-geozarr convert failed: exit code {result.returncode}")

    logger.info("-" * 78)
    logger.info("✅ Conversion completed successfully!")
    logger.info("-" * 78)
    logger.info(f"Output: {output_url}")

    return output_url


def main(argv: list[str] | None = None) -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Run GeoZarr conversion workflow")
    parser.add_argument("--source-url", required=True, help="Source STAC item or Zarr URL")
    parser.add_argument("--collection", required=True, help="Collection ID")
    parser.add_argument("--s3-output-bucket", required=True, help="S3 output bucket")
    parser.add_argument("--s3-output-prefix", required=True, help="S3 output prefix")
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")

    args = parser.parse_args(argv)

    try:
        output_url = run_conversion(
            args.source_url,
            args.collection,
            args.s3_output_bucket,
            args.s3_output_prefix,
            args.verbose,
        )
        logger.info(f"Success: {output_url}")
        return 0
    except Exception as e:
        logger.error(f"Conversion failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
