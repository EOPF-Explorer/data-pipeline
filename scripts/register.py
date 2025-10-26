#!/usr/bin/env python3
"""STAC registration entry point - orchestrates item creation and registration."""

from __future__ import annotations

import argparse
import logging
import sys
from urllib.parse import urlparse

import httpx
from augment_stac_item import augment
from create_geozarr_item import create_geozarr_item
from pystac import Item
from register_stac import register_item

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def run_registration(
    source_url: str,
    collection: str,
    stac_api_url: str,
    raster_api_url: str,
    s3_endpoint: str,
    s3_output_bucket: str,
    s3_output_prefix: str,
    mode: str = "create-or-skip",
) -> None:
    """Run STAC registration workflow.

    Args:
        source_url: Source STAC item URL
        collection: Target collection ID
        stac_api_url: STAC API base URL
        raster_api_url: TiTiler raster API base URL
        s3_endpoint: S3 endpoint for HTTP access
        s3_output_bucket: S3 bucket name
        s3_output_prefix: S3 prefix path
        mode: Registration mode (create-or-skip | upsert | replace)

    Raises:
        RuntimeError: If registration fails
    """
    # Extract item ID from source URL and construct geozarr URL
    item_id = urlparse(source_url).path.rstrip("/").split("/")[-1]
    geozarr_url = f"s3://{s3_output_bucket}/{s3_output_prefix}/{collection}/{item_id}.zarr"

    logger.info(f"Starting registration for {item_id} in {collection}")
    logger.info(f"GeoZarr: {geozarr_url}")
    # Step 1: Create STAC item from source
    logger.info("Creating STAC item from source...")
    item_dict = create_geozarr_item(source_url, collection, geozarr_url, s3_endpoint)

    # Step 2: Register to STAC API
    logger.info("Registering item in STAC API...")
    register_item(stac_api_url, collection, item_dict, mode)

    # Step 3: Augment with preview links and CRS metadata
    logger.info("Adding preview links and metadata...")
    logger.info(f"  Raster API: {raster_api_url}")

    # Fetch the registered item
    item_url = f"{stac_api_url.rstrip('/')}/collections/{collection}/items/{item_id}"
    r = httpx.get(item_url, timeout=30.0)
    r.raise_for_status()
    item = Item.from_dict(r.json())

    # Augment in place
    augment(item, raster_base=raster_api_url, collection_id=collection)

    # Update via PUT
    r = httpx.put(
        item_url,
        json=item.to_dict(),
        headers={"Content-Type": "application/json"},
        timeout=30.0,
    )
    r.raise_for_status()

    logger.info(f"✅ Registered and augmented {item_id} in {collection}")
    logger.info(f"   STAC API: {stac_api_url}/collections/{collection}/items/{item_id}")
    logger.info(f"   GeoZarr:  {geozarr_url}")


def main(argv: list[str] | None = None) -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Run STAC registration workflow")
    parser.add_argument("--source-url", required=True, help="Source STAC item URL")
    parser.add_argument("--collection", required=True, help="Target collection ID")
    parser.add_argument("--stac-api-url", required=True, help="STAC API base URL")
    parser.add_argument("--raster-api-url", required=True, help="TiTiler raster API base URL")
    parser.add_argument("--s3-endpoint", required=True, help="S3 endpoint for HTTP access")
    parser.add_argument("--s3-output-bucket", required=True, help="S3 bucket name")
    parser.add_argument("--s3-output-prefix", required=True, help="S3 prefix path")
    parser.add_argument(
        "--mode",
        default="create-or-skip",
        choices=["create-or-skip", "upsert", "replace"],
        help="Registration mode (default: create-or-skip)",
    )

    args = parser.parse_args(argv)

    try:
        run_registration(
            args.source_url,
            args.collection,
            args.stac_api_url,
            args.raster_api_url,
            args.s3_endpoint,
            args.s3_output_bucket,
            args.s3_output_prefix,
            args.mode,
        )
        return 0
    except Exception as e:
        logger.error(f"Registration failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
