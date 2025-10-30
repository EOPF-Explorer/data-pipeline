#!/usr/bin/env python3
"""STAC registration entry point - orchestrates item creation and registration."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import tempfile
from pathlib import Path

from augment_stac_item import augment
from create_geozarr_item import create_geozarr_item
from pystac import Item
from register_stac import register_item
from utils import extract_item_id

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def run_registration(
    source_url: str,
    collection: str,
    geozarr_url: str,
    stac_api_url: str,
    raster_api_url: str,
    s3_endpoint: str,
    verbose: bool = False,
    mode: str = "upsert",
) -> None:
    """Run STAC registration workflow.

    Args:
        source_url: Source STAC item URL
        collection: Target collection ID
        geozarr_url: GeoZarr output URL (s3://...)
        stac_api_url: STAC API base URL
        raster_api_url: TiTiler raster API base URL
        s3_endpoint: S3 endpoint for HTTP access
        verbose: Enable verbose logging
        mode: Registration mode (create-or-skip | upsert | replace)

    Raises:
        RuntimeError: If registration fails
    """
    logger.info("=" * 78)
    logger.info("  STEP 2/2: STAC REGISTRATION & AUGMENTATION")
    logger.info("=" * 78)

    # Extract item ID from source URL
    item_id = extract_item_id(source_url)
    logger.info(f"Item ID: {item_id}")
    logger.info(f"Collection: {collection}")
    logger.info(f"STAC API: {stac_api_url}")

    # Create temporary file for item JSON
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
        item_json_path = tmp.name

    try:
        # Step 1: Create STAC item from source
        logger.info("Creating STAC item from source...")
        create_geozarr_item(source_url, collection, geozarr_url, s3_endpoint, item_json_path)

        # Step 2: Register to STAC API
        logger.info("Registering item in STAC API...")
        with open(item_json_path) as f:
            item_dict = json.load(f)
        register_item(stac_api_url, collection, item_dict, mode)

        # Step 3: Augment with preview links and CRS metadata
        logger.info("Adding preview links and metadata...")
        logger.info(f"  Raster API: {raster_api_url}")

        # Fetch the registered item to augment
        import httpx

        item_url = f"{stac_api_url.rstrip('/')}/collections/{collection}/items/{item_id}"
        with httpx.Client() as client:
            r = client.get(item_url, timeout=30.0)
            r.raise_for_status()
            item = Item.from_dict(r.json())

        # Augment in place
        augment(item, raster_base=raster_api_url, collection_id=collection, verbose=verbose)

        # Update via PUT
        with httpx.Client() as client:
            r = client.put(
                item_url,
                json=item.to_dict(),
                headers={"Content-Type": "application/json"},
                timeout=30.0,
            )
            r.raise_for_status()
            if verbose:
                logger.info(f"PUT {item_url} → {r.status_code}")

        logger.info("✅ Registration & augmentation completed successfully!")
        logger.info("")
        logger.info("=" * 78)
        logger.info("  🎉 PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("=" * 78)
        logger.info("")
        logger.info("📍 View item in STAC API:")
        logger.info(f"   {stac_api_url}/collections/{collection}/items/{item_id}")
        logger.info("")
        logger.info("📦 GeoZarr output location:")
        logger.info(f"   {geozarr_url}")
        logger.info("")

    finally:
        # Clean up temp file
        Path(item_json_path).unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Run STAC registration workflow")
    parser.add_argument("--source-url", required=True, help="Source STAC item URL")
    parser.add_argument("--collection", required=True, help="Target collection ID")
    parser.add_argument("--geozarr-url", required=True, help="GeoZarr output URL (s3://...)")
    parser.add_argument("--stac-api-url", required=True, help="STAC API base URL")
    parser.add_argument("--raster-api-url", required=True, help="TiTiler raster API base URL")
    parser.add_argument("--s3-endpoint", required=True, help="S3 endpoint for HTTP access")
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")
    parser.add_argument(
        "--mode",
        default="upsert",
        choices=["create-or-skip", "upsert", "replace"],
        help="Registration mode",
    )

    args = parser.parse_args(argv)

    try:
        run_registration(
            args.source_url,
            args.collection,
            args.geozarr_url,
            args.stac_api_url,
            args.raster_api_url,
            args.s3_endpoint,
            args.verbose,
            args.mode,
        )
        return 0
    except Exception as e:
        logger.error(f"Registration failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
