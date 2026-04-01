#!/usr/bin/env python3
"""
Query STAC API for new items to process.

This script searches for items in a source collection that were updated within
a specified time window and checks if they already exist in the target collection
to avoid reprocessing. Uses the 'updated' property for harvesting use cases.
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from urllib.parse import urlparse

from pystac_client import Client

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _require_https(url: str, name: str) -> None:
    """Raise SystemExit if url is not an HTTPS URL."""
    if urlparse(url).scheme != "https":
        sys.exit(f"Error: {name} must be an HTTPS URL, got: {url!r}")


def _validate_bbox(bbox: object) -> None:
    """Raise SystemExit if bbox is not a list of exactly 4 floats."""
    if not isinstance(bbox, list) or len(bbox) != 4:
        sys.exit(f"Error: AOI_BBOX must be a JSON array of 4 numbers, got: {bbox!r}")
    for i, v in enumerate(bbox):
        if not isinstance(v, int | float):
            sys.exit(f"Error: AOI_BBOX[{i}] must be a number, got: {v!r}")


def main() -> None:
    """Main entry point for STAC query script."""
    parser = argparse.ArgumentParser(description="Query STAC API for new items to process.")
    parser.add_argument("source_stac_api_url", metavar="SOURCE_STAC_API_URL")
    parser.add_argument("source_collection", metavar="SOURCE_COLLECTION")
    parser.add_argument("target_stac_api_url", metavar="TARGET_STAC_API_URL")
    parser.add_argument("target_collection", metavar="TARGET_COLLECTION")
    parser.add_argument("scheduled_end_time", metavar="SCHEDULED_END_TIME")
    parser.add_argument("window_hours", metavar="WINDOW_HOURS", type=int)
    parser.add_argument("aoi_bbox", metavar="AOI_BBOX", type=json.loads)
    args = parser.parse_args()

    SOURCE_STAC_API_URL = args.source_stac_api_url
    SOURCE_COLLECTION = args.source_collection
    TARGET_STAC_API_URL = args.target_stac_api_url
    TARGET_COLLECTION = args.target_collection
    SCHEDULED_END_TIME = args.scheduled_end_time
    WINDOW_HOURS = args.window_hours
    AOI_BBOX = args.aoi_bbox

    _require_https(SOURCE_STAC_API_URL, "SOURCE_STAC_API_URL")
    _require_https(TARGET_STAC_API_URL, "TARGET_STAC_API_URL")
    _validate_bbox(AOI_BBOX)

    # Parse scheduled end time and calculate start time
    end_time = datetime.fromisoformat(SCHEDULED_END_TIME.replace("Z", "+00:00"))
    start_time = end_time - timedelta(hours=WINDOW_HOURS)

    # Format datetime for STAC API (replace +00:00 with Z)
    start_time_str = start_time.isoformat().replace("+00:00", "Z")
    end_time_str = end_time.isoformat().replace("+00:00", "Z")

    logger.info(f"Querying source STAC API: {SOURCE_STAC_API_URL}")
    logger.info(f"Source collection: {SOURCE_COLLECTION}")
    logger.info(f"Target STAC API: {TARGET_STAC_API_URL}")
    logger.info(f"Target collection: {TARGET_COLLECTION}")
    logger.info(f"Scheduled end time: {SCHEDULED_END_TIME}")
    logger.info(f"Time window: {WINDOW_HOURS} hours")
    logger.info(f"Query time range: {start_time_str} to {end_time_str}")
    logger.info(f"Area of Interest (bbox): {AOI_BBOX}")

    # Connect to source STAC catalog
    source_catalog = Client.open(SOURCE_STAC_API_URL)

    # Connect to target STAC catalog (may be different)
    target_catalog = Client.open(TARGET_STAC_API_URL)

    # Search for items by updated time (for harvesting use case)
    # Query items that were updated within the time window, not by acquisition date
    search = source_catalog.search(
        collections=[SOURCE_COLLECTION],
        filter={
            "op": "between",
            "args": [{"property": "updated"}, start_time_str, end_time_str],
        },
        filter_lang="cql2-json",
        bbox=AOI_BBOX,
        limit=100,  # Items per page for efficient pagination
    )

    # Collect items to process
    items_to_process = []
    checked_count = 0
    page_count = 0

    logger.info("Starting pagination through search results...")
    for page in search.pages():
        page_count += 1
        page_items = list(page.items)
        logger.info(f"Processing page {page_count} with {len(page_items)} items")

        # Safety check: if we get an empty page, log it but continue
        if not page_items:
            logger.warning(
                f"Empty page {page_count} encountered - this may indicate pagination issues"
            )
            continue

        for item in page_items:
            checked_count += 1

            # Get item URL
            item_url = next(
                (link.href for link in item.links if link.rel == "self"),
                None,
            )

            if not item_url:
                logger.warning(f"Skipping {item.id}: No self link")
                continue

            # Check if already converted (prevent wasteful reprocessing)
            try:
                target_search = target_catalog.search(
                    collections=[TARGET_COLLECTION],
                    ids=[item.id],
                )
                existing_items = list(target_search.items())

                if existing_items:
                    logger.info(f"Skipping {item.id}: Already converted")
                    continue
            except Exception as e:
                logger.warning(f"Could not check {item.id}: {e}")
                # On error, process it to be safe

            # Add to processing queue
            items_to_process.append(
                {
                    "source_url": item_url,
                    "collection": TARGET_COLLECTION,
                    "item_id": item.id,
                }
            )

    logger.info(
        f"📊 Summary: Processed {page_count} pages, checked {checked_count} items, {len(items_to_process)} to process"
    )

    # Output ONLY JSON to stdout (for Argo withParam)
    sys.stdout.write(json.dumps(items_to_process))
    sys.stdout.flush()


if __name__ == "__main__":
    main()
