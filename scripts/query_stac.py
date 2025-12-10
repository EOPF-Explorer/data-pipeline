#!/usr/bin/env python3
"""
Query STAC API for new items to process.

This script searches for items in a source collection within a specified time window
and checks if they already exist in the target collection to avoid reprocessing.
"""

import json
import logging
import os
import sys
from datetime import UTC, datetime, timedelta

from pystac_client import Client

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Main entry point for STAC query script."""
    # Configuration from Argo workflow parameters
    STAC_API_URL = sys.argv[1]
    SOURCE_COLLECTION = sys.argv[2]
    TARGET_COLLECTION = sys.argv[3]
    END_TIME_OFFSET_HOURS = int(sys.argv[4])
    LOOKBACK_HOURS = int(sys.argv[5])
    AOI_BBOX = json.loads(sys.argv[6])

    # Calculate time window
    end_time = datetime.now(UTC) - timedelta(hours=END_TIME_OFFSET_HOURS)
    start_time = end_time - timedelta(hours=LOOKBACK_HOURS)

    # Format datetime for STAC API (replace +00:00 with Z)
    start_time_str = start_time.isoformat().replace("+00:00", "Z")
    end_time_str = end_time.isoformat().replace("+00:00", "Z")

    logger.info(f"Querying STAC API: {STAC_API_URL}")
    logger.info(f"Collection: {SOURCE_COLLECTION}")
    logger.info(f"Time range: {start_time_str} to {end_time_str}")

    # Connect to STAC catalog
    catalog = Client.open(STAC_API_URL)

    # Search for items
    search = catalog.search(
        collections=[SOURCE_COLLECTION],
        datetime=f"{start_time_str}/{end_time_str}",
        bbox=AOI_BBOX,
    )

    # Collect items to process
    items_to_process = []
    checked_count = 0

    for page in search.pages():
        for item in page.items:
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
                target_search = catalog.search(
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

    logger.info(f"📊 Summary: Checked {checked_count} items, {len(items_to_process)} to process")

    # Output ONLY JSON to stdout (for Argo withParam)
    sys.stdout.write(json.dumps(items_to_process))
    sys.stdout.flush()


if __name__ == "__main__":
    main()
