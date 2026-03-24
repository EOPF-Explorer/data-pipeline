#!/usr/bin/env python3
"""Query STAC items needing storage tier migration.

Searches for items in a 36h time window around age_days, filters out items
already at the target storage tier (via storage:refs metadata), and caps
output at max_batch_size. Outputs JSON array of item IDs to stdout for
Argo withParam.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import UTC, datetime, timedelta

from pystac import Item
from pystac_client import Client
from update_stac_storage_tier import TIER_TO_SCHEME

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

for lib in ["botocore", "boto3", "urllib3", "httpx", "httpcore"]:
    logging.getLogger(lib).setLevel(logging.WARNING)


def get_storage_ref(s3_info: dict) -> str | None:
    """Extract primary storage:refs value from an asset's alternate.s3 info.

    Handles both list and string forms defensively.

    Args:
        s3_info: The alternate.s3 dict from a STAC asset.

    Returns:
        The first storage:refs value, or None if absent/empty.
    """
    refs = s3_info.get("storage:refs", [])
    if isinstance(refs, list):
        return refs[0] if refs else None
    if isinstance(refs, str):
        return refs
    return None


def is_already_migrated(item: Item, target_storage_ref: str) -> bool:
    """Check if all S3 assets of an item already have the target storage tier.

    An item is considered already migrated only if it has at least one asset
    with alternate.s3 AND all such assets have storage:refs matching the target.
    Items with no S3 assets are treated as needing work (safe default).

    Args:
        item: STAC item to check.
        target_storage_ref: Expected storage:refs value (e.g., "glacier", "standard").

    Returns:
        True if all S3 assets are already at the target tier, False otherwise.
    """
    s3_assets = []
    for asset in item.assets.values():
        alt = asset.extra_fields.get("alternate", {})
        if not isinstance(alt, dict):
            continue
        s3 = alt.get("s3", {})
        if not isinstance(s3, dict):
            continue
        if s3:
            s3_assets.append(s3)

    if not s3_assets:
        return False

    return all(get_storage_ref(a) == target_storage_ref for a in s3_assets)


def query_items(
    stac_api_url: str,
    collection: str,
    age_days: int,
    target_storage_ref: str,
    max_batch_size: int,
) -> list[str]:
    """Query STAC and return item IDs needing storage tier change.

    Queries a 36h time window (age_days-0.5 to age_days+1 days old),
    filters out already-migrated items, and caps at max_batch_size.

    Args:
        stac_api_url: STAC API base URL.
        collection: STAC collection ID to query.
        age_days: Target age in days for storage tier transition.
        target_storage_ref: The storage:refs value indicating target tier.
        max_batch_size: Maximum number of items to return.

    Returns:
        List of item IDs needing storage tier change.
    """
    # 36h time window: from (age_days+1) days ago to (age_days-0.5) days ago
    now = datetime.now(UTC).replace(tzinfo=None)
    window_end = now - timedelta(days=age_days - 0.5)
    window_start = now - timedelta(days=age_days + 1)

    logger.info(f"Querying STAC: {stac_api_url} collection={collection}")
    logger.info(f"Time window: {window_start.isoformat()}Z to {window_end.isoformat()}Z")
    logger.info(f"Target storage ref: {target_storage_ref}, max batch: {max_batch_size}")

    catalog = Client.open(stac_api_url)
    search = catalog.search(
        collections=[collection],
        datetime=f"{window_start.isoformat()}Z/{window_end.isoformat()}Z",
    )

    total_found = 0
    already_migrated = 0
    need_work: list[str] = []

    for page in search.pages():
        for item in page.items:
            total_found += 1
            if is_already_migrated(item, target_storage_ref):
                already_migrated += 1
            else:
                need_work.append(item.id)

    capped = need_work[:max_batch_size]
    logger.info(f"Total found: {total_found}")
    logger.info(f"Already migrated: {already_migrated}")
    logger.info(f"Need work: {len(need_work)}")
    logger.info(f"Processing (capped at {max_batch_size}): {len(capped)}")

    return capped


def main(argv: list[str] | None = None) -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Query STAC items needing storage tier migration")
    parser.add_argument("--stac-api-url", required=True, help="STAC API base URL")
    parser.add_argument("--collection", required=True, help="STAC collection ID")
    parser.add_argument(
        "--age-days", type=int, required=True, help="Target age in days for tier transition"
    )
    parser.add_argument(
        "--to-storage-class",
        required=True,
        choices=list(TIER_TO_SCHEME.keys()),
        help="Target S3 storage class (e.g., STANDARD_IA, STANDARD, EXPRESS_ONEZONE)",
    )
    parser.add_argument(
        "--max-batch-size",
        type=int,
        default=100,
        help="Maximum number of items to return (default: 100)",
    )

    args = parser.parse_args(argv)

    try:
        target_storage_ref = TIER_TO_SCHEME[args.to_storage_class]
        items = query_items(
            stac_api_url=args.stac_api_url,
            collection=args.collection,
            age_days=args.age_days,
            target_storage_ref=target_storage_ref,
            max_batch_size=args.max_batch_size,
        )
        sys.stdout.write(json.dumps(items))
        sys.stdout.flush()
        return 0
    except Exception as e:
        logger.error(f"Failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
