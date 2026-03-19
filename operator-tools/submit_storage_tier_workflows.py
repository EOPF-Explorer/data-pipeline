#!/usr/bin/env python3
"""Submit storage tier change workflows via NATS JetStream for a date range of STAC items.

Queries STAC in 24h windows and publishes one JetStream message per window to the
'storage-tier-changes' subject. The publish ack confirms durable storage before
advancing, preventing silent drops that occurred with the HTTP webhook EventSource.
Each message triggers the eopf-storage-tier-batch-job WorkflowTemplate via Argo Events.
"""

import argparse
import asyncio
import json
import logging
import sys
import time
from datetime import UTC, datetime, timedelta
from typing import cast

import nats
from pystac_client import Client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

NATS_SUBJECT = "storage-tier-changes"


def generate_time_windows(
    start_date: datetime, end_date: datetime, window_hours: int = 24
) -> list[tuple[str, str]]:
    """Return list of (window_start_iso, window_end_iso) tuples covering start_date to end_date.

    Each window is window_hours long; the last window is truncated to end_date.
    """
    windows = []
    current = start_date
    delta = timedelta(hours=window_hours)
    while current < end_date:
        window_end = min(current + delta, end_date)
        windows.append(
            (
                current.isoformat().replace("+00:00", "Z"),
                window_end.isoformat().replace("+00:00", "Z"),
            )
        )
        current = window_end
    return windows


def query_stac_items(
    stac_api_url: str, collection: str, window_start: str, window_end: str
) -> list[str]:
    """Query STAC for items with datetime in the given window. Returns list of item IDs."""
    catalog = Client.open(stac_api_url)
    search = catalog.search(
        collections=[collection],
        datetime=f"{window_start}/{window_end}",
        limit=100,
    )
    return [item.id for item in search.items()]


async def submit_batch(nats_url: str, payload: dict[str, object], dry_run: bool) -> bool:
    """Publish a JSON payload to NATS JetStream. Returns True on publish ack."""
    if dry_run:
        logger.info(f"[dry-run] Would publish to NATS {NATS_SUBJECT}: {payload}")
        return True
    nc = None
    try:
        nc = await nats.connect(nats_url)
        js = nc.jetstream()
        data = json.dumps(payload).encode()
        ack = await js.publish(NATS_SUBJECT, data)
        n_items = len(cast(list[str], payload.get("item_ids", [])))
        logger.info(f"Published {n_items} items to {NATS_SUBJECT}, ack seq={ack.seq}")
        return True
    except Exception as e:
        logger.error(f"Error publishing to NATS: {e}")
        return False
    finally:
        if nc is not None:
            await nc.drain()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Submit storage tier batch workflows via NATS JetStream for a date range of STAC items."
    )
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--collection", required=True, help="STAC collection ID")
    parser.add_argument("--storage-class", default="STANDARD_IA", help="S3 storage class")
    parser.add_argument("--stac-api-url", default="https://api.explorer.eopf.copernicus.eu/stac")
    parser.add_argument("--s3-endpoint", default="https://s3.de.io.cloud.ovh.net")
    parser.add_argument("--pipeline-image-version", default="v1.6.1")
    parser.add_argument("--process-all-assets", action="store_true")
    parser.add_argument("--nats-url", default="nats://localhost:4222")
    parser.add_argument(
        "--delay", type=float, default=1.0, help="Delay between window submissions in seconds"
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    try:
        start_date = datetime.fromisoformat(args.start_date).replace(tzinfo=UTC)
        end_date = datetime.fromisoformat(args.end_date).replace(tzinfo=UTC)
    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        sys.exit(1)

    if end_date <= start_date:
        logger.error("--end-date must be after --start-date")
        sys.exit(1)

    windows = generate_time_windows(start_date, end_date)
    logger.info(f"Processing {len(windows)} 24h windows from {args.start_date} to {args.end_date}")

    total_submitted = 0
    total_failed = 0

    for i, (window_start, window_end) in enumerate(windows, 1):
        logger.info(f"[{i}/{len(windows)}] Querying window {window_start} to {window_end}")
        item_ids = query_stac_items(args.stac_api_url, args.collection, window_start, window_end)
        logger.info(f"  Found {len(item_ids)} items")

        if not item_ids:
            logger.info("  Skipping empty window")
            continue

        payload: dict[str, object] = {
            "action": "batch-change-storage-tier",
            "item_ids": item_ids,
            "collection": args.collection,
            "storage_class": args.storage_class,
            "stac_api_url": args.stac_api_url,
            "s3_endpoint": args.s3_endpoint,
            "pipeline_image_version": args.pipeline_image_version,
            "process_all_assets": str(args.process_all_assets).lower(),
        }
        success = asyncio.run(submit_batch(args.nats_url, payload, args.dry_run))
        if success:
            total_submitted += 1
        else:
            total_failed += 1

        if i < len(windows):
            time.sleep(args.delay)

    logger.info(f"Done. Submitted: {total_submitted}, Failed: {total_failed}")


if __name__ == "__main__":
    main()
