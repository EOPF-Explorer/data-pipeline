#!/usr/bin/env python3
"""
Query STAC API for new items to process.

This script searches for items in a source collection that were updated within
a specified time window and checks if they already exist in the target collection
to avoid reprocessing. Uses the 'updated' property for harvesting use cases.

Two modes (subcommands):

- ``discover``: run the STAC query and write the full item list to ``items.json``
  in ``--out-dir`` (Argo ships it as an output artifact), plus ``count`` and
  ``num_batches`` files for the workflow to fan out over. The list is NOT written
  to stdout: Argo caps a step's stdout/result at ~256 KB, and a large window would
  truncate it and break the downstream ``withParam`` loop.
- ``read-batch``: print one bounded slice ``[index*size : (index+1)*size]`` of a
  previously written items file as a JSON list — small enough for ``withParam``.
"""

import argparse
import json
import logging
import math
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

from pystac_client import Client

DEFAULT_BATCH_SIZE = 200

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


def _optional_float(value: str) -> float | None:
    """argparse type for a float flag that may arrive blank.

    The recent-data-processor uses a single shared WorkflowTemplate, so a cron that wants
    no cloud-cover filter (e.g. a SAR/non-optical collection) cannot omit the arg — it can
    only pass an empty value. Treat empty/whitespace as ``None`` (filter disabled) so that
    reuse path works cleanly; otherwise parse as a float.
    """
    if value.strip() == "":
        return None
    return float(value)


def _validate_bbox(bbox: object) -> None:
    """Raise SystemExit if bbox is not a list of exactly 4 floats."""
    if not isinstance(bbox, list) or len(bbox) != 4:
        sys.exit(f"Error: AOI_BBOX must be a JSON array of 4 numbers, got: {bbox!r}")
    for i, v in enumerate(bbox):
        if not isinstance(v, int | float):
            sys.exit(f"Error: AOI_BBOX[{i}] must be a number, got: {v!r}")


def _to_utc(dt: datetime) -> datetime:
    """Normalize a datetime to UTC-aware so comparisons/sorts never mix naive and aware.

    Naive datetimes are assumed UTC (pystac preserves naive inputs); aware ones are
    converted to UTC. Centralizing this keeps the sort key and the acquisition filter
    using identical semantics.
    """
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def _acquisition_sort_key(item: dict) -> datetime:
    """Sort key for ordering processing items by acquisition datetime (newest first).

    Parses the stored ISO timestamp so the comparison is chronological (not lexicographic,
    which would break across mixed UTC offsets). Items with no datetime sort oldest.
    """
    raw = item.get("datetime")
    if not raw:
        return datetime.min.replace(tzinfo=UTC)
    return _to_utc(datetime.fromisoformat(raw))


def discover(args: argparse.Namespace) -> None:
    """Query STAC and write the item manifest files for the workflow to fan out over."""
    SOURCE_STAC_API_URL = args.source_stac_api_url
    SOURCE_COLLECTION = args.source_collection
    TARGET_STAC_API_URL = args.target_stac_api_url
    TARGET_COLLECTION = args.target_collection
    SCHEDULED_END_TIME = args.scheduled_end_time
    WINDOW_HOURS = args.window_hours
    AOI_BBOX = args.aoi_bbox
    MAX_ACQUISITION_AGE_DAYS = args.max_acquisition_age_days
    MAX_CLOUD_COVER = args.max_cloud_cover

    _require_https(SOURCE_STAC_API_URL, "SOURCE_STAC_API_URL")
    _require_https(TARGET_STAC_API_URL, "TARGET_STAC_API_URL")
    _validate_bbox(AOI_BBOX)
    if MAX_ACQUISITION_AGE_DAYS is not None and MAX_ACQUISITION_AGE_DAYS <= 0:
        sys.exit(f"Error: --max-acquisition-age-days must be > 0, got: {MAX_ACQUISITION_AGE_DAYS}")
    if MAX_CLOUD_COVER is not None and not 0 < MAX_CLOUD_COVER <= 100:
        sys.exit(f"Error: --max-cloud-cover must be in (0, 100], got: {MAX_CLOUD_COVER}")

    # Parse scheduled end time and calculate start time
    end_time = datetime.fromisoformat(SCHEDULED_END_TIME.replace("Z", "+00:00"))
    start_time = end_time - timedelta(hours=WINDOW_HOURS)

    # Format datetime for STAC API (replace +00:00 with Z)
    start_time_str = start_time.isoformat().replace("+00:00", "Z")
    end_time_str = end_time.isoformat().replace("+00:00", "Z")

    # Acquisition-date floor for real-time processing: keep only items observed within the
    # last N days of the window end, so a reingestion that re-stamps `updated` on old
    # acquisitions doesn't flood the queue. Floor only (open upper bound) — never drop the
    # freshest data. Unset → no acquisition filter (original behaviour).
    min_acquisition_dt = (
        end_time - timedelta(days=MAX_ACQUISITION_AGE_DAYS)
        if MAX_ACQUISITION_AGE_DAYS is not None
        else None
    )

    logger.info(f"Querying source STAC API: {SOURCE_STAC_API_URL}")
    logger.info(f"Source collection: {SOURCE_COLLECTION}")
    logger.info(f"Target STAC API: {TARGET_STAC_API_URL}")
    logger.info(f"Target collection: {TARGET_COLLECTION}")
    logger.info(f"Scheduled end time: {SCHEDULED_END_TIME}")
    logger.info(f"Time window: {WINDOW_HOURS} hours")
    logger.info(f"Query time range: {start_time_str} to {end_time_str}")
    logger.info(f"Area of Interest (bbox): {AOI_BBOX}")
    if min_acquisition_dt is not None:
        logger.info(
            f"Acquisition floor: {min_acquisition_dt.isoformat().replace('+00:00', 'Z')} "
            f"(max age {MAX_ACQUISITION_AGE_DAYS} days)"
        )
    if MAX_CLOUD_COVER is not None:
        logger.info(f"Cloud-cover filter: eo:cloud_cover < {MAX_CLOUD_COVER}")

    # Connect to source STAC catalog
    source_catalog = Client.open(SOURCE_STAC_API_URL)

    # Connect to target STAC catalog (may be different)
    target_catalog = Client.open(TARGET_STAC_API_URL)

    # Search for items by updated time (for harvesting use case)
    # Query items that were updated within the time window, not by acquisition date
    updated_filter: dict[str, object] = {
        "op": "between",
        "args": [{"property": "updated"}, start_time_str, end_time_str],
    }
    # Optionally narrow to low-cloud optical scenes server-side, so clouded items are trimmed
    # before pagination and never cost a per-item dedup call. CQL2 three-valued logic drops
    # items whose eo:cloud_cover is absent/null (harmless for S2 L2A, which always has it);
    # unset ⇒ predicate omitted ⇒ nothing dropped (the SAR/non-optical reuse path).
    filter_expr: dict[str, object] = updated_filter
    if MAX_CLOUD_COVER is not None:
        filter_expr = {
            "op": "and",
            "args": [
                updated_filter,
                {"op": "<", "args": [{"property": "eo:cloud_cover"}, MAX_CLOUD_COVER]},
            ],
        }
    search_kwargs: dict[str, object] = {
        "collections": [SOURCE_COLLECTION],
        "filter": filter_expr,
        "filter_lang": "cql2-json",
        "bbox": AOI_BBOX,
        "limit": 100,  # Items per page for efficient pagination
    }
    # Narrow by acquisition datetime server-side too (open upper bound), so the flood is
    # trimmed before pagination. The client-side guard below remains authoritative.
    if min_acquisition_dt is not None:
        search_kwargs["datetime"] = f"{min_acquisition_dt.isoformat().replace('+00:00', 'Z')}/.."
    search = source_catalog.search(**search_kwargs)

    # Collect items to process
    items_to_process = []
    checked_count = 0
    skipped_old_count = 0
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

            # Acquisition-date filter (real-time): drop items observed before the floor, or
            # with no datetime (can't be proven recent). Done before the per-item dedup
            # search below so filtered items cost no target-API call.
            if min_acquisition_dt is not None and (
                item.datetime is None or _to_utc(item.datetime) < min_acquisition_dt
            ):
                skipped_old_count += 1
                continue

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
                    "datetime": item.datetime.isoformat() if item.datetime else None,
                }
            )

    logger.info(
        f"📊 Summary: Processed {page_count} pages, checked {checked_count} items, "
        f"{skipped_old_count} skipped (out of acquisition window), "
        f"{len(items_to_process)} to process"
    )

    # Process most-recent-first: during a large reingestion backlog, order the queue by
    # acquisition datetime descending so the freshest observations are converted before the
    # long tail drains. The stable sort preserves pagination order within equal datetimes;
    # items lacking a datetime sort last.
    items_to_process.sort(key=_acquisition_sort_key, reverse=True)

    # Write the full list as files: items.json becomes an Argo output artifact; count
    # and num_batches drive the workflow's batch fan-out. Nothing large goes to stdout,
    # so we never hit Argo's ~256 KB result cap regardless of how many items match.
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "items.json").write_text(json.dumps(items_to_process))
    (out_dir / "count").write_text(str(len(items_to_process)))
    num_batches = math.ceil(len(items_to_process) / args.batch_size)
    (out_dir / "num_batches").write_text(str(num_batches))
    logger.info(
        f"Wrote {len(items_to_process)} items to {out_dir / 'items.json'} "
        f"(batch_size={args.batch_size}, num_batches={num_batches})"
    )


def read_batch(args: argparse.Namespace) -> None:
    """Print one bounded slice ``[index*size : (index+1)*size]`` of an items file."""
    items = json.loads(Path(args.items_file).read_text())
    start = args.index * args.batch_size
    batch = items[start : start + args.batch_size]
    sys.stdout.write(json.dumps(batch))
    sys.stdout.flush()


def main() -> None:
    """Dispatch to the ``discover`` or ``read-batch`` subcommand."""
    parser = argparse.ArgumentParser(description="Query STAC API for new items to process.")
    sub = parser.add_subparsers(dest="mode", required=True)

    d = sub.add_parser("discover", help="Run the STAC query and write the item manifest.")
    d.add_argument("source_stac_api_url", metavar="SOURCE_STAC_API_URL")
    d.add_argument("source_collection", metavar="SOURCE_COLLECTION")
    d.add_argument("target_stac_api_url", metavar="TARGET_STAC_API_URL")
    d.add_argument("target_collection", metavar="TARGET_COLLECTION")
    d.add_argument("scheduled_end_time", metavar="SCHEDULED_END_TIME")
    d.add_argument("window_hours", metavar="WINDOW_HOURS", type=int)
    d.add_argument("aoi_bbox", metavar="AOI_BBOX", type=json.loads)
    d.add_argument(
        "--out-dir", required=True, help="Directory to write items.json/count/num_batches."
    )
    d.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    d.add_argument(
        "--max-acquisition-age-days",
        type=int,
        default=None,
        help="Real-time filter: keep only items whose acquisition datetime is within the last "
        "N days of the scheduled window end. Omit for no acquisition filter (default).",
    )
    d.add_argument(
        "--max-cloud-cover",
        type=_optional_float,
        default=None,
        help="Optical filter: keep only items with eo:cloud_cover strictly below this "
        "percentage, in (0, 100]. Omit or pass blank for no cloud-cover filter (default).",
    )
    d.set_defaults(func=discover)

    r = sub.add_parser("read-batch", help="Print one bounded slice of an items file.")
    r.add_argument("items_file", metavar="ITEMS_FILE")
    r.add_argument("index", metavar="INDEX", type=int)
    r.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    r.set_defaults(func=read_batch)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
