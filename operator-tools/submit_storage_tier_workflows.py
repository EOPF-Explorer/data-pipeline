#!/usr/bin/env python3
"""Submit storage tier change workflows via HTTP webhook for a date range of STAC items.

Queries STAC in 24h windows and POSTs one webhook payload per window,
triggering the eopf-storage-tier-batch-job WorkflowTemplate via Argo Events.
Each workflow fans out to per-item parallel processing within Argo.
"""

import argparse
import logging
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import requests
from pystac_client import Client

# The tier-aware selection reuses the proven client-side predicate and tier map
# from the scripts/ package. Bootstrap scripts/ onto sys.path so this operator
# tool imports them at runtime the same way the test config (pythonpath) does.
_SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from query_storage_tier_items import is_already_migrated  # noqa: E402
from update_stac_storage_tier import TIER_TO_SCHEME  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    """Return the current UTC time. Seam for injecting a fixed 'today' in tests."""
    return datetime.now(UTC)


def compute_age_cutoff(min_age_days: int, *, today: datetime) -> datetime:
    """Return the `created` upper bound for age-based selection: ``today - min_age_days``.

    Items whose ``created`` is *before* this cutoff are older than ``min_age_days``.
    """
    return today - timedelta(days=min_age_days)


def resolve_window_bounds(
    *,
    min_age_days: int | None,
    start_date: str | None,
    end_date: str | None,
    today: datetime,
) -> tuple[datetime | None, datetime]:
    """Resolve the two selection modes to ``(start, end)`` datetimes.

    - Age mode (``min_age_days`` set): single-sided, returns ``(None, today - min_age_days)``.
      The open lower bound relies on the tier-aware filter to stay cheap.
    - Explicit mode (``start_date`` and ``end_date`` set): windowed, returns both bounds.

    The two modes are mutually exclusive. Raises ``ValueError`` on an invalid combination.
    """
    has_explicit = start_date is not None or end_date is not None
    if min_age_days is not None and has_explicit:
        raise ValueError("--min-age-days and --start-date/--end-date are mutually exclusive")
    if min_age_days is not None:
        if min_age_days < 0:
            raise ValueError("--min-age-days must be >= 0")
        return None, compute_age_cutoff(min_age_days, today=today)
    if not (start_date is not None and end_date is not None):
        raise ValueError("provide --min-age-days, or both --start-date and --end-date")
    start = datetime.fromisoformat(start_date).replace(tzinfo=UTC)
    end = datetime.fromisoformat(end_date).replace(tzinfo=UTC)
    if end <= start:
        raise ValueError("--end-date must be after --start-date")
    return start, end


def chunk_item_ids(item_ids: list[str], max_batch_size: int | None) -> list[list[str]]:
    """Split ``item_ids`` into batches of at most ``max_batch_size`` (one webhook payload each).

    Bounds the payload size / per-workflow fan-out for age mode, whose single-sided
    query has no time window to chunk on. ``max_batch_size`` of ``None`` or ``<= 0``
    means no chunking — a single batch, preserving the pre-existing behaviour. An
    empty input yields no batches (the caller skips empty selections).
    """
    if not item_ids:
        return []
    if max_batch_size is None or max_batch_size <= 0:
        return [item_ids]
    return [item_ids[i : i + max_batch_size] for i in range(0, len(item_ids), max_batch_size)]


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
    stac_api_url: str,
    collection: str,
    window_start: str | None,
    window_end: str,
    date_field: str = "datetime",
    target_storage_ref: str | None = None,
) -> list[str]:
    """Query STAC for items whose ``date_field`` falls in the window. Returns item IDs.

    ``date_field="datetime"`` uses pystac-client's native ``datetime=`` range
    (the item's sensing time). Other fields (``created``, ``updated``) filter on
    the registration/update timestamp via a CQL2 ``between`` filter, since those
    properties are not reachable through the ``datetime=`` kwarg.

    ``window_start=None`` issues a single-sided selection (``date_field < window_end``)
    for the recurring age-based tier-down: every item older than the cutoff, with no
    lower bound. This is only meaningful for the ``created``/``updated`` fields.

    ``target_storage_ref`` (e.g. ``"standard"``) filters out items already at the
    target tier, using the same asset-level ``storage:refs`` check as the optimizer
    (``is_already_migrated``). This keeps a recurring run cheap and makes a re-run a
    zero-item no-op. When ``None`` no tier filter is applied.
    """
    catalog = Client.open(stac_api_url)
    if window_start is None:
        # Single-sided open lower bound for age-based selection.
        search = catalog.search(
            collections=[collection],
            filter={"op": "<", "args": [{"property": date_field}, window_end]},
            filter_lang="cql2-json",
            limit=100,
        )
    elif date_field == "datetime":
        search = catalog.search(
            collections=[collection],
            datetime=f"{window_start}/{window_end}",
            limit=100,
        )
    else:
        search = catalog.search(
            collections=[collection],
            filter={
                "op": "between",
                "args": [{"property": date_field}, window_start, window_end],
            },
            filter_lang="cql2-json",
            limit=100,
        )
    if target_storage_ref is not None:
        return [
            item.id for item in search.items() if not is_already_migrated(item, target_storage_ref)
        ]
    return [item.id for item in search.items()]


def submit_batch(webhook_url: str, payload: dict[str, object], dry_run: bool) -> bool:
    """POST a JSON payload to the webhook endpoint. Returns True on success."""
    if dry_run:
        logger.info(f"[dry-run] Would submit: {payload}")
        return True
    try:
        response = requests.post(
            webhook_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        if response.status_code != 200:
            logger.warning(
                f"Non-200 response for window with {len(cast(list[str], payload.get('item_ids', [])))} items: "
                f"{response.status_code} {response.text}"
            )
        return response.status_code == 200
    except Exception as e:
        logger.error(f"Error submitting batch: {e}")
        return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Submit storage tier batch workflows via webhook for a date range of STAC items."
    )
    parser.add_argument(
        "--start-date", help="Start date (YYYY-MM-DD). Use with --end-date for a fixed window."
    )
    parser.add_argument(
        "--end-date", help="End date (YYYY-MM-DD). Use with --start-date for a fixed window."
    )
    parser.add_argument(
        "--min-age-days",
        type=int,
        help="Select items older than this many days by --date-field (single-sided, "
        "no lower bound). For the recurring tier-down cron. Mutually exclusive with "
        "--start-date/--end-date.",
    )
    parser.add_argument("--collection", required=True, help="STAC collection ID")
    parser.add_argument(
        "--date-field",
        choices=["datetime", "created", "updated"],
        default=None,
        help="Item date to select on: sensing 'datetime', registration 'created', "
        "or last-modified 'updated'. Non-'datetime' fields use a CQL2 filter. "
        "Defaults to 'created' with --min-age-days, else 'datetime'.",
    )
    parser.add_argument(
        "--max-batch-size",
        type=int,
        default=None,
        help="Max item IDs per webhook payload (per spawned workflow). Larger "
        "selections are split across multiple payloads. Default: no limit (one "
        "payload per window). Recommended for the recurring cron to bound fan-out.",
    )
    parser.add_argument("--storage-class", default="STANDARD", help="S3 storage class")
    parser.add_argument("--stac-api-url", default="https://api.explorer.eopf.copernicus.eu/stac")
    parser.add_argument("--s3-endpoint", default="https://s3.de.io.cloud.ovh.net")
    parser.add_argument("--pipeline-image-version", default="v1.6.1")
    parser.add_argument("--process-all-assets", action="store_true")
    parser.add_argument("--webhook-url", default="http://localhost:12000/samples")
    parser.add_argument(
        "--delay", type=float, default=1.0, help="Delay between window submissions in seconds"
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    try:
        start_date, end_date = resolve_window_bounds(
            min_age_days=args.min_age_days,
            start_date=args.start_date,
            end_date=args.end_date,
            today=_utcnow(),
        )
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)

    # Default the date field per mode: age-based selection is meaningful on the
    # registration timestamp, so an omitted --date-field means `created` there.
    date_field = args.date_field or ("created" if args.min_age_days is not None else "datetime")

    target_storage_ref = TIER_TO_SCHEME.get(args.storage_class)
    if target_storage_ref is None:
        logger.warning(
            f"--storage-class {args.storage_class!r} is not a known tier "
            f"({sorted(TIER_TO_SCHEME)}); the already-at-target filter is DISABLED, "
            "so every item in range will be selected."
        )

    if start_date is None:
        # Age mode: single-sided, one open-lower-bound query and payload.
        end_iso = end_date.isoformat().replace("+00:00", "Z")
        windows: list[tuple[str | None, str]] = [(None, end_iso)]
        logger.info(f"Age mode: selecting items with {date_field} < {end_iso}")
    else:
        windows = list(generate_time_windows(start_date, end_date))
        logger.info(
            f"Processing {len(windows)} 24h windows from {args.start_date} to {args.end_date}"
        )

    total_submitted = 0
    total_failed = 0
    submissions = 0

    for i, (window_start, window_end) in enumerate(windows, 1):
        logger.info(f"[{i}/{len(windows)}] Querying window {window_start} to {window_end}")
        item_ids = query_stac_items(
            args.stac_api_url,
            args.collection,
            window_start,
            window_end,
            date_field,
            target_storage_ref,
        )
        if not item_ids:
            logger.info("  Found 0 items — skipping")
            continue

        batches = chunk_item_ids(item_ids, args.max_batch_size)
        logger.info(f"  Found {len(item_ids)} items → {len(batches)} payload(s)")

        for batch in batches:
            # Space out submissions (webhook politeness); no leading/trailing sleep.
            if submissions > 0:
                time.sleep(args.delay)
            payload: dict[str, object] = {
                "action": "batch-change-storage-tier",
                "item_ids": batch,
                "collection": args.collection,
                "storage_class": args.storage_class,
                "stac_api_url": args.stac_api_url,
                "s3_endpoint": args.s3_endpoint,
                "pipeline_image_version": args.pipeline_image_version,
                "process_all_assets": str(args.process_all_assets).lower(),
            }
            success = submit_batch(args.webhook_url, payload, args.dry_run)
            submissions += 1
            if success:
                total_submitted += 1
            else:
                total_failed += 1

    logger.info(f"Done. Submitted: {total_submitted}, Failed: {total_failed}")


if __name__ == "__main__":
    main()
