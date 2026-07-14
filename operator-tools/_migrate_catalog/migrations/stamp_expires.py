"""Backfill migration: stamp ``properties.expires`` on existing STAC items.

Part of the expiry-driven retention design (coordination#183). New items get
``expires`` at registration; this migration backfills the ones already in the
catalogue so the cleanup cron can act on them.

Strict rule: ``expires = created + DEFAULT_RETENTION_DAYS``. Because pipeline
items older than the retention window are then immediately past-expiry, the
first cleanup runs drain a backlog (bounded by the cron's ``--max-items``).

Demo-data protection is layered:
- **Primary**: an explicit exclude list of item IDs (env ``EXPIRES_EXCLUDE_FILE``)
  that are never stamped — items with no ``expires`` are structurally
  undeletable.
- **Secondary**: an optional ``created − datetime`` gap check
  (env ``EXPIRES_DEMO_GAP_DAYS``). It is **disabled by default** because a naive
  threshold would wrongly skip bulk-converted historical data (which also has a
  large gap). Run a dry-run first, review the outcome histogram, and only then
  decide a threshold or (preferably) enumerate demo items in the exclude file.

Every outcome is tallied in ``SKIP_HISTOGRAM`` and logged, so a dry-run doubles
as the histogram the team reviews before committing to a threshold.
"""

from __future__ import annotations

import copy
import logging
import os
import sys
from collections import Counter
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from _migrate_catalog.migrations._registry import migration

if TYPE_CHECKING:
    from _migrate_catalog.types import MigrationResult

# scripts/ (baked into the pipeline image) is the single source for the retention
# constant, the timestamps extension URL, and the expires timestamp helpers.
# operator-tools/ is not on the path at runtime, so bootstrap it like
# manage_item.py does.
_scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
if str(_scripts_dir) not in sys.path:
    sys.path.insert(0, str(_scripts_dir))

from s3_item_cleanup import (  # noqa: E402
    DEFAULT_RETENTION_DAYS,
    TIMESTAMPS_EXTENSION,
    env_int,
    format_expires,
    load_exclude_ids,
    parse_stac_timestamp,
)

logger = logging.getLogger(__name__)

# Outcome histogram (reason -> count), including "stamped". A dry-run's totals
# are the histogram the team reviews. Reset with reset_histogram().
SKIP_HISTOGRAM: Counter[str] = Counter()


def reset_histogram() -> None:
    SKIP_HISTOGRAM.clear()


def report(result: MigrationResult) -> str:
    """Render the outcome histogram and cross-check it against the run's own
    counts, so the surfaced numbers can't silently drift from what happened.

    Only ``stamped`` items are written, so they end up either modified or failed;
    every other reason is a skip. If that identity doesn't hold — a stale count,
    a miscount, or items that errored before they could be classified — say so
    loudly rather than print a breakdown that looks authoritative.
    """
    lines = ["Outcome histogram:"]
    for reason in sorted(SKIP_HISTOGRAM):
        lines.append(f"  {reason:<16} {SKIP_HISTOGRAM[reason]}")

    stamped = SKIP_HISTOGRAM.get("stamped", 0)
    skips = sum(SKIP_HISTOGRAM.values()) - stamped
    reconciles = (
        stamped == result.items_modified + result.items_failed and skips == result.items_skipped
    )
    if not reconciles:
        lines.append(
            "  WARNING: histogram does not reconcile with run counts "
            f"(processed={result.items_processed}, modified={result.items_modified}, "
            f"skipped={result.items_skipped}, failed={result.items_failed})"
        )
    return "\n".join(lines)


def classify_and_stamp(
    item: dict[str, Any],
    *,
    retention_days: int,
    exclude_ids: set[str],
    gap_threshold_days: int | None,
) -> tuple[dict[str, Any] | None, str]:
    """Decide an item's fate and, if stamping, return a modified copy.

    Returns (modified_item_or_None, reason). ``None`` means skip (the migration
    framework treats that as unchanged). Never mutates the input.
    """
    props = item.get("properties", {})

    if props.get("expires"):
        return None, "already_stamped"
    if item.get("id") in exclude_ids:
        return None, "excluded"

    created = props.get("created")
    if not created:
        return None, "no_created"

    if gap_threshold_days is not None:
        acquired = props.get("datetime")
        if acquired:
            gap_days = (parse_stac_timestamp(created) - parse_stac_timestamp(acquired)).days
            if gap_days > gap_threshold_days:
                return None, "demo_gap"

    expires = parse_stac_timestamp(created) + timedelta(days=retention_days)
    result = copy.deepcopy(item)
    result.setdefault("properties", {})["expires"] = format_expires(expires)
    extensions = result.setdefault("stac_extensions", [])
    if TIMESTAMPS_EXTENSION not in extensions:
        extensions.append(TIMESTAMPS_EXTENSION)
    return result, "stamped"


def _resolve_config() -> tuple[int, set[str], int | None]:
    retention_days = env_int("EXPIRES_RETENTION_DAYS", DEFAULT_RETENTION_DAYS)
    exclude_ids = load_exclude_ids(os.getenv("EXPIRES_EXCLUDE_FILE"))
    gap_env = os.getenv("EXPIRES_DEMO_GAP_DAYS")
    gap_threshold_days = int(gap_env) if gap_env else None
    return retention_days, exclude_ids, gap_threshold_days


@migration(
    "stamp_expires",
    "Backfill properties.expires = created + retention (timestamps ext); "
    "skips already-stamped, excluded, and (optionally) large created-datetime gaps",
    reporter=report,
    reset=reset_histogram,
)
def stamp_expires(item: dict[str, Any]) -> dict[str, Any] | None:
    """Stamp ``expires`` on one item. Config from the environment
    (EXPIRES_RETENTION_DAYS, EXPIRES_EXCLUDE_FILE, EXPIRES_DEMO_GAP_DAYS)."""
    retention_days, exclude_ids, gap_threshold_days = _resolve_config()
    result, reason = classify_and_stamp(
        item,
        retention_days=retention_days,
        exclude_ids=exclude_ids,
        gap_threshold_days=gap_threshold_days,
    )
    SKIP_HISTOGRAM[reason] += 1
    if reason != "stamped":
        logger.info("stamp_expires skip: id=%s reason=%s", item.get("id"), reason)
    return result
