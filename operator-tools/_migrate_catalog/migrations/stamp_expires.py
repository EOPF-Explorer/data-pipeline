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
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from _migrate_catalog.migrations._registry import migration

# scripts/ (baked into the pipeline image) is the single source for the
# retention constant and the timestamps extension URL. operator-tools/ is not on
# the path at runtime, so bootstrap it the way manage_item.py does.
_scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
if str(_scripts_dir) not in sys.path:
    sys.path.insert(0, str(_scripts_dir))

from s3_item_cleanup import DEFAULT_RETENTION_DAYS, TIMESTAMPS_EXTENSION  # noqa: E402

logger = logging.getLogger(__name__)

ISO_Z = "%Y-%m-%dT%H:%M:%SZ"

# Outcome histogram (reason -> count), including "stamped". A dry-run's totals
# are the histogram the team reviews. Reset with reset_histogram().
SKIP_HISTOGRAM: Counter[str] = Counter()


def reset_histogram() -> None:
    SKIP_HISTOGRAM.clear()


def _parse(value: str) -> datetime:
    """Parse a STAC RFC3339 timestamp (``Z`` or ``+00:00``) to aware UTC."""
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _load_exclude_ids(path: str | None) -> set[str]:
    """Newline-delimited item-ID denylist; blank lines and ``#`` comments
    ignored. Same format as the cleanup script's ``--exclude-file``."""
    if not path:
        return set()
    ids: set[str] = set()
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                ids.add(stripped)
    return ids


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
            gap_days = (_parse(created) - _parse(acquired)).days
            if gap_days > gap_threshold_days:
                return None, "demo_gap"

    expires = _parse(created) + timedelta(days=retention_days)
    result = copy.deepcopy(item)
    result.setdefault("properties", {})["expires"] = expires.strftime(ISO_Z)
    extensions = result.setdefault("stac_extensions", [])
    if TIMESTAMPS_EXTENSION not in extensions:
        extensions.append(TIMESTAMPS_EXTENSION)
    return result, "stamped"


def _resolve_config() -> tuple[int, set[str], int | None]:
    retention_days = int(os.getenv("EXPIRES_RETENTION_DAYS", str(DEFAULT_RETENTION_DAYS)))
    exclude_ids = _load_exclude_ids(os.getenv("EXPIRES_EXCLUDE_FILE"))
    gap_env = os.getenv("EXPIRES_DEMO_GAP_DAYS")
    gap_threshold_days = int(gap_env) if gap_env else None
    return retention_days, exclude_ids, gap_threshold_days


@migration(
    "stamp_expires",
    "Backfill properties.expires = created + retention (timestamps ext); "
    "skips already-stamped, excluded, and (optionally) large created-datetime gaps",
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
