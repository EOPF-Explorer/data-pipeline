"""Backfill migration: stamp ``properties.expires`` on existing STAC items.

Part of the expiry-driven retention design (coordination#183). New items get
``expires`` at registration; this migration backfills the ones already in the
catalogue so the cleanup cron can act on them.

Strict rule: ``expires = properties.datetime + DEFAULT_RETENTION_DAYS`` — i.e.
retention is measured from **acquisition** (data age), not from ``created``.
``created`` records when an item was *converted/registered*, and the catalogue
holds multiple bulk-conversion cohorts (e.g. items acquired the same week can
carry ``created`` dates months apart, and a re-conversion resets it), so a
``created``-based expiry is unstable and disconnected from data age. Acquisition
``datetime`` is stable across re-conversions. Because items older than the
retention window are then immediately past-expiry, the first cleanup runs drain
a backlog (bounded by the cron's ``--max-items``).

Demo-data protection is layered — **the exclude list is the real protection**:

- **Primary — the exclude list** (env ``EXPIRES_EXCLUDE_FILE``, the same
  ``scripts/demo_exclude_ids.txt`` that ``register_v1`` and the cleanup honor).
  Demo scenes are scattered across 2021→2026 and interleaved with pipeline data
  — several are acquired *after* any pipeline-era floor — so enumerating their
  ids is the only complete protection. Excluded ids are never stamped, carry no
  ``expires``, and are structurally undeletable. This check runs *before* the
  floor, so an excluded id is protected regardless of its acquisition date.
- **Secondary — the acquisition floor** (env ``EXPIRES_MIN_DATETIME``, an RFC3339
  timestamp or a bare ``YYYY-MM-DD`` date). Items acquired **before** the floor
  are skipped (``before_floor``) and never stamped. Its job is to bound the first
  cleanup's blast radius and coarsely cover the pre-pipeline tail — it does
  **not** protect a demo acquired on or after it; those must be in the exclude
  list.

A stale or mistyped exclude id would silently protect nothing, so ``report()``
warns when a configured exclude id matched zero items during a run.

Every outcome is tallied in ``SKIP_HISTOGRAM`` and logged, so a dry-run doubles
as the histogram the team reviews (``stamped`` vs ``before_floor`` etc.) before
committing to a floor.
"""

from __future__ import annotations

import copy
import logging
import os
import sys
from collections import Counter
from datetime import datetime, timedelta
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
    parse_stac_timestamp,
    resolve_exclude_ids,
)

logger = logging.getLogger(__name__)

# Outcome histogram (reason -> count), including "stamped". A dry-run's totals
# are the histogram the team reviews. Reset with reset_histogram().
SKIP_HISTOGRAM: Counter[str] = Counter()

# The configured exclude ids and the subset actually seen in the catalogue this
# run. The exclude list is the crown-jewel demo protection, so an id that matches
# nothing (typo, or a scene renamed/reconverted to a new id) would silently
# protect nothing. report() warns on the difference. Reset with the histogram.
_EXCLUDE_IDS: set[str] = set()
_MATCHED_EXCLUDE_IDS: set[str] = set()


def reset_histogram() -> None:
    SKIP_HISTOGRAM.clear()
    _EXCLUDE_IDS.clear()
    _MATCHED_EXCLUDE_IDS.clear()


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

    unmatched = _EXCLUDE_IDS - _MATCHED_EXCLUDE_IDS
    if unmatched:
        lines.append(
            f"  WARNING: {len(unmatched)} exclude-file id(s) matched no item "
            f"(typo, or stale/reconverted id?) — verify demo protection: "
            f"{', '.join(sorted(unmatched))}"
        )
    return "\n".join(lines)


def classify_and_stamp(
    item: dict[str, Any],
    *,
    retention_days: int,
    exclude_ids: set[str],
    min_datetime: datetime | None,
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

    # ``datetime`` is a mandatory STAC core field and S2 L2A always populates it.
    # A range-only item (``datetime=null`` with start/end_datetime) would skip
    # here and never expire — a cost leak, not a safety risk. None exist in S2.
    acquired = props.get("datetime")
    if not acquired:
        return None, "no_datetime"

    acquired_dt = parse_stac_timestamp(acquired)
    if min_datetime is not None and acquired_dt < min_datetime:
        return None, "before_floor"

    expires = acquired_dt + timedelta(days=retention_days)
    result = copy.deepcopy(item)
    result.setdefault("properties", {})["expires"] = format_expires(expires)
    extensions = result.setdefault("stac_extensions", [])
    if TIMESTAMPS_EXTENSION not in extensions:
        extensions.append(TIMESTAMPS_EXTENSION)
    return result, "stamped"


def _parse_floor(value: str) -> datetime:
    """Parse ``EXPIRES_MIN_DATETIME``. Accepts a full RFC3339 timestamp or a
    bare ``YYYY-MM-DD`` date (normalised to midnight UTC so a naive local-time
    interpretation can't shift the floor by the machine's offset)."""
    # A bare date has no time separator; RFC3339 allows lower- or upper-case "T".
    if "T" not in value.upper():
        value = f"{value}T00:00:00Z"
    # Annotate to launder the Any from the runtime-only s3_item_cleanup import
    # (mypy can't resolve it statically) into a concrete datetime.
    floor: datetime = parse_stac_timestamp(value)
    return floor


def _resolve_config() -> tuple[int, set[str], datetime | None]:
    retention_days = env_int("EXPIRES_RETENTION_DAYS", DEFAULT_RETENTION_DAYS)
    exclude_ids = resolve_exclude_ids()
    floor_env = os.getenv("EXPIRES_MIN_DATETIME")
    min_datetime = _parse_floor(floor_env) if floor_env else None
    return retention_days, exclude_ids, min_datetime


@migration(
    "stamp_expires",
    "Backfill properties.expires = datetime (acquisition) + retention (timestamps "
    "ext); skips already-stamped, excluded, and items acquired before the floor",
    reporter=report,
    reset=reset_histogram,
)
def stamp_expires(item: dict[str, Any]) -> dict[str, Any] | None:
    """Stamp ``expires`` on one item. Config from the environment
    (EXPIRES_RETENTION_DAYS, EXPIRES_EXCLUDE_FILE, EXPIRES_MIN_DATETIME)."""
    retention_days, exclude_ids, min_datetime = _resolve_config()
    # Track which exclude ids are actually present so report() can flag any that
    # matched nothing. Record on id-presence (not on the "excluded" outcome) so an
    # excluded item that is already stamped still counts as matched.
    _EXCLUDE_IDS.update(exclude_ids)
    item_id = item.get("id")
    if item_id in exclude_ids:
        _MATCHED_EXCLUDE_IDS.add(item_id)
    result, reason = classify_and_stamp(
        item,
        retention_days=retention_days,
        exclude_ids=exclude_ids,
        min_datetime=min_datetime,
    )
    SKIP_HISTOGRAM[reason] += 1
    if reason != "stamped":
        logger.info("stamp_expires skip: id=%s reason=%s", item_id, reason)
    return result
