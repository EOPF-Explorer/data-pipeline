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

import copy
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from _migrate_catalog.migrations._expires_common import ExpiresRunState, resolve_config
from _migrate_catalog.migrations._registry import migration
from _migrate_catalog.types import MigrationResult

# scripts/ (baked into the pipeline image) is the single source for the retention
# constant, the timestamps extension URL, and the expires timestamp helpers.
# operator-tools/ is not on the path at runtime, so bootstrap it like
# manage_item.py does.
_scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
if str(_scripts_dir) not in sys.path:
    sys.path.insert(0, str(_scripts_dir))

from s3_item_cleanup import (  # noqa: E402
    TIMESTAMPS_EXTENSION,
    format_expires,
    parse_stac_timestamp,
)

_STATE = ExpiresRunState(written_reason="stamped")

# Outcome histogram (reason -> count), including "stamped". A dry-run's totals
# are the histogram the team reviews. Aliased (never rebound) so importers keep
# seeing the live counter across resets.
SKIP_HISTOGRAM = _STATE.histogram


def reset_histogram() -> None:
    _STATE.reset()


def report(result: MigrationResult) -> str:
    return _STATE.render_report(result)


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
    retention_days, exclude_ids, min_datetime = resolve_config()
    item_id = item.get("id")
    _STATE.observe(item_id, exclude_ids)
    result, reason = classify_and_stamp(
        item,
        retention_days=retention_days,
        exclude_ids=exclude_ids,
        min_datetime=min_datetime,
    )
    _STATE.record("stamp_expires", item_id, reason)
    return result
