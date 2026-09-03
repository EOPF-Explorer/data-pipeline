"""Re-stamp migration: shorten ``properties.expires`` to a new retention.

``stamp_expires`` backfills items that have no ``expires`` and deliberately skips
``already_stamped`` ones, so it cannot express a *policy change*: when the
retention window shrinks (coordination#178 — S2 moving from 183 d to a shorter
window), the ~112k items already carrying a 183 d ``expires`` would keep it and
age out on the old schedule for another three months. This migration rewrites
them.

One rule makes it safe to run repeatedly and safe to run with the wrong number:

**it only ever moves ``expires`` earlier.** A recomputed value that is later than
(or equal to) the stored one is a skip (``already_shorter``), never a write. So a
run with too *long* a retention is a no-op rather than a silent extension of the
catalogue's life, and re-running after the cleanup cron has started deleting can
never resurrect an item's lifetime.

The other rules mirror ``stamp_expires``, deliberately:

- ``expires = properties.datetime (acquisition) + retention`` — same acquisition
  basis, so a re-stamp is idempotent with the backfill and stable across
  re-conversions (``created`` is not).
- The **exclude list is the crown-jewel demo protection** (``EXPIRES_EXCLUDE_FILE``,
  default ``scripts/demo_exclude_ids.txt``): excluded ids are never written,
  checked before anything else, and ``report()`` warns when a listed id matched
  no item during a *complete* scan.
- The optional acquisition floor (``EXPIRES_MIN_DATETIME``) skips items acquired
  before it (``before_floor``).

Items with **no** ``expires`` are skipped (``not_stamped``), not stamped: an
unstamped item is deliberately undeletable (a demo, or something the backfill's
floor protected), and quietly stamping it here would turn a shortening run into
a fresh deletion authorisation. Use ``stamp_expires`` for those.

Bound a prod run with ``--dry-run`` first, then ``--max-writes N``; the histogram
(``restamped`` / ``already_shorter`` / ``excluded`` / ``not_stamped`` /
``before_floor`` / ``no_datetime`` / ``bad_expires``) is what the team reviews
before committing. Deletion follows from ``expires``, and the bucket has no
versioning, so this dry-run is the last cheap checkpoint.
"""

import copy
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from _migrate_catalog.migrations._expires_common import ExpiresRunState, resolve_config
from _migrate_catalog.migrations._registry import migration
from _migrate_catalog.types import MigrationResult

# scripts/ (baked into the pipeline image) is the single source for the timestamps
# extension URL and the expires timestamp helpers. operator-tools/ is not on the
# path at runtime, so bootstrap it like manage_item.py does.
_scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
if str(_scripts_dir) not in sys.path:
    sys.path.insert(0, str(_scripts_dir))

from s3_item_cleanup import (  # noqa: E402
    TIMESTAMPS_EXTENSION,
    format_expires,
    parse_stac_timestamp,
)

_STATE = ExpiresRunState(written_reason="restamped")

# Outcome histogram (reason -> count), including "restamped". Aliased (never
# rebound) so importers keep seeing the live counter across resets.
SKIP_HISTOGRAM = _STATE.histogram


def reset_histogram() -> None:
    _STATE.reset()


def report(result: MigrationResult) -> str:
    return _STATE.render_report(result)


def classify_and_restamp(
    item: dict[str, Any],
    *,
    retention_days: int,
    exclude_ids: set[str],
    min_datetime: datetime | None,
) -> tuple[dict[str, Any] | None, str]:
    """Decide an item's fate and, if re-stamping, return a modified copy.

    Returns (modified_item_or_None, reason). ``None`` means skip (the migration
    framework treats that as unchanged). Never mutates the input.
    """
    props = item.get("properties", {})

    # Demo protection first: an excluded id is never written, whatever else is
    # true of it.
    if item.get("id") in exclude_ids:
        return None, "excluded"

    current = props.get("expires")
    if not current:
        # Never deletable today; stamping it here would be a new authorisation.
        return None, "not_stamped"

    acquired = props.get("datetime")
    if not acquired:
        return None, "no_datetime"

    acquired_dt = parse_stac_timestamp(acquired)
    if min_datetime is not None and acquired_dt < min_datetime:
        return None, "before_floor"

    try:
        current_dt = parse_stac_timestamp(current)
    except ValueError:
        # An unparseable expires is not ours to interpret: pgstac compares it as
        # a string, so rewriting on a guess could move an item's deletion in
        # either direction. Skip loudly instead.
        return None, "bad_expires"

    new_dt = acquired_dt + timedelta(days=retention_days)
    if new_dt >= current_dt:
        # Never extend a lifetime — see the module docstring.
        return None, "already_shorter"

    result = copy.deepcopy(item)
    result.setdefault("properties", {})["expires"] = format_expires(new_dt)
    extensions = result.setdefault("stac_extensions", [])
    if TIMESTAMPS_EXTENSION not in extensions:
        extensions.append(TIMESTAMPS_EXTENSION)
    return result, "restamped"


@migration(
    "restamp_expires",
    "Shorten properties.expires to datetime (acquisition) + retention; writes "
    "only when the new value is EARLIER, never extends, skips excluded and "
    "unstamped items",
    reporter=report,
    reset=reset_histogram,
)
def restamp_expires(item: dict[str, Any]) -> dict[str, Any] | None:
    """Re-stamp ``expires`` on one item. Config from the environment
    (EXPIRES_RETENTION_DAYS, EXPIRES_EXCLUDE_FILE, EXPIRES_MIN_DATETIME)."""
    retention_days, exclude_ids, min_datetime = resolve_config()
    item_id = item.get("id")
    _STATE.observe(item_id, exclude_ids)
    result, reason = classify_and_restamp(
        item,
        retention_days=retention_days,
        exclude_ids=exclude_ids,
        min_datetime=min_datetime,
    )
    _STATE.record("restamp_expires", item_id, reason)
    return result
