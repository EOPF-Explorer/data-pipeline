"""Shared configuration and per-run state for the ``expires`` migrations.

``stamp_expires`` (backfill) and ``restamp_expires`` (shorten to a new retention)
classify items into an outcome histogram and both depend on the same crown-jewel
demo protection, so they share one implementation of the tally, the exclude-id
match tracking and the end-of-run report. Duplicating the report would let the
two drift — and the subtle part (a bounded run must NOT cry wolf about exclude
ids it never reached) is exactly the part that must never drift.

Each migration module owns one ``ExpiresRunState`` and names the reason that
means "this item was written" (``stamped`` / ``restamped``), which is what the
report reconciles against the runner's own counters.

Both also read the same environment: ``EXPIRES_RETENTION_DAYS``,
``EXPIRES_EXCLUDE_FILE`` and ``EXPIRES_MIN_DATETIME``.
"""

import logging
import os
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from _migrate_catalog.types import MigrationResult

# scripts/ (baked into the pipeline image) is the single source for the retention
# constant and the expires timestamp helpers. operator-tools/ is not on the path
# at runtime, so bootstrap it like manage_item.py does.
_scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
if str(_scripts_dir) not in sys.path:
    sys.path.insert(0, str(_scripts_dir))

from s3_item_cleanup import (  # noqa: E402
    DEFAULT_RETENTION_DAYS,
    env_int,
    parse_stac_timestamp,
    resolve_exclude_ids,
)

logger = logging.getLogger(__name__)


def parse_floor(value: str) -> datetime:
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


def resolve_config() -> tuple[int, set[str], datetime | None]:
    """Read (retention_days, exclude_ids, acquisition floor) from the environment."""
    retention_days = env_int("EXPIRES_RETENTION_DAYS", DEFAULT_RETENTION_DAYS)
    exclude_ids = resolve_exclude_ids()
    floor_env = os.getenv("EXPIRES_MIN_DATETIME")
    min_datetime = parse_floor(floor_env) if floor_env else None
    return retention_days, exclude_ids, min_datetime


@dataclass
class ExpiresRunState:
    """Outcome histogram + exclude-id bookkeeping for one migration run."""

    # The histogram key meaning "written" (every other reason is a skip).
    written_reason: str
    # reason -> count, including written_reason. A dry-run's totals are the
    # histogram the team reviews before committing.
    histogram: Counter[str] = field(default_factory=Counter)
    # The configured exclude ids, and the subset actually seen in the catalogue
    # this run. The exclude list is the crown-jewel demo protection, so an id
    # that matches nothing (typo, or a scene renamed/reconverted to a new id)
    # would silently protect nothing — report() surfaces the difference.
    exclude_ids: set[str] = field(default_factory=set)
    matched_exclude_ids: set[str] = field(default_factory=set)

    def reset(self) -> None:
        """Clear everything between runs. Mutates in place so module-level
        aliases (``SKIP_HISTOGRAM``) stay bound to the live counter."""
        self.histogram.clear()
        self.exclude_ids.clear()
        self.matched_exclude_ids.clear()

    def observe(self, item_id: str | None, exclude_ids: set[str]) -> None:
        """Record the configured exclude ids and whether this item is one of
        them. Tracked on id-presence rather than on the "excluded" outcome, so an
        excluded item that some earlier check short-circuits still counts as
        matched."""
        self.exclude_ids.update(exclude_ids)
        if item_id is not None and item_id in exclude_ids:
            self.matched_exclude_ids.add(item_id)

    def record(self, migration_name: str, item_id: str | None, reason: str) -> None:
        self.histogram[reason] += 1
        if reason != self.written_reason:
            logger.info("%s skip: id=%s reason=%s", migration_name, item_id, reason)

    def render_report(self, result: MigrationResult) -> str:
        """Render the outcome histogram and cross-check it against the run's own
        counts, so the surfaced numbers can't silently drift from what happened.

        Only ``written_reason`` items are written, so they end up either modified
        or failed; every other reason is a skip. If that identity doesn't hold —
        a stale count, a miscount, or items that errored before they could be
        classified — say so loudly rather than print a breakdown that looks
        authoritative.
        """
        lines = ["Outcome histogram:"]
        for reason in sorted(self.histogram):
            lines.append(f"  {reason:<16} {self.histogram[reason]}")

        written = self.histogram.get(self.written_reason, 0)
        skips = sum(self.histogram.values()) - written
        reconciles = (
            written == result.items_modified + result.items_failed and skips == result.items_skipped
        )
        if not reconciles:
            lines.append(
                "  WARNING: histogram does not reconcile with run counts "
                f"(processed={result.items_processed}, modified={result.items_modified}, "
                f"skipped={result.items_skipped}, failed={result.items_failed})"
            )

        unmatched = self.exclude_ids - self.matched_exclude_ids
        if unmatched:
            # Only a run that scanned the WHOLE collection can conclude an id
            # matches nothing. A run that stopped early (--max-writes, or the
            # circuit breaker) simply never reached the rest, so raising the alarm
            # here would fire on every bounded chunk — and an alarm that cries wolf
            # on routine runs is one operators learn to ignore, which is how a
            # genuinely stale id would slip through. Surface the ids either way;
            # only call it a protection failure when the scan was complete.
            partial = result.reached_max_writes or result.aborted
            if partial:
                lines.append(
                    f"  Note: {len(unmatched)} exclude-file id(s) not seen — this run "
                    f"stopped early, so the scan was partial and this is NOT a protection "
                    f"failure. Confirm with a full dry-run. Not seen: "
                    f"{', '.join(sorted(unmatched))}"
                )
            else:
                lines.append(
                    f"  WARNING: {len(unmatched)} exclude-file id(s) matched no item "
                    f"(typo, or stale/reconverted id?) — verify demo protection: "
                    f"{', '.join(sorted(unmatched))}"
                )
        return "\n".join(lines)
