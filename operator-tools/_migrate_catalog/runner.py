import copy
import json
import logging
import os
import sys
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import click
import pystac
import requests
from pystac_client import Client
from pystac_client.stac_api_io import StacApiIO
from urllib3.util.retry import Retry

from _migrate_catalog.types import MigrationFn, MigrationResult

# Add scripts directory to path for the shared OIDC auth helper
_scripts_dir = Path(__file__).parent.parent.parent / "scripts"
if str(_scripts_dir) not in sys.path:
    sys.path.insert(0, str(_scripts_dir))

import stac_auth  # noqa: E402

logger = logging.getLogger(__name__)

# Resilience for the search-pagination client on long backfills. Two failures
# seen live against the prod STAC API:
#   - no timeout -> a stalled socket hangs the whole run forever (4.5h wall /
#     26s CPU, never past "Found N items").
#   - weak default retries -> a transient ConnectionReset mid-pagination aborts
#     the entire run (crashed at ~20% of a 23k-item staging backfill).
# _resilient_stac_io gives the pagination client a per-request timeout plus
# urllib3 retries with exponential backoff on connection errors and 5xx, for GET
# and the POST /search pagination. urllib3 cannot always retry a reset mid-body,
# so this is best-effort; the migration is idempotent (skips already-stamped),
# so anything that still slips through is recovered by simply re-running.
# Override the timeout via STAC_HTTP_TIMEOUT.
_SEARCH_TIMEOUT = float(os.getenv("STAC_HTTP_TIMEOUT", "60"))


def _resilient_stac_io() -> StacApiIO:
    retry = Retry(
        total=8,
        backoff_factor=1.0,
        status_forcelist=(429, 502, 503, 504),
        allowed_methods=frozenset({"GET", "POST"}),
        raise_on_status=False,
    )
    return StacApiIO(timeout=_SEARCH_TIMEOUT, max_retries=retry)


def _transaction_body(item_dict: dict[str, Any]) -> dict[str, Any] | None:
    """A transaction-valid write body for a raw STAC-API item dict, or ``None`` if one can't be built.

    The GET/search representation omits nullable-but-required fields — notably
    ``properties.datetime`` on datacube items (null datetime) — which the transaction API
    rejects with 400. pystac re-materializes them (and preserves link order). Returns ``None`` for
    items pystac can't model (e.g. an asset with no href): the caller must report the item failed
    and write nothing — the item stays untouched in the catalogue (writes are a single atomic PUT),
    and a PUT the API is guaranteed to 400 would only burn write budget.
    """
    try:
        return pystac.Item.from_dict(item_dict).to_dict()
    except Exception:
        return None


def compose_migrations(fns: list[MigrationFn]) -> MigrationFn:
    """Return a single MigrationFn that applies all fns in a single pass."""

    def composed(item: dict[str, Any]) -> dict[str, Any] | None:
        current = item
        changed = False
        for fn in fns:
            result = fn(current)
            if result is not None:
                current = result
                changed = True
        return current if changed else None

    return composed


class STACMigrationRunner:
    def __init__(self, api_url: str, recovery_dir: Path | None = None) -> None:
        self.api_url = api_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        # Authenticate the migration writes (no-op when OIDC env is unset).
        self.session.auth = stac_auth.bearer_auth
        # requests.Session is not guaranteed thread-safe, so each thread that
        # writes gets its own (see _session). The constructing thread keeps
        # `self.session` — the sequential paths (clone_collection, and
        # run_migration at the default concurrency=1) then behave exactly as
        # they did before parallel writes existed.
        self._local = threading.local()
        self._local.session = self.session
        # Serialises the recovery-file append. A buffered O_APPEND write appears to
        # be atomic in practice, but that is a platform property, not a guarantee
        # (it can split on short writes, and does not hold on every filesystem).
        # This file is the only way back from a kill mid-DELETE-POST, so it does
        # not rest on that; the lock costs nothing next to the HTTP round-trip.
        self._recovery_lock = threading.Lock()
        self._recovery_file: Path | None = None
        if recovery_dir is not None:
            timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
            self._recovery_file = recovery_dir / f".migration_recovery_{timestamp}.jsonl"

    def _session(self) -> requests.Session:
        """Return the calling thread's Session, creating it on first use."""
        session: requests.Session | None = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers.update({"Content-Type": "application/json"})
            # Same auth as the constructing thread's session, or concurrent
            # writes would silently go unauthenticated.
            session.auth = stac_auth.bearer_auth
            self._local.session = session
        return session

    def _update_item(self, collection_id: str, item_id: str, item_dict: dict[str, Any]) -> None:
        """Replace one item with a single atomic PUT.

        This used to DELETE then POST, on the belief that "pgSTAC doesn't support
        PUT" — introduced 2026-03 with no recorded rationale and repeated at five
        sites. It is false: the API advertises the OGC Features transaction
        extension and PUT works. Verified live against prod on a throwaway
        collection: PUT's stored result is equivalent to DELETE+POST's on the same
        input, it 404s rather than creating a missing item, it is correct under 8
        concurrent workers, and it is 1.94x faster (412 vs 800 ms/item — one round
        trip instead of two).

        Speed is the lesser point. DELETE-then-POST left a window in which the item
        existed nowhere: a timeout, crash or kill between the two destroyed it for
        good, because a re-run cannot heal what /search no longer returns — only the
        recovery file could. Both halves of that were observed live (a kill tore 3
        items; write timeouts run ~1 per 10k). A PUT either replaces the item or
        leaves it untouched, so a torn item is impossible rather than merely rare.

        The recovery file is kept as a record of what was written. Note it holds the
        POST-update content, so it documents rather than undoes.
        """
        if self._recovery_file is not None:
            with self._recovery_lock, open(self._recovery_file, "a") as f:
                f.write(json.dumps(item_dict) + "\n")
        resp = self._session().put(
            f"{self.api_url}/collections/{collection_id}/items/{item_id}",
            json=item_dict,
            timeout=30,
        )
        resp.raise_for_status()

    def _safe_update(self, task: tuple[str, str, dict[str, Any]]) -> tuple[str, str | None]:
        """Run one write, converting failure into a value.

        The ONLY thing dispatched to the worker pool: it does the network I/O and
        touches no migration state, so the caller can tally outcomes single-threaded.
        Returns (item_id, None) on success or (item_id, error_message) on failure —
        one item's failure must never abort the rest of the batch.
        """
        collection_id, item_id, item_dict = task
        try:
            self._update_item(collection_id, item_id, item_dict)
            return item_id, None
        except Exception as e:
            return item_id, str(e)

    def run_migration(
        self,
        collection_id: str,
        migration_fn: MigrationFn,
        migration_name: str,
        dry_run: bool = False,
        page_size: int = 100,
        ids: list[str] | None = None,
        concurrency: int = 1,
        max_consecutive_failures: int = 25,
        max_writes: int | None = None,
    ) -> MigrationResult:
        """Apply migration_fn to every item in collection_id.

        `ids` restricts the run to specific item ids (the single-tile canary path)
        through the exact same code path, recovery file, and history as a full run.

        The per-item DELETE+POST is a network round-trip, so a sequential run is
        latency-bound (~5-6k items/hour against prod). `concurrency` > 1 dispatches
        those writes to a thread pool — the work is I/O-bound, so threads scale it.

        Only the WRITES are parallelized. `migration_fn` keeps non-thread-safe
        module state (stamp_expires' outcome histogram and exclude-id sets, whose
        `+=` is not atomic) and its reporter reconciles that histogram against these
        counters, so it runs on the calling thread and the tally stays lock-free.
        Writes are bounded to one page at a time, capping in-flight requests.

        concurrency=1 (the default) bypasses the pool entirely and is the exact
        sequential path used before this existed, so no other migration silently
        gains write load against prod pgSTAC.

        max_consecutive_failures stops a run whose writes are failing wholesale
        (see the circuit breaker below); 0 disables it and restores run-to-completion.
        Sets `result.aborted`.

        max_writes bounds a run to N *attempted* writes and then stops cleanly,
        setting `result.reached_max_writes`. A failed write still counts against the
        budget: its DELETE may already have landed, so it has spent real blast
        radius, and counting only successes would keep retrying past N on exactly
        the failing run you most want bounded. Skips are free (the head of a
        collection is typically already migrated). Use it for a bounded run rather than
        killing the process: the unit of work is a non-atomic DELETE-then-POST, so
        a kill can leave items deleted-but-not-restored, and a signal-based stop is
        unreliable anyway (a process backgrounded from a non-interactive shell
        inherits SIGINT=SIG_IGN, and CPython then never installs its handler). The
        budget is checked *before* migration_fn runs, so an item that is not written
        is never counted as processed and the histogram still reconciles.
        """
        if concurrency < 1:
            raise ValueError(f"concurrency must be >= 1, got {concurrency}")
        if max_consecutive_failures < 0:
            raise ValueError(
                f"max_consecutive_failures must be >= 0, got {max_consecutive_failures}"
            )
        if max_writes is not None and max_writes < 1:
            raise ValueError(f"max_writes must be >= 1 or None, got {max_writes}")

        started_at = datetime.now(UTC).isoformat()
        result = MigrationResult(
            migration_name=migration_name,
            collection_id=collection_id,
            started_at=started_at,
            completed_at="",
            items_processed=0,
            items_modified=0,
            items_skipped=0,
            items_failed=0,
            dry_run=dry_run,
            errors=[],
        )

        catalog = Client.open(self.api_url, stac_io=_resilient_stac_io())
        # `ids` restricts the run to specific items (the canary path) via the same code path,
        # recovery JSONL, and history as the full run. Omitted from the call when unset so the
        # full-collection search stays byte-identical (backcompat).
        if ids:
            search = catalog.search(
                collections=[collection_id], ids=ids, max_items=None, limit=page_size
            )
        else:
            search = catalog.search(collections=[collection_id], max_items=None, limit=page_size)

        total = search.matched()
        if total is not None:
            click.echo(
                f"Found {total} items in '{collection_id}'."
                f" Processing{' (dry run)' if dry_run else ''}..."
            )
        else:
            click.echo(
                f"Processing items from '{collection_id}'{' (dry run)' if dry_run else ''}..."
            )

        pool = ThreadPoolExecutor(max_workers=concurrency) if concurrency > 1 else None
        consecutive_failures = 0
        budget_used = 0  # items modified (or, in a dry run, that would be)
        try:
            with click.progressbar(length=total, show_pos=True, show_percent=True) as bar:
                # Iterate raw item-dict pages, not pystac Item objects: some live items carry an
                # asset with no href (e.g. s1-rtc-30TWQ) that pystac's Item.from_dict rejects, and
                # one such item must not abort the whole run. The migration functions operate on
                # dicts anyway; _transaction_body re-materializes each write body via pystac.
                for page in search.pages_as_dicts():
                    pending: list[tuple[str, str, dict[str, Any]]] = []

                    # Classify the page on this thread; queue the writes it earned.
                    for item_dict in page.get("features", []):
                        # Check the budget BEFORE classifying. Trimming the batch
                        # afterwards would leave items counted as processed but never
                        # written, so neither the tally nor stamp_expires' histogram
                        # would reconcile.
                        # The flag is only raised once a further item is *inspected*,
                        # so a budget landing exactly on a page boundary costs one
                        # extra page fetch (no extra writes). Harmless — not a bug.
                        if max_writes is not None and budget_used >= max_writes:
                            result.reached_max_writes = True
                            break

                        item_id = item_dict.get("id", "unknown")
                        result.items_processed += 1
                        try:
                            modified = migration_fn(item_dict)
                        except Exception as e:
                            result.items_failed += 1
                            result.errors.append({"item_id": item_id, "error": str(e)})
                            bar.update(1)
                            continue
                        if modified is None:
                            result.items_skipped += 1  # skips are free: no budget spent
                            bar.update(1)
                        elif dry_run:
                            result.items_modified += 1
                            budget_used += 1
                            bar.update(1)
                        else:
                            body = _transaction_body(modified)
                            if body is None:
                                # Can't build a transaction-valid write body (pystac can't
                                # model the item) — report it failed, write nothing. No
                                # budget spent: no write was attempted.
                                result.items_failed += 1
                                result.errors.append(
                                    {
                                        "item_id": item_id,
                                        "error": "cannot build a transaction-valid body "
                                        "(pystac can't model it); skipped without writing",
                                    }
                                )
                                bar.update(1)
                            else:
                                pending.append((collection_id, item_id, body))
                                budget_used += 1

                    # Then run this page's writes and tally them back here.
                    writes: Iterator[tuple[str, str | None]] = (
                        pool.map(self._safe_update, pending)
                        if pool is not None
                        else map(self._safe_update, pending)
                    )
                    for item_id, error in writes:
                        if error is None:
                            result.items_modified += 1
                            consecutive_failures = 0
                        else:
                            result.items_failed += 1
                            consecutive_failures += 1
                            result.errors.append({"item_id": item_id, "error": error})
                        bar.update(1)

                    # Circuit breaker, checked once per page. A write is DELETE-then-POST,
                    # so a POST that fails after its DELETE landed removes the item from
                    # the catalogue, and a re-run cannot heal it (search no longer returns
                    # it) — only the recovery file can. If the API starts refusing writes
                    # wholesale, running to completion would empty the collection. Stop
                    # instead, bounding the loss to roughly one page past the trip point.
                    if (
                        max_consecutive_failures
                        and consecutive_failures >= max_consecutive_failures
                    ):
                        result.aborted = True
                        break

                    # Budget spent: stop between pages, cleanly. No signal, no kill,
                    # so no item can be left torn between its DELETE and its POST.
                    if result.reached_max_writes:
                        break
        finally:
            if pool is not None:
                # cancel_futures: on Ctrl-C, drop writes still queued rather than
                # pushing a page's worth into prod after the operator asked to stop.
                # Without this the behaviour is not deterministic: it depends on
                # whether the interrupt lands inside pool.map's generator (which
                # cancels) or outside it (which drains).
                # wait: let in-flight writes finish their DELETE+POST, so a stop
                # leaves no item torn between the two.
                pool.shutdown(wait=True, cancel_futures=True)

        result.completed_at = datetime.now(UTC).isoformat()
        return result

    def _fetch_existing_ids(self, collection_id: str, page_size: int) -> set[str]:
        """Return the set of item IDs already present in collection_id."""
        catalog = Client.open(self.api_url, stac_io=_resilient_stac_io())
        search = catalog.search(
            collections=[collection_id],
            max_items=None,
            limit=page_size,
        )
        return {item.id for page in search.pages() for item in page.items}

    def clone_collection(
        self, source_id: str, target_id: str, page_size: int = 100, resume: bool = False
    ) -> tuple[int, int, int]:
        """Clone collection metadata and all items.

        Returns (items_copied, items_skipped, items_failed).
        When resume=True, existing target item IDs are pre-fetched into a set upfront so that
        already-copied items are skipped without issuing a POST at all.
        """
        resp = self.session.get(f"{self.api_url}/collections/{source_id}", timeout=30)
        resp.raise_for_status()
        collection_data: dict[str, Any] = resp.json()

        collection_data["id"] = target_id
        resp = self.session.post(f"{self.api_url}/collections", json=collection_data, timeout=30)
        if resume and resp.status_code == 409:
            click.echo(f"Collection '{target_id}' already exists, resuming copy...")
        else:
            resp.raise_for_status()
            click.echo(f"Created collection '{target_id}'")

        existing_ids: set[str] = set()
        if resume:
            click.echo(f"Fetching existing item IDs from '{target_id}'...")
            existing_ids = self._fetch_existing_ids(target_id, page_size)
            click.echo(f"Found {len(existing_ids)} items already in '{target_id}', skipping them.")

        catalog = Client.open(self.api_url, stac_io=_resilient_stac_io())
        search = catalog.search(collections=[source_id], max_items=None, limit=page_size)

        total = search.matched()
        if total is not None:
            click.echo(f"Copying {total} items from '{source_id}' to '{target_id}'...")
        else:
            click.echo(f"Copying items from '{source_id}' to '{target_id}'...")

        copied = 0
        skipped = 0
        failed = 0
        pending_bar = 0
        with click.progressbar(
            length=total, show_pos=True, show_percent=True, update_min_steps=100
        ) as bar:
            for page in search.pages():
                for item_dict in (item.to_dict() for item in page.items):
                    item_id = item_dict.get("id", "unknown")
                    try:
                        if item_id in existing_ids:
                            skipped += 1
                        else:
                            item_copy = copy.deepcopy(item_dict)
                            item_copy["collection"] = target_id
                            resp = self.session.post(
                                f"{self.api_url}/collections/{target_id}/items",
                                json=item_copy,
                                timeout=30,
                            )
                            resp.raise_for_status()
                            copied += 1
                    except Exception as e:
                        logger.warning("Failed to copy item %s: %s", item_id, e)
                        failed += 1
                    pending_bar += 1
                    if pending_bar >= 100:
                        bar.update(pending_bar)
                        pending_bar = 0
            if pending_bar:
                bar.update(pending_bar)

        return copied, skipped, failed
