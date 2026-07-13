import copy
import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import click
import pystac
import requests
from pystac_client import Client

from _migrate_catalog.types import MigrationFn, MigrationResult

# Add scripts directory to path for the shared OIDC auth helper
_scripts_dir = Path(__file__).parent.parent.parent / "scripts"
if str(_scripts_dir) not in sys.path:
    sys.path.insert(0, str(_scripts_dir))

import stac_auth  # noqa: E402

logger = logging.getLogger(__name__)


def _transaction_body(item_dict: dict[str, Any]) -> dict[str, Any] | None:
    """A transaction-valid POST body for a raw STAC-API item dict, or ``None`` if one can't be built.

    The GET/search representation omits nullable-but-required fields — notably
    ``properties.datetime`` on datacube items (null datetime) — which the transaction POST
    rejects with 400. pystac re-materializes them (and preserves link order). Returns ``None`` for
    items pystac can't model (e.g. an asset with no href): such an item can't be safely round-tripped
    through the transaction API, so the caller must skip it WITHOUT deleting — a delete-then-failed
    POST would lose the item.
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
        # Authenticate the delete/post migration writes (no-op when OIDC env is unset).
        self.session.auth = stac_auth.bearer_auth
        self._recovery_file: Path | None = None
        if recovery_dir is not None:
            timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
            self._recovery_file = recovery_dir / f".migration_recovery_{timestamp}.jsonl"

    def _update_item(self, collection_id: str, item_id: str, item_dict: dict[str, Any]) -> None:
        """Delete then POST (pgSTAC doesn't support PUT). Logs item to recovery file before delete."""
        if self._recovery_file is not None:
            with open(self._recovery_file, "a") as f:
                f.write(json.dumps(item_dict) + "\n")
        self.session.delete(
            f"{self.api_url}/collections/{collection_id}/items/{item_id}", timeout=30
        )
        resp = self.session.post(
            f"{self.api_url}/collections/{collection_id}/items",
            json=item_dict,
            timeout=30,
        )
        resp.raise_for_status()

    def run_migration(
        self,
        collection_id: str,
        migration_fn: MigrationFn,
        migration_name: str,
        dry_run: bool = False,
        page_size: int = 100,
        ids: list[str] | None = None,
    ) -> MigrationResult:
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

        catalog = Client.open(self.api_url)
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

        # Iterate raw item dicts, not pystac Item objects: some live items carry an asset with no
        # href (e.g. s1-rtc-30TWQ) that pystac's Item.from_dict rejects, and one such item must not
        # abort the whole run. The migration functions operate on dicts anyway.
        with click.progressbar(length=total, show_pos=True, show_percent=True) as bar:
            for item_dict in search.items_as_dicts():
                item_id = item_dict.get("id", "unknown")
                result.items_processed += 1
                try:
                    modified = migration_fn(item_dict)
                    if modified is None:
                        result.items_skipped += 1
                    elif dry_run:
                        result.items_modified += 1
                    else:
                        body = _transaction_body(modified)
                        if body is None:
                            # Can't build a valid POST body — skip WITHOUT deleting (a
                            # delete-then-failed-POST would lose the item).
                            result.items_failed += 1
                            result.errors.append(
                                {
                                    "item_id": item_id,
                                    "error": "cannot build a transaction-valid POST body "
                                    "(pystac can't model it); skipped without deleting",
                                }
                            )
                        else:
                            self._update_item(collection_id, item_id, body)
                            result.items_modified += 1
                except Exception as e:
                    result.items_failed += 1
                    result.errors.append({"item_id": item_id, "error": str(e)})
                bar.update(1)

        result.completed_at = datetime.now(UTC).isoformat()
        return result

    def _fetch_existing_ids(self, collection_id: str, page_size: int) -> set[str]:
        """Return the set of item IDs already present in collection_id."""
        catalog = Client.open(self.api_url)
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

        catalog = Client.open(self.api_url)
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
