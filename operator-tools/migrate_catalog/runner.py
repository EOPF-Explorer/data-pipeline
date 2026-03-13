import copy
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import click
import requests
from pystac_client import Client

from migrate_catalog.types import MigrationFn, MigrationResult

logger = logging.getLogger(__name__)


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
        self._recovery_file: Path | None = None
        if recovery_dir is not None:
            timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
            self._recovery_file = recovery_dir / f".migration_recovery_{timestamp}.jsonl"

    def get_items(self, collection_id: str) -> list[dict[str, Any]]:
        catalog = Client.open(self.api_url)
        search = catalog.search(collections=[collection_id], max_items=None)
        return [item.to_dict() for item in search.items()]

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

        click.echo(f"Fetching items from collection '{collection_id}'...")
        items = self.get_items(collection_id)
        click.echo(f"Found {len(items)} items. Processing{' (dry run)' if dry_run else ''}...")

        with click.progressbar(items, show_pos=True, show_percent=True) as bar:
            for item_dict in bar:
                item_id = item_dict.get("id", "unknown")
                result.items_processed += 1
                try:
                    modified = migration_fn(item_dict)
                    if modified is None:
                        result.items_skipped += 1
                    elif dry_run:
                        result.items_modified += 1
                    else:
                        self._update_item(collection_id, item_id, modified)
                        result.items_modified += 1
                except Exception as e:
                    result.items_failed += 1
                    result.errors.append({"item_id": item_id, "error": str(e)})

        result.completed_at = datetime.now(UTC).isoformat()
        return result

    def clone_collection(self, source_id: str, target_id: str) -> tuple[int, int]:
        """Clone collection metadata and all items. Returns (items_copied, items_failed)."""
        resp = self.session.get(f"{self.api_url}/collections/{source_id}", timeout=30)
        resp.raise_for_status()
        collection_data: dict[str, Any] = resp.json()

        collection_data["id"] = target_id
        resp = self.session.post(f"{self.api_url}/collections", json=collection_data, timeout=30)
        resp.raise_for_status()
        click.echo(f"Created collection '{target_id}'")

        click.echo(f"Fetching items from '{source_id}'...")
        items = self.get_items(source_id)
        click.echo(f"Copying {len(items)} items to '{target_id}'...")

        copied = 0
        failed = 0
        with click.progressbar(items, show_pos=True, show_percent=True) as bar:
            for item_dict in bar:
                item_id = item_dict.get("id", "unknown")
                try:
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

        return copied, failed
