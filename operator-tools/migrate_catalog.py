#!/usr/bin/env python3
"""
STAC Catalogue Migration Tool

Reusable framework for catalogue-wide STAC item fixes with history tracking.

Usage:
    python migrate_catalog.py list
    python migrate_catalog.py run fix_url_encoding sentinel-2-l2a --dry-run
    python migrate_catalog.py run fix_url_encoding sentinel-2-l2a
    python migrate_catalog.py verify fix_url_encoding sentinel-2-l2a
    python migrate_catalog.py clone sentinel-2-l2a sentinel-2-l2a-backup-20260312
    python migrate_catalog.py history
"""

import copy
import dataclasses
import json
import logging
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import click
import requests
from pystac_client import Client

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

_DEFAULT_API_URL = "https://api.explorer.eopf.copernicus.eu/stac"
_DEFAULT_HISTORY_FILE = Path(__file__).parent / ".migration_history.json"


# === Migration Functions ===
# Each function takes a raw item dict and returns:
#   - Modified dict if changes are needed
#   - None if item already conforms (no change needed)


def fix_url_encoding(item: dict[str, Any]) -> dict[str, Any] | None:
    """Replace + with %20 in query string portions of asset and link hrefs (RFC 3986)."""
    changed = False
    item = copy.deepcopy(item)

    def _fix(href: str) -> str:
        nonlocal changed
        if "?" not in href:
            return href
        path, query = href.split("?", 1)
        if "+" not in query:
            return href
        changed = True
        return f"{path}?{query.replace('+', '%20')}"

    for asset in item.get("assets", {}).values():
        if isinstance(asset.get("href"), str):
            asset["href"] = _fix(asset["href"])

    for link in item.get("links", []):
        if isinstance(link.get("href"), str):
            link["href"] = _fix(link["href"])

    return item if changed else None


def fix_zarr_media_type(item: dict[str, Any]) -> dict[str, Any] | None:
    """Replace application/vnd+zarr with application/vnd.zarr in asset type fields."""
    changed = False
    item = copy.deepcopy(item)

    for asset in item.get("assets", {}).values():
        media_type = asset.get("type", "")
        if "application/vnd+zarr" in media_type:
            asset["type"] = media_type.replace("application/vnd+zarr", "application/vnd.zarr")
            changed = True

    return item if changed else None


MigrationFn = Callable[[dict[str, Any]], dict[str, Any] | None]

MIGRATIONS: dict[str, tuple[MigrationFn, str]] = {
    "fix_url_encoding": (
        fix_url_encoding,
        "Replace + with %20 in asset/link href query strings (RFC 3986 compliance)",
    ),
    "fix_zarr_media_type": (
        fix_zarr_media_type,
        "Replace application/vnd+zarr with application/vnd.zarr (MIME convention)",
    ),
}


# === Result Tracking ===


@dataclasses.dataclass
class MigrationResult:
    migration_name: str
    collection_id: str
    started_at: str
    completed_at: str
    items_processed: int
    items_modified: int
    items_skipped: int
    items_failed: int
    dry_run: bool
    errors: list[dict[str, str]]


# === History Tracking ===


def load_history(history_file: Path) -> dict[str, Any]:
    if not history_file.exists():
        return {"runs": []}
    with open(history_file) as f:
        data: dict[str, Any] = json.load(f)
    return data


def save_history(history_file: Path, history: dict[str, Any]) -> None:
    with open(history_file, "w") as f:
        json.dump(history, f, indent=2)
        f.write("\n")


def record_run(history_file: Path, result: MigrationResult) -> None:
    history = load_history(history_file)
    history["runs"].append(dataclasses.asdict(result))
    save_history(history_file, history)


def was_migration_run(history_file: Path, migration_name: str, collection_id: str) -> bool:
    """Return True if this migration was previously applied (non-dry-run) to this collection."""
    for run in load_history(history_file)["runs"]:
        if (
            run.get("migration_name") == migration_name
            and run.get("collection_id") == collection_id
            and not run.get("dry_run", True)
        ):
            return True
    return False


# === Migration Runner ===


class STACMigrationRunner:
    def __init__(self, api_url: str) -> None:
        self.api_url = api_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

    def get_items(self, collection_id: str) -> list[dict[str, Any]]:
        catalog = Client.open(self.api_url)
        search = catalog.search(collections=[collection_id], max_items=None)
        return [item.to_dict() for item in search.items()]

    def _update_item(self, collection_id: str, item_id: str, item_dict: dict[str, Any]) -> None:
        """Delete then POST (pgSTAC doesn't support PUT)."""
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


# === CLI ===


@click.group()
@click.option(
    "--api-url",
    default=None,
    envvar="STAC_API_URL",
    help=f"STAC API URL (default: {_DEFAULT_API_URL})",
)
@click.option(
    "--history-file",
    default=None,
    type=click.Path(),
    help="Migration history JSON file",
)
@click.pass_context
def cli(ctx: click.Context, api_url: str | None, history_file: str | None) -> None:
    """STAC Catalogue Migration Tool for EOPF Explorer."""
    ctx.ensure_object(dict)
    ctx.obj["api_url"] = api_url or _DEFAULT_API_URL
    ctx.obj["history_file"] = Path(history_file) if history_file else _DEFAULT_HISTORY_FILE


@cli.command()
@click.argument("migration")
@click.argument("collection_id")
@click.option("--dry-run", is_flag=True, help="Show changes without updating")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompt")
@click.pass_context
def run(ctx: click.Context, migration: str, collection_id: str, dry_run: bool, yes: bool) -> None:
    """Run a migration on a collection."""
    if migration not in MIGRATIONS:
        click.echo(f"Unknown migration: {migration}", err=True)
        click.echo(f"Available: {', '.join(MIGRATIONS)}", err=True)
        sys.exit(1)

    api_url: str = ctx.obj["api_url"]
    history_file: Path = ctx.obj["history_file"]
    migration_fn, description = MIGRATIONS[migration]

    if not dry_run and was_migration_run(history_file, migration, collection_id):
        click.echo(f"Warning: '{migration}' was already applied to '{collection_id}'.")
        if not yes and not click.confirm("Run again?", default=False):
            click.echo("Aborted.")
            return

    if not dry_run and not yes:
        click.echo(f"Migration:   {migration}")
        click.echo(f"Description: {description}")
        click.echo(f"Collection:  {collection_id}")
        click.echo(f"API:         {api_url}")
        if not click.confirm("Proceed?", default=False):
            click.echo("Aborted.")
            return

    runner = STACMigrationRunner(api_url)
    result = runner.run_migration(collection_id, migration_fn, migration, dry_run=dry_run)

    click.echo()
    click.echo("=" * 50)
    click.echo(f"{'DRY RUN ' if dry_run else ''}Migration complete: {migration}")
    click.echo(f"  Items processed:              {result.items_processed}")
    click.echo(f"  Items {'would be ' if dry_run else ''}modified: {result.items_modified}")
    click.echo(f"  Items skipped (no change):    {result.items_skipped}")
    if result.items_failed:
        click.echo(f"  Items failed:                 {result.items_failed}", err=True)
        for err in result.errors[:5]:
            click.echo(f"    - {err['item_id']}: {err['error']}", err=True)
    click.echo("=" * 50)

    if not dry_run:
        record_run(history_file, result)
        click.echo(f"Run recorded to {history_file}")


@cli.command()
@click.argument("source_id")
@click.argument("target_id")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompt")
@click.pass_context
def clone(ctx: click.Context, source_id: str, target_id: str, yes: bool) -> None:
    """Clone a collection (metadata + all items) to a new collection."""
    api_url: str = ctx.obj["api_url"]

    if not yes:
        click.echo(f"Clone '{source_id}' -> '{target_id}' via {api_url}")
        if not click.confirm("Proceed?", default=False):
            click.echo("Aborted.")
            return

    runner = STACMigrationRunner(api_url)
    try:
        copied, failed = runner.clone_collection(source_id, target_id)
        click.echo(f"Done. Items copied: {copied}, failed: {failed}")
        if failed:
            sys.exit(1)
    except requests.HTTPError as e:
        click.echo(f"HTTP error: {e}", err=True)
        sys.exit(1)


@cli.command("list")
def list_migrations() -> None:
    """List available migrations."""
    click.echo("Available migrations:")
    for name, (_, description) in MIGRATIONS.items():
        click.echo(f"  {name:<25} {description}")


@cli.command()
@click.option("--migration", default=None, help="Filter by migration name")
@click.option("--collection", default=None, help="Filter by collection ID")
@click.pass_context
def history(ctx: click.Context, migration: str | None, collection: str | None) -> None:
    """Show past migration runs."""
    history_file: Path = ctx.obj["history_file"]
    runs: list[dict[str, Any]] = load_history(history_file)["runs"]

    if migration:
        runs = [r for r in runs if r.get("migration_name") == migration]
    if collection:
        runs = [r for r in runs if r.get("collection_id") == collection]

    if not runs:
        click.echo("No migration runs found.")
        return

    for r in runs:
        dry = " (dry-run)" if r.get("dry_run") else ""
        click.echo(
            f"{r['started_at'][:19]}  {r['migration_name']:<25}  "
            f"{r['collection_id']:<30}  "
            f"modified={r['items_modified']}  skipped={r['items_skipped']}  "
            f"failed={r['items_failed']}{dry}"
        )


@cli.command()
@click.argument("migration")
@click.argument("collection_id")
@click.pass_context
def verify(ctx: click.Context, migration: str, collection_id: str) -> None:
    """Check if a migration is fully applied to a collection."""
    if migration not in MIGRATIONS:
        click.echo(f"Unknown migration: {migration}", err=True)
        sys.exit(1)

    api_url: str = ctx.obj["api_url"]
    migration_fn, _ = MIGRATIONS[migration]

    click.echo(f"Verifying '{migration}' on '{collection_id}'...")
    runner = STACMigrationRunner(api_url)
    result = runner.run_migration(collection_id, migration_fn, migration, dry_run=True)

    click.echo(f"  Items scanned:            {result.items_processed}")
    click.echo(f"  Items already fixed:      {result.items_skipped}")
    click.echo(f"  Items needing migration:  {result.items_modified}")

    if result.items_modified == 0:
        click.echo(f"✓ Migration '{migration}' is fully applied on '{collection_id}'.")
    else:
        click.echo(
            f"✗ Not fully applied. Run: python migrate_catalog.py run {migration} {collection_id}"
        )
        sys.exit(1)


if __name__ == "__main__":
    cli()
