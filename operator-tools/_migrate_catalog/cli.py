import sys
from pathlib import Path
from typing import Any

import click
import requests

from _migrate_catalog.history import load_history, record_run, was_migration_run
from _migrate_catalog.migrations import MIGRATIONS
from _migrate_catalog.runner import STACMigrationRunner, compose_migrations
from _migrate_catalog.types import MigrationFn

_DEFAULT_API_URL = "https://api.explorer.eopf.copernicus.eu/stac"
_DEFAULT_HISTORY_FILE = Path(__file__).parent.parent / ".migration_history.json"


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
@click.argument("collection_id")
@click.option(
    "--migration",
    "migration_names",
    multiple=True,
    required=True,
    help="Migration to run (repeatable to compose multiple)",
)
@click.option("--dry-run", is_flag=True, help="Show changes without updating")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompt")
@click.option("--page-size", default=100, show_default=True, help="Items per page when fetching")
@click.pass_context
def run(
    ctx: click.Context,
    collection_id: str,
    migration_names: tuple[str, ...],
    dry_run: bool,
    yes: bool,
    page_size: int,
) -> None:
    """Run one or more migrations on a collection."""
    for name in migration_names:
        if name not in MIGRATIONS:
            click.echo(f"Unknown migration: {name}", err=True)
            click.echo(f"Available: {', '.join(MIGRATIONS)}", err=True)
            sys.exit(1)

    api_url: str = ctx.obj["api_url"]
    history_file: Path = ctx.obj["history_file"]

    if len(migration_names) == 1:
        migration_name = migration_names[0]
        migration_fn: MigrationFn = MIGRATIONS[migration_name][0]
        description = MIGRATIONS[migration_name][1]
    else:
        migration_name = "+".join(migration_names)
        migration_fn = compose_migrations([MIGRATIONS[n][0] for n in migration_names])
        description = "Composed: " + ", ".join(migration_names)

    if not dry_run and was_migration_run(history_file, migration_name, collection_id):
        click.echo(f"Warning: '{migration_name}' was already applied to '{collection_id}'.")
        if not yes and not click.confirm("Run again?", default=False):
            click.echo("Aborted.")
            return

    if not dry_run and not yes:
        click.echo(f"Migration:   {migration_name}")
        click.echo(f"Description: {description}")
        click.echo(f"Collection:  {collection_id}")
        click.echo(f"API:         {api_url}")
        if not click.confirm("Proceed?", default=False):
            click.echo("Aborted.")
            return

    runner = STACMigrationRunner(api_url, recovery_dir=history_file.parent)
    result = runner.run_migration(
        collection_id, migration_fn, migration_name, dry_run=dry_run, page_size=page_size
    )

    click.echo()
    click.echo("=" * 50)
    click.echo(f"{'DRY RUN ' if dry_run else ''}Migration complete: {migration_name}")
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
@click.option("--page-size", default=100, show_default=True, help="Items per page when fetching")
@click.option(
    "--resume",
    is_flag=True,
    help="Skip items that already exist in the target (safe to re-run after interruption)",
)
@click.pass_context
def clone(
    ctx: click.Context, source_id: str, target_id: str, yes: bool, page_size: int, resume: bool
) -> None:
    """Clone a collection (metadata + all items) to a new collection."""
    api_url: str = ctx.obj["api_url"]

    if not yes:
        click.echo(f"Clone '{source_id}' -> '{target_id}' via {api_url}")
        if not click.confirm("Proceed?", default=False):
            click.echo("Aborted.")
            return

    runner = STACMigrationRunner(api_url)
    try:
        copied, skipped, failed = runner.clone_collection(
            source_id, target_id, page_size=page_size, resume=resume
        )
        click.echo(
            f"Done. Items copied: {copied}, skipped (already existed): {skipped}, failed: {failed}"
        )
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
@click.argument("collection_id")
@click.option(
    "--migration",
    "migration_names",
    multiple=True,
    required=True,
    help="Migration to verify (repeatable to compose multiple)",
)
@click.option("--page-size", default=100, show_default=True, help="Items per page when fetching")
@click.pass_context
def verify(
    ctx: click.Context,
    collection_id: str,
    migration_names: tuple[str, ...],
    page_size: int,
) -> None:
    """Check if a migration is fully applied to a collection."""
    for name in migration_names:
        if name not in MIGRATIONS:
            click.echo(f"Unknown migration: {name}", err=True)
            sys.exit(1)

    api_url: str = ctx.obj["api_url"]

    if len(migration_names) == 1:
        migration_name = migration_names[0]
        migration_fn: MigrationFn = MIGRATIONS[migration_name][0]
    else:
        migration_name = "+".join(migration_names)
        migration_fn = compose_migrations([MIGRATIONS[n][0] for n in migration_names])

    click.echo(f"Verifying '{migration_name}' on '{collection_id}'...")
    runner = STACMigrationRunner(api_url)
    result = runner.run_migration(
        collection_id, migration_fn, migration_name, dry_run=True, page_size=page_size
    )

    click.echo(f"  Items scanned:            {result.items_processed}")
    click.echo(f"  Items already fixed:      {result.items_skipped}")
    click.echo(f"  Items needing migration:  {result.items_modified}")

    if result.items_modified == 0:
        click.echo(f"✓ Migration '{migration_name}' is fully applied on '{collection_id}'.")
    else:
        click.echo(
            f"✗ Not fully applied. Run: uv run operator-tools/migrate_catalog.py run "
            f"--migration {migration_name} {collection_id}"
        )
        sys.exit(1)
