#!/usr/bin/env python3
"""
STAC Collection Management Tool

Manage collections in the EOPF STAC catalog using the Transaction API.
Supports:
- Cleaning collections (removing all items)
- Creating/updating collections from templates
- Deleting collections
"""

import json
from pathlib import Path
from typing import Any

import click
import requests
from pystac import Collection
from pystac_client import Client


class STACCollectionManager:
    """Manager for STAC collection operations using Transaction API."""

    def __init__(self, api_url: str):
        """
        Initialize the STAC Collection Manager.

        Args:
            api_url: Base URL of the STAC API (e.g., https://api.explorer.eopf.copernicus.eu/stac)
        """
        self.api_url = api_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

    def get_collection_items(self, collection_id: str) -> list[dict[str, Any]]:
        """
        Get all items from a collection.

        Args:
            collection_id: ID of the collection

        Returns:
            List of item dictionaries
        """
        click.echo(f"Fetching items from collection: {collection_id}")
        items = []

        try:
            catalog = Client.open(self.api_url)
            search = catalog.search(collections=[collection_id], max_items=None)

            for item in search.items():
                items.append(item.to_dict())

            click.echo(f"Found {len(items)} items in collection {collection_id}")
            return items

        except Exception as e:
            click.echo(f"❌ Error fetching items: {e}", err=True)
            raise

    def delete_item(self, collection_id: str, item_id: str) -> bool:
        """
        Delete a single item from a collection using Transaction API.

        Args:
            collection_id: ID of the collection
            item_id: ID of the item to delete

        Returns:
            True if successful, False otherwise
        """
        url = f"{self.api_url}/collections/{collection_id}/items/{item_id}"

        try:
            response = self.session.delete(url)
            response.raise_for_status()
            return True
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                click.echo(f"⚠️  Item {item_id} not found (already deleted?)", err=True)
                return True  # Consider it success if already gone
            click.echo(f"❌ Failed to delete item {item_id}: {e}", err=True)
            return False
        except Exception as e:
            click.echo(f"❌ Error deleting item {item_id}: {e}", err=True)
            return False

    def clean_collection(self, collection_id: str, dry_run: bool = False) -> int:
        """
        Remove all items from a collection.

        Args:
            collection_id: ID of the collection to clean
            dry_run: If True, only show what would be deleted without actually deleting

        Returns:
            Number of items deleted
        """
        click.echo(f"\n{'DRY RUN: ' if dry_run else ''}Cleaning collection: {collection_id}")

        items = self.get_collection_items(collection_id)

        if not items:
            click.echo("✅ Collection is already empty")
            return 0

        if dry_run:
            click.echo(f"\nWould delete {len(items)} items:")
            for item in items[:10]:  # Show first 10
                click.echo(f"  - {item['id']}")
            if len(items) > 10:
                click.echo(f"  ... and {len(items) - 10} more")
            return 0

        deleted_count = 0
        failed_count = 0

        with click.progressbar(
            items, label="Deleting items", show_pos=True, show_percent=True
        ) as bar:
            for item in bar:
                if self.delete_item(collection_id, item["id"]):
                    deleted_count += 1
                else:
                    failed_count += 1

        click.echo(
            f"\n✅ Deleted {deleted_count} items"
            + (f" (failed: {failed_count})" if failed_count > 0 else "")
        )
        return deleted_count

    def delete_collection(self, collection_id: str) -> bool:
        """
        Delete a collection using Transaction API.

        Args:
            collection_id: ID of the collection to delete

        Returns:
            True if successful, False otherwise
        """
        url = f"{self.api_url}/collections/{collection_id}"

        try:
            response = self.session.delete(url, timeout=30)
            response.raise_for_status()
            click.echo(f"✅ Collection {collection_id} deleted successfully")
            return True
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                click.echo(f"⚠️  Collection {collection_id} not found", err=True)
                return False
            click.echo(
                f"❌ Failed to delete collection {collection_id}: {e.response.status_code} - {e.response.text}",
                err=True,
            )
            return False
        except Exception as e:
            click.echo(f"❌ Error deleting collection {collection_id}: {e}", err=True)
            return False

    def create_or_update_collection(
        self, collection_data: dict[str, Any], update: bool = False
    ) -> bool:
        """
        Create or update a collection using Transaction API.

        Args:
            collection_data: Collection data as dictionary
            update: If True, update existing collection; if False, create new

        Returns:
            True if successful, False otherwise
        """
        collection_id = collection_data.get("id")
        if not collection_id:
            click.echo("❌ Collection data missing 'id' field", err=True)
            return False

        url = f"{self.api_url}/collections"
        method = "PUT" if update else "POST"

        try:
            # Validate it's a proper STAC collection
            Collection.from_dict(collection_data)

            response = self.session.request(
                method, url, data=json.dumps(collection_data), timeout=30
            )
            response.raise_for_status()

            action = "updated" if update else "created"
            click.echo(f"✅ Collection {collection_id} {action} successfully")
            return True

        except requests.exceptions.HTTPError as e:
            click.echo(
                f"❌ Failed to {method} collection {collection_id}: {e.response.status_code} - {e.response.text}",
                err=True,
            )
            return False
        except Exception as e:
            click.echo(f"❌ Error processing collection {collection_id}: {e}", err=True)
            return False

    def load_collection_from_template(self, template_path: Path) -> dict[str, Any] | None:
        """
        Load a collection from a JSON template file.

        Args:
            template_path: Path to the JSON template file

        Returns:
            Collection data as dictionary, or None if failed
        """
        try:
            with open(template_path) as f:
                data: dict[str, Any] = json.load(f)

            # Validate it has required fields
            if "id" not in data or "type" not in data:
                click.echo(f"❌ Template {template_path} missing required fields", err=True)
                return None

            if data.get("type") != "Collection":
                click.echo(
                    f"❌ Template {template_path} is not a Collection (type: {data.get('type')})",
                    err=True,
                )
                return None

            return data

        except json.JSONDecodeError as e:
            click.echo(f"❌ Invalid JSON in template {template_path}: {e}", err=True)
            return None
        except Exception as e:
            click.echo(f"❌ Error reading template {template_path}: {e}", err=True)
            return None


@click.group()
@click.option(
    "--api-url",
    default="https://api.explorer.eopf.copernicus.eu/stac",
    help="STAC API URL",
    show_default=True,
)
@click.pass_context
def cli(ctx: click.Context, api_url: str) -> None:
    """STAC Collection Management Tool for EOPF Explorer."""
    ctx.ensure_object(dict)
    ctx.obj["manager"] = STACCollectionManager(api_url)
    ctx.obj["api_url"] = api_url


@cli.command()
@click.argument("collection_id")
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be deleted without actually deleting",
)
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    help="Skip confirmation prompt",
)
@click.pass_context
def clean(ctx: click.Context, collection_id: str, dry_run: bool, yes: bool) -> None:
    """
    Remove all items from a collection.

    Example:
        manage_collections.py clean sentinel-2-l2a-staging
        manage_collections.py clean sentinel-2-l2a-staging --dry-run
    """
    manager: STACCollectionManager = ctx.obj["manager"]

    if not dry_run and not yes:
        click.confirm(
            f"⚠️  This will delete ALL items from collection '{collection_id}'. Continue?",
            abort=True,
        )

    try:
        manager.clean_collection(collection_id, dry_run=dry_run)
    except Exception as e:
        click.echo(f"❌ Operation failed: {e}", err=True)
        raise click.Abort() from e


@cli.command()
@click.argument("template_path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--update",
    is_flag=True,
    help="Update existing collection instead of creating new",
)
@click.pass_context
def create(ctx: click.Context, template_path: Path, update: bool) -> None:
    """
    Create or update a collection from a template file.

    TEMPLATE_PATH should be a JSON file containing a STAC Collection.

    Examples:
        manage_collections.py create stac/sentinel-2-l2a.json
        manage_collections.py create stac/sentinel-2-l2a.json --update
    """
    manager: STACCollectionManager = ctx.obj["manager"]

    click.echo(f"Loading collection template from: {template_path}")
    collection_data = manager.load_collection_from_template(template_path)

    if not collection_data:
        raise click.Abort()

    collection_id = collection_data["id"]
    action = "update" if update else "create"

    click.echo(f"\nCollection ID: {collection_id}")
    click.echo(f"Title: {collection_data.get('title', 'N/A')}")
    click.echo(f"Action: {action}")

    if not click.confirm(f"\nProceed to {action} collection?"):
        click.echo("Aborted.")
        return

    success = manager.create_or_update_collection(collection_data, update=update)

    if not success:
        raise click.Abort()


@cli.command()
@click.argument("directory", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--update",
    is_flag=True,
    help="Update existing collections instead of creating new",
)
@click.option(
    "--pattern",
    default="*.json",
    help="File pattern to match",
    show_default=True,
)
@click.pass_context
def batch_create(ctx: click.Context, directory: Path, update: bool, pattern: str) -> None:
    """
    Create or update multiple collections from template files in a directory.

    Example:
        manage_collections.py batch-create stac/
        manage_collections.py batch-create stac/ --update
    """
    manager: STACCollectionManager = ctx.obj["manager"]

    template_files = list(directory.glob(pattern))

    if not template_files:
        click.echo(f"❌ No template files found matching '{pattern}' in {directory}")
        return

    click.echo(f"Found {len(template_files)} template files:")
    for template_file in template_files:
        click.echo(f"  - {template_file.name}")

    action = "update" if update else "create"
    if not click.confirm(f"\nProceed to {action} {len(template_files)} collections?"):
        click.echo("Aborted.")
        return

    success_count = 0
    failed_count = 0

    for template_file in template_files:
        click.echo(f"\n{'='*60}")
        click.echo(f"Processing: {template_file.name}")

        collection_data = manager.load_collection_from_template(template_file)
        if not collection_data:
            failed_count += 1
            continue

        if manager.create_or_update_collection(collection_data, update=update):
            success_count += 1
        else:
            failed_count += 1

    click.echo(f"\n{'='*60}")
    click.echo(f"✅ Successfully processed: {success_count}")
    if failed_count > 0:
        click.echo(f"❌ Failed: {failed_count}")


@cli.command()
@click.argument("collection_id")
@click.option(
    "--clean-first",
    is_flag=True,
    help="Remove all items from the collection before deleting it",
)
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    help="Skip confirmation prompt",
)
@click.pass_context
def delete(ctx: click.Context, collection_id: str, clean_first: bool, yes: bool) -> None:
    """
    Delete a collection.

    Note: Some STAC servers require the collection to be empty before deletion.
    Use --clean-first to automatically remove all items first.

    Examples:
        manage_collections.py delete sentinel-2-l2a-staging
        manage_collections.py delete sentinel-2-l2a-staging --clean-first
        manage_collections.py delete sentinel-2-l2a-staging --clean-first --yes
    """
    manager: STACCollectionManager = ctx.obj["manager"]

    if not yes:
        click.confirm(
            f"⚠️  This will permanently delete collection '{collection_id}'. Continue?",
            abort=True,
        )

    try:
        # Clean collection first if requested
        if clean_first:
            click.echo("\n📋 Cleaning collection before deletion...")
            manager.clean_collection(collection_id, dry_run=False)

        # Delete the collection
        click.echo(f"\n🗑️  Deleting collection: {collection_id}")
        success = manager.delete_collection(collection_id)

        if not success:
            raise click.Abort()

    except Exception as e:
        click.echo(f"❌ Operation failed: {e}", err=True)
        raise click.Abort() from e


@cli.command()
@click.argument("collection_id")
@click.pass_context
def info(ctx: click.Context, collection_id: str) -> None:
    """
    Show information about a collection.

    Example:
        manage_collections.py info sentinel-2-l2a-staging
    """
    manager: STACCollectionManager = ctx.obj["manager"]
    api_url: str = ctx.obj["api_url"]

    try:
        catalog = Client.open(api_url)
        collection = catalog.get_collection(collection_id)

        click.echo(f"\n{'='*60}")
        click.echo(f"Collection: {collection.id}")
        click.echo(f"Title: {collection.title}")
        click.echo(f"Description: {collection.description[:200]}...")
        click.echo(f"License: {collection.license}")

        # Get item count
        items = manager.get_collection_items(collection_id)
        click.echo(f"Items: {len(items)}")

        if collection.extent and collection.extent.spatial:
            click.echo(f"Spatial extent: {collection.extent.spatial.bboxes}")
        if collection.extent and collection.extent.temporal:
            click.echo(f"Temporal extent: {collection.extent.temporal.intervals}")

        click.echo(f"{'='*60}\n")

    except Exception as e:
        click.echo(f"❌ Error fetching collection info: {e}", err=True)
        raise click.Abort() from e


if __name__ == "__main__":
    cli()
