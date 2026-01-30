#!/usr/bin/env python3
"""
STAC Collection Management Tool

Manage collections in the EOPF STAC catalog using the Transaction API.
Supports:
- Cleaning collections (removing all items)
- Cleaning collections with S3 data deletion
- Creating/updating collections from templates
- Deleting collections
- Viewing S3 storage statistics

This tool uses manage_item.py for all item-level operations.
"""

import json
import os
import sys
from pathlib import Path
from typing import Any

import boto3
import click
import requests

# Import item management functionality
from manage_item import (
    STACItemManager,
    count_s3_objects_for_item,
    extract_s3_object_counts,
    extract_s3_urls_from_item,
    extract_stac_object_counts,
)
from pystac import Collection, Item
from pystac_client import Client

# Add scripts directory to path for storage tier utilities
scripts_dir = Path(__file__).parent.parent / "scripts"
if str(scripts_dir) not in sys.path:
    sys.path.insert(0, str(scripts_dir))

from storage_tier_utils import get_s3_storage_info  # noqa: E402


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
        self.item_manager = STACItemManager(api_url)

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

    def clean_collection(
        self,
        collection_id: str,
        dry_run: bool = False,
        clean_s3: bool = False,
        s3_client: Any = None,
    ) -> tuple[int, int, int]:
        """
        Remove all items from a collection, optionally cleaning S3 data.

        Args:
            collection_id: ID of the collection to clean
            dry_run: If True, only show what would be deleted
            clean_s3: If True, also delete S3 data
            s3_client: Boto3 S3 client (required if clean_s3=True)

        Returns:
            Tuple of (items_deleted, s3_objects_deleted, s3_objects_failed)
        """
        click.echo(f"\n{'DRY RUN: ' if dry_run else ''}Cleaning collection: {collection_id}")
        if clean_s3:
            click.echo("  📦 S3 data will also be deleted")

        items = self.get_collection_items(collection_id)

        if not items:
            click.echo("✅ Collection is already empty")
            return 0, 0, 0

        if dry_run:
            click.echo(f"\nWould delete {len(items)} STAC items:")
            for item in items[:10]:
                click.echo(f"  - {item['id']}")
            if len(items) > 10:
                click.echo(f"  ... and {len(items) - 10} more")

            if clean_s3 and s3_client:
                click.echo(
                    f"\nS3 data that would be deleted (sampling {min(5, len(items))} of {len(items)} items for preview):"
                )
                click.echo("NOTE: Actual deletion will process ALL items in the collection")

                # Sample a few items to show S3 paths and count objects
                total_preview_objects = 0
                sample_size = min(5, len(items))

                for idx, item in enumerate(items[:sample_size], 1):
                    s3_urls = extract_s3_urls_from_item(item)
                    if s3_urls:
                        click.echo(f"\n  Sample item {idx}/{len(items)}: {item['id']}")
                        # Count objects for this item
                        item_objects = count_s3_objects_for_item(s3_client, s3_urls)
                        total_preview_objects += item_objects
                        click.echo(f"    S3 objects: {item_objects:,}")
                        click.echo(f"    Asset URLs ({len(s3_urls)}):")
                        for url in list(s3_urls)[:5]:
                            click.echo(f"      • {url}")
                        if len(s3_urls) > 5:
                            click.echo(f"      ... and {len(s3_urls) - 5} more")

                if sample_size > 0 and total_preview_objects > 0:
                    click.echo(f"\n  {'─'*60}")
                    click.echo(
                        f"  Sample total: {total_preview_objects:,} S3 objects from {sample_size} items"
                    )

                    if len(items) > sample_size:
                        # Estimate total for ALL items
                        avg_objects = total_preview_objects / sample_size
                        estimated_total = int(avg_objects * len(items))
                        click.echo(
                            f"  Estimated total for ALL {len(items)} items: ~{estimated_total:,} S3 objects"
                        )
                        click.echo(f"  {'─'*60}")
                        click.echo(
                            f"\n  ⚠️  IMPORTANT: Actual deletion will process ALL {len(items)} items"
                        )
                        click.echo(
                            f"  ⚠️  This will delete approximately {estimated_total:,} S3 objects"
                        )

            return 0, 0, 0

        # Actual deletion
        items_deleted = 0
        items_skipped = 0
        s3_objects_deleted = 0
        s3_objects_failed = 0

        click.echo(f"\nProcessing ALL {len(items)} items in collection...")
        if clean_s3:
            click.echo("  • Deleting S3 objects for each item")
            click.echo("  • Validating S3 deletion succeeded")
            click.echo("  • Removing STAC item only if S3 cleanup succeeded")
            click.echo("")

        with click.progressbar(
            items,
            label="Deleting items and S3 data" if clean_s3 else "Deleting items",
            show_pos=True,
            show_percent=True,
        ) as bar:
            for item in bar:
                item_id = item["id"]

                # Use item manager's delete_item method
                success, s3_deleted, s3_failed = self.item_manager.delete_item(
                    collection_id=collection_id,
                    item_id=item_id,
                    clean_s3=clean_s3,
                    s3_client=s3_client,
                    item_dict=item,
                    validate_s3=True,
                )

                if success:
                    items_deleted += 1
                else:
                    items_skipped += 1

                s3_objects_deleted += s3_deleted
                s3_objects_failed += s3_failed

        # Summary
        click.echo("\n" + "=" * 60)
        click.echo("CLEANUP SUMMARY")
        click.echo("=" * 60)
        click.echo(f"Total items processed: {len(items)}")
        click.echo(f"✅ STAC items deleted: {items_deleted}")
        if items_skipped > 0:
            click.echo(f"⏭️  STAC items skipped: {items_skipped} (due to S3 failures)")

        if clean_s3:
            click.echo(f"\n✅ S3 objects deleted: {s3_objects_deleted:,}")
            if s3_objects_failed > 0:
                click.echo(f"❌ S3 objects failed: {s3_objects_failed:,}")

            if items_skipped > 0:
                click.echo(
                    f"\n⚠️  WARNING: {items_skipped} items were NOT deleted from STAC catalog"
                )
                click.echo("    because their S3 data could not be fully removed.")
                click.echo("    Fix S3 access issues and re-run to process these items.")

        click.echo("=" * 60)

        return items_deleted, s3_objects_deleted, s3_objects_failed

    def sync_storage_tiers(
        self,
        collection_id: str,
        s3_endpoint: str,
        add_missing: bool = False,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """
        Sync storage tier metadata for all items in a collection with S3.

        Args:
            collection_id: ID of the collection
            s3_endpoint: S3 endpoint URL
            add_missing: If True, add alternate.s3 to assets that don't have it
            dry_run: If True, show changes without updating

        Returns:
            Dictionary with sync statistics including problems and corrections
        """
        from update_stac_storage_tier import update_item_storage_tiers  # noqa: E402

        items = self.get_collection_items(collection_id)

        if not items:
            click.echo("✅ Collection is empty - nothing to sync")
            return {
                "items_processed": 0,
                "items_updated": 0,
                "items_no_changes": 0,
                "items_failed": 0,
                "total_assets_updated": 0,
                "total_assets_added": 0,
                "total_assets_failed": 0,
                "problems": [],
                "corrections": [],
            }

        # Statistics tracking
        items_updated = 0
        items_no_changes = 0
        items_failed = 0
        total_assets_updated = 0
        total_assets_added = 0
        total_assets_failed = 0
        problems: list[dict[str, Any]] = []
        corrections: list[dict[str, Any]] = []
        total_s3_object_counts: dict[str, int] = {}
        total_stac_object_counts: dict[str, int] = {}

        click.echo(
            f"\n{'DRY RUN: ' if dry_run else ''}Syncing storage tiers for collection: {collection_id}"
        )
        click.echo(f"Processing {len(items)} items...")

        with click.progressbar(
            items,
            label="Syncing storage tiers" if not dry_run else "Analyzing storage tiers",
            show_pos=True,
            show_percent=True,
        ) as bar:
            for item_dict in bar:
                item_id = item_dict.get("id", "unknown")
                try:
                    # Convert dict to pystac Item
                    item = Item.from_dict(item_dict)

                    # Track object-level mismatches for this item
                    item_mismatches: list[dict[str, Any]] = []
                    item_s3_objects: dict[str, int] = {}
                    item_stac_objects: dict[str, int] = {}

                    # Process each asset to compare STAC vs S3
                    for asset_key, asset in item.assets.items():
                        if asset.roles and "thumbnail" in asset.roles:
                            continue

                        # Extract STAC metadata
                        alternate = getattr(asset, "extra_fields", {}).get("alternate", {})
                        if not isinstance(alternate, dict) or "s3" not in alternate:
                            continue

                        s3_info = alternate["s3"]
                        if not isinstance(s3_info, dict) or "href" not in s3_info:
                            continue

                        s3_url = s3_info.get("href", "")
                        if not isinstance(s3_url, str) or not s3_url.startswith("s3://"):
                            continue

                        storage_scheme = (
                            s3_info.get("storage:scheme") if isinstance(s3_info, dict) else None
                        )
                        stac_objects = extract_stac_object_counts(storage_scheme)

                        # Query S3 for current distribution (query all objects for accurate sync)
                        s3_storage_info = get_s3_storage_info(s3_url, s3_endpoint, query_all=True)
                        s3_objects = extract_s3_object_counts(s3_storage_info)

                        # Check for mismatches and aggregate object counts in one pass
                        has_mismatch = s3_objects != stac_objects
                        if has_mismatch:
                            item_mismatches.append(
                                {
                                    "asset": asset_key,
                                    "s3_url": s3_url,
                                    "s3_tier": s3_storage_info.get("tier")
                                    if s3_storage_info
                                    else None,
                                    "s3_objects": s3_objects,
                                    "stac_tier": storage_scheme.get("tier")
                                    if isinstance(storage_scheme, dict)
                                    else None,
                                    "stac_objects": stac_objects,
                                }
                            )

                        # Aggregate object counts
                        for tier, count in s3_objects.items():
                            item_s3_objects[tier] = item_s3_objects.get(tier, 0) + count
                            total_s3_object_counts[tier] = (
                                total_s3_object_counts.get(tier, 0) + count
                            )
                        for tier, count in stac_objects.items():
                            item_stac_objects[tier] = item_stac_objects.get(tier, 0) + count
                            total_stac_object_counts[tier] = (
                                total_stac_object_counts.get(tier, 0) + count
                            )

                    # Update storage tiers
                    (
                        assets_updated,
                        assets_with_alternate_s3,
                        assets_with_tier,
                        assets_added,
                        assets_skipped,
                        assets_s3_failed,
                    ) = update_item_storage_tiers(item, s3_endpoint, add_missing)

                    # Track problems (mismatches found)
                    if item_mismatches:
                        problems.append(
                            {
                                "item_id": item_id,
                                "mismatches": item_mismatches,
                                "s3_objects": item_s3_objects,
                                "stac_objects": item_stac_objects,
                            }
                        )

                    # Track corrections (items that were updated)
                    if assets_updated > 0:
                        corrections.append(
                            {
                                "item_id": item_id,
                                "assets_updated": assets_updated,
                                "assets_added": assets_added,
                            }
                        )

                    total_assets_updated += assets_updated
                    total_assets_added += assets_added
                    total_assets_failed += assets_s3_failed

                    # Update STAC item if changes were made and not dry run
                    if assets_updated > 0 and not dry_run:
                        try:
                            # Use DELETE then POST (pgstac doesn't support PUT)
                            delete_url = (
                                f"{self.api_url}/collections/{collection_id}/items/{item_id}"
                            )
                            self.session.delete(delete_url, timeout=30)

                            create_url = f"{self.api_url}/collections/{collection_id}/items"
                            self.session.post(
                                create_url,
                                json=item.to_dict(),
                                headers={"Content-Type": "application/json"},
                                timeout=30,
                            )
                            items_updated += 1
                        except Exception as e:
                            click.echo(f"\n  ⚠️  Failed to update item {item_id}: {e}", err=True)
                            items_failed += 1
                    elif assets_updated > 0:
                        # Dry run - just count as would-be updated
                        items_updated += 1
                    else:
                        items_no_changes += 1

                except Exception as e:
                    click.echo(f"\n  ⚠️  Error processing item {item_id}: {e}", err=True)
                    items_failed += 1

        return {
            "items_processed": len(items),
            "items_updated": items_updated,
            "items_no_changes": items_no_changes,
            "items_failed": items_failed,
            "total_assets_updated": total_assets_updated,
            "total_assets_added": total_assets_added,
            "total_assets_failed": total_assets_failed,
            "problems": problems,
            "corrections": corrections,
            "s3_object_counts": total_s3_object_counts,
            "stac_object_counts": total_stac_object_counts,
        }

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

        url = (
            f"{self.api_url}/collections/{collection_id}"
            if update
            else f"{self.api_url}/collections"
        )
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


# === CLI Commands ===


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
@click.option(
    "--clean-s3",
    is_flag=True,
    help="Also delete S3 data (Zarr stores) referenced by items",
)
@click.option(
    "--s3-endpoint",
    help="S3 endpoint URL (optional, uses AWS_ENDPOINT_URL env var if not specified)",
)
@click.pass_context
def clean(
    ctx: click.Context,
    collection_id: str,
    dry_run: bool,
    yes: bool,
    clean_s3: bool,
    s3_endpoint: str | None,
) -> None:
    """
    Remove all items from a collection.

    Example:
        manage_collections.py clean sentinel-2-l2a-staging --dry-run
        manage_collections.py clean sentinel-2-l2a-staging
        manage_collections.py clean sentinel-2-l2a-staging --clean-s3
    """
    manager: STACCollectionManager = ctx.obj["manager"]

    # Confirmation prompt
    if not dry_run and not yes:
        warning = f"⚠️  This will delete ALL items from collection '{collection_id}'."
        if clean_s3:
            warning += (
                "\n⚠️  This will also DELETE ALL S3 DATA (Zarr stores) referenced by these items!"
            )
            warning += "\n⚠️  This action CANNOT be undone!"
        click.confirm(f"{warning}\n\nContinue?", abort=True)

    try:
        # Initialize S3 client if needed
        s3_client = None
        if clean_s3:
            s3_config = {}
            if s3_endpoint:
                s3_config["endpoint_url"] = s3_endpoint
            elif os.getenv("AWS_ENDPOINT_URL"):
                endpoint = os.getenv("AWS_ENDPOINT_URL")
                if endpoint is not None:
                    s3_config["endpoint_url"] = endpoint

            if "endpoint_url" in s3_config:
                s3_client = boto3.client("s3", endpoint_url=s3_config["endpoint_url"])
            else:
                s3_client = boto3.client("s3")
            click.echo(
                f"📦 S3 client initialized (endpoint: {s3_config.get('endpoint_url', 'default')})"
            )

        manager.clean_collection(
            collection_id, dry_run=dry_run, clean_s3=clean_s3, s3_client=s3_client
        )

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

    if not update:
        click.confirm("\nCreate this collection?", abort=True)
    else:
        click.confirm("\nUpdate this collection?", abort=True)

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
@click.pass_context
def batch_create(ctx: click.Context, directory: Path, update: bool) -> None:
    """
    Create or update multiple collections from a directory of templates.

    DIRECTORY should contain JSON files with STAC Collections.

    Examples:
        manage_collections.py batch-create stac/
        manage_collections.py batch-create stac/ --update
    """
    manager: STACCollectionManager = ctx.obj["manager"]

    # Find all JSON files
    json_files = list(directory.glob("*.json"))

    if not json_files:
        click.echo(f"❌ No JSON files found in {directory}", err=True)
        raise click.Abort()

    click.echo(f"Found {len(json_files)} JSON files in {directory}")
    click.confirm("\nProcess all files?", abort=True)

    success_count = 0
    fail_count = 0

    for json_file in json_files:
        click.echo(f"\n{'='*60}")
        click.echo(f"Processing: {json_file.name}")

        collection_data = manager.load_collection_from_template(json_file)
        if not collection_data:
            fail_count += 1
            continue

        collection_id = collection_data["id"]
        click.echo(f"Collection ID: {collection_id}")

        if manager.create_or_update_collection(collection_data, update=update):
            success_count += 1
        else:
            fail_count += 1

    click.echo(f"\n{'='*60}")
    click.echo(f"✅ Success: {success_count}")
    if fail_count > 0:
        click.echo(f"❌ Failed: {fail_count}")


@cli.command()
@click.argument("collection_id")
@click.option(
    "--clean-first",
    is_flag=True,
    help="Clean all items from collection before deleting it",
)
@click.pass_context
def delete(ctx: click.Context, collection_id: str, clean_first: bool) -> None:
    """
    Delete a collection.

    Example:
        manage_collections.py delete sentinel-2-l2a-staging
        manage_collections.py delete sentinel-2-l2a-staging --clean-first
    """
    manager: STACCollectionManager = ctx.obj["manager"]

    try:
        # Confirmation prompt
        warning = f"⚠️  This will DELETE collection '{collection_id}'."
        if clean_first:
            warning += "\n⚠️  This will first remove all items from the collection."
        warning += "\n⚠️  This action CANNOT be undone!"

        click.confirm(f"{warning}\n\nContinue?", abort=True)

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
@click.option(
    "--s3-stats",
    is_flag=True,
    help="Include S3 storage statistics (samples first 5 items)",
)
@click.option(
    "--s3-stac-info",
    is_flag=True,
    help="Query STAC API and compute storage tier statistics for all assets of all items",
)
@click.option(
    "--s3-endpoint",
    help="S3 endpoint URL (optional, uses AWS_ENDPOINT_URL env var if not specified)",
)
@click.option(
    "--debug",
    is_flag=True,
    help="Show detailed debug information about S3 URL extraction",
)
@click.pass_context
def info(
    ctx: click.Context,
    collection_id: str,
    s3_stats: bool,
    s3_stac_info: bool,
    s3_endpoint: str | None,
    debug: bool,
) -> None:
    """
    Show information about a collection.

    Example:
        manage_collections.py info sentinel-2-l2a-staging
        manage_collections.py info sentinel-2-l2a-staging --s3-stats
        manage_collections.py info sentinel-2-l2a-staging --s3-stats --debug
        manage_collections.py info sentinel-2-l2a-staging --s3-stac-info
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

        # Storage tier statistics from STAC metadata
        if s3_stac_info and items:
            click.echo(f"\n{'─'*60}")
            click.echo("Storage Tier Statistics (from STAC metadata):")

            # Aggregate statistics across all items
            total_tier_object_counts: dict[str, int] = {}
            total_tier_distributions: dict[str, dict[str, int]] = {}
            total_assets_with_tier = 0
            total_assets_without_tier = 0
            total_assets = 0
            total_objects = 0
            items_with_tier_info = 0
            items_without_tier_info = 0

            click.echo(f"Processing {len(items)} items...")

            with click.progressbar(
                items,
                label="Analyzing storage tiers",
                show_pos=True,
                show_percent=True,
            ) as bar:
                for item in bar:
                    stats = manager.item_manager.get_item_storage_tier_stats(item)

                    # Aggregate object counts per tier
                    for tier, count in stats["tier_object_counts"].items():
                        total_tier_object_counts[tier] = (
                            total_tier_object_counts.get(tier, 0) + count
                        )

                    # Aggregate tier distributions (for mixed storage display)
                    for tier, distribution in stats["tier_distributions"].items():
                        if tier not in total_tier_distributions:
                            total_tier_distributions[tier] = {}
                        for sub_tier, count in distribution.items():
                            total_tier_distributions[tier][sub_tier] = (
                                total_tier_distributions[tier].get(sub_tier, 0) + count
                            )

                    total_assets_with_tier += stats["assets_with_tier"]
                    total_assets_without_tier += stats["assets_without_tier"]
                    total_assets += stats["total_assets"]
                    total_objects += stats["total_objects"]

                    if stats["assets_with_tier"] > 0:
                        items_with_tier_info += 1
                    else:
                        items_without_tier_info += 1

            # Display aggregated statistics
            click.echo("\n  Summary:")
            click.echo(f"    Items with tier info: {items_with_tier_info}")
            click.echo(f"    Items without tier info: {items_without_tier_info}")
            click.echo(f"    Total assets: {total_assets}\n")
            click.echo(f"    Assets with tier info: {total_assets_with_tier}")
            click.echo(f"    Assets without tier info: {total_assets_without_tier}")
            click.echo(f"    Total objects (with tier info): {total_objects:,}")

            if total_tier_object_counts:
                click.echo("\n  Storage Tier Distribution (by object count):")
                # Sort tiers for consistent output
                sorted_tiers = sorted(
                    total_tier_object_counts.items(), key=lambda x: x[1], reverse=True
                )
                for tier, count in sorted_tiers:
                    percentage = (count / total_objects * 100) if total_objects > 0 else 0
                    click.echo(f"    {tier}: {count:,} objects ({percentage:.1f}%)")

                    # Show distribution breakdown if this tier has mixed storage
                    if tier in total_tier_distributions and total_tier_distributions[tier]:
                        dist = total_tier_distributions[tier]
                        total_dist_count = sum(dist.values())
                        click.echo("      Distribution:")
                        for sub_tier, sub_count in sorted(
                            dist.items(), key=lambda x: x[1], reverse=True
                        ):
                            sub_percentage = (
                                (sub_count / total_dist_count * 100) if total_dist_count > 0 else 0
                            )
                            click.echo(
                                f"        {sub_tier}: {sub_count:,} objects ({sub_percentage:.1f}%)"
                            )
            else:
                click.echo("\n  ⚠️  No storage tier information found in STAC metadata")
                click.echo("      Items may need to be updated with storage tier metadata")

        # S3 storage statistics
        if s3_stats and items:
            click.echo(f"\n{'─'*60}")
            click.echo("S3 Storage Statistics:")

            # Initialize S3 client
            s3_config = {}
            if s3_endpoint:
                s3_config["endpoint_url"] = s3_endpoint
            elif os.getenv("AWS_ENDPOINT_URL"):
                endpoint = os.getenv("AWS_ENDPOINT_URL")
                if endpoint is not None:
                    s3_config["endpoint_url"] = endpoint

            try:
                if "endpoint_url" in s3_config:
                    s3_client = boto3.client("s3", endpoint_url=s3_config["endpoint_url"])
                else:
                    s3_client = boto3.client("s3")

                # Sample first few items
                sample_size = min(5, len(items))
                sample_items = items[:sample_size]

                total_objects = 0
                total_size = 0
                items_with_s3 = 0
                items_without_s3 = 0
                sample_urls = []

                click.echo(f"Sampling {sample_size} of {len(items)} items...")

                for item in sample_items:
                    # Use item manager's method to get S3 stats
                    obj_count, size, urls = manager.item_manager.get_item_s3_stats(
                        item, s3_client, debug=debug
                    )

                    if obj_count > 0:
                        items_with_s3 += 1
                        total_objects += obj_count
                        total_size += size
                        sample_urls.extend(urls[:2])  # Keep a few sample URLs
                    else:
                        items_without_s3 += 1

                if debug:
                    click.echo("\n  Summary of sampled items:")
                    click.echo(f"    Items with S3 data: {items_with_s3}")
                    click.echo(f"    Items without S3 data: {items_without_s3}")

                if items_with_s3 > 0:
                    click.echo("\n  Sample S3 URLs:")
                    for url in sample_urls[:5]:
                        click.echo(f"    • {url}")
                    if len(sample_urls) > 5:
                        click.echo(f"    ... and {len(sample_urls) - 5} more")

                    click.echo("\n  Sample statistics:")
                    click.echo(f"    Objects: {total_objects:,}")
                    click.echo(f"    Size: {total_size / (1024**3):.2f} GB")

                    # Estimate total
                    if len(items) > sample_size:
                        avg_objects = total_objects / items_with_s3
                        avg_size = total_size / items_with_s3
                        est_objects = int(avg_objects * len(items))
                        est_size = avg_size * len(items)
                        click.echo(f"\n  Estimated total (all {len(items)} items):")
                        click.echo(f"    Objects: ~{est_objects:,}")
                        click.echo(f"    Size: ~{est_size / (1024**3):.2f} GB")
                else:
                    click.echo("  ⚠️  No S3 data found in sampled items")
                    if debug:
                        click.echo("\n  Troubleshooting tips:")
                        click.echo("    • Check if items have 'alternate.s3.href' in their assets")
                        click.echo("    • Verify that main asset 'href' fields contain s3:// URLs")
                        click.echo("    • Run with --debug flag for detailed URL extraction info")

            except Exception as e:
                click.echo(f"  ⚠️  Could not fetch S3 statistics: {e}", err=True)

        click.echo(f"{'='*60}\n")

    except Exception as e:
        click.echo(f"❌ Error fetching collection info: {e}", err=True)
        raise click.Abort() from e


@cli.command()
@click.argument("collection_id")
@click.option(
    "--s3-endpoint",
    help="S3 endpoint URL (required, uses AWS_ENDPOINT_URL env var if not specified)",
)
@click.option(
    "--add-missing",
    is_flag=True,
    help="Add alternate.s3 to assets that don't have it (for legacy items)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be updated without actually updating",
)
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    help="Skip confirmation prompt",
)
@click.pass_context
def sync_storage_tiers(
    ctx: click.Context,
    collection_id: str,
    s3_endpoint: str | None,
    add_missing: bool,
    dry_run: bool,
    yes: bool,
) -> None:
    """
    Sync storage tier metadata for all items in a collection with S3.

    This command queries S3 for current storage classes and updates STAC item metadata
    to match. It identifies mismatches and shows a detailed summary of problems found
    and corrections made.

    Example:
        manage_collections.py sync-storage-tiers sentinel-2-l2a-staging --s3-endpoint https://s3.de.io.cloud.ovh.net --dry-run
        manage_collections.py sync-storage-tiers sentinel-2-l2a-staging --s3-endpoint https://s3.de.io.cloud.ovh.net
        manage_collections.py sync-storage-tiers sentinel-2-l2a-staging --s3-endpoint https://s3.de.io.cloud.ovh.net --add-missing
    """
    manager: STACCollectionManager = ctx.obj["manager"]

    # Get S3 endpoint
    if not s3_endpoint:
        s3_endpoint = os.getenv("AWS_ENDPOINT_URL")
        if not s3_endpoint:
            click.echo(
                "❌ S3 endpoint required. Use --s3-endpoint or set AWS_ENDPOINT_URL", err=True
            )
            raise click.Abort()

    # Confirmation prompt
    if not dry_run and not yes:
        warning = f"⚠️  This will update storage tier metadata for ALL items in collection '{collection_id}'."
        if add_missing:
            warning += "\n⚠️  This will also ADD alternate.s3 to assets that don't have it."
        warning += "\n⚠️  This will sync STAC metadata with current S3 storage classes."
        click.confirm(f"{warning}\n\nContinue?", abort=True)

    try:
        # Sync storage tiers
        stats = manager.sync_storage_tiers(
            collection_id=collection_id,
            s3_endpoint=s3_endpoint,
            add_missing=add_missing,
            dry_run=dry_run,
        )

        # Display summary
        click.echo("\n" + "=" * 60)
        click.echo("SYNC SUMMARY")
        click.echo("=" * 60)
        click.echo(f"Items processed: {stats['items_processed']}")
        click.echo(f"✅ Items updated: {stats['items_updated']}")
        click.echo(f"✓ Items with no changes: {stats['items_no_changes']}")
        if stats["items_failed"] > 0:
            click.echo(f"❌ Items failed: {stats['items_failed']}")

        click.echo("\nAssets:")
        click.echo(f"  Updated: {stats['total_assets_updated']}")
        if stats["total_assets_added"] > 0:
            click.echo(f"  Added (alternate.s3): {stats['total_assets_added']}")
        if stats["total_assets_failed"] > 0:
            click.echo(f"  ⚠️  Failed to query S3: {stats['total_assets_failed']}")

        # Display object-level statistics
        s3_object_counts = stats.get("s3_object_counts", {})
        stac_object_counts = stats.get("stac_object_counts", {})
        total_s3_objects = sum(s3_object_counts.values())
        total_stac_objects = sum(stac_object_counts.values())

        if total_s3_objects > 0 or total_stac_objects > 0:
            click.echo(f"\n{'─'*60}")
            click.echo("OBJECT-LEVEL STATISTICS")
            click.echo(f"{'─'*60}")

            # S3 object counts
            if s3_object_counts:
                click.echo("\n  S3 (current storage - ALL OBJECTS QUERIED):")
                click.echo(f"    Total objects: {total_s3_objects:,}")
                click.echo("    ✅ All objects were queried for accurate storage tier detection")
                for tier in sorted(s3_object_counts.keys()):
                    count = s3_object_counts[tier]
                    percentage = (count / total_s3_objects * 100) if total_s3_objects > 0 else 0
                    click.echo(f"      {tier}: {count:,} objects ({percentage:.1f}%)")
            else:
                click.echo("\n  S3 (current storage):")
                click.echo("    No objects found or failed to query")

            # STAC object counts
            if stac_object_counts:
                click.echo("\n  STAC (metadata):")
                click.echo(f"    Total objects: {total_stac_objects:,}")
                for tier in sorted(stac_object_counts.keys()):
                    count = stac_object_counts[tier]
                    percentage = (count / total_stac_objects * 100) if total_stac_objects > 0 else 0
                    click.echo(f"      {tier}: {count:,} objects ({percentage:.1f}%)")
            else:
                click.echo("\n  STAC (metadata):")
                click.echo("    No tier information in metadata")

        # Display problems (mismatches)
        if stats["problems"]:
            click.echo(f"\n{'─'*60}")
            click.echo(f"🔍 MISMATCHES FOUND: {len(stats['problems'])} item(s)")
            click.echo(f"{'─'*60}")
            for problem in stats["problems"]:
                item_id = problem["item_id"]
                click.echo(f"\n  Item: {item_id}")
                for mismatch in problem["mismatches"]:
                    click.echo(f"    Asset: {mismatch['asset']}")
                    if mismatch["s3_objects"]:
                        s3_str = ", ".join(
                            f"{tier}: {count}"
                            for tier, count in sorted(mismatch["s3_objects"].items())
                        )
                        click.echo(f"      S3 objects: {s3_str}")
                        click.echo("        ✅ All objects queried for accurate comparison")
                    else:
                        click.echo("      S3 objects: (not available)")
                    if mismatch["stac_objects"]:
                        stac_str = ", ".join(
                            f"{tier}: {count}"
                            for tier, count in sorted(mismatch["stac_objects"].items())
                        )
                        click.echo(f"      STAC objects: {stac_str}")
                    else:
                        click.echo("      STAC objects: (not in metadata)")

        # Display corrections
        if stats["corrections"]:
            click.echo(f"\n{'─'*60}")
            click.echo(f"✅ CORRECTIONS MADE: {len(stats['corrections'])} item(s) updated")
            click.echo(f"{'─'*60}")
            for correction in stats["corrections"][:10]:  # Show first 10
                item_id = correction["item_id"]
                assets_updated = correction["assets_updated"]
                assets_added = correction.get("assets_added", 0)
                click.echo(f"  {item_id}: {assets_updated} asset(s) updated", nl=False)
                if assets_added > 0:
                    click.echo(f", {assets_added} asset(s) added", nl=False)
                click.echo()
            if len(stats["corrections"]) > 10:
                click.echo(f"  ... and {len(stats['corrections']) - 10} more item(s)")

        if dry_run:
            click.echo(f"\n{'─'*60}")
            click.echo("DRY RUN - No changes were made")
            click.echo(f"{'─'*60}")

        click.echo("=" * 60)

    except Exception as e:
        click.echo(f"❌ Operation failed: {e}", err=True)
        raise click.Abort() from e


if __name__ == "__main__":
    cli()
