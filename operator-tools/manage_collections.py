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
"""

import json
import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import boto3
import click
import requests
from botocore.exceptions import ClientError
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

        items_deleted = 0
        items_failed = 0
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

                # Delete S3 data first if requested
                s3_deletion_successful = True
                if clean_s3 and s3_client:
                    s3_urls = extract_s3_urls_from_item(item)

                    if s3_urls:
                        # Delete all S3 objects for this item
                        deleted, failed = delete_s3_objects_for_item(s3_client, s3_urls)
                        s3_objects_deleted += deleted
                        s3_objects_failed += failed

                        # Validation: verify all S3 objects were deleted
                        if failed > 0:
                            click.echo(f"\n⚠️  Item {item_id}: Failed to delete {failed} S3 objects")
                            s3_deletion_successful = False
                        else:
                            # Double-check: verify no objects remain
                            remaining = count_s3_objects_for_item(s3_client, s3_urls)
                            if remaining > 0:
                                click.echo(
                                    f"\n⚠️  Item {item_id}: Validation failed - {remaining} S3 objects still exist"
                                )
                                s3_deletion_successful = False

                # Only delete STAC item if S3 cleanup succeeded (or wasn't requested)
                if s3_deletion_successful:
                    if self.delete_item(collection_id, item_id):
                        items_deleted += 1
                    else:
                        items_failed += 1
                else:
                    click.echo(
                        f"\n⚠️  Skipping STAC item deletion for {item_id} due to S3 cleanup failures"
                    )
                    items_skipped += 1

        # Summary
        click.echo("\n" + "=" * 60)
        click.echo("CLEANUP SUMMARY")
        click.echo("=" * 60)
        click.echo(f"Total items processed: {len(items)}")
        click.echo(f"✅ STAC items deleted: {items_deleted}")
        if items_failed > 0:
            click.echo(f"❌ STAC items failed: {items_failed}")
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


# === S3 Helper Functions ===


def extract_s3_urls_from_item(item_dict: dict) -> set[str]:
    """Extract all S3 URLs from a STAC item's assets.

    Tries multiple locations in order:
    1. alternate.s3.href (preferred, new format)
    2. main href if it's an s3:// URL
    """
    s3_urls = set()

    for _asset_key, asset in item_dict.get("assets", {}).items():
        # Skip thumbnails
        if "thumbnail" in asset.get("roles", []):
            continue

        # Try alternate.s3.href first (preferred location)
        alternate = asset.get("alternate", {})
        if isinstance(alternate, dict):
            s3_info = alternate.get("s3", {})
            if isinstance(s3_info, dict):
                href = s3_info.get("href", "")
                if href.startswith("s3://"):
                    s3_urls.add(href)
                    continue

        # Fallback: check if main href is an S3 URL
        href = asset.get("href", "")
        if href.startswith("s3://"):
            s3_urls.add(href)

    return s3_urls


def get_zarr_root_from_urls(s3_urls: set[str]) -> str | None:
    """Extract the root Zarr store URL from S3 URLs.

    Returns the first Zarr root found, or None if no Zarr URLs present.
    """
    for url in s3_urls:
        if ".zarr/" in url:
            return url.split(".zarr/")[0] + ".zarr"
    return None


def delete_s3_zarr_store(
    s3_client: Any,
    zarr_root: str,
    dry_run: bool = False,
) -> tuple[int, int]:
    """Delete all objects in a Zarr store.

    Args:
        s3_client: Boto3 S3 client
        zarr_root: Root Zarr URL (e.g., s3://bucket/path/file.zarr)
        dry_run: If True, only show what would be deleted

    Returns:
        Tuple of (deleted_count, failed_count)
    """
    parsed = urlparse(zarr_root)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/") + "/"

    deleted = 0
    failed = 0

    try:
        paginator = s3_client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            objects = page.get("Contents", [])

            if not objects:
                continue

            for obj in objects:
                key = obj["Key"]

                if dry_run:
                    deleted += 1
                else:
                    try:
                        s3_client.delete_object(Bucket=bucket, Key=key)
                        deleted += 1
                    except ClientError as e:
                        click.echo(f"      ❌ Failed to delete s3://{bucket}/{key}: {e}", err=True)
                        failed += 1

        return deleted, failed

    except ClientError as e:
        click.echo(f"    ❌ Error listing/deleting S3 objects: {e}", err=True)
        return 0, 1


def delete_s3_objects_for_item(
    s3_client: Any,
    s3_urls: set[str],
) -> tuple[int, int]:
    """Delete all S3 objects referenced by a STAC item's assets.

    Handles both individual files and prefixes (directories/Zarr stores).

    Args:
        s3_client: Boto3 S3 client
        s3_urls: Set of S3 URLs from the item's assets

    Returns:
        Tuple of (deleted_count, failed_count)
    """
    deleted = 0
    failed = 0

    # Group URLs by bucket for efficiency
    urls_by_bucket: dict[str, list[str]] = {}
    for url in s3_urls:
        parsed = urlparse(url)
        bucket = parsed.netloc
        if bucket not in urls_by_bucket:
            urls_by_bucket[bucket] = []
        urls_by_bucket[bucket].append(url)

    for bucket, urls in urls_by_bucket.items():
        # Determine if we need to handle prefixes
        prefixes_to_delete = set()
        individual_keys = set()

        for url in urls:
            parsed = urlparse(url)
            key = parsed.path.lstrip("/")

            # Check if this is a prefix (directory or Zarr store)
            if ".zarr/" in key:
                # Extract the Zarr root prefix
                zarr_root = key.split(".zarr/")[0] + ".zarr/"
                prefixes_to_delete.add(zarr_root)
            elif key.endswith("/"):
                # It's a directory prefix
                prefixes_to_delete.add(key)
            else:
                # It's an individual file
                individual_keys.add(key)

        # Delete objects under prefixes
        for prefix in prefixes_to_delete:
            try:
                paginator = s3_client.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                    objects = page.get("Contents", [])
                    for obj in objects:
                        try:
                            s3_client.delete_object(Bucket=bucket, Key=obj["Key"])
                            deleted += 1
                        except ClientError:
                            failed += 1
            except ClientError:
                failed += 1

        # Delete individual files
        for key in individual_keys:
            try:
                s3_client.delete_object(Bucket=bucket, Key=key)
                deleted += 1
            except ClientError as e:
                # Handle 404 as success (already deleted)
                if e.response.get("Error", {}).get("Code") == "NoSuchKey":
                    deleted += 1
                else:
                    failed += 1

    return deleted, failed


def count_s3_objects_for_item(
    s3_client: Any,
    s3_urls: set[str],
) -> int:
    """Count how many S3 objects exist for a STAC item's assets.

    Handles both individual files and prefixes (directories/Zarr stores).

    Args:
        s3_client: Boto3 S3 client
        s3_urls: Set of S3 URLs from the item's assets

    Returns:
        Total count of S3 objects
    """
    count = 0

    # Group URLs by bucket for efficiency
    urls_by_bucket: dict[str, list[str]] = {}
    for url in s3_urls:
        parsed = urlparse(url)
        bucket = parsed.netloc
        if bucket not in urls_by_bucket:
            urls_by_bucket[bucket] = []
        urls_by_bucket[bucket].append(url)

    for bucket, urls in urls_by_bucket.items():
        # Determine if we need to handle prefixes
        prefixes_to_check = set()
        individual_keys = set()

        for url in urls:
            parsed = urlparse(url)
            key = parsed.path.lstrip("/")

            # Check if this is a prefix (directory or Zarr store)
            if ".zarr/" in key:
                # Extract the Zarr root prefix
                zarr_root = key.split(".zarr/")[0] + ".zarr/"
                prefixes_to_check.add(zarr_root)
            elif key.endswith("/"):
                # It's a directory prefix
                prefixes_to_check.add(key)
            else:
                # It's an individual file
                individual_keys.add(key)

        # Count objects under prefixes
        for prefix in prefixes_to_check:
            try:
                paginator = s3_client.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                    count += len(page.get("Contents", []))
            except ClientError:
                pass

        # Count individual files
        for key in individual_keys:
            try:
                s3_client.head_object(Bucket=bucket, Key=key)
                count += 1
            except ClientError:
                pass

    return count


def get_s3_storage_info(s3_client, zarr_root: str) -> tuple[int, int]:  # type: ignore
    """Get object count and total size for a Zarr store.

    Returns:
        Tuple of (object_count, total_size_bytes)
    """
    parsed = urlparse(zarr_root)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/") + "/"

    count = 0
    total_size = 0

    try:
        paginator = s3_client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                count += 1
                total_size += obj.get("Size", 0)

        return count, total_size

    except ClientError as e:
        click.echo(f"    ⚠️  Error accessing S3: {e}", err=True)
        return 0, 0


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
            endpoint_url: str | None = None
            if s3_endpoint is not None:
                endpoint_url = s3_endpoint
            elif os.getenv("AWS_ENDPOINT_URL") is not None:
                endpoint_url = os.getenv("AWS_ENDPOINT_URL")
            if endpoint_url:
                s3_client = boto3.client("s3", endpoint_url=endpoint_url)
                click.echo(f"📦 S3 client initialized (endpoint: {endpoint_url})")
            else:
                s3_client = boto3.client("s3")
                click.echo("📦 S3 client initialized (endpoint: default)")

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
@click.option(
    "--s3-stats",
    is_flag=True,
    help="Include S3 storage statistics (samples first 5 items)",
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
    s3_endpoint: str | None,
    debug: bool,
) -> None:
    """
    Show information about a collection.

    Example:
        manage_collections.py info sentinel-2-l2a-staging
        manage_collections.py info sentinel-2-l2a-staging --s3-stats
        manage_collections.py info sentinel-2-l2a-staging --s3-stats --debug
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

        # S3 storage statistics
        if s3_stats and items:
            click.echo(f"\n{'─'*60}")
            click.echo("S3 Storage Statistics:")

            # Initialize S3 client
            endpoint_url: str | None = None
            if s3_endpoint is not None:
                endpoint_url = s3_endpoint
            elif os.getenv("AWS_ENDPOINT_URL") is not None:
                env_url = os.getenv("AWS_ENDPOINT_URL")
                if env_url is not None:
                    endpoint_url = env_url

            try:
                if endpoint_url:
                    s3_client = boto3.client("s3", endpoint_url=endpoint_url)
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
                    s3_urls = extract_s3_urls_from_item(item)

                    if debug:
                        click.echo(f"\n  📄 Item: {item['id']}")
                        click.echo(f"     Found {len(s3_urls)} S3 URLs")
                        for url in list(s3_urls)[:3]:
                            click.echo(f"       • {url}")
                        if len(s3_urls) > 3:
                            click.echo(f"       ... and {len(s3_urls) - 3} more")

                    if s3_urls:
                        items_with_s3 += 1
                        sample_urls.extend(list(s3_urls)[:2])  # Keep a few sample URLs

                        # Count objects for this item (handles both files and prefixes)
                        obj_count = count_s3_objects_for_item(s3_client, s3_urls)
                        total_objects += obj_count

                        # Calculate size by checking actual objects
                        # Group URLs by bucket/prefix for efficiency
                        for url in s3_urls:
                            parsed = urlparse(url)
                            bucket = parsed.netloc
                            key = parsed.path.lstrip("/")

                            # Check if this is a prefix or individual file
                            if ".zarr/" in key:
                                zarr_root = key.split(".zarr/")[0] + ".zarr/"
                                prefix = zarr_root
                            elif key.endswith("/"):
                                prefix = key
                            else:
                                # Individual file
                                try:
                                    response = s3_client.head_object(Bucket=bucket, Key=key)
                                    total_size += response.get("ContentLength", 0)
                                except ClientError:
                                    pass
                                continue

                            # For prefixes, sum up all object sizes
                            try:
                                paginator = s3_client.get_paginator("list_objects_v2")
                                for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                                    for obj in page.get("Contents", []):
                                        total_size += obj.get("Size", 0)
                            except ClientError:
                                pass

                        if debug:
                            click.echo(
                                f"     Objects: {obj_count}, Size: {total_size / (1024**3):.2f} GB (cumulative)"
                            )
                    else:
                        items_without_s3 += 1
                        if debug:
                            click.echo("     ⚠️  No S3 URLs found")

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


if __name__ == "__main__":
    cli()
