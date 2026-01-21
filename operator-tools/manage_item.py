#!/usr/bin/env python3
"""
STAC Item Management Tool

Manage individual STAC items in the EOPF STAC catalog.
Supports:
- Viewing item information with S3 statistics
- Deleting items with optional S3 data cleanup
- S3 operations validation and debugging
"""

import os
from typing import Any
from urllib.parse import urlparse

import boto3
import click
import requests
from botocore.exceptions import ClientError


class STACItemManager:
    """Manager for STAC item operations."""

    def __init__(self, api_url: str):
        """
        Initialize the STAC Item Manager.

        Args:
            api_url: Base URL of the STAC API (e.g., https://api.explorer.eopf.copernicus.eu/stac)
        """
        self.api_url = api_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

    def get_item(self, collection_id: str, item_id: str) -> dict[str, Any] | None:
        """
        Get a single item from a collection.

        Args:
            collection_id: ID of the collection
            item_id: ID of the item

        Returns:
            Item dictionary or None if not found
        """
        url = f"{self.api_url}/collections/{collection_id}/items/{item_id}"

        try:
            response = self.session.get(url)
            response.raise_for_status()
            result: dict[str, Any] = response.json()
            return result
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                click.echo(f"❌ Item {item_id} not found in collection {collection_id}", err=True)
                return None
            click.echo(f"❌ Error fetching item: {e}", err=True)
            return None
        except Exception as e:
            click.echo(f"❌ Error fetching item: {e}", err=True)
            return None

    def delete_item(
        self,
        collection_id: str,
        item_id: str,
        clean_s3: bool = False,
        s3_client: Any = None,
        item_dict: dict[str, Any] | None = None,
        validate_s3: bool = True,
    ) -> tuple[bool, int, int]:
        """
        Delete a single item from a collection, optionally cleaning S3 data.

        Args:
            collection_id: ID of the collection
            item_id: ID of the item to delete
            clean_s3: If True, also delete S3 data
            s3_client: Boto3 S3 client (required if clean_s3=True)
            item_dict: Optional pre-fetched item dictionary (to avoid re-fetching)
            validate_s3: If True, validate S3 deletion before removing STAC item

        Returns:
            Tuple of (success: bool, s3_deleted: int, s3_failed: int)
        """
        # Fetch item if not provided
        if item_dict is None and clean_s3:
            item_dict = self.get_item(collection_id, item_id)
            if item_dict is None:
                return False, 0, 0

        s3_deleted = 0
        s3_failed = 0
        s3_deletion_successful = True

        # Delete S3 data first if requested
        if clean_s3 and s3_client and item_dict:
            s3_urls = extract_s3_urls_from_item(item_dict)

            if s3_urls:
                click.echo(f"  Deleting S3 data for {item_id}...")
                s3_deleted, s3_failed = delete_s3_objects_for_item(s3_client, s3_urls)

                # Validate deletion if requested
                if validate_s3:
                    if s3_failed > 0:
                        click.echo(f"    ⚠️  Failed to delete {s3_failed} S3 objects")
                        s3_deletion_successful = False
                    else:
                        # Double-check: verify no objects remain
                        remaining = count_s3_objects_for_item(s3_client, s3_urls)
                        if remaining > 0:
                            click.echo(
                                f"    ⚠️  Validation failed - {remaining} S3 objects still exist"
                            )
                            s3_deletion_successful = False
                        else:
                            click.echo(f"    ✅ Deleted {s3_deleted} S3 objects")

        # Only delete STAC item if S3 cleanup succeeded (or wasn't requested)
        if not s3_deletion_successful:
            click.echo("  ⚠️  Skipping STAC item deletion due to S3 cleanup failures")
            return False, s3_deleted, s3_failed

        # Delete the STAC item
        url = f"{self.api_url}/collections/{collection_id}/items/{item_id}"

        try:
            response = self.session.delete(url)
            response.raise_for_status()
            click.echo(f"  ✅ Deleted STAC item {item_id}")
            return True, s3_deleted, s3_failed
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                click.echo(f"  ⚠️  Item {item_id} not found (already deleted?)")
                return True, s3_deleted, s3_failed  # Consider it success if already gone
            click.echo(f"  ❌ Failed to delete item {item_id}: {e}", err=True)
            return False, s3_deleted, s3_failed
        except Exception as e:
            click.echo(f"  ❌ Error deleting item {item_id}: {e}", err=True)
            return False, s3_deleted, s3_failed

    def get_item_s3_stats(
        self,
        item_dict: dict[str, Any],
        s3_client: Any,
        debug: bool = False,
    ) -> tuple[int, int, list[str]]:
        """
        Get S3 storage statistics for a single item.

        Args:
            item_dict: Item dictionary
            s3_client: Boto3 S3 client
            debug: If True, show detailed debug information

        Returns:
            Tuple of (object_count, total_size_bytes, s3_urls)
        """
        s3_urls = extract_s3_urls_from_item(item_dict)

        if debug:
            click.echo(f"\n  📄 Item: {item_dict['id']}")
            click.echo(f"     Found {len(s3_urls)} S3 URLs")
            for url in list(s3_urls)[:3]:
                click.echo(f"       • {url}")
            if len(s3_urls) > 3:
                click.echo(f"       ... and {len(s3_urls) - 3} more")

        if not s3_urls:
            if debug:
                click.echo("     ⚠️  No S3 URLs found")
            return 0, 0, []

        # Count objects
        obj_count = count_s3_objects_for_item(s3_client, s3_urls)

        # Calculate total size
        total_size = 0
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
            click.echo(f"     Objects: {obj_count}, Size: {total_size / (1024**3):.2f} GB")

        return obj_count, total_size, list(s3_urls)


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

    import click

    # First, gather all keys to delete for progress bar
    all_keys = []
    for bucket, urls in urls_by_bucket.items():
        prefixes_to_delete = set()
        individual_keys = set()
        for url in urls:
            parsed = urlparse(url)
            key = parsed.path.lstrip("/")
            if ".zarr/" in key:
                zarr_root = key.split(".zarr/")[0] + ".zarr/"
                prefixes_to_delete.add(zarr_root)
            elif key.endswith("/"):
                prefixes_to_delete.add(key)
            else:
                individual_keys.add(key)
        # Collect all keys under prefixes
        for prefix in prefixes_to_delete:
            try:
                paginator = s3_client.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                    objects = page.get("Contents", [])
                    all_keys.extend([(bucket, obj["Key"]) for obj in objects])
            except ClientError:
                pass
        # Add individual files
        for key in individual_keys:
            all_keys.append((bucket, key))

    # Now, delete with progress bar
    # Group all_keys by bucket for batch deletion
    from collections import defaultdict

    bucket_to_keys = defaultdict(list)
    for bucket, key in all_keys:
        bucket_to_keys[bucket].append(key)

    total = len(all_keys)
    BATCH_SIZE = 200
    with click.progressbar(
        length=total, label="Deleting S3 objects", show_pos=True, show_percent=True
    ) as bar:
        for bucket, keys in bucket_to_keys.items():
            for i in range(0, len(keys), BATCH_SIZE):
                batch = keys[i : i + BATCH_SIZE]
                if not batch:
                    continue
                try:
                    resp = s3_client.delete_objects(
                        Bucket=bucket, Delete={"Objects": [{"Key": k} for k in batch]}
                    )
                    deleted += len(resp.get("Deleted", []))
                    for err in resp.get("Errors", []):
                        if err.get("Code") == "NoSuchKey":
                            deleted += 1
                        else:
                            failed += 1
                except ClientError:
                    failed += len(batch)
                bar.update(len(batch))

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
    """STAC Item Management Tool for EOPF Explorer."""
    ctx.ensure_object(dict)
    ctx.obj["manager"] = STACItemManager(api_url)
    ctx.obj["api_url"] = api_url


@cli.command()
@click.argument("collection_id")
@click.argument("item_id")
@click.option(
    "--s3-stats",
    is_flag=True,
    help="Include S3 storage statistics",
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
    item_id: str,
    s3_stats: bool,
    s3_endpoint: str | None,
    debug: bool,
) -> None:
    """
    Show information about a specific STAC item.

    Example:
        manage_item.py info sentinel-2-l2a-staging S2A_MSIL2A_20210917T115221_N0500_R123_T28RBS_20230110T165456
        manage_item.py info sentinel-2-l2a-staging S2A_MSIL2A_20210917T115221_N0500_R123_T28RBS_20230110T165456 --s3-stats
        manage_item.py info sentinel-2-l2a-staging S2A_MSIL2A_20210917T115221_N0500_R123_T28RBS_20230110T165456 --s3-stats --debug
    """
    manager: STACItemManager = ctx.obj["manager"]

    try:
        item = manager.get_item(collection_id, item_id)
        if not item:
            raise click.Abort()

        click.echo(f"\n{'='*60}")
        click.echo(f"Item: {item['id']}")
        click.echo(f"Collection: {item['collection']}")

        if "properties" in item:
            props = item["properties"]
            if "datetime" in props:
                click.echo(f"Datetime: {props['datetime']}")
            if "platform" in props:
                click.echo(f"Platform: {props['platform']}")
            if "instruments" in props:
                click.echo(f"Instruments: {', '.join(props['instruments'])}")

        if "geometry" in item:
            click.echo(f"Geometry: {item['geometry']['type']}")

        # Show assets
        assets = item.get("assets", {})
        click.echo(f"Assets: {len(assets)}")
        for asset_key in list(assets.keys())[:5]:
            asset = assets[asset_key]
            click.echo(f"  - {asset_key}: {asset.get('type', 'N/A')}")
        if len(assets) > 5:
            click.echo(f"  ... and {len(assets) - 5} more")

        # S3 storage statistics
        if s3_stats:
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

                obj_count, total_size, s3_urls = manager.get_item_s3_stats(
                    item, s3_client, debug=debug
                )

                if obj_count > 0:
                    click.echo(f"\n  S3 URLs ({len(s3_urls)}):")
                    for url in s3_urls[:5]:
                        click.echo(f"    • {url}")
                    if len(s3_urls) > 5:
                        click.echo(f"    ... and {len(s3_urls) - 5} more")

                    click.echo("\n  Statistics:")
                    click.echo(f"    Objects: {obj_count:,}")
                    click.echo(f"    Total size: {total_size / (1024**3):.2f} GB")
                else:
                    click.echo("  ⚠️  No S3 data found")
                    if debug:
                        click.echo("\n  Troubleshooting tips:")
                        click.echo("    • Check if item has 'alternate.s3.href' in assets")
                        click.echo("    • Verify that main asset 'href' fields contain s3:// URLs")

            except Exception as e:
                click.echo(f"  ⚠️  Could not fetch S3 statistics: {e}", err=True)

        click.echo(f"{'='*60}\n")

    except Exception as e:
        click.echo(f"❌ Error fetching item info: {e}", err=True)
        raise click.Abort() from e


@cli.command()
@click.argument("collection_id")
@click.argument("item_id")
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
    help="Also delete S3 data (Zarr stores) referenced by item",
)
@click.option(
    "--s3-endpoint",
    help="S3 endpoint URL (optional, uses AWS_ENDPOINT_URL env var if not specified)",
)
@click.pass_context
def delete(
    ctx: click.Context,
    collection_id: str,
    item_id: str,
    dry_run: bool,
    yes: bool,
    clean_s3: bool,
    s3_endpoint: str | None,
) -> None:
    """
    Delete a single STAC item, optionally cleaning S3 data.

    Example:
        manage_item.py delete sentinel-2-l2a-staging S2A_MSIL2A_20210917T115221_N0500_R123_T28RBS_20230110T165456 --dry-run
        manage_item.py delete sentinel-2-l2a-staging S2A_MSIL2A_20210917T115221_N0500_R123_T28RBS_20230110T165456
        manage_item.py delete sentinel-2-l2a-staging S2A_MSIL2A_20210917T115221_N0500_R123_T28RBS_20230110T165456 --clean-s3 -y
    """
    manager: STACItemManager = ctx.obj["manager"]

    # Fetch item first
    item = manager.get_item(collection_id, item_id)
    if not item:
        raise click.Abort()

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

    # Dry run preview
    if dry_run:
        click.echo(f"\n{'='*60}")
        click.echo(f"DRY RUN: Would delete item {item_id}")
        click.echo(f"Collection: {collection_id}")

        if clean_s3 and s3_client:
            s3_urls = extract_s3_urls_from_item(item)
            if s3_urls:
                click.echo("\nS3 data that would be deleted:")
                obj_count = count_s3_objects_for_item(s3_client, s3_urls)
                click.echo(f"  S3 objects: {obj_count:,}")
                click.echo(f"  Asset URLs ({len(s3_urls)}):")
                for url in list(s3_urls)[:5]:
                    click.echo(f"    • {url}")
                if len(s3_urls) > 5:
                    click.echo(f"    ... and {len(s3_urls) - 5} more")
            else:
                click.echo("\n  ⚠️  No S3 data found")

        click.echo(f"{'='*60}\n")
        return

    # Confirmation prompt
    if not yes:
        warning = f"⚠️  This will delete item '{item_id}' from collection '{collection_id}'."
        if clean_s3:
            warning += "\n⚠️  This will also DELETE ALL S3 DATA referenced by this item!"
            warning += "\n⚠️  This action CANNOT be undone!"
        click.confirm(f"{warning}\n\nContinue?", abort=True)

    try:
        click.echo(f"\n{'='*60}")
        click.echo(f"Deleting item: {item_id}")

        success, s3_deleted, s3_failed = manager.delete_item(
            collection_id=collection_id,
            item_id=item_id,
            clean_s3=clean_s3,
            s3_client=s3_client,
            item_dict=item,
            validate_s3=True,
        )

        click.echo(f"{'='*60}")
        click.echo("\nDELETION SUMMARY:")
        click.echo(f"{'='*60}")

        if success:
            click.echo("✅ STAC item deleted successfully")
        else:
            click.echo("❌ STAC item deletion failed or skipped")

        if clean_s3:
            click.echo(f"✅ S3 objects deleted: {s3_deleted:,}")
            if s3_failed > 0:
                click.echo(f"❌ S3 objects failed: {s3_failed:,}")

        click.echo(f"{'='*60}\n")

    except Exception as e:
        click.echo(f"❌ Operation failed: {e}", err=True)
        raise click.Abort() from e


if __name__ == "__main__":
    cli()
