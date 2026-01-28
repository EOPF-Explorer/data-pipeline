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
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import boto3
import click
import requests
from botocore.exceptions import ClientError
from pystac import Item

# Add scripts directory to path for storage tier utilities
scripts_dir = Path(__file__).parent.parent / "scripts"
if str(scripts_dir) not in sys.path:
    sys.path.insert(0, str(scripts_dir))

from storage_tier_utils import StorageTierInfo, get_s3_storage_info  # noqa: E402

# === Helper Functions for Storage Tier Sync ===


def extract_stac_object_counts(
    storage_scheme: dict[str, Any] | None,
) -> dict[str, int]:
    """Extract object counts from STAC storage:scheme metadata.

    Args:
        storage_scheme: storage:scheme dictionary from STAC metadata

    Returns:
        Dictionary mapping tier to object count
    """
    if not isinstance(storage_scheme, dict):
        return {}

    stac_distribution = storage_scheme.get("tier_distribution")
    stac_tier = storage_scheme.get("tier")

    if stac_distribution and isinstance(stac_distribution, dict):
        # Explicitly construct dict[str, int] to satisfy type checker
        result: dict[str, int] = {}
        for tier, count in stac_distribution.items():
            if isinstance(tier, str) and isinstance(count, int):
                result[tier] = count
        return result
    elif stac_tier and isinstance(stac_tier, str):
        return {stac_tier: 1}
    return {}


def extract_s3_object_counts(
    storage_info: StorageTierInfo | None,
) -> dict[str, int]:
    """Extract object counts from S3 storage_info.

    Note: The accuracy depends on how get_s3_storage_info was called:
    - If query_all=True: Returns accurate counts from all objects
    - If query_all=False (sampling): Returns sample-based counts (up to 100 files)

    For sync operations, query_all=True is used to ensure accuracy.

    Args:
        storage_info: StorageTierInfo dictionary from get_s3_storage_info

    Returns:
        Dictionary mapping tier to object count
    """
    if not storage_info:
        return {}

    s3_distribution = storage_info.get("distribution")
    s3_tier = storage_info.get("tier")

    # For uniform storage (not MIXED), distribution is just a sample
    # We should use it, but understand it's approximate
    if s3_distribution and isinstance(s3_distribution, dict):
        # Explicitly construct dict[str, int] to satisfy type checker
        result: dict[str, int] = {}
        for tier, count in s3_distribution.items():
            if isinstance(tier, str) and isinstance(count, int):
                result[tier] = count
        return result
    elif s3_tier and isinstance(s3_tier, str):
        # Single file or uniform Zarr (no distribution available)
        return {s3_tier: 1}
    return {}


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

    def get_item_storage_tier_stats(
        self,
        item_dict: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Get storage tier statistics for a single item from STAC metadata.

        Extracts storage tier information from assets' alternate.s3.storage:scheme metadata.
        Does not query S3 directly - only reads from STAC item metadata.

        Statistics are computed based on object counts, not asset counts:
        - Assets with tier_distribution: use the object counts from distribution
        - Assets without tier_distribution (single files): count as 1 object

        Args:
            item_dict: Item dictionary

        Returns:
            Dictionary with tier statistics:
            {
                'tier_object_counts': {tier: count},  # Total object count per tier
                'tier_distributions': {tier: {sub_tier: count}},  # Distribution for mixed storage
                'assets_with_tier': int,  # Total assets with tier info
                'assets_without_tier': int,  # Assets without tier info
                'total_assets': int,  # Total assets (excluding thumbnails)
                'total_objects': int  # Total objects across all tiers
            }
        """
        tier_object_counts: dict[str, int] = {}
        tier_distributions: dict[str, dict[str, int]] = {}
        assets_with_tier = 0
        assets_without_tier = 0
        total_assets = 0
        total_objects = 0

        assets = item_dict.get("assets", {})
        for _asset_key, asset in assets.items():
            # Skip thumbnails
            if "thumbnail" in asset.get("roles", []):
                continue

            total_assets += 1

            # Check for alternate.s3.storage:scheme
            alternate = asset.get("alternate", {})
            if not isinstance(alternate, dict):
                assets_without_tier += 1
                continue

            s3_info = alternate.get("s3", {})
            if not isinstance(s3_info, dict):
                assets_without_tier += 1
                continue

            storage_scheme = s3_info.get("storage:scheme", {})
            if not isinstance(storage_scheme, dict):
                assets_without_tier += 1
                continue

            tier = storage_scheme.get("tier")
            if tier:
                assets_with_tier += 1

                # Check for tier distribution (for mixed storage or Zarr directories)
                tier_distribution = storage_scheme.get("tier_distribution")
                if isinstance(tier_distribution, dict) and tier_distribution:
                    # Asset has object count distribution (Zarr directory)
                    # Aggregate object counts per tier
                    for sub_tier, count in tier_distribution.items():
                        tier_object_counts[sub_tier] = tier_object_counts.get(sub_tier, 0) + count
                        total_objects += count

                    # Store distribution for mixed storage display
                    if tier == "MIXED":
                        if tier not in tier_distributions:
                            tier_distributions[tier] = {}
                        for sub_tier, count in tier_distribution.items():
                            tier_distributions[tier][sub_tier] = (
                                tier_distributions[tier].get(sub_tier, 0) + count
                            )
                else:
                    # Single file asset (no distribution) - count as 1 object
                    tier_object_counts[tier] = tier_object_counts.get(tier, 0) + 1
                    total_objects += 1
            else:
                assets_without_tier += 1

        return {
            "tier_object_counts": tier_object_counts,
            "tier_distributions": tier_distributions,
            "assets_with_tier": assets_with_tier,
            "assets_without_tier": assets_without_tier,
            "total_assets": total_assets,
            "total_objects": total_objects,
        }


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
    "--s3-stac-info",
    is_flag=True,
    help="Query STAC API and compute storage tier statistics for all assets",
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
    s3_stac_info: bool,
    s3_endpoint: str | None,
    debug: bool,
) -> None:
    """
    Show information about a specific STAC item.

    Example:
        manage_item.py info sentinel-2-l2a-staging S2A_MSIL2A_20210917T115221_N0500_R123_T28RBS_20230110T165456
        manage_item.py info sentinel-2-l2a-staging S2A_MSIL2A_20210917T115221_N0500_R123_T28RBS_20230110T165456 --s3-stats
        manage_item.py info sentinel-2-l2a-staging S2A_MSIL2A_20210917T115221_N0500_R123_T28RBS_20230110T165456 --s3-stats --debug
        manage_item.py info sentinel-2-l2a-staging S2A_MSIL2A_20210917T115221_N0500_R123_T28RBS_20230110T165456 --s3-stac-info
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

        # Storage tier statistics from STAC metadata
        if s3_stac_info:
            click.echo(f"\n{'─'*60}")
            click.echo("Storage Tier Statistics (from STAC metadata):")

            try:
                stats = manager.get_item_storage_tier_stats(item)

                click.echo("\n  Summary:")
                click.echo(f"    Total assets: {stats['total_assets']}")
                click.echo(f"    Assets with tier info: {stats['assets_with_tier']}")
                click.echo(f"    Assets without tier info: {stats['assets_without_tier']}")

                if stats["tier_object_counts"]:
                    click.echo(f"\n  Total objects (with tier info): {stats['total_objects']:,}")
                    click.echo("\n  Storage Tier Distribution (by object count):")
                    # Sort tiers for consistent output
                    sorted_tiers = sorted(
                        stats["tier_object_counts"].items(), key=lambda x: x[1], reverse=True
                    )
                    for tier, count in sorted_tiers:
                        percentage = (
                            (count / stats["total_objects"] * 100)
                            if stats["total_objects"] > 0
                            else 0
                        )
                        click.echo(f"    {tier}: {count:,} objects ({percentage:.1f}%)")

                        # Show distribution breakdown if this tier has mixed storage
                        if (
                            tier in stats["tier_distributions"]
                            and stats["tier_distributions"][tier]
                        ):
                            dist = stats["tier_distributions"][tier]
                            total_dist_count = sum(dist.values())
                            click.echo("      Distribution:")
                            for sub_tier, sub_count in sorted(
                                dist.items(), key=lambda x: x[1], reverse=True
                            ):
                                sub_percentage = (
                                    (sub_count / total_dist_count * 100)
                                    if total_dist_count > 0
                                    else 0
                                )
                                click.echo(
                                    f"        {sub_tier}: {sub_count:,} objects ({sub_percentage:.1f}%)"
                                )
                else:
                    click.echo("\n  ⚠️  No storage tier information found in STAC metadata")
                    click.echo("      Item may need to be updated with storage tier metadata")

            except Exception as e:
                click.echo(f"  ⚠️  Could not fetch storage tier statistics: {e}", err=True)

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


@cli.command()
@click.argument("collection_id")
@click.argument("item_id")
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
@click.pass_context
def sync_storage_tiers(
    ctx: click.Context,
    collection_id: str,
    item_id: str,
    s3_endpoint: str | None,
    add_missing: bool,
    dry_run: bool,
) -> None:
    """
    Sync storage tier metadata for a single STAC item with S3.

    This command queries S3 for current storage classes and updates STAC item metadata
    to match. It identifies mismatches and shows a detailed summary of problems found
    and corrections made.

    Example:
        manage_item.py sync-storage-tiers sentinel-2-l2a-staging ITEM_ID --s3-endpoint https://s3.de.io.cloud.ovh.net --dry-run
        manage_item.py sync-storage-tiers sentinel-2-l2a-staging ITEM_ID --s3-endpoint https://s3.de.io.cloud.ovh.net
        manage_item.py sync-storage-tiers sentinel-2-l2a-staging ITEM_ID --s3-endpoint https://s3.de.io.cloud.ovh.net --add-missing
    """
    manager: STACItemManager = ctx.obj["manager"]

    # Get S3 endpoint
    if not s3_endpoint:
        s3_endpoint = os.getenv("AWS_ENDPOINT_URL")
        if not s3_endpoint:
            click.echo(
                "❌ S3 endpoint required. Use --s3-endpoint or set AWS_ENDPOINT_URL", err=True
            )
            raise click.Abort()

    try:
        # Fetch item
        item_dict = manager.get_item(collection_id, item_id)
        if not item_dict:
            raise click.Abort()

        click.echo(f"\n{'DRY RUN: ' if dry_run else ''}Syncing storage tiers for item: {item_id}")
        click.echo(f"Collection: {collection_id}")

        # Convert dict to pystac Item
        item = Item.from_dict(item_dict)

        # Track object-level mismatches and aggregate counts
        mismatches: list[dict[str, Any]] = []
        s3_object_counts: dict[str, int] = {}
        stac_object_counts: dict[str, int] = {}

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

            storage_scheme = s3_info.get("storage:scheme") if isinstance(s3_info, dict) else None
            stac_objects = extract_stac_object_counts(storage_scheme)

            # Query S3 for current distribution (query all objects for accurate sync)
            s3_storage_info = get_s3_storage_info(s3_url, s3_endpoint, query_all=True)
            s3_objects = extract_s3_object_counts(s3_storage_info)

            # Aggregate object counts and check for mismatches in one pass
            has_mismatch = s3_objects != stac_objects
            if has_mismatch:
                mismatches.append(
                    {
                        "asset": asset_key,
                        "s3_url": s3_url,
                        "s3_tier": s3_storage_info.get("tier") if s3_storage_info else None,
                        "s3_objects": s3_objects,
                        "stac_tier": storage_scheme.get("tier")
                        if isinstance(storage_scheme, dict)
                        else None,
                        "stac_objects": stac_objects,
                    }
                )

            # Aggregate object counts
            for tier, count in s3_objects.items():
                s3_object_counts[tier] = s3_object_counts.get(tier, 0) + count
            for tier, count in stac_objects.items():
                stac_object_counts[tier] = stac_object_counts.get(tier, 0) + count

        # Import update function
        from update_stac_storage_tier import update_item_storage_tiers  # noqa: E402

        # Update storage tiers (this will query S3 again, but that's necessary for the update logic)
        (
            assets_updated,
            assets_with_alternate_s3,
            assets_with_tier,
            assets_added,
            assets_skipped,
            assets_s3_failed,
        ) = update_item_storage_tiers(item, s3_endpoint, add_missing)

        # Display summary
        click.echo(f"\n{'='*60}")
        click.echo("SYNC SUMMARY")
        click.echo(f"{'='*60}")

        click.echo("\nAssets:")
        click.echo(f"  With alternate.s3: {assets_with_alternate_s3 + assets_added}")
        if assets_added > 0:
            click.echo(f"    - Newly added: {assets_added}")
        if assets_with_alternate_s3 > 0:
            click.echo(f"    - Already present: {assets_with_alternate_s3}")
        click.echo(f"  With tier info: {assets_with_tier}")
        click.echo(f"  Updated: {assets_updated}")
        if assets_skipped > 0:
            click.echo(f"  Skipped: {assets_skipped}")
        if assets_s3_failed > 0:
            click.echo(f"  ⚠️  Failed to query S3: {assets_s3_failed}")

        # Display object-level statistics
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
        if mismatches:
            click.echo(f"\n{'─'*60}")
            click.echo(f"🔍 MISMATCHES FOUND: {len(mismatches)} asset(s)")
            click.echo(f"{'─'*60}")
            for mismatch in mismatches:
                click.echo(f"\n  Asset: {mismatch['asset']}")
                click.echo(f"    S3 URL: {mismatch['s3_url']}")
                if mismatch["s3_objects"]:
                    s3_str = ", ".join(
                        f"{tier}: {count}" for tier, count in sorted(mismatch["s3_objects"].items())
                    )
                    click.echo(f"    S3 objects: {s3_str}")
                    click.echo("      ✅ All objects queried for accurate comparison")
                else:
                    click.echo("    S3 objects: (not available)")
                if mismatch["stac_objects"]:
                    stac_str = ", ".join(
                        f"{tier}: {count}"
                        for tier, count in sorted(mismatch["stac_objects"].items())
                    )
                    click.echo(f"    STAC objects: {stac_str}")
                else:
                    click.echo("    STAC objects: (not in metadata)")

        # Display corrections
        if assets_updated > 0:
            click.echo(f"\n{'─'*60}")
            click.echo(f"✅ CORRECTIONS MADE: {assets_updated} asset(s) updated")
            click.echo(f"{'─'*60}")
            if assets_added > 0:
                click.echo(f"  Added alternate.s3 to {assets_added} asset(s)")
            if assets_updated > assets_added:
                click.echo(f"  Updated storage tier for {assets_updated - assets_added} asset(s)")

        # Update STAC item if changes were made and not dry run
        if assets_updated > 0 and not dry_run:
            try:
                # Use DELETE then POST (pgstac doesn't support PUT)
                delete_url = f"{manager.api_url}/collections/{collection_id}/items/{item_id}"
                manager.session.delete(delete_url, timeout=30)

                create_url = f"{manager.api_url}/collections/{collection_id}/items"
                manager.session.post(
                    create_url,
                    json=item.to_dict(),
                    headers={"Content-Type": "application/json"},
                    timeout=30,
                )
                click.echo(f"\n✅ Updated STAC item {item_id}")
            except Exception as e:
                click.echo(f"\n❌ Failed to update STAC item: {e}", err=True)
                raise click.Abort() from e
        elif assets_updated > 0:
            click.echo(f"\n{'─'*60}")
            click.echo("DRY RUN - No changes were made")
            click.echo(f"{'─'*60}")
        else:
            click.echo("\n✓ No changes needed - STAC metadata is in sync with S3")

        click.echo(f"{'='*60}\n")

    except Exception as e:
        click.echo(f"❌ Operation failed: {e}", err=True)
        raise click.Abort() from e


if __name__ == "__main__":
    cli()
