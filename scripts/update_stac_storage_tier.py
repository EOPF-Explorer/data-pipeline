#!/usr/bin/env python3
"""Update STAC items with current S3 storage tier metadata.

This script fetches STAC items and updates their alternate.s3 objects with
current storage tier information from S3. It can also be use for updating existing items
that were registered before storage tier tracking was implemented.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

# Add scripts directory to path to import from register_v1
scripts_dir = Path(__file__).parent
if str(scripts_dir) not in sys.path:
    sys.path.insert(0, str(scripts_dir))

import httpx  # noqa: E402
from pystac import Item  # noqa: E402
from pystac_client import Client  # noqa: E402
from register_v1 import https_to_s3  # noqa: E402
from storage_tier_utils import (  # noqa: E402
    extract_region_from_endpoint,
    get_s3_storage_info,
)

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

for lib in ["botocore", "boto3", "urllib3", "httpx", "httpcore"]:
    logging.getLogger(lib).setLevel(logging.WARNING)


def update_item_storage_tiers(
    item: Item, s3_endpoint: str, add_missing: bool = False
) -> tuple[int, int, int, int, int, int]:
    """Update storage tier metadata for all assets in a STAC item.

    Args:
        item: STAC item to update
        s3_endpoint: S3 endpoint URL
        add_missing: If True, add alternate.s3 to assets that don't have it

    Returns:
        Tuple of (assets_updated, assets_with_alternate_s3, assets_with_tier,
                  assets_added, assets_skipped, assets_s3_failed)
    """
    # Ensure required extensions are present
    extensions = [
        "https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json",
        "https://stac-extensions.github.io/storage/v2.0.0/schema.json",
    ]

    if not hasattr(item, "stac_extensions"):
        item.stac_extensions = []

    for ext in extensions:
        if ext not in item.stac_extensions:
            item.stac_extensions.append(ext)

    region = extract_region_from_endpoint(s3_endpoint)

    assets_updated = 0
    assets_with_alternate_s3 = 0
    assets_with_tier = 0
    assets_added = 0
    assets_skipped = 0
    assets_s3_failed = 0

    for asset_key, asset in item.assets.items():
        # Skip thumbnail and other non-data assets
        if asset.roles and "thumbnail" in asset.roles:
            continue

        # Initialize extra_fields if needed
        if not hasattr(asset, "extra_fields"):
            asset.extra_fields = {}

        # Check if asset has alternate.s3
        has_alternate = (
            "alternate" in asset.extra_fields
            and isinstance(asset.extra_fields["alternate"], dict)
            and "s3" in asset.extra_fields["alternate"]
        )

        # If no alternate.s3 and add_missing is True, try to add it
        if not has_alternate and add_missing:
            if not asset.href or not asset.href.startswith("https://"):
                logger.info(f"  {asset_key}: Skipping (href: {asset.href})")
                assets_skipped += 1
                continue

            # Try to convert HTTPS URL to S3 URL
            s3_url = https_to_s3(asset.href)
            if not s3_url:
                logger.info(f"  {asset_key}: Could not convert to S3 URL (href: {asset.href})")
                assets_skipped += 1
                continue

            # Query storage tier from S3 (s3_url is guaranteed to be str here)
            storage_info = get_s3_storage_info(s3_url, s3_endpoint)

            if storage_info is None:
                logger.warning(
                    f"  {asset_key}: Could not query storage tier from S3 (check credentials/permissions)"
                )
                logger.warning(f"  {asset_key}: Skipping - cannot verify S3 object exists")
                assets_s3_failed += 1
                assets_skipped += 1
                continue

            tier = storage_info["tier"]

            # Preserve existing alternate structure if present
            existing_alternate = asset.extra_fields.get("alternate", {})
            if not isinstance(existing_alternate, dict):
                existing_alternate = {}

            # Create storage scheme object following v2.0 spec
            storage_scheme = {
                "platform": "OVHcloud",
                "region": region,
                "requester_pays": False,
            }

            # Add tier to scheme (standard field in v2.0)
            if tier:
                storage_scheme["tier"] = tier

            # Create alternate.s3 object
            s3_alternate = {
                "href": s3_url,
                "storage:scheme": storage_scheme,
            }

            # Add distribution if storage is mixed or multiple files sampled
            if storage_info["distribution"] is not None:
                s3_alternate["ovh:storage_tier_distribution"] = storage_info["distribution"]

            # Preserve other alternate formats (e.g., alternate.xarray if it exists)
            existing_alternate["s3"] = s3_alternate
            asset.extra_fields["alternate"] = existing_alternate
            assets_added += 1
            assets_updated += 1
            assets_with_tier += 1
            logger.info(f"  {asset_key}: Added alternate.s3 with tier {tier}")
            continue

        # If no alternate.s3 and not adding, skip
        if not has_alternate:
            continue

        # Asset has alternate.s3
        assets_with_alternate_s3 += 1

        # Update existing alternate.s3
        s3_info = asset.extra_fields["alternate"]["s3"]
        if not isinstance(s3_info, dict) or "href" not in s3_info:
            continue

        s3_url = s3_info["href"]
        if not isinstance(s3_url, str):
            continue

        # Query current storage tier from S3
        storage_info = get_s3_storage_info(s3_url, s3_endpoint)

        if storage_info is None:
            logger.warning(
                f"  {asset_key}: Could not query storage tier from S3 (check credentials/permissions)"
            )
            assets_s3_failed += 1
            storage_tier: str | None = None
        else:
            storage_tier = storage_info["tier"]

        # Get or create storage:scheme object (v2.0 format)
        storage_scheme = s3_info.get("storage:scheme", {})
        if not isinstance(storage_scheme, dict):
            storage_scheme = {}

        # Track if anything changed
        asset_changed = False
        old_tier = storage_scheme.get("tier")

        # Update scheme fields (only if missing)
        scheme_changed = False
        if "platform" not in storage_scheme:
            storage_scheme["platform"] = "OVHcloud"
            scheme_changed = True
        if "region" not in storage_scheme:
            storage_scheme["region"] = region
            scheme_changed = True
        if "requester_pays" not in storage_scheme:
            storage_scheme["requester_pays"] = False
            scheme_changed = True

        # Add/update tier in scheme (standard v2.0 field)
        if storage_tier:
            if storage_scheme.get("tier") != storage_tier:
                storage_scheme["tier"] = storage_tier
                scheme_changed = True
            assets_with_tier += 1

            # Add or update distribution if available
            if storage_info and storage_info.get("distribution") is not None:
                s3_info["ovh:storage_tier_distribution"] = storage_info["distribution"]
                if storage_tier == "MIXED":
                    logger.info(
                        f"  {asset_key}: Mixed storage detected - {storage_info['distribution']}"
                    )
            else:
                # Remove distribution if no longer mixed
                if "ovh:storage_tier_distribution" in s3_info:
                    del s3_info["ovh:storage_tier_distribution"]
                    asset_changed = True

            if old_tier != storage_tier:
                asset_changed = True
                logger.debug(f"  {asset_key}: {old_tier or 'none'} -> {storage_tier}")
        else:
            # Remove tier if it cannot be determined
            if "tier" in storage_scheme:
                del storage_scheme["tier"]
                scheme_changed = True
                logger.debug(f"  {asset_key}: removed tier (not available)")
            # Also remove distribution if present
            if "ovh:storage_tier_distribution" in s3_info:
                del s3_info["ovh:storage_tier_distribution"]
                asset_changed = True

        # Update storage:scheme in s3_info if changed
        if scheme_changed:
            s3_info["storage:scheme"] = storage_scheme
            asset_changed = True

        if asset_changed:
            assets_updated += 1

    return (
        assets_updated,
        assets_with_alternate_s3,
        assets_with_tier,
        assets_added,
        assets_skipped,
        assets_s3_failed,
    )


def update_stac_item(
    stac_item_url: str,
    stac_api_url: str,
    s3_endpoint: str,
    dry_run: bool = False,
    add_missing: bool = False,
) -> dict[str, int]:
    """Update storage tier metadata for a STAC item.

    Args:
        stac_item_url: STAC item URL (can be from STAC API or standalone)
        stac_api_url: STAC API base URL for updates
        s3_endpoint: S3 endpoint URL
        dry_run: If True, show changes without updating
        add_missing: If True, add alternate.s3 to assets that don't have it

    Returns:
        Dictionary with update statistics
    """
    # Extract collection and item ID from URL
    # Expected format: .../collections/{collection}/items/{item_id}
    parts = stac_item_url.rstrip("/").split("/")
    if "items" in parts:
        item_idx = parts.index("items")
        item_id = parts[item_idx + 1]
        collection_id = parts[parts.index("collections") + 1] if "collections" in parts else None
    else:
        logger.error("Could not extract item ID from URL")
        return {"updated": 0, "with_tier": 0}

    logger.info(f"Processing: {item_id}")

    # Fetch STAC item
    with httpx.Client(timeout=30.0, follow_redirects=True) as http:
        resp = http.get(stac_item_url)
        resp.raise_for_status()
        item = Item.from_dict(resp.json())

    # Update storage tiers
    (
        assets_updated,
        assets_with_alternate_s3,
        assets_with_tier,
        assets_added,
        assets_skipped,
        assets_s3_failed,
    ) = update_item_storage_tiers(item, s3_endpoint, add_missing)

    total_with_alternate = assets_with_alternate_s3 + assets_added
    logger.info(f"  Assets with alternate.s3: {total_with_alternate}")
    if assets_added > 0:
        logger.info(f"    - Newly added: {assets_added}")
    if assets_with_alternate_s3 > 0:
        logger.info(f"    - Already present: {assets_with_alternate_s3}")

    logger.info(f"  Assets with queryable storage tier: {assets_with_tier}")

    if assets_s3_failed > 0:
        logger.warning(f"  ⚠️  Failed to query storage tier from S3 for {assets_s3_failed} asset(s)")
        logger.warning(
            "      Check AWS credentials, S3 permissions, or if objects are Zarr directories"
        )

    if assets_skipped > 0:
        logger.info(f"  Assets skipped: {assets_skipped}")

    logger.info(f"  Assets updated: {assets_updated}")

    if dry_run:
        logger.info("  (DRY RUN - no changes made)")
        return {
            "updated": assets_updated,
            "with_alternate_s3": total_with_alternate,
            "with_tier": assets_with_tier,
            "added": assets_added,
            "skipped": assets_skipped,
            "s3_failed": assets_s3_failed,
        }

    # Update STAC item if changes were made
    if assets_updated > 0:
        if not collection_id:
            logger.error("Could not determine collection ID - cannot update item")
            return {
                "updated": 0,
                "with_alternate_s3": total_with_alternate,
                "with_tier": assets_with_tier,
                "s3_failed": assets_s3_failed,
            }

        client = Client.open(stac_api_url)
        base_url = str(client.self_href).rstrip("/")

        # DELETE then POST (pgstac doesn't support PUT for items)
        delete_url = f"{base_url}/collections/{collection_id}/items/{item_id}"
        try:
            assert client._stac_io is not None
            resp = client._stac_io.session.delete(delete_url, timeout=30)
            resp.raise_for_status()
            logger.debug(f"  Deleted existing {item_id}")
        except Exception as e:
            logger.warning(f"  Failed to delete existing item (may not exist): {e}")
            # Continue with POST anyway - might be first-time creation

        assert client._stac_io is not None
        create_url = f"{base_url}/collections/{collection_id}/items"
        resp = client._stac_io.session.post(
            create_url,
            json=item.to_dict(),
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()
        logger.info(f"  ✅ Updated {item_id} (HTTP {resp.status_code})")
    else:
        logger.info("  No changes needed")

    return {"updated": assets_updated, "with_tier": assets_with_tier, "added": assets_added}


def main(argv: list[str] | None = None) -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Update STAC items with current S3 storage tier metadata"
    )
    parser.add_argument("--stac-item-url", required=True, help="STAC item URL")
    parser.add_argument("--stac-api-url", required=True, help="STAC API base URL for updates")
    parser.add_argument("--s3-endpoint", required=True, help="S3 endpoint URL")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
    parser.add_argument(
        "--add-missing",
        action="store_true",
        help="Add alternate.s3 to assets that don't have it (for legacy items)",
    )

    args = parser.parse_args(argv)

    try:
        update_stac_item(
            args.stac_item_url,
            args.stac_api_url,
            args.s3_endpoint,
            args.dry_run,
            args.add_missing,
        )
        return 0
    except Exception as e:
        logger.error(f"Failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
