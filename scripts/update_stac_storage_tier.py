#!/usr/bin/env python3
"""Update STAC items with current S3 storage tier metadata.

This script fetches STAC items and updates storage metadata to follow the
storage extension v2.0 pattern: item-level properties["storage:schemes"] with
standard/performance/glacier schemes, and at each asset alternate.s3 it sets
storage:refs (linking to a scheme) and objects_per_storage_class (object counts per
storage class). It can also add alternate.s3 for legacy items that lack it.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

# Add scripts directory to path to import from register_v1
scripts_dir = Path(__file__).parent
if str(scripts_dir) not in sys.path:
    sys.path.insert(0, str(scripts_dir))

import httpx  # noqa: E402
import stac_auth  # noqa: E402
from pystac import Item  # noqa: E402
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

# Default OVH S3 platform and bucket for storage schemes (item-level)
STORAGE_SCHEMES_PLATFORM = "https://s3.de.io.cloud.ovh.net/"
STORAGE_SCHEMES_BUCKET = "esa-zarr-sentinel-explorer-fra"

# Map S3 storage class (from API) to scheme key in properties["storage:schemes"]
# OVH returns STANDARD, EXPRESS_ONEZONE, STANDARD_IA only
TIER_TO_SCHEME: dict[str, str] = {
    "STANDARD": "standard",
    "EXPRESS_ONEZONE": "performance",
    "STANDARD_IA": "glacier",
    "MIXED": "mixed",
}


# Link rels the raster API generates for every registered item (see register_v1).
# A stac-auth-proxy link-rewrite bug (platform-deploy#343, repaired by
# operator-tools/repair_stac_raster_links.py) once served these back with a
# "/stac/raster/..." prefix, and the read-modify-write below echoed the corruption
# straight into pgstac. The guard makes that fail closed instead. #374.
RASTER_LINK_RELS = ("xyz", "tilejson", "viewer")


class RasterLinkMismatchError(RuntimeError):
    """A raster-API href does not sit under the expected --raster-api-url prefix."""


def _is_raster_shaped(href: str) -> bool:
    """True when an href looks like a raster-API item URL, whatever host it claims.

    Shape, not host: the rewrite faults this guard exists to catch can change the
    host (a proxy rebuilding hrefs from the forwarded host, or an internal read
    yielding svc.cluster.local -- see #374). Keying on host equality would make the
    guard blind to exactly that. An S3 or external thumbnail has no
    /collections/.../items/... path and is still left alone.
    """
    path = urlparse(href).path
    return "/collections/" in path and "/items/" in path


def check_raster_links(item: dict, raster_api_url: str, where: str) -> None:
    """Raise if any raster-API href is not under ``{raster_api_url}/collections/``.

    Only hrefs that are raster-API links *by construction* are judged, so an item
    that legitimately has no xyz/tilejson/viewer links (S1, S3-OLCI) and a thumbnail
    served from S3 or another host passes untouched -- a false refusal here fails a
    real workflow step.

    Args:
        item: STAC item as a plain dict (as read, or as about to be written)
        raster_api_url: expected raster API base URL
        where: short label naming the call site, for the log line

    Raises:
        RasterLinkMismatchError: on any mismatch; the caller must not write.
    """
    expected = raster_api_url.rstrip("/") + "/collections/"
    offenders: list[tuple[str, str]] = []

    for link in item.get("links") or []:
        if link.get("rel") not in RASTER_LINK_RELS:
            continue
        href = link.get("href")
        # These rels are raster-API links by construction, so a missing or
        # non-string href is malformed, not exempt: fail closed on it too.
        if not isinstance(href, str) or not href.startswith(expected):
            offenders.append((link["rel"], href if isinstance(href, str) else repr(href)))

    for key, asset in (item.get("assets") or {}).items():
        if "thumbnail" not in (asset.get("roles") or []):
            continue
        href = asset.get("href")
        if isinstance(href, str) and _is_raster_shaped(href) and not href.startswith(expected):
            offenders.append((f"asset:{key}", href))

    if not offenders:
        return

    for rel, href in offenders:
        logger.error(f"  ❌ raster link guard ({where}): {rel} href not under {expected} -> {href}")
    raise RasterLinkMismatchError(
        f"{len(offenders)} raster href(s) outside {expected} ({where}) - refusing to write"
    )


def _build_storage_schemes(region: str) -> dict:
    """Build item-level storage:schemes (storage extension v2) for OVH S3."""
    return {
        "standard": {
            "type": "custom-s3",
            "platform": STORAGE_SCHEMES_PLATFORM,
            "bucket": STORAGE_SCHEMES_BUCKET,
            "region": region,
            "requester_pays": False,
            "storage_class": "STANDARD",
        },
        "performance": {
            "type": "custom-s3",
            "platform": STORAGE_SCHEMES_PLATFORM,
            "bucket": STORAGE_SCHEMES_BUCKET,
            "region": region,
            "requester_pays": False,
            "storage_class": "EXPRESS_ONEZONE",
        },
        "glacier": {
            "type": "custom-s3",
            "platform": STORAGE_SCHEMES_PLATFORM,
            "bucket": STORAGE_SCHEMES_BUCKET,
            "region": region,
            "requester_pays": False,
            "storage_class": "STANDARD_IA",
        },
        "mixed": {
            "type": "custom-s3",
            "platform": STORAGE_SCHEMES_PLATFORM,
            "bucket": STORAGE_SCHEMES_BUCKET,
            "region": region,
            "requester_pays": False,
            "storage_class": "MIXED",  # mixed storage class that contains objects with different storage classes
        },
    }


def _tier_to_scheme_ref(tier: str | None, distribution: dict[str, int] | None) -> str:
    """Return scheme key for storage:refs from S3 tier."""
    if not tier:
        return "standard"
    if tier == "MIXED":
        return "mixed"
    return TIER_TO_SCHEME.get(tier, "standard")


def update_item_storage_tiers(
    item: Item, s3_endpoint: str, add_missing: bool = False
) -> tuple[int, int, int, int, int, int]:
    """Update storage metadata for all assets in a STAC item.

    Sets item.properties["storage:schemes"] (standard/performance/glacier) and
    each asset alternate.s3 with storage:refs and optional objects_per_storage_class.

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

    # Ensure item-level storage:schemes (storage extension v2 - schemes at properties)
    if not hasattr(item, "properties"):
        item.properties = {}
    if "storage:schemes" not in item.properties or not item.properties["storage:schemes"]:
        item.properties["storage:schemes"] = _build_storage_schemes(region)

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

            # Query storage tier from S3 (query all objects for accuracy)
            storage_info = get_s3_storage_info(s3_url, s3_endpoint, query_all=True)

            if storage_info is None:
                logger.warning(
                    f"  {asset_key}: Could not query storage tier from S3 (check credentials/permissions)"
                )
                logger.warning(f"  {asset_key}: Skipping - cannot verify S3 object exists")
                assets_s3_failed += 1
                assets_skipped += 1
                continue

            tier = storage_info["tier"]
            distribution = storage_info.get("distribution")

            # Preserve existing alternate structure if present
            existing_alternate = asset.extra_fields.get("alternate", {})
            if not isinstance(existing_alternate, dict):
                existing_alternate = {}

            # Scheme ref from tier (links to item properties["storage:schemes"])
            scheme_ref = _tier_to_scheme_ref(tier, distribution)

            # Create alternate.s3: href, storage:refs, optional objects_per_storage_class
            s3_alternate: dict = {
                "href": s3_url,
                "storage:refs": [scheme_ref],
            }
            if distribution is not None:
                s3_alternate["objects_per_storage_class"] = distribution

            # Preserve other alternate formats (e.g., alternate.xarray if it exists)
            existing_alternate["s3"] = s3_alternate
            asset.extra_fields["alternate"] = existing_alternate
            assets_added += 1
            assets_updated += 1
            assets_with_tier += 1
            logger.info(f"  {asset_key}: Added alternate.s3 with scheme ref {scheme_ref}")
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

        # Query current storage tier from S3 (query all objects for accuracy)
        storage_info = get_s3_storage_info(s3_url, s3_endpoint, query_all=True)

        if storage_info is None:
            logger.warning(
                f"  {asset_key}: Could not query storage tier from S3 (check credentials/permissions)"
            )
            assets_s3_failed += 1
            storage_tier: str | None = None
            distribution = None
        else:
            storage_tier = storage_info["tier"]
            distribution = storage_info.get("distribution")

        # Get or create storage:refs and objects_per_storage_class (v2.0: refs at asset, schemes in properties)
        old_refs = s3_info.get("storage:refs")
        if not isinstance(old_refs, list):
            old_refs = []
        old_distribution = s3_info.get("objects_per_storage_class")

        scheme_ref = _tier_to_scheme_ref(storage_tier, distribution)
        new_refs = [scheme_ref]
        new_distribution = distribution if distribution is not None else None

        asset_changed = False
        if storage_tier:
            assets_with_tier += 1
            if storage_tier == "MIXED" and distribution:
                logger.info(f"  {asset_key}: Mixed storage detected - {distribution}")

        if old_refs != new_refs:
            s3_info["storage:refs"] = new_refs
            asset_changed = True
            logger.debug(f"  {asset_key}: storage:refs -> {new_refs}")

        if new_distribution is not None:
            if old_distribution != new_distribution:
                s3_info["objects_per_storage_class"] = new_distribution
                asset_changed = True
        else:
            if "objects_per_storage_class" in s3_info:
                del s3_info["objects_per_storage_class"]
                asset_changed = True

        # Remove legacy storage:scheme if present (migration to item-level schemes)
        if "storage:scheme" in s3_info:
            del s3_info["storage:scheme"]
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
    raster_api_url: str | None = None,
) -> dict[str, int]:
    """Update storage tier metadata for a STAC item.

    Args:
        stac_item_url: STAC item URL (can be from STAC API or standalone)
        stac_api_url: STAC API base URL for updates
        s3_endpoint: S3 endpoint URL
        dry_run: If True, show changes without updating
        add_missing: If True, add alternate.s3 to assets that don't have it
        raster_api_url: If set, refuse to write back an item whose raster links do
            not sit under it (#374). Unset disables the guard, loudly.

    Returns:
        Dictionary with update statistics

    Raises:
        RasterLinkMismatchError: raster_api_url is set and a raster href is corrupt
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

    if not raster_api_url:
        # Never silent: a skipped guard must be distinguishable from a passing one.
        logger.warning("  ⚠️  --raster-api-url unset - write-back link guard NOT active (#374)")

    # Fetch STAC item
    with httpx.Client(timeout=30.0, follow_redirects=True) as http:
        resp = http.get(stac_item_url)
        resp.raise_for_status()
        item_json = resp.json()
        # Check what came back, so a proxy fault reports at the read rather than
        # surfacing as a mystery refusal after all the S3 work below.
        if raster_api_url:
            check_raster_links(item_json, raster_api_url, "as read")
        item = Item.from_dict(item_json)

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

        client = stac_auth.open_client(stac_api_url)
        base_url = str(client.self_href).rstrip("/")
        # Private attr; always set after open_client() but guard for strict typing and -O.
        if client._stac_io is None:
            raise RuntimeError("pystac-client session not initialized")
        stac_session = client._stac_io.session

        # POST, and on 409 (item exists) PUT to replace. Server-reported conflict is
        # more robust than an existence pre-check (#186); single PUT means no
        # deleted-but-not-recreated window (#352).
        item_dict = item.to_dict()
        # The actual gate: judge the document that is about to be written, not the
        # one that was read.
        if raster_api_url:
            check_raster_links(item_dict, raster_api_url, "before write")
        headers = {"Content-Type": "application/json"}
        resp = stac_session.post(
            f"{base_url}/collections/{collection_id}/items",
            json=item_dict,
            headers=headers,
            timeout=30,
        )
        if resp.status_code == 409:
            resp = stac_session.put(
                f"{base_url}/collections/{collection_id}/items/{item_id}",
                json=item_dict,
                headers=headers,
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
    parser.add_argument(
        "--raster-api-url",
        help=(
            "Raster API base URL, e.g. https://api.example.com/raster. When set, refuse to "
            "write back an item whose xyz/tilejson/viewer links or raster-hosted thumbnail "
            "do not sit under it (#374). Omit to disable the guard (a warning is logged)."
        ),
    )

    args = parser.parse_args(argv)

    try:
        update_stac_item(
            args.stac_item_url,
            args.stac_api_url,
            args.s3_endpoint,
            args.dry_run,
            args.add_missing,
            raster_api_url=args.raster_api_url,
        )
        return 0
    except Exception as e:
        logger.error(f"Failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
