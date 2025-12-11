#!/usr/bin/env python3
"""Change storage tier for S3 objects referenced in a STAC item.

This script fetches a STAC item, extracts S3 URLs from alternate.s3.href fields,
and changes the storage class of objects in S3 storage using the AWS S3 API.
"""

from __future__ import annotations

import argparse
import fnmatch
import logging
import os
import sys
from urllib.parse import urlparse

import boto3
import httpx
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

for lib in ["botocore", "boto3", "urllib3", "httpx", "httpcore"]:
    logging.getLogger(lib).setLevel(logging.WARNING)

# Valid S3 storage classes
# Using OVH Cloud Storage naming: STANDARD_IA (Infrequent Access) instead of AWS GLACIER
VALID_STORAGE_CLASSES = frozenset(["STANDARD", "STANDARD_IA", "EXPRESS_ONEZONE"])


def validate_storage_class(storage_class: str) -> bool:
    """Validate that storage class is a supported S3 storage class.

    Args:
        storage_class: The storage class to validate

    Returns:
        True if valid, False otherwise
    """
    return storage_class in VALID_STORAGE_CLASSES


def extract_s3_urls(stac_item: dict) -> set[str]:
    """Extract S3 URLs from STAC item alternate.s3.href fields."""
    s3_urls = set()

    for _, asset in stac_item.get("assets", {}).items():
        if asset.get("roles") and "thumbnail" in asset.get("roles", []):
            continue

        alternate = asset.get("alternate", {})
        if isinstance(alternate, dict):
            s3_info = alternate.get("s3", {})
            if isinstance(s3_info, dict):
                href = s3_info.get("href", "")
                if href.startswith("s3://"):
                    s3_urls.add(href)

    return s3_urls


def get_zarr_root(s3_urls: set[str]) -> str | None:
    """Extract root Zarr store URL from S3 URLs."""
    for url in s3_urls:
        if ".zarr/" in url:
            return url.split(".zarr/")[0] + ".zarr"
    return None


def list_objects(s3_client, bucket: str, prefix: str) -> list[tuple[str, str]]:  # type: ignore
    """List all objects under S3 prefix with their storage class.

    Returns:
        List of tuples (key, storage_class)
    """
    objects = []
    paginator = s3_client.get_paginator("list_objects_v2")

    for page_count, page in enumerate(paginator.paginate(Bucket=bucket, Prefix=prefix), start=1):
        page_objects = page.get("Contents", [])
        for obj in page_objects:
            # Storage class is included in list_objects response
            # Note: S3 returns "STANDARD" implicitly when StorageClass is not present
            # (objects in STANDARD tier don't always have the field set)
            storage_class = obj.get("StorageClass", "STANDARD")
            objects.append((obj["Key"], storage_class))

        # Log progress every 10 pages (typically 10,000 objects)
        if page_count % 10 == 0:
            logger.info(f"  Listed {len(objects)} objects so far ({page_count} pages)...")

    return objects


def filter_paths(
    objects: list[tuple[str, str]],
    include_patterns: list[str] | None = None,
    exclude_patterns: list[str] | None = None,
    zarr_prefix: str = "",
) -> tuple[list[tuple[str, str]], list[str]]:
    """Filter objects based on include/exclude patterns.

    Args:
        objects: List of (key, storage_class) tuples
        include_patterns: Patterns to include (relative to Zarr root)
        exclude_patterns: Patterns to exclude (relative to Zarr root)
        zarr_prefix: Prefix to remove from paths for pattern matching

    Returns:
        Tuple of (filtered_objects, excluded_keys)
    """
    if not include_patterns and not exclude_patterns:
        return objects, []

    filtered = []
    excluded = []

    for key, storage_class in objects:
        # Get relative path from Zarr root for pattern matching
        if zarr_prefix and key.startswith(zarr_prefix):
            relative_path = key[len(zarr_prefix) :]
        else:
            relative_path = key

        # Apply include patterns
        if include_patterns:
            included = any(fnmatch.fnmatch(relative_path, pattern) for pattern in include_patterns)
            if not included:
                excluded.append(key)
                continue

        # Apply exclude patterns
        if exclude_patterns:
            excluded_match = any(
                fnmatch.fnmatch(relative_path, pattern) for pattern in exclude_patterns
            )
            if excluded_match:
                excluded.append(key)
                continue

        filtered.append((key, storage_class))

    return filtered, excluded


def change_object_storage_class(  # type: ignore
    s3_client,
    bucket: str,
    key: str,
    current_storage_class: str,
    target_storage_class: str,
    dry_run: bool,
) -> tuple[bool, str]:
    """Change storage class of single S3 object.

    Args:
        s3_client: Boto3 S3 client
        bucket: S3 bucket name
        key: S3 object key
        current_storage_class: Current storage class (from list_objects)
        target_storage_class: Desired storage class
        dry_run: If True, don't make actual changes

    Returns:
        Tuple of (success: bool, current_storage_class: str)
    """
    try:
        if dry_run:
            if current_storage_class == target_storage_class:
                logger.debug(f"[DRY RUN] Already {target_storage_class}: s3://{bucket}/{key}")
            else:
                logger.debug(
                    f"[DRY RUN] Would change {current_storage_class} -> {target_storage_class}: s3://{bucket}/{key}"
                )
            return True, current_storage_class

        if current_storage_class == target_storage_class:
            logger.debug(f"Already {target_storage_class}: s3://{bucket}/{key}")
            return True, current_storage_class

        # Only make API call when actually changing storage class
        s3_client.copy_object(
            Bucket=bucket,
            Key=key,
            CopySource={"Bucket": bucket, "Key": key},
            StorageClass=target_storage_class,
            MetadataDirective="COPY",
        )
        logger.debug(
            f"Changed {current_storage_class} -> {target_storage_class}: s3://{bucket}/{key}"
        )
        return True, current_storage_class

    except ClientError as e:
        logger.error(f"Failed to change s3://{bucket}/{key}: {e}")
        return False, current_storage_class


def process_stac_item(
    stac_item_url: str,
    storage_class: str,
    dry_run: bool,
    s3_endpoint: str | None,
    include_patterns: list[str] | None = None,
    exclude_patterns: list[str] | None = None,
) -> dict[str, int]:
    """Process STAC item and change storage tier."""
    # Validate storage class
    if not validate_storage_class(storage_class):
        logger.error(
            f"Invalid storage class: {storage_class}. "
            f"Valid options: {', '.join(sorted(VALID_STORAGE_CLASSES))}"
        )
        return {"processed": 0, "succeeded": 0, "failed": 0}

    item_id = urlparse(stac_item_url).path.split("/")[-1]
    logger.info(f"Processing: {item_id}")
    logger.info(f"Target storage class: {storage_class}")

    if include_patterns:
        logger.info(f"Include patterns: {', '.join(include_patterns)}")
    if exclude_patterns:
        logger.info(f"Exclude patterns: {', '.join(exclude_patterns)}")

    # Fetch STAC item
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        resp = client.get(stac_item_url)
        resp.raise_for_status()
        stac_item = resp.json()

    # Extract S3 URLs
    s3_urls = extract_s3_urls(stac_item)
    if not s3_urls:
        logger.warning("No S3 URLs found in alternate assets")
        return {"processed": 0, "succeeded": 0, "failed": 0}

    logger.info(f"Found {len(s3_urls)} S3 URLs")

    # Get Zarr root
    zarr_root = get_zarr_root(s3_urls)
    if not zarr_root:
        logger.error("Could not find Zarr root")
        return {"processed": 0, "succeeded": 0, "failed": 0}

    logger.info(f"Zarr root: {zarr_root}")

    # Initialize S3 client
    s3_config = {}
    if s3_endpoint:
        s3_config["endpoint_url"] = s3_endpoint
    elif os.getenv("AWS_ENDPOINT_URL"):
        s3_config["endpoint_url"] = os.getenv("AWS_ENDPOINT_URL")  # type: ignore

    s3_client = boto3.client("s3", **s3_config)  # type: ignore

    # Parse bucket and prefix
    parsed = urlparse(zarr_root)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/") + "/"

    logger.info(f"Listing objects in s3://{bucket}/{prefix}")
    all_objects = list_objects(s3_client, bucket, prefix)
    logger.info(f"Found {len(all_objects)} total objects")

    # Filter objects based on patterns
    objects, excluded = filter_paths(all_objects, include_patterns, exclude_patterns, prefix)

    if include_patterns or exclude_patterns:
        logger.info(f"After filtering: {len(objects)} objects to process, {len(excluded)} excluded")

    # Process objects
    stats = {"processed": 0, "succeeded": 0, "failed": 0, "skipped": len(excluded)}
    storage_class_counts: dict[str, int] = {}

    # Separate objects that need changes from those that don't
    objects_to_change = [(key, current) for key, current in objects if current != storage_class]
    objects_already_correct = [
        (key, current) for key, current in objects if current == storage_class
    ]

    total_objects = len(objects)

    # Count storage class distribution
    for _, current_class in objects:
        storage_class_counts[current_class] = storage_class_counts.get(current_class, 0) + 1

    # Show initial distribution before processing
    if storage_class_counts:
        logger.info("")
        logger.info("Initial storage class distribution (before changes):")
        total = sum(storage_class_counts.values())
        for sc in sorted(storage_class_counts.keys()):
            count = storage_class_counts[sc]
            percentage = (count / total * 100) if total > 0 else 0
            logger.info(f"  {sc}: {count} objects ({percentage:.1f}%)")

        # Show expected distribution after changes
        if not dry_run and len(objects_to_change) > 0:
            logger.info("")
            logger.info("Expected storage class distribution (after changes):")
            expected_counts = storage_class_counts.copy()
            # Remove changed objects from their old classes
            for _, old_class in objects_to_change:
                expected_counts[old_class] = expected_counts.get(old_class, 0) - 1
                if expected_counts[old_class] == 0:
                    del expected_counts[old_class]
            # Add changed objects to target class
            expected_counts[storage_class] = expected_counts.get(storage_class, 0) + len(
                objects_to_change
            )

            expected_total = sum(expected_counts.values())
            for sc in sorted(expected_counts.keys()):
                count = expected_counts[sc]
                percentage = (count / expected_total * 100) if expected_total > 0 else 0
                logger.info(f"  {sc}: {count} objects ({percentage:.1f}%)")

        if dry_run:
            logger.info("  (DRY RUN)")
        logger.info("")

    logger.info(f"Processing {total_objects} objects...")
    logger.info(
        f"  {len(objects_already_correct)} already have target storage class {storage_class}"
    )
    logger.info(f"  {len(objects_to_change)} need to be changed")

    # Count objects that already have correct storage class (no API calls needed)
    stats["processed"] += len(objects_already_correct)
    stats["succeeded"] += len(objects_already_correct)

    # Process objects that need to change
    for processed_count, (obj_key, current_class) in enumerate(objects_to_change, start=1):
        stats["processed"] += 1

        success, _ = change_object_storage_class(
            s3_client, bucket, obj_key, current_class, storage_class, dry_run
        )
        if success:
            stats["succeeded"] += 1
        else:
            stats["failed"] += 1

        # Log progress every 100 objects or at the end
        if processed_count % 100 == 0 or processed_count == len(objects_to_change):
            logger.info(
                f"  Progress: {stats['processed']}/{total_objects} objects ({stats['processed']*100//total_objects}%)"
            )

    # Summary
    logger.info("=" * 60)
    logger.info(f"Summary for {item_id}:")
    logger.info(f"  Total objects: {len(all_objects)}")
    logger.info(f"  Skipped (filtered): {stats['skipped']}")
    logger.info(f"  Already correct storage class: {len(objects_already_correct)}")
    logger.info(f"  Changed: {len(objects_to_change)}")
    logger.info(f"  Succeeded: {stats['succeeded']}")
    logger.info(f"  Failed: {stats['failed']}")

    return stats


def main(argv: list[str] | None = None) -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Change S3 storage tier for STAC item")
    parser.add_argument("--stac-item-url", required=True, help="STAC item URL")
    parser.add_argument(
        "--storage-class",
        default="STANDARD",
        choices=["STANDARD", "STANDARD_IA", "EXPRESS_ONEZONE"],
        help="Target storage class",
    )
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
    parser.add_argument("--s3-endpoint", help="S3 endpoint URL")
    parser.add_argument(
        "--include-pattern",
        action="append",
        dest="include_patterns",
        help="Include paths matching this pattern (fnmatch, relative to Zarr root). Can be specified multiple times.",
    )
    parser.add_argument(
        "--exclude-pattern",
        action="append",
        dest="exclude_patterns",
        help="Exclude paths matching this pattern (fnmatch, relative to Zarr root). Can be specified multiple times.",
    )

    args = parser.parse_args(argv)

    try:
        stats = process_stac_item(
            args.stac_item_url,
            args.storage_class,
            args.dry_run,
            args.s3_endpoint,
            args.include_patterns,
            args.exclude_patterns,
        )
        return 1 if stats.get("failed", 0) > 0 else 0
    except Exception as e:
        logger.error(f"Failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
