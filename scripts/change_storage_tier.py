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


def list_objects(s3_client, bucket: str, prefix: str) -> list[str]:
    """List all objects under S3 prefix."""
    objects = []
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            objects.append(obj["Key"])

    return objects


def filter_paths(
    paths: list[str],
    include_patterns: list[str] | None = None,
    exclude_patterns: list[str] | None = None,
    zarr_prefix: str = "",
) -> tuple[list[str], list[str]]:
    """Filter paths based on include/exclude patterns.

    Args:
        paths: List of object keys to filter
        include_patterns: List of fnmatch patterns to include (relative to Zarr root)
        exclude_patterns: List of fnmatch patterns to exclude (relative to Zarr root)
        zarr_prefix: The Zarr root prefix to strip when matching patterns

    Returns:
        Tuple of (filtered_paths, excluded_paths)
    """
    if not include_patterns and not exclude_patterns:
        return paths, []

    filtered = []
    excluded = []

    for path in paths:
        # Get relative path from Zarr root for pattern matching
        if zarr_prefix and path.startswith(zarr_prefix):
            relative_path = path[len(zarr_prefix) :]
        else:
            relative_path = path

        # Apply include patterns
        if include_patterns:
            included = any(fnmatch.fnmatch(relative_path, pattern) for pattern in include_patterns)
            if not included:
                excluded.append(path)
                continue

        # Apply exclude patterns
        if exclude_patterns:
            excluded_match = any(
                fnmatch.fnmatch(relative_path, pattern) for pattern in exclude_patterns
            )
            if excluded_match:
                excluded.append(path)
                continue

        filtered.append(path)

    return filtered, excluded


def change_object_storage_class(
    s3_client, bucket: str, key: str, storage_class: str, dry_run: bool
) -> bool:
    """Change storage class of single S3 object."""
    if dry_run:
        logger.debug(f"[DRY RUN] Would change s3://{bucket}/{key} to {storage_class}")
        return True

    try:
        head = s3_client.head_object(Bucket=bucket, Key=key)
        current = head.get("StorageClass", "STANDARD")

        if current == storage_class:
            logger.debug(f"Already {storage_class}: s3://{bucket}/{key}")
            return True

        s3_client.copy_object(
            Bucket=bucket,
            Key=key,
            CopySource={"Bucket": bucket, "Key": key},
            StorageClass=storage_class,
            MetadataDirective="COPY",
        )
        logger.debug(f"Changed {current} -> {storage_class}: s3://{bucket}/{key}")
        return True

    except ClientError as e:
        logger.error(f"Failed to change s3://{bucket}/{key}: {e}")
        return False


def process_stac_item(
    stac_item_url: str,
    storage_class: str,
    dry_run: bool,
    s3_endpoint: str | None,
    include_patterns: list[str] | None = None,
    exclude_patterns: list[str] | None = None,
) -> dict[str, int]:
    """Process STAC item and change storage tier."""
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
        s3_config["endpoint_url"] = os.getenv("AWS_ENDPOINT_URL")

    s3_client = boto3.client("s3", **s3_config)

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

    for obj_key in objects:
        stats["processed"] += 1
        if change_object_storage_class(s3_client, bucket, obj_key, storage_class, dry_run):
            stats["succeeded"] += 1
        else:
            stats["failed"] += 1

    # Summary
    logger.info("=" * 60)
    logger.info(f"Summary for {item_id}:")
    logger.info(f"  Total objects: {len(all_objects)}")
    logger.info(f"  Skipped (filtered): {stats['skipped']}")
    logger.info(f"  Processed: {stats['processed']}")
    logger.info(f"  Succeeded: {stats['succeeded']}")
    logger.info(f"  Failed: {stats['failed']}")
    if dry_run:
        logger.info("  (DRY RUN)")

    return stats


def main(argv: list[str] | None = None) -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Change S3 storage tier for STAC item")
    parser.add_argument("--stac-item-url", required=True, help="STAC item URL")
    parser.add_argument(
        "--storage-class",
        default="STANDARD",
        choices=["STANDARD", "GLACIER", "EXPRESS_ONEZONE"],
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
        return 1 if stats["failed"] > 0 else 0
    except Exception as e:
        logger.error(f"Failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
