#!/usr/bin/env python3
"""Shared S3-deletion helpers for STAC items.

Extracted from ``operator-tools/manage_item.py`` (coordination#183) so both the
operator CLIs and the ``scripts/``-hosted cleanup cron can share one
recursive-delete implementation. ``scripts/`` is baked into the pipeline image
(``docker/Dockerfile``) whereas ``operator-tools/`` is not, so this module must
not depend on ``click`` — batch progress is emitted through ``logging``.

Behaviour preserved from the original:
- assets are resolved to S3 URLs via ``alternate.s3.href`` then main ``href``
- ``.zarr/`` URLs are expanded to every object under the store root
- deletes are issued in 200-key batches
- a ``NoSuchKey`` delete error counts as already-deleted, not a failure
"""

import logging
import os
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse

from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# Single source of truth for the retention window (coordination#183).
# Shared by register_v1 (stamp at registration) and the backfill migration.
DEFAULT_RETENTION_DAYS = 183

# STAC timestamps extension that defines properties.expires. Shared by the
# register-time stamp and the backfill migration so both declare the same URL.
TIMESTAMPS_EXTENSION = "https://stac-extensions.github.io/timestamps/v1.1.0/schema.json"

# Canonical rendering of properties.expires. LOAD-BEARING: pgstac compares
# `expires` as a JSONB string, so string ordering must equal chronological
# ordering. A single zero-padded UTC "Z" format guarantees that. Every producer
# (register, backfill) and the cleanup discovery query MUST use this one format
# — do not introduce a second one (coordination#183, verified live 2026-07-10).
EXPIRES_TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# S3 delete_objects accepts at most 1000 keys per call; we stay well under.
BATCH_SIZE = 200


def format_expires(dt: datetime) -> str:
    """Render a datetime as the canonical STAC ``expires`` timestamp."""
    return dt.strftime(EXPIRES_TS_FORMAT)


def parse_stac_timestamp(value: str) -> datetime:
    """Parse a STAC RFC3339 timestamp (``Z`` or ``+00:00``) to aware UTC."""
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def env_int(name: str, default: int) -> int:
    """Read an int env var. Unset **or empty** returns ``default`` — an empty
    value in a manifest (e.g. ``EXPIRES_RETENTION_DAYS: ""``) must not crash the
    caller. ``"0"`` is honoured as 0."""
    value = os.getenv(name)
    return int(value) if value else default


def load_exclude_ids(path: str | None) -> set[str]:
    """Read a newline-delimited item-ID denylist. Blank lines and ``#`` comments
    are ignored. Shared by the cleanup script (``--exclude-file``) and the
    backfill migration (``EXPIRES_EXCLUDE_FILE``) so the format cannot drift."""
    if not path:
        return set()
    ids: set[str] = set()
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                ids.add(stripped)
    return ids


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


def _collect_keys_by_bucket(
    s3_client: Any,
    s3_urls: set[str],
) -> dict[str, list[str]]:
    """Resolve a set of S3 URLs to concrete object keys per bucket.

    Zarr stores and directory prefixes are expanded via list_objects_v2;
    individual files are kept as-is.
    """
    urls_by_bucket: dict[str, list[str]] = defaultdict(list)
    for url in s3_urls:
        parsed = urlparse(url)
        urls_by_bucket[parsed.netloc].append(url)

    keys_by_bucket: dict[str, list[str]] = defaultdict(list)
    for bucket, urls in urls_by_bucket.items():
        prefixes_to_delete = set()
        individual_keys = set()
        for url in urls:
            key = urlparse(url).path.lstrip("/")
            if ".zarr/" in key:
                zarr_root = key.split(".zarr/")[0] + ".zarr/"
                prefixes_to_delete.add(zarr_root)
            elif key.endswith("/"):
                prefixes_to_delete.add(key)
            else:
                individual_keys.add(key)

        for prefix in prefixes_to_delete:
            # Fail closed: an unlistable prefix means we cannot know its object
            # set. Silently treating it as empty would let the caller "validate
            # 0 remaining" and delete the STAC item while the data lives on
            # (orphaned). Let the ClientError propagate so the caller keeps the
            # item and reports s3_validation_failed.
            paginator = s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    keys_by_bucket[bucket].append(obj["Key"])

        keys_by_bucket[bucket].extend(individual_keys)

    return keys_by_bucket


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

    keys_by_bucket = _collect_keys_by_bucket(s3_client, s3_urls)
    total = sum(len(keys) for keys in keys_by_bucket.values())
    processed = 0

    for bucket, keys in keys_by_bucket.items():
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
            processed += len(batch)
            logger.info("Deleting S3 objects: %d/%d (bucket=%s)", processed, total, bucket)

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
    urls_by_bucket: dict[str, list[str]] = defaultdict(list)
    for url in s3_urls:
        parsed = urlparse(url)
        urls_by_bucket[parsed.netloc].append(url)

    for bucket, urls in urls_by_bucket.items():
        # Determine if we need to handle prefixes
        prefixes_to_check = set()
        individual_keys = set()

        for url in urls:
            key = urlparse(url).path.lstrip("/")

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
            # Fail closed (see _collect_keys_by_bucket): an unlistable prefix must
            # not be silently counted as 0 — that defeats the validate-0 gate the
            # caller relies on before deleting the STAC item. Let it propagate.
            paginator = s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                count += len(page.get("Contents", []))

        # Count individual files
        for key in individual_keys:
            try:
                s3_client.head_object(Bucket=bucket, Key=key)
                count += 1
            except ClientError as exc:
                # A genuinely-absent key (404) legitimately counts as 0; any other
                # error is unverifiable state and must propagate (fail closed).
                code = exc.response.get("Error", {}).get("Code", "")
                if code not in ("404", "NoSuchKey", "NotFound"):
                    raise
                logger.debug("head_object 404 for s3://%s/%s — not counted", bucket, key)

    return count
