#!/usr/bin/env python3
"""Shared utilities for S3 storage tier operations."""

from __future__ import annotations

import logging
import os
from typing import Literal, TypedDict
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# Valid OVH Cloud storage tiers (as confirmed in PR #62)
# https://github.com/EOPF-Explorer/data-pipeline/pull/62
OVHStorageTier = Literal["STANDARD", "STANDARD_IA", "EXPRESS_ONEZONE"]
VALID_STORAGE_TIERS: frozenset[str] = frozenset(["STANDARD", "STANDARD_IA", "EXPRESS_ONEZONE"])

# Special tier value for mixed storage detection
StorageTier = Literal["STANDARD", "STANDARD_IA", "EXPRESS_ONEZONE", "MIXED"]


class StorageTierInfo(TypedDict):
    """Storage tier information including distribution for mixed storage.

    Attributes:
        tier: Storage class - must be one of: STANDARD, STANDARD_IA, EXPRESS_ONEZONE, or MIXED
        distribution: Count of objects per tier (None for single files, dict for Zarr directories)
    """

    tier: str  # StorageTier - using str for backwards compatibility
    distribution: dict[str, int] | None


def validate_storage_tier(tier: str) -> bool:
    """Validate that a storage tier is a valid OVH Cloud storage class.

    Args:
        tier: Storage tier string to validate

    Returns:
        True if tier is valid (STANDARD, STANDARD_IA, or EXPRESS_ONEZONE), False otherwise

    Note:
        This function validates against actual OVH Cloud storage classes.
        Do not use AWS-specific tiers like GLACIER, GLACIER_IR, DEEP_ARCHIVE, etc.
    """
    return tier in VALID_STORAGE_TIERS


def get_s3_storage_class(s3_url: str, s3_endpoint: str) -> str | None:
    """Get the storage class of an S3 object.

    Args:
        s3_url: S3 URL (s3://bucket/key)
        s3_endpoint: S3 endpoint URL

    Returns:
        Storage class name (e.g., 'STANDARD', 'STANDARD_IA', 'EXPRESS_ONEZONE') or None if unavailable

    Note: This function returns the most common storage class for Zarr directories.
          Use get_s3_storage_info() for detailed distribution information.
    """
    if not s3_url.startswith("s3://"):
        return None

    parsed = urlparse(s3_url)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    if not key:  # No key specified (root bucket)
        return None

    try:
        # Initialize S3 client with endpoint
        s3_config = {}
        if s3_endpoint:
            s3_config["endpoint_url"] = s3_endpoint
        elif os.getenv("AWS_ENDPOINT_URL"):
            s3_config["endpoint_url"] = os.getenv("AWS_ENDPOINT_URL")  # type: ignore

        s3_client = boto3.client("s3", **s3_config)  # type: ignore

        # Try to query the key directly first
        try:
            response = s3_client.head_object(Bucket=bucket, Key=key)
        except ClientError as e:
            # If 404, this might be a Zarr directory path (without trailing /)
            # Try listing objects under this prefix
            if e.response.get("Error", {}).get("Code") == "404":
                prefix = key if key.endswith("/") else key + "/"
                list_response = s3_client.list_objects_v2(
                    Bucket=bucket,
                    Prefix=prefix,
                    MaxKeys=100,  # Check up to 100 files
                )
                if "Contents" not in list_response or len(list_response["Contents"]) == 0:
                    logger.debug(f"No objects found under prefix {s3_url}")
                    return None

                # Extract storage classes from list response (no need for additional head_object calls)
                storage_classes = []
                for obj in list_response["Contents"]:
                    # StorageClass field is included in list_objects_v2 response
                    obj_class = obj.get("StorageClass", "STANDARD")
                    storage_classes.append(obj_class)

                if not storage_classes:
                    logger.debug(f"Could not determine storage class for any object under {s3_url}")
                    return None

                # Count occurrences of each storage class
                storage_class_counts: dict[str, int] = {}
                for sc in storage_classes:
                    storage_class_counts[sc] = storage_class_counts.get(sc, 0) + 1

                # Check for mixed storage classes
                unique_classes = set(storage_classes)
                if len(unique_classes) > 1:
                    total_files = len(storage_classes)
                    distribution = ", ".join(
                        f"{sc}: {count}/{total_files}"
                        for sc, count in sorted(storage_class_counts.items())
                    )
                    logger.warning(
                        f"Mixed storage classes detected for {s3_url}: {distribution}. "
                        f"Returning most common class."
                    )

                # Return the most common storage class (or first alphabetically if tied)
                most_common = max(storage_class_counts.items(), key=lambda x: (x[1], x[0]))
                storage_class: str = most_common[0]
                logger.debug(
                    f"Sampled {len(storage_classes)} files under {s3_url}, "
                    f"storage class: {storage_class}"
                )
                return storage_class
            else:
                raise

        # StorageClass is not present for STANDARD tier, default to STANDARD
        storage_class_value: str = response.get("StorageClass", "STANDARD")
        return storage_class_value
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        logger.debug(f"ClientError ({error_code}) for {s3_url}: {e}")
        return None
    except Exception as e:
        logger.error(
            f"Unexpected error getting storage class for {s3_url}: {type(e).__name__}: {e}"
        )
        return None


def get_s3_storage_info(
    s3_url: str, s3_endpoint: str, max_samples: int = 100
) -> StorageTierInfo | None:
    """Get detailed storage tier information for an S3 object or Zarr directory.

    For single files, returns the storage class. For Zarr directories, samples up to
    max_samples files and returns distribution information. If storage classes are
    mixed, returns "MIXED" with a distribution dictionary.

    Args:
        s3_url: S3 URL (s3://bucket/key)
        s3_endpoint: S3 endpoint URL
        max_samples: Maximum number of files to sample for Zarr directories (default: 100)

    Returns:
        StorageTierInfo dict with 'tier' and 'distribution' keys, or None if unavailable
        - tier: Storage class ("STANDARD", "STANDARD_IA", "MIXED", etc.)
        - distribution: Dict of {storage_class: count} (only present if MIXED or multiple files sampled)

    Examples:
        Single file:
            {'tier': 'STANDARD', 'distribution': None}

        Zarr directory (uniform):
            {'tier': 'STANDARD_IA', 'distribution': {'STANDARD_IA': 50}}

        Zarr directory (mixed):
            {'tier': 'MIXED', 'distribution': {'STANDARD': 450, 'STANDARD_IA': 608}}
    """
    if not s3_url.startswith("s3://"):
        return None

    parsed = urlparse(s3_url)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    if not key:  # No key specified (root bucket)
        return None

    try:
        # Initialize S3 client with endpoint
        s3_config = {}
        if s3_endpoint:
            s3_config["endpoint_url"] = s3_endpoint
        elif os.getenv("AWS_ENDPOINT_URL"):
            s3_config["endpoint_url"] = os.getenv("AWS_ENDPOINT_URL")  # type: ignore

        s3_client = boto3.client("s3", **s3_config)  # type: ignore

        # Try to query the key directly first (single file)
        try:
            response = s3_client.head_object(Bucket=bucket, Key=key)
            storage_class = response.get("StorageClass", "STANDARD")
            return {"tier": storage_class, "distribution": None}
        except ClientError as e:
            # If 404, this might be a Zarr directory path
            if e.response.get("Error", {}).get("Code") == "404":
                prefix = key if key.endswith("/") else key + "/"
                list_response = s3_client.list_objects_v2(
                    Bucket=bucket, Prefix=prefix, MaxKeys=max_samples
                )
                if "Contents" not in list_response or len(list_response["Contents"]) == 0:
                    logger.debug(f"No objects found under prefix {s3_url}")
                    return None

                # Extract storage classes from list response
                storage_classes = []
                for obj in list_response["Contents"]:
                    obj_class = obj.get("StorageClass", "STANDARD")
                    storage_classes.append(obj_class)

                if not storage_classes:
                    logger.debug(f"Could not determine storage class for any object under {s3_url}")
                    return None

                # Count occurrences of each storage class
                storage_class_counts: dict[str, int] = {}
                for sc in storage_classes:
                    storage_class_counts[sc] = storage_class_counts.get(sc, 0) + 1

                # Check for mixed storage classes
                unique_classes = set(storage_classes)
                if len(unique_classes) > 1:
                    # Mixed storage detected
                    total_files = len(storage_classes)
                    distribution_str = ", ".join(
                        f"{sc}: {count}/{total_files}"
                        for sc, count in sorted(storage_class_counts.items())
                    )
                    logger.info(f"Mixed storage classes detected for {s3_url}: {distribution_str}")
                    return {"tier": "MIXED", "distribution": storage_class_counts}
                else:
                    # Uniform storage class
                    storage_class = list(unique_classes)[0]
                    logger.debug(
                        f"Sampled {len(storage_classes)} files under {s3_url}, "
                        f"uniform storage class: {storage_class}"
                    )
                    return {"tier": storage_class, "distribution": storage_class_counts}
            else:
                raise
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        logger.debug(f"ClientError ({error_code}) for {s3_url}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error getting storage info for {s3_url}: {type(e).__name__}: {e}")
        return None


def extract_region_from_endpoint(s3_endpoint: str) -> str:
    """Extract region from S3 endpoint URL.

    For OVHcloud endpoints like "s3.de.io.cloud.ovh.net", extracts region code.

    Args:
        s3_endpoint: S3 endpoint URL

    Returns:
        Region code (e.g., 'de', 'gra', 'sbg', 'uk', 'ca') or 'unknown'
    """
    endpoint_host = urlparse(s3_endpoint).netloc or urlparse(s3_endpoint).path

    if ".de." in endpoint_host:
        return "de"
    elif ".gra." in endpoint_host:
        return "gra"
    elif ".sbg." in endpoint_host:
        return "sbg"
    elif ".uk." in endpoint_host:
        return "uk"
    elif ".ca." in endpoint_host:
        return "ca"

    return "unknown"
