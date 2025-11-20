#!/usr/bin/env python3
"""S2 Optimized GeoZarr conversion entry point - uses S2-specific converter."""

from __future__ import annotations

import argparse
import logging
import os
from urllib.parse import urlparse

import fsspec
import httpx
from eopf_geozarr.s2_optimized import convert_s2_optimized
from eopf_geozarr.conversion.fs_utils import get_storage_options

# Configure logging (set LOG_LEVEL=DEBUG for verbose output)
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)
for lib in ["botocore", "s3fs", "aiobotocore", "urllib3"]:
    logging.getLogger(lib).setLevel(logging.WARNING)


# === S2 Optimized Conversion Parameters ===

# Default parameters for S2 optimized conversion
DEFAULT_SPATIAL_CHUNK = 1024
DEFAULT_COMPRESSION_LEVEL = 5
DEFAULT_ENABLE_SHARDING = True
DEFAULT_DASK_CLUSTER = True


def get_zarr_url(stac_item_url: str) -> str:
    """Get Zarr asset URL from STAC item (priority: product, zarr, any .zarr)."""
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        assets = client.get(stac_item_url).raise_for_status().json().get("assets", {})

    # Try priority assets first
    for key in ["product", "zarr"]:
        if key in assets and (href := assets[key].get("href")):
            return str(href)

    # Fallback: any asset with .zarr in href
    for asset in assets.values():
        if ".zarr" in asset.get("href", ""):
            return str(asset["href"])

    raise RuntimeError("No Zarr asset found in STAC item")


# === Conversion Workflow ===


def run_conversion(
    source_url: str,
    collection: str,
    s3_output_bucket: str,
    s3_output_prefix: str,
    spatial_chunk: int | None = None,
    compression_level: int | None = None,
    enable_sharding: bool | None = None,
    use_dask_cluster: bool = False,
) -> str:
    """Run S2 Optimized GeoZarr conversion workflow.

    Args:
        source_url: Source STAC item URL or direct Zarr URL
        collection: Collection ID (for output path)
        s3_output_bucket: S3 bucket for output
        s3_output_prefix: S3 prefix for output
        spatial_chunk: Override spatial chunk size (default: 1024)
        compression_level: Compression level 1-9 (default: 5)
        enable_sharding: Enable sharding (default: True)
        use_dask_cluster: Use dask cluster for parallel processing

    Returns:
        Output Zarr URL (s3://...)
    """
    item_id = urlparse(source_url).path.rstrip("/").split("/")[-1]
    logger.info(f"🔄 Converting (S2 Optimized): {item_id}")
    logger.info(f"   Collection: {collection}")

    # Resolve source: STAC item or direct Zarr URL
    zarr_url = get_zarr_url(source_url) if "/items/" in source_url else source_url
    logger.info(f"   Source: {zarr_url}")

    # Apply defaults
    spatial_chunk = spatial_chunk or DEFAULT_SPATIAL_CHUNK
    compression_level = compression_level or DEFAULT_COMPRESSION_LEVEL
    enable_sharding = enable_sharding if enable_sharding is not None else DEFAULT_ENABLE_SHARDING
    use_dask_cluster = use_dask_cluster if use_dask_cluster is not None else DEFAULT_DASK_CLUSTER
    
    logger.info(
        f"   Parameters: chunk={spatial_chunk}, compression={compression_level}, sharding={enable_sharding}, dask={use_dask_cluster}"
    )

    # Construct output path and clean existing
    output_url = f"s3://{s3_output_bucket}/{s3_output_prefix}/{collection}/{item_id}.zarr"
    logger.info(f"   Output: {output_url}")

    try:
        fs = fsspec.filesystem("s3", client_kwargs={"endpoint_url": os.getenv("AWS_ENDPOINT_URL")})
        fs.rm(output_url, recursive=True)
        logger.info("   🧹 Cleaned existing output")
    except FileNotFoundError:
        pass
    except Exception as e:
        logger.warning(f"   ⚠️  Cleanup warning: {e}")

    # Run S2 optimized conversion
    convert_s2_optimized(
        input_path=zarr_url,
        output_path=output_url,
        spatial_chunk=spatial_chunk,
        compression_level=compression_level,
        enable_sharding=enable_sharding,
        use_dask_cluster=use_dask_cluster,
    )

    logger.info(f"✅ Conversion complete → {output_url}")
    return output_url


def main() -> None:
    """CLI entry point for S2 Optimized GeoZarr conversion."""
    parser = argparse.ArgumentParser(
        description="Convert EOPF Sentinel-2 Zarr to GeoZarr format using S2-optimized converter"
    )
    parser.add_argument("--source-url", required=True, help="Source STAC item or Zarr URL")
    parser.add_argument("--collection", required=True, help="Collection ID")
    parser.add_argument("--s3-output-bucket", required=True, help="S3 bucket")
    parser.add_argument("--s3-output-prefix", required=True, help="S3 prefix")
    parser.add_argument(
        "--spatial-chunk",
        type=int,
        default=DEFAULT_SPATIAL_CHUNK,
        help=f"Spatial chunk size (default: {DEFAULT_SPATIAL_CHUNK})",
    )
    parser.add_argument(
        "--compression-level",
        type=int,
        default=DEFAULT_COMPRESSION_LEVEL,
        help=f"Compression level 1-9 (default: {DEFAULT_COMPRESSION_LEVEL})",
    )
    parser.add_argument(
        "--enable-sharding",
        action="store_true",
        default=DEFAULT_ENABLE_SHARDING,
        help=f"Enable sharding (default: {DEFAULT_ENABLE_SHARDING})",
    )
    parser.add_argument(
        "--dask-cluster",
        action="store_true",
        default=DEFAULT_DASK_CLUSTER,
        help=f"Use dask cluster for parallel processing (default: {DEFAULT_DASK_CLUSTER})",
    )
    args = parser.parse_args()

    run_conversion(
        source_url=args.source_url,
        collection=args.collection,
        s3_output_bucket=args.s3_output_bucket,
        s3_output_prefix=args.s3_output_prefix,
        spatial_chunk=args.spatial_chunk,
        compression_level=args.compression_level,
        enable_sharding=args.enable_sharding,
        use_dask_cluster=args.dask_cluster,
    )


if __name__ == "__main__":
    main()
