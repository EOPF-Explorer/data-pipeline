#!/usr/bin/env python3
"""Create STAC item for GeoZarr output from source item."""

from __future__ import annotations

import argparse
import json
import logging
from urllib.parse import urlparse

import httpx

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def s3_to_https(s3_url: str, endpoint: str) -> str:
    """Convert s3:// URL to https:// using endpoint."""
    if not s3_url.startswith("s3://"):
        return s3_url

    parsed = urlparse(s3_url)
    bucket = parsed.netloc
    path = parsed.path.lstrip("/")

    # Parse endpoint to get host
    endpoint_parsed = urlparse(endpoint)
    host = endpoint_parsed.netloc or endpoint_parsed.path

    return f"https://{bucket}.{host}/{path}"


def normalize_asset_href(href: str) -> str:
    """Normalize asset href to match GeoZarr output structure.

    GeoZarr stores bands in overview-level subdirectories (0/, 1/, 2/, ...).
    For Sentinel-2 r60m bands which exist as direct subdirectories in source,
    we insert '/0/' to align with GeoZarr's overview structure.
    """
    if "/r60m/" not in href:
        return href

    parts = href.split("/r60m/")
    if len(parts) != 2:
        return href

    base, rest = parts
    # If already has /0/ or /1/ etc, don't modify
    if rest and rest[0].isdigit() and rest[1:2] == "/":
        return href

    # Insert /0/ for native resolution
    return f"{base}/r60m/0/{rest}"


def find_source_zarr_base(source_item: dict) -> str | None:
    """Find the base Zarr URL from source item assets."""
    for asset in source_item.get("assets", {}).values():
        if isinstance(asset, dict) and "href" in asset:
            href: str = asset["href"]
            if ".zarr/" in href:
                return href.split(".zarr/")[0] + ".zarr"
    return None


def create_geozarr_item(
    source_url: str,
    collection: str,
    geozarr_s3_url: str,
    s3_endpoint: str,
    output_path: str,
) -> None:
    """Create STAC item with GeoZarr product from source item.

    Preserves individual band assets and rewrites their hrefs to point to the
    GeoZarr output, allowing TiTiler to access bands correctly.

    Args:
        source_url: Source STAC item URL
        collection: Target collection
        geozarr_s3_url: S3 URL to GeoZarr output (s3://...)
        s3_endpoint: S3 endpoint for HTTP access
        output_path: Path to write item JSON
    """
    logger.info(f"Fetching source item: {source_url}")
    resp = httpx.get(source_url, timeout=30.0, follow_redirects=True)
    resp.raise_for_status()
    source_item_dict = resp.json()

    # Work with dict to preserve all source metadata
    item_dict = json.loads(json.dumps(source_item_dict))

    # Update collection
    item_dict["collection"] = collection

    # Find source Zarr base URL from existing assets
    source_zarr_base = find_source_zarr_base(source_item_dict)

    if source_zarr_base:
        # Ensure both bases end consistently with /
        if not source_zarr_base.endswith("/"):
            source_zarr_base += "/"
        output_zarr_base = geozarr_s3_url.rstrip("/") + "/"
        logger.info(f"Rewriting asset hrefs: {source_zarr_base} -> {output_zarr_base}")

        # Rewrite all asset hrefs from source Zarr to output GeoZarr
        for asset_key, asset_value in list(item_dict.get("assets", {}).items()):
            if isinstance(asset_value, dict) and "href" in asset_value:
                old_href = asset_value["href"]
                if old_href.startswith(source_zarr_base):
                    # Extract subpath and append to output base
                    subpath = old_href[len(source_zarr_base) :]
                    new_href = output_zarr_base + subpath

                    # Normalize asset href to match GeoZarr structure
                    new_href = normalize_asset_href(new_href)

                    # Convert to https if needed
                    if new_href.startswith("s3://"):
                        new_href = s3_to_https(new_href, s3_endpoint)

                    logger.info(f"  {asset_key}: {old_href} -> {new_href}")
                    asset_value["href"] = new_href

    # Write to output (skip local pystac validation - let STAC API validate)
    # The source items have inconsistent raster properties (some assets have them, some don't)
    # but they validate fine in the STAC API, so we preserve the source structure as-is
    with open(output_path, "w") as f:
        json.dump(item_dict, f, indent=2)

    logger.info(f"✅ Created item JSON: {output_path}")
    logger.info(f"   Assets rewritten to: {geozarr_s3_url}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-url", required=True, help="Source STAC item URL")
    parser.add_argument("--collection", required=True)
    parser.add_argument("--geozarr-url", required=True, help="S3 URL to GeoZarr")
    parser.add_argument("--s3-endpoint", required=True)
    parser.add_argument("--output", required=True, help="Output JSON path")
    args = parser.parse_args()

    create_geozarr_item(
        args.source_url,
        args.collection,
        args.geozarr_url,
        args.s3_endpoint,
        args.output,
    )


if __name__ == "__main__":
    main()
