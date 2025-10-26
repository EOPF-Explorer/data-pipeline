#!/usr/bin/env python3
"""Create STAC item for GeoZarr output from source item."""

from __future__ import annotations

import argparse
import logging
import re
from typing import Any
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


def normalize_r60m_href(href: str) -> str:
    """Add /0/ subdirectory to r60m paths to match GeoZarr output structure.

    GeoZarr conversion creates /0/ subdirectories for r60m resolution bands,
    but not for r10m or r20m. This normalizes r60m asset hrefs accordingly.

    Example: .../r60m/b09 → .../r60m/0/b09
    """
    if "/r60m/" not in href:
        return href

    # If already has /0/ or other digit subdirectory, don't modify
    if re.search(r"/r60m/\d+/", href):
        return href

    # Insert /0/ after /r60m/
    return re.sub(r"(/r60m)/", r"\1/0/", href)


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
) -> dict[str, Any]:
    """Create STAC item with GeoZarr product from source item.

    Preserves individual band assets and rewrites their hrefs to point to the
    GeoZarr output, allowing TiTiler to access bands correctly.

    Args:
        source_url: Source STAC item URL
        collection: Target collection
        geozarr_s3_url: S3 URL to GeoZarr output (s3://...)
        s3_endpoint: S3 endpoint for HTTP access

    Returns:
        STAC item dict with rewritten asset hrefs
    """
    logger.info(f"Fetching source item: {source_url}")
    resp = httpx.get(source_url, timeout=30.0, follow_redirects=True)
    resp.raise_for_status()
    source_item_dict = resp.json()

    item_dict: dict[str, Any] = source_item_dict.copy()
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

                    # Normalize r60m paths to include /0/ subdirectory (GeoZarr structure)
                    new_href = normalize_r60m_href(new_href)

                    # Convert to https if needed
                    if new_href.startswith("s3://"):
                        new_href = s3_to_https(new_href, s3_endpoint)

                    logger.info(f"  {asset_key}: {old_href} -> {new_href}")
                    asset_value["href"] = new_href
    else:
        logger.warning("No source Zarr base found in source item - assets not rewritten")

    logger.info(f"✅ Created item dict for {item_dict.get('id', 'unknown')}")
    logger.info(f"   Assets rewritten to: {geozarr_s3_url}")

    return item_dict


def main() -> None:
    """Main entry point."""
    import json

    parser = argparse.ArgumentParser(description="Create STAC item for GeoZarr output")
    parser.add_argument("--source-url", required=True, help="Source STAC item URL")
    parser.add_argument("--collection", required=True, help="Target collection ID")
    parser.add_argument("--geozarr-url", required=True, help="S3 URL to GeoZarr output (s3://...)")
    parser.add_argument("--s3-endpoint", required=True, help="S3 endpoint for HTTP access")
    parser.add_argument("--output", required=True, help="Output JSON file path")
    args = parser.parse_args()

    item_dict = create_geozarr_item(
        args.source_url,
        args.collection,
        args.geozarr_url,
        args.s3_endpoint,
    )

    with open(args.output, "w") as f:
        json.dump(item_dict, f, indent=2)

    logger.info(f"Wrote item to: {args.output}")


if __name__ == "__main__":
    main()
