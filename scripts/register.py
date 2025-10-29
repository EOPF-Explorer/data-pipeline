#!/usr/bin/env python3
"""STAC registration entry point - orchestrates item creation and registration."""

from __future__ import annotations

import argparse
import logging
import os
import sys
import urllib.parse
from typing import Any
from urllib.parse import urlparse

import httpx
import zarr
from pystac import Item, Link
from pystac.extensions.projection import ProjectionExtension
from pystac_client import Client

# Configure logging: INFO by default, DEBUG if LOG_LEVEL=DEBUG env var set
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Silence noisy third-party loggers
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("s3fs").setLevel(logging.WARNING)
logging.getLogger("aiobotocore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

EXPLORER_BASE = os.getenv("EXPLORER_BASE_URL", "https://explorer.eopf.copernicus.eu")


# === Item Creation ===


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


# === Registration ===


def register_item(
    stac_url: str,
    collection_id: str,
    item_dict: dict[str, Any],
    mode: str = "create-or-skip",
) -> None:
    """Register STAC item to STAC API with transaction support.

    Uses pystac-client's StacApiIO for HTTP operations to leverage
    existing session management, retry logic, and request modification.

    Args:
        stac_url: STAC API URL
        collection_id: Target collection
        item_dict: STAC item as dict
        mode: create-or-skip | upsert | replace

    Raises:
        Exception: If registration fails
    """
    item = Item.from_dict(item_dict)
    item_id = item.id

    # Open client to reuse its StacApiIO session
    client = Client.open(stac_url)

    # Check existence
    try:
        existing = client.get_collection(collection_id).get_item(item_id)
    except Exception:
        existing = None

    if existing:
        if mode == "create-or-skip":
            logger.info(f"Item {item_id} exists, skipping")
            return

        # Delete for upsert/replace using StacApiIO's session
        logger.info(f"Replacing {item_id}")
        delete_url = f"{stac_url}/collections/{collection_id}/items/{item_id}"
        try:
            resp = client._stac_io.session.delete(delete_url, timeout=30)
            if resp.status_code not in (200, 204):
                logger.warning(f"Delete returned {resp.status_code}")
        except Exception as e:
            logger.warning(f"Delete failed (item may not exist): {e}")

    # POST item using StacApiIO's session
    create_url = f"{stac_url}/collections/{collection_id}/items"
    item_json = item.to_dict()

    logger.debug(f"POST {create_url}")
    response = client._stac_io.session.post(
        create_url,
        json=item_json,
        headers={"Content-Type": "application/json"},
        timeout=client._stac_io.timeout or 30,
    )
    response.raise_for_status()

    logger.info(f"✅ Registered {item_id} (HTTP {response.status_code})")


# === Augmentation ===


def add_projection(item: Item) -> None:
    """Add ProjectionExtension from zarr spatial_ref attribute."""
    for asset in item.assets.values():
        if asset.media_type == "application/vnd+zarr" and asset.href:
            try:
                store = zarr.open(asset.href, mode="r")
                spatial_ref = store.attrs.get("spatial_ref", {})
                if epsg := spatial_ref.get("spatial_ref"):
                    proj_ext = ProjectionExtension.ext(item, add_if_missing=True)
                    proj_ext.epsg = int(epsg)
                    if wkt := spatial_ref.get("crs_wkt"):
                        proj_ext.wkt2 = wkt
                    return
            except Exception:
                continue


def _get_variable_path_from_asset(asset_href: str, variable_name: str = "grd") -> str | None:
    """Extract variable path from zarr asset href."""
    if not asset_href or ".zarr/" not in asset_href:
        return None
    parts = asset_href.split(".zarr/", 1)
    if len(parts) == 2:
        return f"/{parts[1]}:{variable_name}"
    return None


def _add_tile_links(item: Item, base_url: str, query: str, title: str) -> None:
    """Add xyz and tilejson links with given query parameters."""
    item.add_link(
        Link(
            "xyz",
            f"{base_url}/tiles/WebMercatorQuad/{{z}}/{{x}}/{{y}}.png?{query}",
            "image/png",
            title,
        )
    )
    item.add_link(
        Link(
            "tilejson",
            f"{base_url}/WebMercatorQuad/tilejson.json?{query}",
            "application/json",
            f"TileJSON for {item.id}",
        )
    )


def add_visualization(item: Item, raster_base: str, collection_id: str) -> None:
    """Add viewer/xyz/tilejson links via titiler."""
    base_url = f"{raster_base}/collections/{collection_id}/items/{item.id}"
    item.add_link(Link("viewer", f"{base_url}/viewer", "text/html", f"Viewer for {item.id}"))

    # Detect mission from collection ID
    coll_lower = collection_id.lower()

    if coll_lower.startswith(("sentinel-1", "sentinel1")):
        # S1: Extract dynamic variable path from vh asset
        vh = item.assets.get("vh")
        if vh and (var_path := _get_variable_path_from_asset(vh.href)):
            query = f"variables={urllib.parse.quote(var_path, safe='')}&bidx=1&rescale=0%2C219&assets=vh"
            _add_tile_links(item, base_url, query, "Sentinel-1 GRD VH")

    elif coll_lower.startswith(("sentinel-2", "sentinel2")):
        # S2: Use colon separator for TiTiler variable path
        var_path = "/quality/l2a_quicklook/r10m:tci"
        query = (
            f"variables={urllib.parse.quote(var_path, safe='')}&bidx=1&bidx=2&bidx=3&assets=TCI_10m"
        )
        _add_tile_links(item, base_url, query, "Sentinel-2 L2A True Color")

    item.add_link(
        Link(
            "via",
            f"{EXPLORER_BASE}/collections/{collection_id.lower().replace('_', '-')}/items/{item.id}",
            title="EOPF Explorer",
        )
    )


def augment(item: Item, *, raster_base: str, collection_id: str) -> Item:
    """Augment STAC item with CRS metadata and preview links.

    Args:
        item: STAC item to augment
        raster_base: TiTiler raster API base URL
        collection_id: Collection ID for viewer links

    Returns:
        Augmented item (modified in place)
    """
    add_projection(item)
    add_visualization(item, raster_base, collection_id)
    return item


# === Registration Workflow ===


def run_registration(
    source_url: str,
    collection: str,
    stac_api_url: str,
    raster_api_url: str,
    s3_endpoint: str,
    s3_output_bucket: str,
    s3_output_prefix: str,
    mode: str = "create-or-skip",
) -> None:
    """Run STAC registration workflow.

    Args:
        source_url: Source STAC item URL
        collection: Target collection ID
        stac_api_url: STAC API base URL
        raster_api_url: TiTiler raster API base URL
        s3_endpoint: S3 endpoint for HTTP access
        s3_output_bucket: S3 bucket name
        s3_output_prefix: S3 prefix path
        mode: Registration mode (create-or-skip | upsert | replace)

    Raises:
        RuntimeError: If registration fails
    """
    # Extract item ID from source URL and construct geozarr URL
    item_id = urlparse(source_url).path.rstrip("/").split("/")[-1]
    geozarr_url = f"s3://{s3_output_bucket}/{s3_output_prefix}/{collection}/{item_id}.zarr"

    logger.info(f"Starting registration for {item_id} in {collection}")
    logger.info(f"GeoZarr: {geozarr_url}")
    # Step 1: Create STAC item from source
    logger.info("Creating STAC item from source...")
    item_dict = create_geozarr_item(source_url, collection, geozarr_url, s3_endpoint)

    # Step 2: Register to STAC API
    logger.info("Registering item in STAC API...")
    register_item(stac_api_url, collection, item_dict, mode)

    # Step 3: Augment with preview links and CRS metadata
    logger.info("Adding preview links and metadata...")
    logger.info(f"  Raster API: {raster_api_url}")

    # Fetch the registered item
    item_url = f"{stac_api_url.rstrip('/')}/collections/{collection}/items/{item_id}"
    r = httpx.get(item_url, timeout=30.0)
    r.raise_for_status()
    item = Item.from_dict(r.json())

    # Augment in place
    augment(item, raster_base=raster_api_url, collection_id=collection)

    # Update via PUT
    r = httpx.put(
        item_url,
        json=item.to_dict(),
        headers={"Content-Type": "application/json"},
        timeout=30.0,
    )
    r.raise_for_status()

    logger.info(f"✅ Registered and augmented {item_id} in {collection}")
    logger.info(f"   STAC API: {stac_api_url}/collections/{collection}/items/{item_id}")
    logger.info(f"   GeoZarr:  {geozarr_url}")


def main(argv: list[str] | None = None) -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Run STAC registration workflow")
    parser.add_argument("--source-url", required=True, help="Source STAC item URL")
    parser.add_argument("--collection", required=True, help="Target collection ID")
    parser.add_argument("--stac-api-url", required=True, help="STAC API base URL")
    parser.add_argument("--raster-api-url", required=True, help="TiTiler raster API base URL")
    parser.add_argument("--s3-endpoint", required=True, help="S3 endpoint for HTTP access")
    parser.add_argument("--s3-output-bucket", required=True, help="S3 bucket name")
    parser.add_argument("--s3-output-prefix", required=True, help="S3 prefix path")
    parser.add_argument(
        "--mode",
        default="create-or-skip",
        choices=["create-or-skip", "upsert", "replace"],
        help="Registration mode (default: create-or-skip)",
    )

    args = parser.parse_args(argv)

    try:
        run_registration(
            args.source_url,
            args.collection,
            args.stac_api_url,
            args.raster_api_url,
            args.s3_endpoint,
            args.s3_output_bucket,
            args.s3_output_prefix,
            args.mode,
        )
        return 0
    except Exception as e:
        logger.error(f"Registration failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
