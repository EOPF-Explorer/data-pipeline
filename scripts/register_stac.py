#!/usr/bin/env python3
"""Simplified GeoZarr STAC registration.

Registers a GeoZarr output to a STAC API by:
1. Fetching the source STAC item
2. Creating GeoZarr assets for each group
3. Merging with source metadata
4. POST/PUT to STAC transactions API
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from typing import Any, cast
from urllib.parse import urlparse

import httpx
import xarray as xr
from tenacity import retry, stop_after_attempt, wait_exponential

# Config: override via env vars
TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "30"))
RETRIES = int(os.getenv("RETRY_ATTEMPTS", "3"))
MAX_WAIT = int(os.getenv("RETRY_MAX_WAIT", "60"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@retry(stop=stop_after_attempt(RETRIES), wait=wait_exponential(min=2, max=MAX_WAIT))
def fetch_json(url: str, headers: dict[str, str] | None = None) -> dict[str, Any]:
    """Fetch JSON from URL with automatic retry on transient failures."""
    response = httpx.get(url, timeout=TIMEOUT, headers=headers or {})
    response.raise_for_status()
    return cast(dict[str, Any], response.json())


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
    This function ensures band paths point to the native resolution (level 0).

    For Sentinel-2 r60m bands, which exist as direct subdirectories in source data,
    we insert '/0/' to align with GeoZarr's overview structure.

    Args:
        href: Asset href URL

    Returns:
        Normalized href with level 0 path if needed

    Examples:
        >>> normalize_asset_href("s3://bucket/data.zarr/r60m/b01")
        "s3://bucket/data.zarr/r60m/0/b01"
        >>> normalize_asset_href("s3://bucket/data.zarr/r10m/b02")
        "s3://bucket/data.zarr/r10m/b02"
        >>> normalize_asset_href("s3://bucket/data.zarr/r60m/0/b01")
        "s3://bucket/data.zarr/r60m/0/b01"
    """
    # Pattern: /r<resolution>m/<band> where band is a leaf name (no '/')
    # and doesn't start with a digit (would be an overview level)
    # Only r60m needs this fix due to its subdirectory structure
    if "/r60m/" not in href:
        return href

    parts = href.split("/r60m/")
    if len(parts) != 2:
        return href

    base, suffix = parts
    # Check if suffix is a band name (no slash, not a digit)
    if "/" not in suffix and not suffix[0].isdigit():
        return f"{base}/r60m/0/{suffix}"

    return href


def clean_stac_item_metadata(item: dict[str, Any]) -> None:
    """Remove invalid/deprecated projection metadata from STAC item.

    Modifies item in-place to:
    - Remove proj:shape, proj:transform, proj:code from item properties
    - Remove proj:epsg, proj:code, storage:options from all assets

    These cleanups prevent TiTiler coordinate confusion and STAC API validation errors.

    Args:
        item: STAC item dictionary (modified in-place)
    """
    # Clean item properties
    if "properties" in item:
        removed = []
        for key in ["proj:shape", "proj:transform", "proj:code"]:
            if item["properties"].pop(key, None) is not None:
                removed.append(key)
        if removed:
            logger.info(f"  Cleaned item properties: removed {', '.join(removed)}")

    # Clean all assets
    if "assets" in item:
        for asset_key, asset_value in list(item["assets"].items()):
            if isinstance(asset_value, dict):
                for key in ["proj:epsg", "proj:code", "storage:options"]:
                    if key in asset_value:
                        asset_value.pop(key)
                        logger.info(f"  Removed {key} from asset {asset_key}")


def find_source_zarr_base(source_item: dict[str, Any]) -> str | None:
    """Extract base Zarr URL from source item assets.

    Args:
        source_item: Source STAC item

    Returns:
        Base Zarr URL (ending with .zarr/) or None if not found
    """
    if "assets" not in source_item:
        return None

    for asset in source_item["assets"].values():
        if not isinstance(asset, dict) or "href" not in asset:
            continue

        asset_href: str = asset["href"]
        if not isinstance(asset_href, str) or ".zarr" not in asset_href:
            continue

        # Extract base: everything up to and including .zarr/
        zarr_end = asset_href.find(".zarr/")
        if zarr_end != -1:
            return asset_href[: zarr_end + 6]  # include ".zarr/" (6 chars)

        # Or just .zarr at the end
        if asset_href.endswith(".zarr"):
            return asset_href + "/"  # Add trailing slash

    return None


def extract_projection_metadata(zarr_url: str) -> dict[str, Any]:
    """Extract proj:bbox, proj:shape, proj:transform from Zarr store.

    Args:
        zarr_url: URL to Zarr array (s3:// or https://)

    Returns:
        Dictionary with proj:bbox, proj:shape, proj:transform, proj:code
    """
    try:
        # Open zarr store with anonymous access for public S3
        ds = xr.open_zarr(zarr_url, storage_options={"anon": True})

        # Get spatial coordinates
        if "x" not in ds.coords or "y" not in ds.coords:
            logger.info(f"  Warning: Zarr missing x/y coordinates: {zarr_url}")
            return {}

        x = ds.coords["x"].values
        y = ds.coords["y"].values

        # Get array shape (assuming first data variable)
        data_vars = list(ds.data_vars)
        if not data_vars:
            logger.info(f"  Warning: Zarr has no data variables: {zarr_url}")
            return {}

        shape = ds[data_vars[0]].shape
        height, width = shape[-2:]  # Last two dimensions are y, x

        # Calculate bounds
        x_min, x_max = float(x.min()), float(x.max())
        y_min, y_max = float(y.min()), float(y.max())

        # Calculate pixel resolution
        x_res = (x_max - x_min) / (width - 1) if width > 1 else 10.0
        y_res = (y_max - y_min) / (height - 1) if height > 1 else 10.0

        # Adjust bounds to pixel edges (coordinates are cell centers)
        left = x_min - x_res / 2
        right = x_max + x_res / 2
        top = y_max + abs(y_res) / 2  # y typically decreases
        bottom = y_min - abs(y_res) / 2

        # Get CRS
        crs_code = None
        crs_epsg = None
        if hasattr(ds, "rio") and ds.rio.crs:
            crs = ds.rio.crs
            crs_epsg = crs.to_epsg()
            if crs_epsg:
                crs_code = f"EPSG:{crs_epsg}"
        elif "spatial_ref" in ds.coords:
            # Try to extract from spatial_ref coordinate
            spatial_ref = ds.coords["spatial_ref"]
            if hasattr(spatial_ref, "attrs") and "spatial_ref" in spatial_ref.attrs:
                import rasterio.crs

                try:
                    crs = rasterio.crs.CRS.from_wkt(spatial_ref.attrs["spatial_ref"])
                    crs_epsg = crs.to_epsg()
                    if crs_epsg:
                        crs_code = f"EPSG:{crs_epsg}"
                except Exception:
                    pass

        # Create affine transform
        # Affine transform: [a, b, c, d, e, f, 0, 0, 1]
        # where: x' = a*col + b*row + c, y' = d*col + e*row + f
        # For north-up images: a=x_res, b=0, c=left, d=0, e=-abs(y_res), f=top
        transform = [
            x_res,  # a: pixel width
            0,  # b: rotation (0 for north-up)
            left,  # c: left edge
            0,  # d: rotation (0 for north-up)
            -abs(y_res),  # e: pixel height (negative for north-up)
            top,  # f: top edge
            0,  # padding
            0,  # padding
            1,  # scale
        ]

        # Build result dict
        result: dict[str, Any] = {
            "proj:bbox": [left, bottom, right, top],
            "proj:shape": [int(height), int(width)],
            "proj:transform": transform,
        }

        if crs_code:
            result["proj:code"] = crs_code
        if crs_epsg:
            result["proj:epsg"] = crs_epsg

        logger.info(
            f"  Extracted projection metadata: bbox={result['proj:bbox'][:2]}..., shape={result['proj:shape']}, crs={crs_code}"
        )
        return result

    except Exception as e:
        logger.info(f"  Warning: Could not extract projection metadata from {zarr_url}: {e}")
        return {}


def create_geozarr_item(
    source_item: dict[str, Any],
    geozarr_url: str,
    item_id: str | None = None,
    s3_endpoint: str | None = None,
    collection_id: str | None = None,
) -> dict[str, Any]:
    """Create STAC item for GeoZarr output by copying and adapting source item.

    Args:
        source_item: Source STAC item to copy metadata from
        geozarr_url: URL to the GeoZarr store (s3:// or https://)
        item_id: Optional item ID override (defaults to source item ID)
        s3_endpoint: S3 endpoint for translating s3:// to https://
        collection_id: Optional collection ID to set (defaults to source collection)

    Returns:
        New STAC item dict with merged metadata and GeoZarr assets
    """
    # Start with a copy of source item
    item: dict[str, Any] = json.loads(json.dumps(source_item))

    # Override ID if provided
    if item_id:
        item["id"] = item_id

    # Override collection if provided
    if collection_id:
        item["collection"] = collection_id

    # Clean invalid projection metadata from item
    clean_stac_item_metadata(item)

    # Convert s3:// to https:// if needed
    href = geozarr_url
    if href.startswith("s3://") and s3_endpoint:
        href = s3_to_https(href, s3_endpoint)

    # Find source Zarr base URL from existing assets
    source_zarr_base = find_source_zarr_base(source_item)

    # Rewrite all asset hrefs from source Zarr to output GeoZarr
    # This makes TiTiler able to read the converted data with proper CRS
    if "assets" not in item:
        item["assets"] = {}

    if source_zarr_base:
        # Ensure both bases end consistently with /
        if not source_zarr_base.endswith("/"):
            source_zarr_base += "/"
        output_zarr_base = geozarr_url.rstrip("/") + "/"
        logger.info(f"Rewriting asset hrefs: {source_zarr_base} -> {output_zarr_base}")

        for asset_key, asset_value in list(item["assets"].items()):
            if isinstance(asset_value, dict) and "href" in asset_value:
                old_href = asset_value["href"]
                if old_href.startswith(source_zarr_base):
                    # Extract subpath and append to output base
                    subpath = old_href[len(source_zarr_base) :]
                    new_href = output_zarr_base + subpath

                    # Normalize asset href to match GeoZarr structure
                    new_href = normalize_asset_href(new_href)

                    # Convert to https if needed
                    if new_href.startswith("s3://") and s3_endpoint:
                        new_href = s3_to_https(new_href, s3_endpoint)

                    logger.info(f"  {asset_key}: {old_href} -> {new_href}")
                    asset_value["href"] = new_href

    # NOTE: Do NOT add a main geozarr asset - it confuses TiTiler's bounds calculation
    # TiTiler works correctly when it reads individual band assets directly
    # item["assets"]["geozarr"] = {
    #     "href": href,
    #     "type": "application/vnd+zarr",
    #     "title": "GeoZarr Data",
    #     "roles": ["data", "zarr", "geozarr"],
    # }

    # Add derived_from link to source item if not present
    source_href = source_item.get("links", [])
    for link in source_href:
        if link.get("rel") == "self":
            source_self = link.get("href")
            if source_self:
                has_derived = any(
                    lnk.get("rel") == "derived_from" and lnk.get("href") == source_self
                    for lnk in item.get("links", [])
                )
                if not has_derived:
                    if "links" not in item:
                        item["links"] = []
                    item["links"].append(
                        {
                            "rel": "derived_from",
                            "href": source_self,
                            "type": "application/json",
                        }
                    )
            break

    return item


def register_item(
    stac_url: str,
    collection_id: str,
    item: dict[str, Any],
    mode: str = "create-or-skip",
    headers: dict[str, str] | None = None,
) -> None:
    """Register item to STAC API.

    Args:
        stac_url: Base URL of STAC API
        collection_id: Collection ID to register to
        item: STAC item dict
        mode: Registration mode (create-or-skip, upsert, replace)
        headers: Optional HTTP headers (for auth)
    """
    item_id = item["id"]
    items_url = f"{stac_url.rstrip('/')}/collections/{collection_id}/items"
    item_url = f"{items_url}/{item_id}"

    headers = headers or {}
    headers["Content-Type"] = "application/json"

    with httpx.Client(timeout=TIMEOUT) as client:
        # Check if item exists
        try:
            response = client.get(item_url, headers=headers)
            exists = response.status_code == 200
        except httpx.HTTPError:
            exists = False

        if exists:
            logger.info(f"Item {item_id} already exists")

            if mode == "create-or-skip":
                logger.info("Skipping (mode=create-or-skip)")
                return
            elif mode in ("upsert", "update"):
                logger.info("Updating existing item (mode=upsert)")
                response = client.put(item_url, json=item, headers=headers)
                if response.status_code >= 400:
                    logger.error(f" {response.status_code} {response.reason_phrase}")
                    logger.info(f"Response body: {response.text}")
                response.raise_for_status()
                logger.info(f"Successfully updated item {item_id}")
            elif mode in ("force", "replace"):
                logger.info("Deleting and recreating (mode=replace)")
                client.delete(item_url, headers=headers)
                response = client.post(items_url, json=item, headers=headers)
                if response.status_code >= 400:
                    logger.error(f" {response.status_code} {response.reason_phrase}")
                    logger.info(f"Response body: {response.text}")
                response.raise_for_status()
                logger.info(f"Successfully replaced item {item_id}")
            else:
                raise ValueError(f"Unknown mode: {mode}")
        else:
            logger.info(f"Creating new item {item_id}")
            response = client.post(items_url, json=item, headers=headers)
            if response.status_code >= 400:
                logger.error(f" {response.status_code} {response.reason_phrase}")
                logger.info(f"Response body: {response.text}")
            response.raise_for_status()
            logger.info(f"Successfully created item {item_id}")


def main() -> int:
    """CLI entrypoint."""
    start_time = time.perf_counter()

    parser = argparse.ArgumentParser(description="Register GeoZarr output to STAC API")
    parser.add_argument(
        "--stac",
        required=True,
        help="Base URL of STAC API",
    )
    parser.add_argument(
        "--collection",
        required=True,
        help="Collection ID to register to",
    )
    parser.add_argument(
        "--item-id",
        required=True,
        help="Item ID for the registered item",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="GeoZarr output URL (s3:// or https://)",
    )
    parser.add_argument(
        "--src-item",
        required=True,
        help="Source STAC item URL to fetch and merge metadata from",
    )
    parser.add_argument(
        "--s3-endpoint",
        help="S3 endpoint for translating s3:// URLs to https://",
    )
    parser.add_argument(
        "--mode",
        choices=["create-or-skip", "upsert", "update", "force", "replace"],
        default="update",
        help="Registration mode (default: update - create new or update existing)",
    )
    parser.add_argument(
        "--bearer-token",
        help="Bearer token for STAC API authentication",
    )

    args = parser.parse_args()

    try:
        # Fetch source item
        logger.info(f"Fetching source item from {args.src_item}")
        source_item = fetch_json(args.src_item)
        logger.info(f"Source item ID: {source_item['id']}")

        # Create merged item with GeoZarr assets
        logger.info(f"Creating GeoZarr item for {args.output}")
        item = create_geozarr_item(
            source_item=source_item,
            geozarr_url=args.output,
            item_id=args.item_id,
            s3_endpoint=args.s3_endpoint,
            collection_id=args.collection,
        )

        # Prepare headers
        headers = {}
        if args.bearer_token:
            headers["Authorization"] = f"Bearer {args.bearer_token}"

        # Register to STAC API
        logger.info(f"Registering to {args.stac}/collections/{args.collection}")
        register_item(
            stac_url=args.stac,
            collection_id=args.collection,
            item=item,
            mode=args.mode,
            headers=headers,
        )

        duration = time.perf_counter() - start_time
        logger.info(f"Registration complete in {duration:.2f}s")
        return 0

    except Exception as exc:
        duration = time.perf_counter() - start_time
        logger.error(f"Registration failed after {duration:.2f}s: {exc}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
# Force rebuild 1759574599
