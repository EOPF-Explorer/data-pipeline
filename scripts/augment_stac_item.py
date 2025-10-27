#!/usr/bin/env python3
"""STAC item augmentation: add CRS metadata and preview links."""

import argparse
import logging
import os
import sys
import urllib.parse

import httpx
import zarr
from pystac import Item, Link
from pystac.extensions.projection import ProjectionExtension

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

EXPLORER_BASE = os.getenv("EXPLORER_BASE_URL", "https://explorer.eopf.copernicus.eu")


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
    """Extract variable path from zarr asset href.

    Returns variable path like /S01SIWGRD_.../measurements:grd for S1 or None.
    """
    if not asset_href or ".zarr/" not in asset_href:
        return None
    parts = asset_href.split(".zarr/", 1)
    if len(parts) == 2:
        return f"/{parts[1]}:{variable_name}"
    return None


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
        # S2: Point to overview level 0 for quicklook TCI
        # Use /r10m/0/tci path to access the overview array with spatial_ref
        var_path = "/quality/l2a_quicklook/r10m/0/tci"
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


def augment(item: Item, *, raster_base: str, collection_id: str) -> Item:
    """Augment STAC item with extensions and links.

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


def main(argv: list[str] | None = None) -> int:
    """Main entry point."""
    p = argparse.ArgumentParser(description="Augment STAC item")
    p.add_argument("--stac-api-url", required=True, help="STAC API base URL")
    p.add_argument("--collection-id", required=True, help="Collection ID")
    p.add_argument("--item-id", required=True, help="Item ID")
    p.add_argument("--bearer", default="", help="Bearer token (optional)")
    p.add_argument(
        "--raster-api-url",
        default="https://api.explorer.eopf.copernicus.eu/raster",
        help="TiTiler raster API base URL",
    )
    p.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    args = p.parse_args(argv)

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    headers = {"Authorization": f"Bearer {args.bearer}"} if args.bearer else {}
    item_url = (
        f"{args.stac_api_url.rstrip('/')}/collections/{args.collection_id}/items/{args.item_id}"
    )

    # Fetch, augment, and update item
    try:
        with httpx.Client() as client:
            # Fetch item
            r = client.get(item_url, headers=headers, timeout=30.0)
            r.raise_for_status()
            item = Item.from_dict(r.json())

            # Augment with CRS + preview links
            target_collection = item.collection_id or args.collection_id
            augment(item, raster_base=args.raster_api_url, collection_id=target_collection)

            # Update item via PUT
            target_url = (
                f"{args.stac_api_url.rstrip('/')}/collections/{target_collection}/items/{item.id}"
            )
            r = client.put(
                target_url,
                json=item.to_dict(),
                headers={**headers, "Content-Type": "application/json"},
                timeout=30.0,
            )
            r.raise_for_status()
            if args.verbose:
                logger.debug(f"PUT {target_url} → {r.status_code}")

            logger.info(f"✅ Augmented {item.id} in {target_collection}")
            return 0

    except Exception as e:
        logger.error(f"Failed to augment {args.item_id}: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
