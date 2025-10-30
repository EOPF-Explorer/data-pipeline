#!/usr/bin/env python3
"""STAC item augmentation: add CRS metadata and preview links."""

import argparse
import os
import sys
import urllib.parse

import httpx
import zarr
from pystac import Item, Link
from pystac.extensions.projection import ProjectionExtension

try:
    from metrics import PREVIEW_GENERATION_DURATION
except ImportError:
    PREVIEW_GENERATION_DURATION = None

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
        # S2: Static quicklook path
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


def augment(item: Item, *, raster_base: str, collection_id: str, verbose: bool) -> Item:
    """Augment STAC item with extensions and links."""
    if verbose:
        print(f"[augment] {item.id}")
    add_projection(item)
    add_visualization(item, raster_base, collection_id)
    return item


def main(argv: list[str] | None = None) -> int:
    """Main entry point."""
    p = argparse.ArgumentParser(description="Augment STAC item")
    p.add_argument("--stac", required=True, help="STAC API base")
    p.add_argument("--collection", required=True, help="Collection ID")
    p.add_argument("--item-id", required=True, help="Item ID")
    p.add_argument("--bearer", default="", help="Bearer token")
    p.add_argument(
        "--raster-base",
        default="https://api.explorer.eopf.copernicus.eu/raster",
        help="TiTiler base",
    )
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args(argv)

    headers = {"Authorization": f"Bearer {args.bearer}"} if args.bearer else {}
    item_url = f"{args.stac.rstrip('/')}/collections/{args.collection}/items/{args.item_id}"

    # Fetch item
    try:
        with httpx.Client() as client:
            r = client.get(item_url, headers=headers, timeout=30.0)
            r.raise_for_status()
            item = Item.from_dict(r.json())
    except Exception as e:
        print(f"ERROR: GET failed: {e}", file=sys.stderr)
        return 1

    # Augment with CRS + preview links
    target_collection = item.collection_id or args.collection

    if PREVIEW_GENERATION_DURATION:
        preview_type = (
            "s1_grd" if target_collection.lower().startswith("sentinel-1") else "true_color"
        )
        with PREVIEW_GENERATION_DURATION.labels(
            collection=target_collection, preview_type=preview_type
        ).time():
            augment(
                item,
                raster_base=args.raster_base,
                collection_id=target_collection,
                verbose=args.verbose,
            )
    else:
        augment(
            item,
            raster_base=args.raster_base,
            collection_id=target_collection,
            verbose=args.verbose,
        )

    # Update item via PUT
    target_url = f"{args.stac.rstrip('/')}/collections/{target_collection}/items/{item.id}"
    try:
        with httpx.Client() as client:
            r = client.put(
                target_url,
                json=item.to_dict(),
                headers={**headers, "Content-Type": "application/json"},
                timeout=30.0,
            )
            r.raise_for_status()
            if args.verbose:
                print(f"PUT {target_url} → {r.status_code}")
    except Exception as e:
        print(f"ERROR: PUT failed: {e}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
