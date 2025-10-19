#!/usr/bin/env python3
"""STAC item augmentation using pystac extensions.

Uses ProjectionExtension for CRS metadata and simplified TiTiler integration.
"""

import argparse
import sys
import urllib.parse
from collections.abc import Sequence

import httpx
import zarr
from pystac import Item, Link
from pystac.extensions.projection import ProjectionExtension

try:
    from metrics import PREVIEW_GENERATION_DURATION
except ImportError:
    PREVIEW_GENERATION_DURATION = None

# Preview configuration
_S2_TRUE_COLOR = [
    "/measurements/reflectance/r10m/0:b04",
    "/measurements/reflectance/r10m/0:b03",
    "/measurements/reflectance/r10m/0:b02",
]
_S2_RESCALE = "0,0.1"
_S1_POLARIZATIONS = ["vh", "vv", "hh", "hv"]


def _build_tilejson_query(variables: list[str], rescale: str | None = None) -> str:
    """Build TiTiler query string."""
    pairs = [("variables", var) for var in variables]
    if rescale:
        pairs.extend(("rescale", rescale) for _ in variables)
    if len(variables) == 3:
        pairs.append(("color_formula", "Gamma RGB 1.4"))
    return "&".join(f"{k}={urllib.parse.quote_plus(v)}" for k, v in pairs)


def _get_s1_preview_query(item: Item) -> str:
    """S1 GRD preview (first available polarization)."""
    for pol in _S1_POLARIZATIONS:
        if pol in item.assets and item.assets[pol].href and ".zarr/" in item.assets[pol].href:
            zarr_path = item.assets[pol].href.split(".zarr/")[1]
            return _build_tilejson_query([f"/{zarr_path}:grd"], "0,219")
    return _build_tilejson_query(["/measurements:grd"], "0,219")


def add_projection(item: Item) -> None:
    """Add ProjectionExtension from first zarr asset with spatial_ref."""
    for asset in item.assets.values():
        if asset.media_type == "application/vnd+zarr" and asset.href:
            try:
                store = zarr.open(asset.href.replace("s3://", "s3://"), mode="r")
                spatial_ref = store.attrs.get("spatial_ref", {})
                epsg = spatial_ref.get("spatial_ref")
                if epsg:
                    proj_ext = ProjectionExtension.ext(item, add_if_missing=True)
                    proj_ext.epsg = int(epsg)
                    if wkt := spatial_ref.get("crs_wkt"):
                        proj_ext.wkt2 = wkt
                    return
            except Exception:
                continue


def add_visualization(item: Item, raster_base: str, collection_id: str) -> None:
    """Add preview, tilejson, and viewer links."""
    # Find first zarr asset
    zarr_asset = next(
        (a for a in item.assets.values() if a.media_type == "application/vnd+zarr" and a.href),
        None,
    )
    if not zarr_asset:
        return

    # Build query
    is_s1 = collection_id.lower().startswith(("sentinel-1", "sentinel1"))
    query = (
        _get_s1_preview_query(item) if is_s1 else _build_tilejson_query(_S2_TRUE_COLOR, _S2_RESCALE)
    )

    # Normalize href (s3:// → https://)
    href = zarr_asset.href.replace("s3://eopf-", "https://s3.de.io.cloud.ovh.net/eopf-")

    # Add links
    encoded_url = urllib.parse.quote(href)
    item.add_link(
        Link(
            "preview",
            f"{raster_base}/preview?url={encoded_url}&{query}",
            "image/png",
            "Preview image",
        )
    )
    item.add_link(
        Link(
            "tilejson",
            f"{raster_base}/tilejson.json?url={encoded_url}&{query}",
            "application/json",
            "TileJSON",
        )
    )
    item.add_link(
        Link(
            "via",
            f"https://explorer.eopf.copernicus.eu/collections/{collection_id.lower().replace('_', '-')}/items/{item.id}",
            title="EOPF Explorer",
        )
    )


def augment(item: Item, *, raster_base: str, collection_id: str, verbose: bool) -> Item:
    """Augment STAC item with extensions and links."""
    if verbose:
        print(f"[augment] {item.id}")
    add_projection(item)
    add_visualization(item, raster_base, collection_id)
    return item


def main(argv: Sequence[str] | None = None) -> int:
    """Main entry point."""
    p = argparse.ArgumentParser(description="Augment STAC item using extensions")
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

    # Fetch
    try:
        with httpx.Client() as client:
            r = client.get(item_url, headers=headers, timeout=30.0)
            r.raise_for_status()
            item = Item.from_dict(r.json())
    except Exception as e:
        print(f"[augment] ERROR: GET failed: {e}", file=sys.stderr)
        return 1

    # Augment
    target_collection = item.collection_id or args.collection

    if PREVIEW_GENERATION_DURATION:
        with PREVIEW_GENERATION_DURATION.labels(collection=target_collection).time():
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

    # Update
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
                print(f"[augment] PUT {target_url} → {r.status_code}")
    except Exception as e:
        print(f"[augment] ERROR: PUT failed: {e}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
