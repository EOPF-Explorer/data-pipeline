#!/usr/bin/env python3
"""Extract Zarr URL from STAC item - standalone script for workflow templates."""

import sys

import httpx


def get_zarr_url(stac_item_url: str) -> str:
    """Get Zarr asset URL from STAC item."""
    r = httpx.get(stac_item_url, timeout=30.0, follow_redirects=True)
    r.raise_for_status()
    assets = r.json().get("assets", {})

    # Priority: product, zarr, then any .zarr asset
    for key in ["product", "zarr"]:
        if key in assets and (href := assets[key].get("href")):
            return str(href)

    # Fallback: any asset with .zarr in href
    for asset in assets.values():
        if ".zarr" in asset.get("href", ""):
            return str(asset["href"])

    raise RuntimeError("No Zarr asset found in STAC item")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: get_zarr_url.py <stac_item_url>", file=sys.stderr)
        sys.exit(1)

    try:
        zarr_url = get_zarr_url(sys.argv[1])
        print(zarr_url)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
