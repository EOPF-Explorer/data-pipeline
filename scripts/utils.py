#!/usr/bin/env python3
"""Pipeline utility functions."""

import json
import sys
from urllib.parse import urlparse
from urllib.request import urlopen


def extract_item_id(url: str) -> str:
    """Extract item ID from STAC item URL."""
    return urlparse(url).path.rstrip("/").split("/")[-1]


def get_zarr_url(stac_item_url: str) -> str:
    """Get Zarr asset URL from STAC item."""
    with urlopen(stac_item_url) as response:
        item = json.loads(response.read())

    assets = item.get("assets", {})

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
    # CLI interface for bash scripts
    if len(sys.argv) < 2:
        print("Usage: utils.py <command> <args...>", file=sys.stderr)
        print("Commands: extract-item-id, get-zarr-url", file=sys.stderr)
        sys.exit(1)

    command = sys.argv[1]

    if command == "extract-item-id":
        print(extract_item_id(sys.argv[2]))
    elif command == "get-zarr-url":
        print(get_zarr_url(sys.argv[2]))
    else:
        print(f"Unknown command: {command}", file=sys.stderr)
        sys.exit(1)
