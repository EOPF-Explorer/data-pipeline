#!/usr/bin/env python3
import json
import sys
from urllib.request import urlopen


def get_zarr_url(stac_item_url: str) -> str:
    with urlopen(stac_item_url) as response:
        item = json.loads(response.read())

    assets = item.get("assets", {})

    # Priority: product, zarr, then any .zarr asset
    for key in ["product", "zarr"]:
        if key in assets:
            href = assets[key].get("href")
            if href:
                return str(href)

    # Fallback
    for asset in assets.values():
        href = asset.get("href", "")
        if ".zarr" in href:
            return str(href)

    raise RuntimeError("No Zarr asset found")


if __name__ == "__main__":
    print(get_zarr_url(sys.argv[1]))
