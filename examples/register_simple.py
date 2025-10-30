#!/usr/bin/env python3
"""Minimal example: Register a GeoZarr dataset to STAC API.

This example demonstrates the core functionality without Kubernetes/AMQP complexity.
Perfect for reviewers to understand what the pipeline does.

Requirements:
    pip install httpx pystac

Usage:
    python examples/register_simple.py
"""

import json
from datetime import datetime

import httpx
import pystac

# Configuration
STAC_API = "https://api.explorer.eopf.copernicus.eu/stac"
COLLECTION = "sentinel2-l2a"

# Example GeoZarr dataset
ITEM_ID = "S2B_MSIL2A_20250518_T29RLL_example"
ZARR_URL = "s3://eopf-devseed/geozarr/S2B_MSIL2A_20250518_T29RLL_geozarr.zarr"
BBOX = [-8.75, 39.0, -8.25, 39.5]  # Portugal
DATETIME = "2025-05-18T11:21:19Z"


def create_stac_item() -> dict:
    """Create a minimal STAC item for the GeoZarr dataset."""
    item = pystac.Item(
        id=ITEM_ID,
        geometry={
            "type": "Polygon",
            "coordinates": [
                [
                    [BBOX[0], BBOX[1]],
                    [BBOX[2], BBOX[1]],
                    [BBOX[2], BBOX[3]],
                    [BBOX[0], BBOX[3]],
                    [BBOX[0], BBOX[1]],
                ]
            ],
        },
        bbox=BBOX,
        datetime=datetime.fromisoformat(DATETIME.replace("Z", "+00:00")),
        properties={
            "platform": "sentinel-2b",
            "instruments": ["msi"],
            "constellation": "sentinel-2",
        },
    )

    # Add GeoZarr asset
    item.add_asset(
        "geozarr",
        pystac.Asset(
            href=ZARR_URL,
            media_type="application/vnd+zarr",
            roles=["data"],
            title="GeoZarr optimized data",
        ),
    )

    return item.to_dict()


def register_item(item: dict) -> None:
    """Register STAC item to the API."""
    url = f"{STAC_API}/collections/{COLLECTION}/items"

    print(f"📤 Registering {item['id']} to {COLLECTION}...")

    response = httpx.post(
        url,
        json=item,
        headers={"Content-Type": "application/json"},
        timeout=30.0,
    )

    if response.status_code == 200:
        print("✅ Success! Item registered.")
        print(f"🔗 View: {STAC_API}/collections/{COLLECTION}/items/{item['id']}")
    else:
        print(f"❌ Failed: {response.status_code}")
        print(response.text)


def main() -> None:
    """Run the example."""
    print("🚀 Simple GeoZarr Registration Example\n")

    # Create STAC item
    item = create_stac_item()
    print("📝 Created STAC item:")
    print(json.dumps(item, indent=2)[:300] + "...\n")

    # Register to API
    register_item(item)


if __name__ == "__main__":
    main()
