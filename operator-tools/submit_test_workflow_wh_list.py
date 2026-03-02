#!/usr/bin/env python3
import json
import time

import requests

# List of products to process (from S3 listing)
products = [
    "S2A_MSIL2A_20250810T110701_N0511_R137_T29TPF_20250810T151608",
    "S2A_MSIL2A_20250813T112131_N0511_R037_T29TPF_20250813T160722",
    "S2A_MSIL2A_20250820T110651_N0511_R137_T29TPF_20250820T150715",
    "S2A_MSIL2A_20250820T110651_N0511_R137_T29TPF_20250820T162816",
    "S2A_MSIL2A_20250823T112131_N0511_R037_T29TPF_20250823T162957",
    "S2A_MSIL2A_20250830T110701_N0511_R137_T29TPF_20250830T152014",
    "S2B_MSIL2A_20250813T110619_N0511_R137_T29TPF_20250813T133147",
    "S2B_MSIL2A_20250816T112119_N0511_R037_T29TPF_20250816T122832",
    "S2B_MSIL2A_20250823T110619_N0511_R137_T29TPF_20250823T134914",
    "S2B_MSIL2A_20250826T112119_N0511_R037_T29TPF_20250826T133202",
    "S2C_MSIL2A_20250811T112131_N0511_R037_T29TPF_20250811T152216",
    "S2C_MSIL2A_20250818T110641_N0511_R137_T29TPF_20250818T163305",
    "S2C_MSIL2A_20250828T110641_N0511_R137_T29TPF_20250828T151013",
    "S2C_MSIL2A_20250831T112131_N0511_R037_T29TPF_20250831T154413",
]

# Process each product
for i, product in enumerate(products, 1):
    # Test STAC item submission
    payload = {
        "source_url": f"https://s3.explorer.eopf.copernicus.eu/esa-zarr-sentinel-explorer-fra/cpm-manual/{product}.json",
        "collection": "sentinel-2-l2a",
        "action": "convert-v1-s2-hp",  # specify the action to use the S2 high-priority trigger
    }

    message = json.dumps(payload)

    # Submit via HTTP webhook endpoint
    try:
        response = requests.post(
            "http://localhost:12000/samples",
            data=message,
            headers={"Content-Type": "application/json"},
        )

        print(f"[{i}/{len(products)}] ✅ Published workflow for item: {product}")
        print(f"Response status: {response.status_code}")

        if response.status_code != 200:
            print(f"Warning: Non-200 response for {product}: {response.text}")

    except Exception as e:
        print(f"[{i}/{len(products)}] ❌ Error processing {product}: {str(e)}")

    # Add small delay to avoid overwhelming the server
    if i < len(products):
        time.sleep(1)

print(f"\n✅ Completed processing {len(products)} products.")
