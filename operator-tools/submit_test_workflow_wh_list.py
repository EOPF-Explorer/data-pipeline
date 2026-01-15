#!/usr/bin/env python3
import json
import time

import requests

# List of products to process
products = [
    "S2B_MSIL2A_20210915T120319_N0500_R023_T28RBS_20230108T162359",
    "S2A_MSIL2A_20210920T120331_N0500_R023_T28RBS_20230108T205311",
    "S2B_MSIL2A_20210925T120319_N0500_R023_T28RBS_20230109T005655",
    "S2A_MSIL2A_20210930T120331_N0500_R023_T28RBS_20230109T180123",
    "S2B_MSIL2A_20211005T120329_N0500_R023_T28RBS_20230105T092944",
    "S2A_MSIL2A_20211010T120331_N0500_R023_T28RBS_20230106T001906",
    "S2B_MSIL2A_20211015T120329_N0500_R023_T28RBS_20230104T221431",
    "S2A_MSIL2A_20211020T120331_N0500_R023_T28RBS_20230105T095115",
    "S2B_MSIL2A_20211025T120329_N0500_R023_T28RBS_20230105T094555",
    "S2A_MSIL2A_20211030T120331_N0500_R023_T28RBS_20230105T031900",
    "S2B_MSIL2A_20211104T120329_N0500_R023_T28RBS_20221230T183447",
    "S2A_MSIL2A_20211109T120331_N0500_R023_T28RBS_20221229T073655",
    "S2B_MSIL2A_20211114T120319_N0500_R023_T28RBS_20221229T121936",
    "S2A_MSIL2A_20211119T120321_N0500_R023_T28RBS_20221229T191326",
    "S2A_MSIL2A_20211209T120321_N0500_R023_T28RBS_20221225T010957",
    "S2B_MSIL2A_20220103T120319_N0510_R023_T28RBS_20240423T015628",
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
