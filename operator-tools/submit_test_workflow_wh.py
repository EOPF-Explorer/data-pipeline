#!/usr/bin/env python3
import json

import requests

# Test STAC item submission
payload = {
    # "source_url": "https://stac.core.eopf.eodc.eu/collections/sentinel-2-l2a/items/S2C_MSIL2A_20251117T090251_N0511_R007_T35SMA_20251117T124014",
    "source_url": "https://s3.explorer.eopf.copernicus.eu/esa-zarr-sentinel-explorer-fra/cpm-manual/S2A_MSIL2A_20210917T115221_N0500_R123_T28RBS_20230110T165456.json",
    "collection": "sentinel-2-l2a-staging",
    "action": "convert-v1-s2-hp",  # specify the action to use the S2 high-priority trigger
}

message = json.dumps(payload)

# Submit via HTTP webhook endpoint
response = requests.post(
    "http://localhost:12001/samples",
    data=message,
    headers={"Content-Type": "application/json"},
)

print(f"✅ Published workflow for item: {payload['source_url']}")
print(f"Response status: {response.status_code}")
