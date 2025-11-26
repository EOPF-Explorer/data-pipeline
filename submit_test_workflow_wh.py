#!/usr/bin/env python3
import json

import requests

# Test item that was failing (same as before)
payload = {
    "source_url": "https://stac.core.eopf.eodc.eu/collections/sentinel-2-l2a/items/S2C_MSIL2A_20251117T090251_N0511_R007_T35SMA_20251117T124014",
    "collection": "sentinel-2-l2a-dp-test",
    "action": "convert-v1-s2-hp",  # specify the action to use the S2 high-priority trigger
}

# credentials = pika.PlainCredentials("user", os.getenv("RABBITMQ_PASSWORD"))

message = json.dumps(payload)
# Amke a simple http post request to localhost:12000/samples
# using requests

response = requests.post(
    "http://localhost:12000/samples",
    data=message,
    headers={"Content-Type": "application/json"},
)

print(f"✅ Published workflow for item: {payload['source_url']}")
