#!/usr/bin/env python3
import json
import os

import pika

# Test item that was failing (same as before)
payload = {
    "source_url": "https://stac.core.eopf.eodc.eu/collections/sentinel-2-l2a/items/S2B_MSIL2A_20250619T083559_N0511_R064_T35RQQ_20250619T105831",
    "item_id": "S2B_MSIL2A_20250619T083559_N0511_R064_T35RQQ_20250619T105831",
    "collection": "sentinel-2-l2a-dp-test",
}

credentials = pika.PlainCredentials("user", os.getenv("RABBITMQ_PASSWORD"))
connection = pika.BlockingConnection(pika.ConnectionParameters("localhost", 5672, "/", credentials))
channel = connection.channel()

message = json.dumps(payload)
channel.basic_publish(
    exchange="geozarr-events",
    routing_key="geozarr.convert",
    body=message,
    properties=pika.BasicProperties(content_type="application/json"),
)

print(f"✅ Published workflow for item: {payload['item_id']}")
connection.close()
