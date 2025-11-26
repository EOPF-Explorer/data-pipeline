#!/usr/bin/env python3
import json
import os

import pika

# Test item that was failing (same as before)
payload = {
    "source_url": "https://stac.core.eopf.eodc.eu/collections/sentinel-2-l2a/items/S2C_MSIL2A_20251117T090251_N0511_R007_T35SMA_20251117T124014",
    "collection": "sentinel-2-l2a-dp-test",
}

credentials = pika.PlainCredentials("user", os.getenv("RABBITMQ_PASSWORD"))
connection = pika.BlockingConnection(pika.ConnectionParameters("localhost", 5672, "/", credentials))
channel = connection.channel()

message = json.dumps(payload)
channel.basic_publish(
    exchange="eopf_samples",
    routing_key="eopf_samples.convert.v1",
    body=message,
    properties=pika.BasicProperties(content_type="application/json"),
)

print(f"✅ Published workflow for item: {payload['source_url']}")
connection.close()
