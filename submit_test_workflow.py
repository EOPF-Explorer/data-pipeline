#!/usr/bin/env python3
import json
import os

import pika

# Test item that was failing (same as before)
payload = {
    "source_url": "https://stac.core.eopf.eodc.eu/collections/sentinel-2-l2a/items/S2B_MSIL2A_20251109T103149_N0511_R108_T31TEE_20251109T125446",
    "collection": "sentinel-2-l2a-dp-test",
}

credentials = pika.PlainCredentials("user", os.getenv("RABBITMQ_PASSWORD"))
connection = pika.BlockingConnection(pika.ConnectionParameters("localhost", 5672, "/", credentials))
channel = connection.channel()

message = json.dumps(payload)
channel.basic_publish(
    exchange="eopf_samples",
    routing_key="eopf_samples.convert",
    body=message,
    properties=pika.BasicProperties(content_type="application/json"),
)

print(f"✅ Published workflow for item: {payload['source_url']}")
connection.close()
