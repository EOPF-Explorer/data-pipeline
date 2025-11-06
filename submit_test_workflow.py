#!/usr/bin/env python3
import json
import os

import pika

# Test item that was failing (same as before)
payload = {
    "source_url": "https://stac.core.eopf.eodc.eu/collections/sentinel-2-l2a/items/S2A_MSIL2A_20251025T095111_N0511_R079_T33SVB_20251025T121315",
    "item_id": "S2A_MSIL2A_20251025T095111_N0511_R079_T33SVB_20251025T121315",
    "collection": "sentinel-2-l2a-dp-test",
}

credentials = pika.PlainCredentials("user", os.getenv("RABBITMQ_PASSWORD"))
connection = pika.BlockingConnection(pika.ConnectionParameters("localhost", 5672, "/", credentials))
channel = connection.channel()

message = json.dumps(payload)
channel.basic_publish(
    exchange="geozarr-staging",
    routing_key="eopf.items.found",
    body=message,
    properties=pika.BasicProperties(content_type="application/json"),
)

print(f"✅ Published workflow for item: {payload['item_id']}")
connection.close()
