#!/usr/bin/env python3
"""Submit workflow to geozarr pipeline via RabbitMQ."""

import json
import os
import sys

import pika


def submit_workflow(payload: dict) -> bool:
    """Submit workflow via RabbitMQ."""
    try:
        username = os.getenv("RABBITMQ_USER", "user")
        password = os.getenv("RABBITMQ_PASSWORD")

        if not password:
            print("❌ RABBITMQ_PASSWORD not set")
            print(
                "   Get: kubectl get secret rabbitmq-password -n core -o jsonpath='{.data.rabbitmq-password}' | base64 -d"
            )
            return False

        credentials = pika.PlainCredentials(username, password)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters("localhost", 5672, credentials=credentials)
        )
        channel = connection.channel()

        exchange_name = "geozarr-staging"
        routing_key = "eopf.items.test"

        channel.exchange_declare(exchange=exchange_name, exchange_type="topic", durable=True)
        channel.basic_publish(
            exchange=exchange_name,
            routing_key=routing_key,
            body=json.dumps(payload),
            properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"),
        )

        print(f"✅ Published: {payload['source_url'][:80]}...")
        connection.close()
        return True

    except Exception as e:
        print(f"❌ Failed: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    # ✅ Use STAC item URL (pipeline extracts zarr URL from assets)
    # ❌ NOT direct zarr URL
    item_id = "S2A_MSIL2A_20251022T094121_N0511_R036_T34TDT_20251022T114817"
    payload = {
        "source_url": f"https://stac.core.eopf.eodc.eu/collections/sentinel-2-l2a/items/{item_id}",
        "item_id": item_id,
        "collection": "sentinel-2-l2a-dp-test",
    }

    print("🚀 Submitting workflow via RabbitMQ")
    print(f"   Collection: {payload['collection']}")
    print(f"   Source: {payload['source_url']}")
    print()
    print("Prerequisites:")
    print("  kubectl port-forward -n devseed-staging svc/rabbitmq 5672:5672 &")
    print(
        "  export RABBITMQ_PASSWORD=$(kubectl get secret rabbitmq-password -n core -o jsonpath='{.data.rabbitmq-password}' | base64 -d)"
    )
    print()

    if submit_workflow(payload):
        print("✅ Monitor: kubectl get wf -n devseed-staging --watch")
        sys.exit(0)
    else:
        sys.exit(1)
