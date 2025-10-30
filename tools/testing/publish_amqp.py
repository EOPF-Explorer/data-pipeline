#!/usr/bin/env python3
"""AMQP message publisher for triggering GeoZarr conversion workflows.

Publishes JSON payloads to RabbitMQ exchanges with support for
dynamic routing key templates based on payload fields.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

import pika
from tenacity import retry, stop_after_attempt, wait_exponential

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def load_payload(payload_file: Path) -> dict[str, Any]:
    """Load JSON payload from file."""
    try:
        data: dict[str, Any] = json.loads(payload_file.read_text())
        return data
    except FileNotFoundError:
        logger.exception("Payload file not found", extra={"file": str(payload_file)})
        sys.exit(1)
    except json.JSONDecodeError:
        logger.exception("Invalid JSON in payload file", extra={"file": str(payload_file)})
        sys.exit(1)


def format_routing_key(template: str, payload: dict[str, Any]) -> str:
    """Format routing key template using payload fields.

    Example: "eopf.item.found.{collection}" → "eopf.item.found.sentinel-2-l2a"
    """
    try:
        return template.format(**payload)
    except KeyError:
        logger.exception(
            "Missing required field in payload for routing key template",
            extra={"template": template, "available_fields": list(payload.keys())},
        )
        sys.exit(1)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
def publish_message(
    host: str,
    port: int,
    user: str,
    password: str,
    exchange: str,
    routing_key: str,
    payload: dict[str, Any],
    virtual_host: str = "/",
) -> None:
    """Publish message to RabbitMQ exchange with automatic retry."""
    credentials = pika.PlainCredentials(user, password)
    parameters = pika.ConnectionParameters(
        host=host,
        port=port,
        virtual_host=virtual_host,
        credentials=credentials,
    )

    logger.info("Connecting to amqp://%s@%s:%s%s", user, host, port, virtual_host)
    connection = pika.BlockingConnection(parameters)
    try:
        channel = connection.channel()
        channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=json.dumps(payload),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
            ),
        )
        logger.info("Published to exchange='%s' routing_key='%s'", exchange, routing_key)
        logger.debug("Payload: %s", json.dumps(payload, indent=2))
    finally:
        connection.close()


def main() -> None:
    """CLI entry point for AMQP message publisher."""
    parser = argparse.ArgumentParser(
        description="Publish JSON payload to RabbitMQ exchange for workflow triggers"
    )
    parser.add_argument("--host", required=True, help="RabbitMQ host")
    parser.add_argument("--port", type=int, default=5672, help="RabbitMQ port")
    parser.add_argument("--user", required=True, help="RabbitMQ username")
    parser.add_argument("--password", required=True, help="RabbitMQ password")
    parser.add_argument("--virtual-host", default="/", help="RabbitMQ virtual host")
    parser.add_argument("--exchange", required=True, help="RabbitMQ exchange name")
    parser.add_argument("--routing-key", help="Static routing key")
    parser.add_argument(
        "--routing-key-template",
        help="Template with {field} placeholders (e.g., 'eopf.item.found.{collection}')",
    )
    parser.add_argument("--payload-file", type=Path, required=True, help="JSON payload file path")

    args = parser.parse_args()

    if not args.routing_key and not args.routing_key_template:
        parser.error("Must provide either --routing-key or --routing-key-template")
    if args.routing_key and args.routing_key_template:
        parser.error("Cannot use both --routing-key and --routing-key-template")

    payload = load_payload(args.payload_file)
    routing_key = args.routing_key or format_routing_key(args.routing_key_template, payload)

    try:
        publish_message(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            exchange=args.exchange,
            routing_key=routing_key,
            payload=payload,
            virtual_host=args.virtual_host,
        )
    except Exception:
        logger.exception(
            "Failed to publish AMQP message",
            extra={
                "exchange": args.exchange,
                "routing_key": routing_key,
                "host": args.host,
            },
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
