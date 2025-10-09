#!/usr/bin/env python3
"""Submit GeoZarr conversion jobs via AMQP.

This is the operator interface for the data-pipeline. It publishes messages to RabbitMQ,
which triggers the Argo Workflow via the sensor.

Architecture:
    submit.py → RabbitMQ → Sensor → Argo Workflow → (convert → register → augment)

Requirements:
    pip install pika click

Usage:
    # Submit single item
    python examples/submit.py \
        --stac-url "https://stac.core.eopf.eodc.eu/collections/sentinel-2-l2a/items/S2B_..." \
        --collection sentinel-2-l2a

    # Submit with custom item ID
    python examples/submit.py \
        --stac-url "https://..." \
        --item-id "custom-id" \
        --collection sentinel-2-l2a

    # Check status
    kubectl get workflows -n devseed -w
"""

import json
import sys
from typing import Any

import click
import pika


def publish_message(
    amqp_url: str,
    exchange: str,
    routing_key: str,
    payload: dict[str, Any],
) -> None:
    """Publish message to RabbitMQ.

    Args:
        amqp_url: AMQP connection URL (amqp://user:pass@host:port/vhost)
        exchange: Exchange name (use "" for default)
        routing_key: Routing key for message
        payload: Message payload (will be JSON-encoded)

    Raises:
        Exception: If connection or publish fails
    """
    try:
        # Parse URL and connect
        params = pika.URLParameters(amqp_url)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        # Publish message
        channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=json.dumps(payload),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,  # Persistent
            ),
        )

        connection.close()
        click.echo(f"✅ Published to {routing_key}", err=True)

    except Exception as e:
        click.echo(f"❌ Failed to publish: {e}", err=True)
        raise


@click.command()
@click.option(
    "--stac-url",
    required=True,
    help="Source STAC item URL from EODC",
)
@click.option(
    "--collection",
    default="sentinel-2-l2a",
    help="Target STAC collection for registration",
)
@click.option(
    "--item-id",
    default=None,
    help="Custom item ID (default: extract from STAC URL)",
)
@click.option(
    "--amqp-url",
    default="amqp://user:password@rabbitmq.core.svc.cluster.local:5672/",
    envvar="AMQP_URL",
    help="RabbitMQ connection URL (or set AMQP_URL env var). For local testing with port-forward, use: amqp://user:PASSWORD@localhost:5672/",
)
@click.option(
    "--routing-key",
    default="eopf.items.convert",
    help="RabbitMQ routing key (matches EventSource pattern eopf.items.*)",
)
@click.option(
    "--exchange",
    default="geozarr",
    help="RabbitMQ exchange (must match EventSource configuration)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Print payload without publishing",
)
def main(
    stac_url: str,
    collection: str,
    item_id: str | None,
    amqp_url: str,
    routing_key: str,
    exchange: str,
    dry_run: bool,
) -> None:
    """Submit GeoZarr conversion job via AMQP.

    This publishes a message to RabbitMQ, which triggers the Argo Workflow sensor.
    The workflow will:
      1. Extract Zarr URL from STAC item
      2. Convert to GeoZarr
      3. Register with STAC API
      4. Add visualization links

    Example:
        python examples/submit.py \\
            --stac-url "https://stac.core.eopf.eodc.eu/.../S2B_MSIL2A_20250518..." \\
            --collection sentinel-2-l2a

    Monitor:
        kubectl get workflows -n devseed -w
        kubectl logs -n devseed -l workflows.argoproj.io/workflow=<name> -f
    """
    # Extract item ID from URL if not provided
    if item_id is None:
        # Parse: .../items/S2B_MSIL2A_20250518... → S2B_MSIL2A_20250518...
        if "/items/" in stac_url:
            item_id = stac_url.split("/items/")[-1].split("?")[0]
        else:
            click.echo("❌ Could not extract item_id from URL. Use --item-id", err=True)
            sys.exit(1)

    # Build payload
    payload = {
        "source_url": stac_url,
        "item_id": item_id,
        "collection": collection,
    }

    # Display
    click.echo("📦 Payload:", err=True)
    click.echo(json.dumps(payload, indent=2))
    click.echo("", err=True)

    if dry_run:
        click.echo("🔍 Dry run - not publishing", err=True)
        return

    # Publish
    click.echo(f"📤 Publishing to RabbitMQ ({routing_key})...", err=True)
    try:
        publish_message(amqp_url, exchange, routing_key, payload)
        click.echo("", err=True)
        click.echo("✅ Job submitted successfully!", err=True)
        click.echo("", err=True)
        click.echo("Monitor with:", err=True)
        click.echo("  kubectl get workflows -n devseed -w", err=True)
        click.echo(
            "  kubectl logs -n devseed -l workflows.argoproj.io/workflow=<name> -f", err=True
        )
    except Exception:
        sys.exit(1)


if __name__ == "__main__":
    main()
