#!/usr/bin/env python3
"""Pre-compute STAC item aggregations (daily/monthly counts) for timeline UI.

Queries all items in a STAC collection, counts them by date, and produces
static JSON files following the STAC Aggregation Extension format. Uploads
the results to S3 and adds discoverable links to the STAC collection.

Usage:
    python scripts/aggregate_items.py \
        --collection sentinel-2-l2a \
        --stac-api-url https://api.explorer.eopf.copernicus.eu/stac \
        --s3-bucket esa-zarr-sentinel-explorer-fra \
        --dry-run
"""

from __future__ import annotations

import argparse
import collections
import json
import logging
import os
import sys

import boto3
import httpx
from pystac_client import Client

# Configure logging (set LOG_LEVEL=DEBUG for verbose output)
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Suppress verbose library logging
for _lib in ["botocore", "s3fs", "aiobotocore", "urllib3", "httpx", "httpcore"]:
    logging.getLogger(_lib).setLevel(logging.WARNING)


def count_items_by_datetime(stac_api_url: str, collection_id: str) -> collections.Counter[str]:
    """Query all items and count them by date (YYYY-MM-DD).

    Uses the STAC ``fields`` extension to fetch only ``datetime``, keeping
    payloads minimal for large collections.
    """
    catalog = Client.open(stac_api_url)
    search = catalog.search(
        collections=[collection_id],
        fields={"includes": ["datetime", "properties.datetime"], "excludes": []},
        limit=1000,
    )

    counts: collections.Counter[str] = collections.Counter()
    total = 0

    for page in search.pages():
        page_items = list(page.items)
        for item in page_items:
            dt = item.datetime
            if dt is None:
                # Fallback: try properties.datetime string
                dt_str = item.properties.get("datetime")
                if dt_str:
                    day = dt_str[:10]  # YYYY-MM-DD
                    counts[day] += 1
                    total += 1
                    continue
                logger.warning(f"Skipping item {item.id}: no datetime")
                continue
            counts[dt.strftime("%Y-%m-%d")] += 1
            total += 1

        if total > 0 and total % 1000 < len(page_items):
            logger.info(f"Processed {total} items so far...")

    logger.info(f"Total items counted: {total}")
    return counts


def build_daily_aggregation(daily_counts: collections.Counter[str]) -> dict:
    """Build daily AggregationCollection from per-day counts."""
    buckets = [
        {"key": f"{day}T00:00:00.000Z", "value": count}
        for day, count in sorted(daily_counts.items())
    ]
    return {
        "type": "AggregationCollection",
        "aggregations": [
            {
                "key": "datetime_daily",
                "buckets": buckets,
                "interval": "daily",
            }
        ],
    }


def build_monthly_aggregation(daily_counts: collections.Counter[str]) -> dict:
    """Build monthly AggregationCollection by summing daily counts per month."""
    monthly: collections.Counter[str] = collections.Counter()
    for day, count in daily_counts.items():
        month = day[:7]  # YYYY-MM
        monthly[month] += count

    buckets = [
        {"key": f"{month}-01T00:00:00.000Z", "value": count}
        for month, count in sorted(monthly.items())
    ]
    return {
        "type": "AggregationCollection",
        "aggregations": [
            {
                "key": "datetime_monthly",
                "buckets": buckets,
                "interval": "monthly",
            }
        ],
    }


def upload_to_s3(data: dict, bucket: str, key: str, s3_endpoint: str | None = None) -> None:
    """Upload JSON data to S3."""
    s3_config: dict = {}
    if s3_endpoint:
        s3_config["endpoint_url"] = s3_endpoint
    elif os.getenv("AWS_ENDPOINT_URL"):
        s3_config["endpoint_url"] = os.getenv("AWS_ENDPOINT_URL")

    s3_client = boto3.client("s3", **s3_config)
    body = json.dumps(data, separators=(",", ":")).encode()
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )
    size_kb = len(body) / 1024
    logger.info(f"Uploaded s3://{bucket}/{key} ({size_kb:.1f} KB)")


def update_collection_links(
    stac_api_url: str,
    collection_id: str,
    s3_gateway_url: str,
    s3_bucket: str,
    s3_prefix: str,
) -> None:
    """Fetch collection, replace pre-aggregation links, and PUT back."""
    base_url = stac_api_url.rstrip("/")
    collection_url = f"{base_url}/collections/{collection_id}"

    with httpx.Client(timeout=30.0, follow_redirects=True) as http:
        resp = http.get(collection_url)
        resp.raise_for_status()
        collection_data = resp.json()

        # Remove existing pre-aggregation links
        links = [
            link
            for link in collection_data.get("links", [])
            if link.get("rel") != "pre-aggregation"
        ]

        # Add new pre-aggregation links
        gateway_base = s3_gateway_url.rstrip("/")
        path_prefix = f"{s3_bucket}/{s3_prefix}/{collection_id}"

        links.append(
            {
                "rel": "pre-aggregation",
                "href": f"{gateway_base}/{path_prefix}/daily.json",
                "type": "application/json",
                "title": "Daily Item Aggregation",
                "aggregation:interval": "daily",
            }
        )
        links.append(
            {
                "rel": "pre-aggregation",
                "href": f"{gateway_base}/{path_prefix}/monthly.json",
                "type": "application/json",
                "title": "Monthly Item Aggregation",
                "aggregation:interval": "monthly",
            }
        )

        collection_data["links"] = links

        put_resp = http.put(
            collection_url,
            json=collection_data,
            headers={"Content-Type": "application/json"},
        )
        put_resp.raise_for_status()

    logger.info(f"Updated collection {collection_id} with pre-aggregation links")


def main(argv: list[str] | None = None) -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Pre-compute STAC item aggregations for timeline UI"
    )
    parser.add_argument("--collection", required=True, help="Collection ID")
    parser.add_argument("--stac-api-url", required=True, help="STAC API base URL")
    parser.add_argument("--s3-bucket", required=True, help="S3 bucket for output")
    parser.add_argument(
        "--s3-prefix", default="aggregations", help="S3 key prefix (default: aggregations)"
    )
    parser.add_argument(
        "--s3-endpoint", default=None, help="S3 endpoint URL (falls back to AWS_ENDPOINT_URL)"
    )
    parser.add_argument(
        "--s3-gateway-url",
        default="https://s3.explorer.eopf.copernicus.eu",
        help="Public HTTPS gateway for S3",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate JSON to stdout, skip upload and collection update",
    )

    args = parser.parse_args(argv)

    try:
        logger.info(f"Querying items for collection: {args.collection}")
        daily_counts = count_items_by_datetime(args.stac_api_url, args.collection)

        if not daily_counts:
            logger.info("No items found, nothing to aggregate")
            return 0

        daily_agg = build_daily_aggregation(daily_counts)
        monthly_agg = build_monthly_aggregation(daily_counts)

        daily_buckets = len(daily_agg["aggregations"][0]["buckets"])
        monthly_buckets = len(monthly_agg["aggregations"][0]["buckets"])
        logger.info(f"Daily buckets: {daily_buckets}")
        logger.info(f"Monthly buckets: {monthly_buckets}")

        if args.dry_run:
            logger.info("Dry run - printing daily aggregation to stdout")
            json.dump(daily_agg, sys.stdout, indent=2)
            sys.stdout.write("\n")
            logger.info("Dry run - printing monthly aggregation to stdout")
            json.dump(monthly_agg, sys.stdout, indent=2)
            sys.stdout.write("\n")
            return 0

        # Upload to S3
        prefix = f"{args.s3_prefix}/{args.collection}"
        upload_to_s3(daily_agg, args.s3_bucket, f"{prefix}/daily.json", args.s3_endpoint)
        upload_to_s3(monthly_agg, args.s3_bucket, f"{prefix}/monthly.json", args.s3_endpoint)

        # Update collection links
        update_collection_links(
            args.stac_api_url,
            args.collection,
            args.s3_gateway_url,
            args.s3_bucket,
            args.s3_prefix,
        )

        logger.info("Aggregation complete")
        return 0
    except Exception as e:
        logger.error(f"Aggregation failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
