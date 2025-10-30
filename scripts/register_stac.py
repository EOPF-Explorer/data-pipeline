#!/usr/bin/env python3
"""STAC registration using pystac-client (simplified version)."""

from __future__ import annotations

import argparse
import json
import logging
from typing import Any

from pystac import Item
from pystac_client import Client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def register_item(
    stac_url: str,
    collection_id: str,
    item_dict: dict[str, Any],
    mode: str = "create-or-skip",
) -> None:
    """Register STAC item to STAC API with transaction support.

    Uses pystac-client's StacApiIO for HTTP operations to leverage
    existing session management, retry logic, and request modification.

    Args:
        stac_url: STAC API URL
        collection_id: Target collection
        item_dict: STAC item as dict
        mode: create-or-skip | upsert | replace

    Raises:
        Exception: If registration fails
    """
    item = Item.from_dict(item_dict)
    item_id = item.id

    # Open client to reuse its StacApiIO session
    client = Client.open(stac_url)

    # Check existence
    try:
        existing = client.get_collection(collection_id).get_item(item_id)
    except Exception:
        existing = None

    if existing:
        if mode == "create-or-skip":
            logger.info(f"Item {item_id} exists, skipping")
            return

        # Delete for upsert/replace using StacApiIO's session
        logger.info(f"Replacing {item_id}")
        delete_url = f"{stac_url}/collections/{collection_id}/items/{item_id}"
        try:
            resp = client._stac_io.session.delete(delete_url, timeout=30)
            if resp.status_code not in (200, 204):
                logger.warning(f"Delete returned {resp.status_code}")
        except Exception as e:
            logger.warning(f"Delete failed (item may not exist): {e}")

    # POST item using StacApiIO's session (bypasses request() which only accepts 200)
    create_url = f"{stac_url}/collections/{collection_id}/items"
    item_json = item.to_dict()

    logger.debug(f"POST {create_url}")
    response = client._stac_io.session.post(
        create_url,
        json=item_json,
        headers={"Content-Type": "application/json"},
        timeout=client._stac_io.timeout or 30,
    )
    response.raise_for_status()

    logger.info(f"✅ Registered {item_id} (HTTP {response.status_code})")


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Register STAC item to STAC API")
    parser.add_argument("--stac-api-url", required=True, help="STAC API base URL")
    parser.add_argument("--collection-id", required=True, help="Target collection ID")
    parser.add_argument("--item-json", required=True, help="Path to item JSON file")
    parser.add_argument(
        "--mode",
        default="create-or-skip",
        choices=["create-or-skip", "upsert", "replace"],
        help="Registration mode (default: create-or-skip)",
    )
    args = parser.parse_args()

    with open(args.item_json) as f:
        item_dict = json.load(f)

    register_item(args.stac_api_url, args.collection_id, item_dict, args.mode)


if __name__ == "__main__":
    main()
