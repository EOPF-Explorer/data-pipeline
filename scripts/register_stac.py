#!/usr/bin/env python3
"""STAC registration using pystac-client (simplified version)."""

from __future__ import annotations

import argparse
import json
import logging
from typing import Any

from metrics import STAC_REGISTRATION_TOTAL
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
    """Register STAC item using pystac-client.

    Args:
        stac_url: STAC API URL
        collection_id: Target collection
        item_dict: STAC item as dict
        mode: create-or-skip | upsert | replace
    """
    # Validate before sending
    item = Item.from_dict(item_dict)
    item.validate()

    item_id = item.id
    client = Client.open(stac_url)

    # Check existence
    try:
        existing = client.get_collection(collection_id).get_item(item_id)
        exists = existing is not None
    except Exception:
        exists = False

    if exists:
        if mode == "create-or-skip":
            logger.info(f"Item {item_id} exists, skipping")
            STAC_REGISTRATION_TOTAL.labels(
                collection=collection_id, operation="skip", status="success"
            ).inc()
            return

        # Delete then create for upsert/replace
        logger.info(f"Replacing {item_id}")
        delete_url = f"{stac_url}/collections/{collection_id}/items/{item_id}"
        client._stac_io._session.delete(delete_url)

    # Create item
    client.add_item(item, collection_id)
    logger.info(f"✅ Registered {item_id}")
    STAC_REGISTRATION_TOTAL.labels(
        collection=collection_id,
        operation="create" if not exists else "replace",
        status="success",
    ).inc()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stac-api", required=True)
    parser.add_argument("--collection", required=True)
    parser.add_argument("--item-json", required=True)
    parser.add_argument("--mode", default="create-or-skip")
    args = parser.parse_args()

    with open(args.item_json) as f:
        item_dict = json.load(f)

    register_item(args.stac_api, args.collection, item_dict, args.mode)


if __name__ == "__main__":
    main()
