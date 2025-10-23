#!/usr/bin/env python3
"""STAC registration using pystac-client (simplified version)."""

from __future__ import annotations

import argparse
import json
import logging
from typing import Any

from metrics import STAC_REGISTRATION_TOTAL
from pystac import Item

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
    """
    from pystac_client import Client

    # Load item (skip local validation - STAC API will validate)
    # Working production items have inconsistent raster properties that validate
    # successfully in the STAC API but fail local pystac validation
    item = Item.from_dict(item_dict)

    item_id = item.id

    # Open client to reuse its StacApiIO session
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
            STAC_REGISTRATION_TOTAL.labels(collection=collection_id, status="success").inc()
            return

        # Delete for upsert/replace using StacApiIO's session
        logger.info(f"Replacing {item_id}")
        delete_url = f"{stac_url}/collections/{collection_id}/items/{item_id}"
        try:
            # Use the session directly for DELETE (not in StacApiIO.request)
            resp = client._stac_io.session.delete(delete_url, timeout=30)
            if resp.status_code not in (200, 204):
                logger.warning(f"Delete returned {resp.status_code}")
        except Exception as e:
            logger.warning(f"Delete failed (item may not exist): {e}")

    # Create item via POST using StacApiIO's session
    # Note: StacApiIO.request() only accepts status 200, but STAC Transaction
    # extension returns 201 for creates, so we use the session directly
    create_url = f"{stac_url}/collections/{collection_id}/items"
    item_json = item.to_dict()

    try:
        logger.debug(f"POST {create_url}")
        response = client._stac_io.session.post(
            create_url,
            json=item_json,
            headers={"Content-Type": "application/json"},
            timeout=client._stac_io.timeout or 30,
        )
        response.raise_for_status()

        logger.info(f"✅ Registered {item_id} (HTTP {response.status_code})")
        STAC_REGISTRATION_TOTAL.labels(
            collection=collection_id,
            status="success",
        ).inc()
    except Exception as e:
        logger.error(f"Failed to register {item_id}: {e}")
        STAC_REGISTRATION_TOTAL.labels(
            collection=collection_id,
            status="failure",
        ).inc()
        raise


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
