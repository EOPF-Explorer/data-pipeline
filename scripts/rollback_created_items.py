#!/usr/bin/env python3
"""Roll back items the EODC registration created: DELETE each one, after re-checking it.

The input is a ``--created-ids`` JSONL written by ``register_proxy.py``'s pipeline mode, and
nothing else. Its ``"uncertain": true`` lines (a create that errored and may have committed)
are included: one that never committed answers 404 and is counted as ``absent``. Only the last
line may be torn (the append that failed; the run's "cannot record" error names that id).

A dry run by default. ``--apply`` deletes, after the operator types the collection id. Before
each DELETE the item is fetched again, and it is deleted only if it is exactly what the
pipeline creates: every ``data`` asset on data.eodc.eu, no ``alternate.s3`` anywhere, and
``derived_from`` pointing at the same id in EODC's sentinel-2-l2a-zarr3. Prod ids are EODC
ids, so a converted prod item with the same id fails that check and is refused, never deleted.

Exit 1 when anything was refused or failed, or on a refused input; 0 otherwise.

    uv run scripts/rollback_created_items.py --created-ids created-ids.jsonl \
        --collection sentinel-2-l2a-samples-zarr3-rollback --max-items 10 [--apply]
"""

import argparse
import json
import logging
import sys
from collections import Counter
from pathlib import Path
from urllib.parse import quote, urlparse

import requests
import stac_auth

logger = logging.getLogger(__name__)

EODC_DATA_HOST = "data.eodc.eu"
EODC_SOURCE_ITEMS = "https://stac.core.eopf.eodc.eu/collections/sentinel-2-l2a-zarr3/items/"


def read_created_ids(path: Path, collection: str) -> list[str]:
    """The ids to roll back, in order and deduplicated. Refuses (SystemExit) a line for another
    collection or a bad line anywhere but last."""
    lines = path.read_text().splitlines()
    ids: list[str] = []
    for number, raw in enumerate(lines, 1):
        if not raw.strip():
            continue
        try:
            entry = json.loads(raw)
        except json.JSONDecodeError:
            if number == len(lines):
                logger.warning("%s:%d: torn last line skipped: %r", path, number, raw[:120])
                continue
            raise SystemExit(
                f"{path}:{number}: not JSON, refusing the whole list: {raw[:120]!r}"
            ) from None
        if entry.get("collection") != collection:
            raise SystemExit(
                f"{path}:{number}: line is for {entry.get('collection')!r}, not {collection!r}"
            )
        ids.append(entry["id"])
    return list(dict.fromkeys(ids))


def not_ours(item: dict, item_id: str) -> str | None:
    """None if the item is exactly what the pipeline creates, else why it is not."""
    assets = item.get("assets", {}).values()
    data = [asset for asset in assets if "data" in asset.get("roles", [])]
    if not data:
        return "no data assets"
    for asset in data:
        if urlparse(asset.get("href", "")).hostname != EODC_DATA_HOST:
            return f"data asset not on {EODC_DATA_HOST}: {asset.get('href')}"
    if any("s3" in (asset.get("alternate") or {}) for asset in assets):
        return "an asset has alternate.s3"
    derived = [
        link.get("href") for link in item.get("links", []) if link.get("rel") == "derived_from"
    ]
    if derived != [EODC_SOURCE_ITEMS + item_id]:
        return f"derived_from is {derived}, not {EODC_SOURCE_ITEMS}{item_id}"
    return None


def roll_back(
    session: requests.Session, stac_api_url: str, collection: str, ids: list[str], apply: bool
) -> Counter[str]:
    counts: Counter[str] = Counter()
    base = f"{stac_api_url.rstrip('/')}/collections/{quote(collection, safe='')}/items/"
    for item_id in ids:
        url = base + quote(item_id, safe="")
        try:
            got = session.get(url, timeout=30)
            if got.status_code == 404:
                logger.info("absent %s", item_id)
                counts["absent"] += 1
                continue
            got.raise_for_status()
            why = not_ours(got.json(), item_id)
            if why:
                logger.error("REFUSED %s: %s", item_id, why)
                counts["refused"] += 1
                continue
            if not apply:
                logger.info("would delete %s", item_id)
                counts["would_delete"] += 1
                continue
            deleted = session.delete(url, timeout=30)
        except requests.RequestException as exc:
            logger.error("FAILED %s: %s", item_id, exc)
            counts["failed"] += 1
            continue
        if deleted.status_code in (200, 202, 204):
            logger.info("deleted %s", item_id)
            counts["deleted"] += 1
        else:
            logger.error("FAILED %s: DELETE answered %d", item_id, deleted.status_code)
            counts["failed"] += 1
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--created-ids", type=Path, required=True)
    parser.add_argument("--collection", required=True, help="must match every line")
    parser.add_argument("--max-items", type=int, required=True, help="refuse a longer list whole")
    parser.add_argument("--stac-api-url", default="https://api.explorer.eopf.copernicus.eu/stac")
    parser.add_argument("--apply", action="store_true", help="delete (default: dry run)")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    ids = read_created_ids(args.created_ids, args.collection)
    if len(ids) > args.max_items:
        logger.error("Refusing: %d ids exceed --max-items %d", len(ids), args.max_items)
        return 1
    if args.apply:
        typed = input(
            f"DELETE up to {len(ids)} item(s) from {args.collection} on {args.stac_api_url}.\n"
            "Type the collection id to confirm: "
        )
        if typed.strip() != args.collection:
            logger.error("Not confirmed; nothing deleted")
            return 1

    session = requests.Session()
    session.auth = stac_auth.bearer_auth
    counts = roll_back(session, args.stac_api_url, args.collection, ids, args.apply)
    logger.info("Summary for %s: %s", args.collection, dict(sorted(counts.items())))
    return 1 if counts["refused"] or counts["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
