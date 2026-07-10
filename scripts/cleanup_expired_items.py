#!/usr/bin/env python3
"""Drain expired STAC items — S3 delete, validate, then STAC delete.

Discovers items whose STAC ``expires`` property is in the past (timestamps
extension, stamped by register_v1 / the backfill migration) and removes them:
delete the S3 objects, verify nothing remains, then delete the STAC item.
Coordination#183.

Safety model (a destructive tool, so the defaults are conservative):
- **Dry-run is the default**; real deletion requires ``--execute``.
- An item with no ``expires`` is skipped — structurally undeletable, which is
  how manual/demo data is protected.
- Every s3:// asset must live under ``--allowed-bucket`` or the item is skipped.
- S3 deletion is validated (0 objects remain) before the STAC item is removed.
- Optional ``--exclude-file`` denylist of item IDs is always skipped.
- One JSON line per item is written to stdout (audit trail), plus a summary.

Modelled on the single-pod frame-cache-evict cron: one coherent JSONL log, no
fan-out; concurrency is bounded by the CronWorkflow semaphore.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse

import boto3
import requests
from pystac_client import Client
from s3_item_cleanup import (
    count_s3_objects_for_item,
    delete_s3_objects_for_item,
    extract_s3_urls_from_item,
)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)
for lib in ["botocore", "s3fs", "aiobotocore", "urllib3", "httpx", "httpcore"]:
    logging.getLogger(lib).setLevel(logging.WARNING)

DEFAULT_ALLOWED_BUCKET = "esa-zarr-sentinel-explorer-fra"
DEFAULT_MAX_ITEMS = 100
ISO_Z = "%Y-%m-%dT%H:%M:%SZ"


def _now() -> datetime:
    return datetime.now(UTC)


def _parse_expires(value: str) -> datetime:
    """Parse a STAC RFC3339 timestamp (``Z`` or ``+00:00``) to aware UTC."""
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def build_search_kwargs(collection: str, now: datetime, max_items: int) -> dict[str, Any]:
    """CQL2 discovery query for items whose ``expires`` is before ``now``,
    oldest-first, capped at ``max_items``."""
    return {
        "collections": [collection],
        "filter_lang": "cql2-json",
        "filter": {
            "op": "<",
            "args": [{"property": "expires"}, now.strftime(ISO_Z)],
        },
        "sortby": "+properties.expires",
        "max_items": max_items,
    }


def load_exclude_ids(path: str | None) -> set[str]:
    """Read a newline-delimited item-ID denylist. Blank lines and ``#``
    comments are ignored."""
    if not path:
        return set()
    ids: set[str] = set()
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                ids.add(stripped)
    return ids


def evaluate_guards(
    item: dict[str, Any],
    *,
    now: datetime,
    exclude_ids: set[str],
    allowed_bucket: str,
) -> tuple[bool, str]:
    """Decide whether an item may be deleted. Returns (ok, reason).

    Order matters: an item with no ``expires`` is undeletable regardless of
    anything else, and an excluded item must never touch S3.
    """
    expires = item.get("properties", {}).get("expires")
    if not expires:
        return False, "no_expires"
    if _parse_expires(expires) >= now:
        return False, "not_expired"
    if item.get("id") in exclude_ids:
        return False, "excluded"
    for url in extract_s3_urls_from_item(item):
        if urlparse(url).netloc != allowed_bucket:
            return False, "wrong_bucket"
    return True, "ok"


def _audit(item: dict[str, Any], dry_run: bool, status: str, **fields: Any) -> dict[str, Any]:
    """Build one audit record with a stable field set."""
    record = {
        "ts": _now().strftime(ISO_Z),
        "event": "cleanup_item",
        "dry_run": dry_run,
        "collection": item.get("collection"),
        "item_id": item.get("id"),
        "expires": item.get("properties", {}).get("expires"),
        "s3_objects_deleted": 0,
        "s3_objects_failed": 0,
        "s3_remaining": 0,
        "stac_deleted": False,
        "status": status,
    }
    record.update(fields)
    return record


def _delete_stac_item(
    session: requests.Session,
    stac_base_url: str,
    collection: str,
    item_id: str,
) -> tuple[bool, str]:
    """DELETE the STAC item. 404 is success (idempotent); 401/403 is a distinct
    ``auth_required`` signal (expected once stac-auth-proxy enforcement lands)."""
    url = f"{stac_base_url.rstrip('/')}/collections/{collection}/items/{item_id}"
    resp = session.delete(url, timeout=30)
    if resp.status_code in (200, 202, 204, 404):
        return True, "deleted"
    if resp.status_code in (401, 403):
        return False, "auth_required"
    return False, f"stac_delete_http_{resp.status_code}"


def process_item(
    item: dict[str, Any],
    *,
    now: datetime,
    exclude_ids: set[str],
    allowed_bucket: str,
    s3_client: Any,
    session: requests.Session,
    stac_base_url: str,
    dry_run: bool,
) -> dict[str, Any]:
    """Process one already-refetched item; return its audit record.

    Never raises for expected per-item failures — they become a status so the
    run can continue and the audit log stays complete.
    """
    ok, reason = evaluate_guards(
        item, now=now, exclude_ids=exclude_ids, allowed_bucket=allowed_bucket
    )
    if not ok:
        return _audit(item, dry_run, reason)

    s3_urls = extract_s3_urls_from_item(item)

    if dry_run:
        would_delete = count_s3_objects_for_item(s3_client, s3_urls)
        return _audit(item, dry_run, "dry_run", s3_remaining=would_delete)

    deleted, failed = delete_s3_objects_for_item(s3_client, s3_urls)
    if failed > 0:
        return _audit(
            item,
            dry_run,
            "s3_validation_failed",
            s3_objects_deleted=deleted,
            s3_objects_failed=failed,
        )

    remaining = count_s3_objects_for_item(s3_client, s3_urls)
    if remaining > 0:
        return _audit(
            item,
            dry_run,
            "s3_validation_failed",
            s3_objects_deleted=deleted,
            s3_remaining=remaining,
        )

    stac_ok, status = _delete_stac_item(
        session, stac_base_url, item.get("collection", ""), item.get("id", "")
    )
    return _audit(
        item,
        dry_run,
        status,
        s3_objects_deleted=deleted,
        s3_remaining=0,
        stac_deleted=stac_ok,
    )


def _session(stac_api_url: str) -> requests.Session:
    """Build the STAC HTTP session.

    Auth seam: when the stac-auth-proxy branch lands, wire the bearer token
    here (2-line change) — e.g. ``import stac_auth; stac_auth.apply(session)``.
    Today it is an unauthenticated session, matching main.
    """
    session = requests.Session()
    session.headers.update({"Content-Type": "application/json"})
    return session


def _s3_client(s3_endpoint: str | None) -> Any:
    endpoint = s3_endpoint or os.getenv("AWS_ENDPOINT_URL")
    if endpoint:
        return boto3.client("s3", endpoint_url=endpoint)
    return boto3.client("s3")


def run_cleanup(args: argparse.Namespace) -> int:
    """Discover and process expired items; emit JSONL; return exit code."""
    now = _now()
    exclude_ids = load_exclude_ids(args.exclude_file)
    dry_run = not args.execute

    client = Client.open(args.stac_api_url)
    session = _session(args.stac_api_url)
    s3_client = _s3_client(args.s3_endpoint)
    stac_base_url = str(client.self_href).rstrip("/")

    logger.info(
        "Cleanup start: collection=%s dry_run=%s max_items=%d allowed_bucket=%s",
        args.collection,
        dry_run,
        args.max_items,
        args.allowed_bucket,
    )

    search = client.search(**build_search_kwargs(args.collection, now, args.max_items))

    counts: dict[str, int] = {}
    failures = 0
    processed = 0
    for stale in search.items_as_dicts():
        # Re-fetch fresh: the search index can lag the catalogue.
        fresh = _fetch_item(session, stac_base_url, args.collection, stale["id"]) or stale
        record = process_item(
            fresh,
            now=now,
            exclude_ids=exclude_ids,
            allowed_bucket=args.allowed_bucket,
            s3_client=s3_client,
            session=session,
            stac_base_url=stac_base_url,
            dry_run=dry_run,
        )
        print(json.dumps(record), flush=True)
        counts[record["status"]] = counts.get(record["status"], 0) + 1
        processed += 1
        if record["status"] in ("s3_validation_failed", "auth_required") or record[
            "status"
        ].startswith("stac_delete_http_"):
            failures += 1

    summary = {
        "ts": _now().strftime(ISO_Z),
        "event": "cleanup_summary",
        "dry_run": dry_run,
        "collection": args.collection,
        "processed": processed,
        "by_status": counts,
        "failures": failures,
    }
    print(json.dumps(summary), flush=True)
    return 1 if failures else 0


def _fetch_item(
    session: requests.Session,
    stac_base_url: str,
    collection: str,
    item_id: str,
) -> dict[str, Any] | None:
    url = f"{stac_base_url.rstrip('/')}/collections/{collection}/items/{item_id}"
    try:
        resp = session.get(url, timeout=30)
        if resp.status_code == 200:
            result: dict[str, Any] = resp.json()
            return result
    except requests.RequestException as exc:
        logger.warning("Re-fetch failed for %s: %s", item_id, exc)
    return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stac-api-url", required=True, help="STAC API base URL")
    parser.add_argument("--collection", required=True, help="Collection to scan")
    parser.add_argument(
        "--s3-endpoint",
        default=None,
        help="S3 endpoint URL (default: AWS_ENDPOINT_URL env)",
    )
    parser.add_argument(
        "--allowed-bucket",
        default=DEFAULT_ALLOWED_BUCKET,
        help="Every s3:// asset must be under this bucket or the item is skipped",
    )
    parser.add_argument(
        "--max-items",
        type=int,
        default=DEFAULT_MAX_ITEMS,
        help="Cap on items processed per run",
    )
    parser.add_argument(
        "--exclude-file",
        default=os.getenv("EXPIRES_EXCLUDE_FILE"),
        help="Newline-delimited item-ID denylist (always skipped)",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually delete. Omit for a dry-run (the default).",
    )
    args = parser.parse_args(argv)
    return run_cleanup(args)


if __name__ == "__main__":
    sys.exit(main())
