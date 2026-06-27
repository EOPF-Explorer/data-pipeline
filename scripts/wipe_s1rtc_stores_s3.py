"""Backup and delete S1 RTC cube stores from S3 for the geoid-fix reprocess (A1, issue #306).

For each tile in --tiles:
  1. List all objects under ``tests-output/sentinel-1-grd-rtc-staging/s1-rtc-<tile>.zarr/``
  2. Server-side copy (``copy_object``) each to ``backups/s1-306-geoid-20260626/<tile>/...``
     using a thread pool (--workers, default 200).
  3. Verify backup object count == source count. Abort the tile if counts diverge (fail-closed).
  4. ``delete_objects`` the source prefix in batches of 1000.

Dry-run by default — lists what would happen without touching S3.
Pass ``--execute`` to actually copy+delete.

Credentials via env: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY.
Endpoint: https://s3.de.io.cloud.ovh.net (OVH de region).

Usage:
    # dry-run
    uv run python scripts/wipe_s1rtc_stores_s3.py \\
        --tiles 30TWM,31TCG,32TLP
    # execute (destructive — backup + delete)
    uv run python scripts/wipe_s1rtc_stores_s3.py \\
        --tiles 30TUM,30TUN,...  \\
        --execute
"""

from __future__ import annotations

import argparse
import logging
import os
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("wipe_s1rtc_stores_s3")

BUCKET = "esa-zarr-sentinel-explorer-fra"
STORE_PREFIX = "tests-output/sentinel-1-grd-rtc-staging"
BACKUP_PREFIX = "backups/s1-306-geoid-20260626"
ENDPOINT = "https://s3.de.io.cloud.ovh.net"
REGION = "de"


def backup_key(source_key: str, tile: str, store_prefix: str, backup_prefix: str) -> str:
    """Map a source object key to its backup key under backup_prefix/<tile>/."""
    store_root = f"{store_prefix}/s1-rtc-{tile}.zarr/"
    rel = source_key[len(store_root) :]
    return f"{backup_prefix}/{tile}/{rel}"


def _chunk(items: list, size: int) -> Generator[list]:
    """Yield successive size-length chunks from items."""
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _list_objects(s3: Any, prefix: str) -> list[str]:
    """Return all object keys under prefix (follows pagination)."""
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def _copy_one(s3: Any, src_key: str, dst_key: str) -> str:
    """Server-side copy src_key → dst_key within BUCKET. Returns dst_key."""
    s3.copy_object(
        Bucket=BUCKET,
        CopySource={"Bucket": BUCKET, "Key": src_key},
        Key=dst_key,
    )
    return dst_key


def _backup_tile(s3: Any, tile: str, src_keys: list[str], *, workers: int, execute: bool) -> int:
    """Copy all src_keys to the backup prefix; verify count; return object count.

    Raises RuntimeError if the backup count diverges from the source count.
    """
    if not execute:
        for k in src_keys:
            dst = backup_key(k, tile, STORE_PREFIX, BACKUP_PREFIX)
            log.info("[dry-run] would copy s3://%s/%s → %s", BUCKET, k, dst)
        return len(src_keys)

    log.info(
        "tile %s: copying %d objects to backup prefix (workers=%d)", tile, len(src_keys), workers
    )
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_copy_one, s3, k, backup_key(k, tile, STORE_PREFIX, BACKUP_PREFIX)): k
            for k in src_keys
        }
        for done, fut in enumerate(as_completed(futures), start=1):
            fut.result()  # re-raise any exception
            if done % 500 == 0:
                log.info("  tile %s: %d/%d copied", tile, done, len(src_keys))

    # Verify backup count
    bk_prefix = f"{BACKUP_PREFIX}/{tile}/"
    bk_keys = _list_objects(s3, bk_prefix)
    if len(bk_keys) != len(src_keys):
        raise RuntimeError(
            f"tile {tile}: backup count {len(bk_keys)} != source count {len(src_keys)} — "
            "aborting delete for this tile (backup may be partial)"
        )
    log.info("tile %s: backup verified (%d objects)", tile, len(bk_keys))
    return len(src_keys)


def _delete_tile(s3: Any, tile: str, src_keys: list[str], *, execute: bool) -> int:
    """Delete all src_keys from BUCKET in batches of 1000."""
    if not execute:
        log.info("[dry-run] would delete %d objects for tile %s", len(src_keys), tile)
        return len(src_keys)

    deleted = 0
    for batch in _chunk(src_keys, 1000):
        objects = [{"Key": k} for k in batch]
        resp = s3.delete_objects(Bucket=BUCKET, Delete={"Objects": objects, "Quiet": True})
        errors = resp.get("Errors", [])
        if errors:
            raise RuntimeError(f"tile {tile}: delete_objects errors: {errors}")
        deleted += len(batch)
    log.info("tile %s: deleted %d source objects", tile, deleted)
    return deleted


def wipe_tile_store(
    s3: Any, tile: str, *, workers: int, execute: bool, skip_backup: bool = False
) -> dict[str, int]:
    """Backup then delete one tile's cube store. Returns {source, backup, deleted} counts."""
    src_prefix = f"{STORE_PREFIX}/s1-rtc-{tile}.zarr/"
    src_keys = _list_objects(s3, src_prefix)
    if not src_keys:
        log.info("tile %s: no objects found at %s — already wiped?", tile, src_prefix)
        return {"source": 0, "backed_up": 0, "deleted": 0}

    log.info("tile %s: found %d source objects", tile, len(src_keys))
    backed_up = 0
    if not skip_backup:
        backed_up = _backup_tile(s3, tile, src_keys, workers=workers, execute=execute)
    elif execute:
        log.info("tile %s: skipping backup (--skip-backup)", tile)
    deleted = _delete_tile(s3, tile, src_keys, execute=execute)
    return {"source": len(src_keys), "backed_up": backed_up, "deleted": deleted}


def _make_s3() -> Any:
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        region_name=REGION,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tiles", required=True, help="comma-separated MGRS tiles, e.g. 30TWM,31TCG")
    ap.add_argument(
        "--workers", type=int, default=200, help="concurrent copy workers per tile (default 200)"
    )
    ap.add_argument(
        "--tile-workers", type=int, default=1, help="tiles processed in parallel (default 1)"
    )
    ap.add_argument(
        "--execute",
        action="store_true",
        help="actually backup + delete (default: dry-run, lists what would happen)",
    )
    ap.add_argument(
        "--skip-backup",
        action="store_true",
        help="delete source objects without backing up first (use when data is corrupted/unwanted)",
    )
    args = ap.parse_args()
    tiles = [t.strip() for t in args.tiles.split(",") if t.strip()]

    s3 = _make_s3()

    results: list[dict[str, int]] = []
    failed: list[str] = []

    def _wipe(tile: str) -> dict[str, int]:
        return wipe_tile_store(
            s3, tile, workers=args.workers, execute=args.execute, skip_backup=args.skip_backup
        )

    with ThreadPoolExecutor(max_workers=args.tile_workers) as pool:
        future_to_tile = {pool.submit(_wipe, tile): tile for tile in tiles}
        for fut in as_completed(future_to_tile):
            tile = future_to_tile[fut]
            try:
                results.append(fut.result())
            except Exception as exc:
                log.error("tile %s FAILED: %s", tile, exc)
                failed.append(tile)

    total_src = sum(r["source"] for r in results)
    total_bk = sum(r["backed_up"] for r in results)
    total_del = sum(r["deleted"] for r in results)

    verb = "dry-run" if not args.execute else "wiped"
    log.info(
        "%s: %d tiles, %d source objects, %d backed up, %d deleted",
        verb,
        len(tiles),
        total_src,
        total_bk,
        total_del,
    )
    if failed:
        log.error("FAILED tiles (%d): %s", len(failed), ", ".join(failed))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
