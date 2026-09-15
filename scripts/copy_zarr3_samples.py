#!/usr/bin/env python3
"""Copy Samples Service Zarr v3 stores from EODC (HTTPS) to our OVH S3 bucket.

Track B of the S2 Samples proxy plan (coordination#287). OVH serves HTTP Range
correctly where EODC's gateway does not, so a copy on OVH is what produces the
tile / GDAL / OpenLayers evidence without waiting on EODC.

Three things about the source shape the design:

1. **There is no listing.** ``data.eodc.eu`` 404s on a directory URL and its
   ``?list-type=2`` returns EODC's own JSON API, not S3 XML. The key set is instead
   *derived* from the consolidated metadata in the store's root ``zarr.json``,
   which is exact: one ``zarr.json`` per node, plus one object per chunk of the
   outer chunk grid.

2. **The arrays are sharded** — a 10980x10980 band is a single 11264x11264 shard,
   i.e. one object. That is also precisely why EODC's Range bug bites: reading an
   inner chunk needs a non-zero-offset range into that shard.

3. **Therefore we never issue a ranged read.** Every object is fetched whole. A
   non-zero-offset range against EODC returns the *head* of the object with the
   right Content-Length, so a ranged copy would silently write corrupt data and
   every size-based check would pass. Whole-object GETs sidestep it entirely.

The copy is flat — ``<prefix>/<store>.zarr/<key>`` — to match
``register_proxy.py --store-root-base``, which rebuilds the root as
``<base>/<store>.zarr``.
"""

import argparse
import hashlib
import itertools
import json
import logging
import math
import sys
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

import boto3

logger = logging.getLogger("copy_zarr3_samples")

DEFAULT_S3_ENDPOINT = "https://s3.de.io.cloud.ovh.net"

# A run that writes more stores than this is a mistake, not a big job: the whole
# Track B sample is 4 stores. The cap is enforced here, inside the tool, rather
# than by a watcher or a kill -- a copy killed mid-store leaves a partial Zarr
# store that reads as valid metadata over missing chunks.
MAX_STORES_CEILING = 12

HTTP_TIMEOUT = 300


class CopyError(RuntimeError):
    """A copy could not be completed safely."""


@dataclass
class StorePlan:
    """The exact object list for one store, derived before anything is written."""

    root: str
    name: str
    keys: list[str]
    required_keys: set[str] = field(default_factory=set)


def fetch_json(url: str) -> dict[str, Any]:
    """GET and parse a JSON document. Whole object, never ranged."""
    with urllib.request.urlopen(url, timeout=HTTP_TIMEOUT) as response:  # noqa: S310  # nosec B310 -- https source store
        parsed: dict[str, Any] = json.loads(response.read())
        return parsed


def chunk_keys_for_array(path: str, meta: dict[str, Any]) -> list[str]:
    """Return the outer-chunk-grid object keys for one array.

    A chunk object may legitimately be absent -- Zarr reads a missing chunk as the
    array's fill value. Measured on a real store: 23 of 187 chunks are absent, 19
    of them 0-d coordinate arrays and 4 whole ``quality/mask`` arrays that are
    entirely fill. So absence is copied as absence rather than treated as an error.
    """
    chunk_shape = meta["chunk_grid"]["configuration"]["chunk_shape"]
    separator = meta.get("chunk_key_encoding", {}).get("configuration", {}).get("separator", "/")
    shape = meta["shape"]
    if not shape:
        return [f"{path}/c"]

    grid = (math.ceil(size / chunk) for size, chunk in zip(shape, chunk_shape, strict=True))
    return [
        f"{path}/c{separator}{separator.join(map(str, ix))}"
        for ix in itertools.product(*(range(extent) for extent in grid))
    ]


def plan_store(root: str) -> StorePlan:
    """Derive every object key in ``root`` from its consolidated metadata."""
    root = root.rstrip("/")
    root_meta = fetch_json(f"{root}/zarr.json")
    consolidated = root_meta.get("consolidated_metadata")
    if not consolidated:
        raise CopyError(
            f"{root}: root zarr.json has no consolidated_metadata, so the key set "
            "cannot be derived and the source offers no listing"
        )

    nodes = consolidated["metadata"]
    # Every node's zarr.json is required: it is named in the consolidated metadata, so
    # its absence means the source is incomplete rather than that a default applies.
    required = {"zarr.json", *(f"{path}/zarr.json" for path in nodes)}
    keys = sorted(required)
    for path, meta in nodes.items():
        if meta.get("node_type") == "array":
            keys.extend(chunk_keys_for_array(path, meta))

    name = root.rsplit("/", 1)[-1]
    logger.info(
        "   📋 %s: %d keys derived (%d metadata required, %d chunks)",
        name,
        len(keys),
        len(required),
        len(keys) - len(required),
    )
    return StorePlan(root=root, name=name, keys=keys, required_keys=required)


def parse_confinement(spec: str) -> tuple[str, str]:
    """Split ``s3://bucket/prefix`` into ``(bucket, prefix)`` with a trailing slash."""
    parsed = urlparse(spec)
    if parsed.scheme != "s3":
        raise ValueError(f"confinement must be an s3:// URL, got: {spec!r}")
    if not parsed.netloc:
        raise ValueError(f"confinement is missing a bucket: {spec!r}")
    prefix = parsed.path.lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return parsed.netloc, prefix


def assert_writes_confined(bucket: str, keys: list[str], allowed: tuple[str, str]) -> None:
    """Refuse the whole run unless every destination key sits under ``allowed``.

    Checked before the first PUT, not per object, so a misaimed prefix cannot
    write a single stray key before anything notices.
    """
    allowed_bucket, allowed_prefix = allowed
    if bucket != allowed_bucket:
        raise CopyError(f"refusing to write to bucket {bucket!r}, confined to {allowed_bucket!r}")
    stray = [k for k in keys if not k.startswith(allowed_prefix)]
    if stray:
        raise CopyError(
            f"refusing to write {len(stray)} key(s) outside {allowed_prefix!r}, first: {stray[0]!r}"
        )


def copy_object(
    s3_client: Any, source_url: str, bucket: str, dest_key: str, *, optional: bool
) -> int | None:
    """Fetch one object whole and PUT it, verifying the round trip by digest.

    Returns the byte count, or ``None`` when an optional object is absent.
    Raises when a required object is missing or the stored digest disagrees.
    """
    try:
        with urllib.request.urlopen(  # noqa: S310  # nosec B310 -- https source store
            source_url, timeout=HTTP_TIMEOUT
        ) as response:
            body = response.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 404 and optional:
            return None
        raise CopyError(f"GET {source_url} failed: HTTP {exc.code}") from exc

    # md5 because that is what S3 returns as the ETag; this is an integrity
    # comparison against the source bytes, not a security hash.
    digest = hashlib.md5(body, usedforsecurity=False).hexdigest()
    result = s3_client.put_object(Bucket=bucket, Key=dest_key, Body=body)
    etag = (result.get("ETag") or "").strip('"')
    if etag != digest:
        raise CopyError(
            f"{dest_key}: stored ETag {etag!r} != source md5 {digest!r} -- "
            "the bytes that landed are not the bytes we read"
        )
    return len(body)


def copy_store(
    s3_client: Any, plan: StorePlan, bucket: str, prefix: str, *, workers: int, dry_run: bool
) -> tuple[int, int, int]:
    """Copy one store. Returns ``(copied, absent, bytes)``."""
    dest_root = f"{prefix}{plan.name}/"
    if dry_run:
        logger.info(
            "   [dry-run] would copy %d keys to s3://%s/%s", len(plan.keys), bucket, dest_root
        )
        return 0, 0, 0

    def one(key: str) -> int | None:
        return copy_object(
            s3_client,
            f"{plan.root}/{key}",
            bucket,
            f"{dest_root}{key}",
            optional=key not in plan.required_keys,
        )

    copied = absent = total = 0
    with ThreadPoolExecutor(workers) as pool:
        for size in pool.map(one, plan.keys):
            if size is None:
                absent += 1
            else:
                copied += 1
                total += size
    logger.info(
        "   ✅ %s: %d objects, %s absent-by-fill-value, %.1f MiB",
        plan.name,
        copied,
        absent,
        total / 2**20,
    )
    return copied, absent, total


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--store-root", action="append", default=[], help="Source store root URL, repeatable"
    )
    parser.add_argument("--dest", required=True, help="Destination as s3://bucket/prefix/")
    parser.add_argument(
        "--confine-to",
        required=True,
        help="s3://bucket/prefix every write must sit under; refuses the run otherwise",
    )
    parser.add_argument("--s3-endpoint", default=DEFAULT_S3_ENDPOINT)
    parser.add_argument(
        "--max-stores",
        type=int,
        required=True,
        help=f"Hard cap on stores written, enforced here (1..{MAX_STORES_CEILING})",
    )
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--dry-run", action="store_true", help="Plan and confine, write nothing")
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = build_parser().parse_args(argv)

    if not 1 <= args.max_stores <= MAX_STORES_CEILING:
        logger.error(
            "--max-stores must be 1..%d, got %d (there is no uncapped mode: a partial "
            "store is a broken store)",
            MAX_STORES_CEILING,
            args.max_stores,
        )
        return 2

    roots = args.store_root
    if not roots:
        logger.error("no store roots given (--store-root)")
        return 2
    if len(roots) > args.max_stores:
        logger.error(
            "Refusing to run: %d store roots exceed --max-stores %d", len(roots), args.max_stores
        )
        return 2

    bucket, prefix = parse_confinement(args.dest)
    allowed = parse_confinement(args.confine_to)

    logger.info("📦 Planning %d store(s)", len(roots))
    plans = [plan_store(root) for root in roots]

    all_keys = [f"{prefix}{plan.name}/{key}" for plan in plans for key in plan.keys]
    assert_writes_confined(bucket, all_keys, allowed)
    logger.info(
        "🔒 %d destination keys all confined to s3://%s/%s", len(all_keys), allowed[0], allowed[1]
    )

    client = boto3.client("s3", endpoint_url=args.s3_endpoint)
    copied = absent = total = 0
    for plan in plans:
        one_copied, one_absent, one_total = copy_store(
            client, plan, bucket, prefix, workers=args.workers, dry_run=args.dry_run
        )
        copied += one_copied
        absent += one_absent
        total += one_total

    logger.info(
        "🏁 %s%d store(s): %d objects, %d absent, %.1f MiB",
        "[dry-run] " if args.dry_run else "",
        len(plans),
        copied,
        absent,
        total / 2**20,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
