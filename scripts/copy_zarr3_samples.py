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
import tempfile
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError
from s3_item_cleanup import check_urls_confined, parse_s3_prefix

logger = logging.getLogger("copy_zarr3_samples")

DEFAULT_S3_ENDPOINT = "https://s3.de.io.cloud.ovh.net"

# A run that writes more stores than this is a mistake, not a big job: the whole
# Track B sample is 4 stores. The cap is enforced here, inside the tool, rather
# than by a watcher or a kill -- a copy killed mid-store leaves a partial Zarr
# store that reads as valid metadata over missing chunks.
MAX_STORES_CEILING = 12

HTTP_TIMEOUT = 300

# Each object streams through a spooled file, so at most SPOOL_BYTES of it sits in
# memory and peak RSS stays near workers x SPOOL_BYTES however big a shard is (a
# lightly compressed 10 m band is one object of up to ~250 MB). The PUT sends the same
# aws-chunked request for a file body as for bytes (botocore 1.42, checked 2026-09-23).
SPOOL_BYTES = 64 * 2**20
READ_BYTES = 2**20

# The store root: its consolidated metadata is what a reader opens first.
ROOT_KEY = "zarr.json"


class CopyError(RuntimeError):
    """A copy could not be completed safely."""


class _RefuseRedirects(urllib.request.HTTPRedirectHandler):
    """Fail on any 3xx, as ``register_proxy.fetch_source_item`` does.

    A redirect could downgrade to http or move to another host, and the bytes would
    still be uploaded as if they came from the store that was asked for: the ETag
    check only proves the upload matches what was read, not where it was read from.
    """

    def redirect_request(self, req, fp, code, _msg, headers, newurl):  # type: ignore[no-untyped-def]
        raise urllib.error.HTTPError(req.full_url, code, f"redirect to {newurl}", headers, fp)


_open = urllib.request.build_opener(_RefuseRedirects).open


@dataclass
class StorePlan:
    """The exact object list for one store, derived before anything is written."""

    root: str
    name: str
    keys: list[str]
    required_keys: set[str] = field(default_factory=set)

    def dest_root(self, prefix: str) -> str:
        """``<prefix><store>/``: the one place the destination layout is spelled, so the
        keys confined before the run are the keys the run writes."""
        return f"{prefix}{self.name}/"


def fetch_json(url: str) -> dict[str, Any]:
    """GET and parse a JSON document. Whole object, never ranged."""
    with _open(url, timeout=HTTP_TIMEOUT) as response:
        parsed: dict[str, Any] = json.loads(response.read())
        return parsed


def chunk_keys_for_array(path: str, meta: dict[str, Any]) -> list[str]:
    """Return the outer-chunk-grid object keys for one array.

    A chunk object may legitimately be absent -- Zarr reads a missing chunk as the
    array's fill value. Measured on a real store: 23 of 187 chunks are absent, 19
    of them 0-d coordinate arrays and 4 whole ``quality/mask`` arrays that are
    entirely fill. So absence is copied as absence rather than treated as an error.

    That tolerance is also why the key form must follow ``chunk_key_encoding``
    exactly: a wrong form 404s on every chunk, and every 404 reads as "absent".
    """
    encoding = meta.get("chunk_key_encoding", {})
    name = encoding.get("name", "default")
    if name not in ("default", "v2"):
        raise CopyError(f"{path}: unknown chunk_key_encoding {name!r}")
    default_separator = "/" if name == "default" else "."
    separator = encoding.get("configuration", {}).get("separator", default_separator)
    chunk_shape = meta["chunk_grid"]["configuration"]["chunk_shape"]
    shape = meta["shape"]
    if not shape:
        return [f"{path}/c" if name == "default" else f"{path}/0"]

    grid = (math.ceil(size / chunk) for size, chunk in zip(shape, chunk_shape, strict=True))
    indices = (separator.join(map(str, ix)) for ix in itertools.product(*map(range, grid)))
    return [f"{path}/c{separator}{ix}" if name == "default" else f"{path}/{ix}" for ix in indices]


def check_node_path(path: str) -> None:
    """Refuse a node path that could step outside the store.

    The paths come verbatim from the source's metadata and end up in both the source URL
    and the S3 key, and the write confinement is a plain prefix match that a ``..``
    segment would pass.
    """
    if any(seg in ("", ".", "..") for seg in path.split("/")) or "?" in path or "#" in path:
        raise CopyError(f"unsafe node path in consolidated metadata: {path!r}")


def store_exists(s3_client: Any, bucket: str, key: str) -> bool:
    """True if ``key`` exists. A 404 is False; any other error is raised, not guessed."""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return False
        raise
    return True


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
    for path in nodes:
        check_node_path(path)
    # Every node's zarr.json is required: it is named in the consolidated metadata, so
    # its absence means the source is incomplete rather than that a default applies.
    required = {ROOT_KEY, *(f"{path}/zarr.json" for path in nodes)}
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


def assert_writes_confined(bucket: str, keys: list[str], allowed: tuple[str, str]) -> None:
    """Refuse the whole run unless every destination key sits under ``allowed``.

    Checked before the first PUT, not per object, so a misaimed prefix cannot
    write a single stray key before anything notices. The check is the cleanup's own
    (``check_urls_confined``), so a fix to one confinement reaches both.
    """
    violations = check_urls_confined({f"s3://{bucket}/{key}" for key in keys}, [allowed])
    if violations:
        url, reason = violations[0]
        raise CopyError(
            f"refusing to write {len(violations)} key(s) outside s3://{allowed[0]}/{allowed[1]}, "
            f"first: {url!r} ({reason})"
        )


def copy_object(
    s3_client: Any, source_url: str, bucket: str, dest_key: str, *, optional: bool
) -> int | None:
    """Fetch one object whole and PUT it, verifying the round trip by digest.

    Returns the byte count, or ``None`` when an optional object is absent.
    Raises when a required object is missing or the stored digest disagrees.
    """
    # md5 because that is what S3 returns as the ETag; this is an integrity
    # comparison against the source bytes, not a security hash.
    md5 = hashlib.md5(usedforsecurity=False)
    with tempfile.SpooledTemporaryFile(max_size=SPOOL_BYTES) as body:
        try:
            with _open(source_url, timeout=HTTP_TIMEOUT) as response:
                while block := response.read(READ_BYTES):
                    md5.update(block)
                    body.write(block)
                # The ETag check below proves only that the PUT holds what was read; this
                # proves the read got the whole object. EODC sends no ETag, so the declared
                # length is the one integrity signal the source gives.
                declared = response.headers.get("Content-Length")
                if declared is not None and body.tell() != int(declared):
                    raise CopyError(
                        f"GET {source_url} ended early: read {body.tell()} of {declared} bytes"
                    )
        except urllib.error.HTTPError as exc:
            if exc.code == 404 and optional:
                return None
            raise CopyError(f"GET {source_url} failed: HTTP {exc.code}") from exc
        size = body.tell()
        body.seek(0)
        result = s3_client.put_object(Bucket=bucket, Key=dest_key, Body=body, ContentLength=size)

    digest = md5.hexdigest()
    etag = (result.get("ETag") or "").strip('"')
    if etag != digest:
        raise CopyError(
            f"{dest_key}: stored ETag {etag!r} != source md5 {digest!r} -- "
            "the bytes that landed are not the bytes we read"
        )
    return size


def copy_store(
    s3_client: Any, plan: StorePlan, bucket: str, prefix: str, *, workers: int, dry_run: bool
) -> tuple[int, int, int]:
    """Copy one store. Returns ``(copied, absent, bytes)``."""
    dest_root = plan.dest_root(prefix)
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

    copied = total = 0
    absent: list[str] = []
    with ThreadPoolExecutor(workers) as pool:
        # The root is held back: see the commit-marker write below.
        futures = {pool.submit(one, key): key for key in plan.keys if key != ROOT_KEY}
        try:
            for future in as_completed(futures):
                size = future.result()
                if size is None:
                    absent.append(futures[future])
                else:
                    copied += 1
                    total += size
        except BaseException as exc:
            # Cancel what has not started, Ctrl-C included, or leaving the `with` would
            # wait for every queued copy to run into a store that is already broken.
            pool.shutdown(cancel_futures=True)
            if isinstance(exc, Exception):
                raise CopyError(f"{plan.name}: {exc}") from exc
            raise
    # Every chunk absent is not a fill-value store: it is a key form the source does not
    # use, or a source that lost its data. Either way the copy is metadata over nothing.
    chunks = len(plan.keys) - len(plan.required_keys)
    if chunks and len(absent) == chunks:
        raise CopyError(
            f"{plan.name}: all {chunks} chunk keys were absent -- the derived key form "
            "is wrong or the source holds no data"
        )
    # The root holds the consolidated metadata a reader opens, so it goes last, as a
    # commit marker: a store that failed or was interrupted part-way 404s on open instead
    # of reading its missing chunks as fill values.
    if ROOT_KEY in plan.keys:
        try:
            total += one(ROOT_KEY) or 0
        except Exception as exc:
            raise CopyError(f"{plan.name}: {exc}") from exc
        copied += 1
    logger.info(
        "   ✅ %s: %d objects, %d absent (404), %.1f MiB",
        plan.name,
        copied,
        len(absent),
        total / 2**20,
    )
    if absent:
        # Named, not only counted: a band that vanished looks exactly like an all-fill
        # array, so the list is what gets diffed against the known-absent baseline.
        logger.info("      absent (404): %s", ", ".join(sorted(absent)))
    return copied, len(absent), total


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
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Copy over a store that already exists at the destination (refused otherwise)",
    )
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
    # Only HTTPS sources: an http or file:// root would be read and uploaded as-is.
    not_https = [root for root in roots if urlparse(root).scheme != "https"]
    if not_https:
        logger.error("--store-root must be an HTTPS URL, got: %r", not_https[0])
        return 2
    # The copy is flat (<prefix>/<store>.zarr/), so two roots with one store name would
    # write the same keys, mixing chunks from two sources under one store.
    names = [root.rstrip("/").rsplit("/", 1)[-1] for root in roots]
    repeated = sorted({name for name in names if names.count(name) > 1})
    if repeated:
        logger.error("Refusing to run: store name(s) given more than once: %s", repeated)
        return 2

    if args.workers < 1:
        logger.error("--workers must be at least 1, got %d", args.workers)
        return 2

    try:
        bucket, prefix = parse_s3_prefix(args.dest)
        allowed = parse_s3_prefix(args.confine_to)
    except ValueError as exc:
        logger.error("%s", exc)
        return 2

    logger.info("📦 Planning %d store(s)", len(roots))
    try:
        plans = [plan_store(root) for root in roots]
    except (CopyError, OSError, ValueError, KeyError) as exc:
        logger.error("Planning failed, nothing written: %s", exc)
        return 1

    all_keys = [f"{plan.dest_root(prefix)}{key}" for plan in plans for key in plan.keys]
    try:
        assert_writes_confined(bucket, all_keys, allowed)
    except CopyError as exc:
        logger.error("%s", exc)
        return 2
    logger.info(
        "🔒 %d destination keys all confined to s3://%s/%s", len(all_keys), allowed[0], allowed[1]
    )

    client = boto3.client("s3", endpoint_url=args.s3_endpoint)
    # A same-named store already there would be mixed with this copy, and its objects
    # that this copy does not rewrite would survive to hide fill-value holes.
    try:
        existing = [
            plan.name
            for plan in plans
            if store_exists(client, bucket, f"{plan.dest_root(prefix)}{ROOT_KEY}")
        ]
    except ClientError as exc:
        logger.error("Could not check the destination for existing stores: %s", exc)
        return 1
    if existing and not args.overwrite:
        logger.error(
            "Refusing to run: store(s) already at the destination: %s (--overwrite replaces "
            "them, leaving any object this copy does not rewrite in place)",
            existing,
        )
        return 2

    copied = absent = total = 0
    partial: list[str] = []
    interrupted = False
    for plan in plans:
        try:
            one_copied, one_absent, one_total = copy_store(
                client, plan, bucket, prefix, workers=args.workers, dry_run=args.dry_run
            )
        except CopyError as exc:
            # Not deleted here: a re-run over a good earlier copy would take it with it.
            partial.append(f"s3://{bucket}/{plan.dest_root(prefix)}")
            logger.error("   ❌ %s", exc)
            continue
        except KeyboardInterrupt:
            partial.append(f"s3://{bucket}/{plan.dest_root(prefix)}")
            logger.error("   ⛔ %s: interrupted", plan.name)
            interrupted = True
            break
        copied += one_copied
        absent += one_absent
        total += one_total

    logger.info(
        "🏁 %s%d store(s): %d objects, %d absent, %.1f MiB",
        "[dry-run] " if args.dry_run else "",
        len(plans) - len(partial),
        copied,
        absent,
        total / 2**20,
    )
    if partial:
        logger.error(
            "PARTIAL store(s) -- delete before registering anything against them: %s",
            ", ".join(partial),
        )
        return 130 if interrupted else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
