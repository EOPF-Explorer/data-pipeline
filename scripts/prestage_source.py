#!/usr/bin/env python3
"""Pre-stage a Sentinel-2 source Zarr from EODC to the OVH output bucket.

Conversion used to read the source over HTTPS through fsspec ``simplecache`` for the
whole 9+ minute convert. That is fragile under load and — since cpm_v270 gave
``quality/atmosphere/aot``/``wvp`` a single 10980x10980 chunk — deterministically
broken: dask splits that one Zarr chunk into ~9 concurrent reads of the *same* object
key, and ``WholeFileCacheFileSystem._cat_file`` downloads a cache miss straight to its
final filename with no temp+rename, so a second reader gets a truncated file and blosc
dies with ``error during blosc decompression: -1`` (data-pipeline#339).

Copying the source to the output bucket first lets convert read it back over the
native ``s3://`` path, which uses s3fs and no simplecache at all — killing both the
race and the EODC-availability coupling. Measured 1.26 GB / 657 objects in 21.6 s
in-cluster, against 9+ minute converts.

Modes:
- ``copy`` (default): stage the source, then print an ``s3://`` URL for convert.
- ``passthrough``: echo the original source URL, touch nothing (the ``prestage_source``
  feature flag being off).
- ``cleanup``: delete one staged copy after a successful register.

Safety model (this deletes S3 objects, so the guards are explicit):
- Only hosts on ``--copyable-hosts`` are copied. Anything else passes through: the
  nginx-s3-gateway serves GETs but answers ListObjectsV2 with an HTML index, so
  gateway-hosted sources (``cpm-manual/``) must keep their existing direct path.
- EODC is strictly read-only. Never attempt a write against the source endpoint.
- A stage is not complete until dest object count *and* byte total match the source.
- ``cleanup`` only ever deletes ``<dest_prefix>/<item_id>/``, and refuses to build
  that prefix from an empty dest_prefix or an empty item segment — a bucket- or
  prefix-wide delete must be unreachable, not merely unlikely.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import boto3
import botocore.handlers
from botocore import UNSIGNED
from botocore.config import Config
from source_url_utils import derive_item_id, resolve_zarr_url

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)
for lib in ["botocore", "s3fs", "aiobotocore", "urllib3", "httpx", "httpcore"]:
    logging.getLogger(lib).setLevel(logging.WARNING)

DEFAULT_DEST_PREFIX = "source-cache"
DEFAULT_COPYABLE_HOSTS = "objects.eodc.eu"
# 16 threads x ~20 s is a short burst against EODC, versus the ~80 sustained
# connections a convert used to hold open. Lower it here if EODC ever throttles.
DEFAULT_COPY_WORKERS = 16
# The Argo template reads these output params from a literal `/tmp/<name>` path, so
# this must stay "/tmp" rather than follow TMPDIR. Writing known filenames to an
# ephemeral single-tenant pod filesystem carries none of the shared-/tmp risk B108
# guards against.
DEFAULT_OUTPUT_DIR = "/tmp"  # noqa: S108  # nosec B108 -- Argo output-param contract

# S3 delete_objects accepts at most 1000 keys per call (matches s3_item_cleanup).
BATCH_SIZE = 1000

# Object metadata we compare to decide "already staged": (size, etag).
ObjectMeta = tuple[int, str]


@dataclass(frozen=True)
class S3Href:
    """An https URL re-expressed as the S3 coordinates behind it."""

    endpoint: str
    bucket: str
    key: str


def parse_https_s3_href(href: str) -> S3Href:
    """Map an S3-backed https href to endpoint/bucket/key.

    EODC's ``objects.eodc.eu`` is a Ceph Object Gateway using path-style addressing,
    so the first path segment is the bucket -- and it legitimately contains a colon
    (Ceph ``tenant:container``).
    """
    parsed = urlparse(href)
    if not parsed.hostname:
        raise ValueError(f"source href has no host: {href!r}")
    port = f":{parsed.port}" if parsed.port and parsed.port != 443 else ""
    bucket, _, key = parsed.path.lstrip("/").partition("/")
    if not bucket or not key:
        raise ValueError(f"source href is not <host>/<bucket>/<key>: {href!r}")
    return S3Href(endpoint=f"https://{parsed.hostname}{port}", bucket=bucket, key=key.rstrip("/"))


def staged_prefix(dest_prefix: str, item_id: str) -> str:
    """Build the staged-copy key prefix ``<dest_prefix>/<item_id>/``.

    This is the only place a staged prefix is constructed, so it is also the place
    that refuses to build one wide enough to delete more than a single item.
    """
    base = dest_prefix.strip("/").strip()
    item = item_id.strip("/").strip()
    if not base:
        raise ValueError("dest_prefix must not be empty — refusing to address a whole bucket")
    if not item:
        raise ValueError(f"empty item segment — refusing to address all of {base!r}")
    return f"{base}/{item}/"


def staged_url(dest_bucket: str, dest_prefix: str, item_id: str) -> str:
    """Native ``s3://`` URL of the staged copy — convert reads this, not https."""
    return f"s3://{dest_bucket}/{staged_prefix(dest_prefix, item_id).rstrip('/')}"


def _source_client(endpoint: str) -> Any:
    """Read-only client for the source object store.

    Signed with the read-only EODC identity when credentials are present, anonymous
    otherwise (both are accepted by EODC today).
    """
    access_key = os.getenv("EODC_ACCESS_KEY_ID")
    secret_key = os.getenv("EODC_SECRET_ACCESS_KEY")
    signed = bool(access_key and secret_key)
    logger.info("Source %s (%s)", endpoint, "signed" if signed else "anonymous")

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(
            max_pool_connections=64,
            retries={"max_attempts": 5, "mode": "adaptive"},
            read_timeout=120,
            connect_timeout=30,
            # None is botocore's default (resolve normally); UNSIGNED skips signing.
            signature_version=None if signed else UNSIGNED,
        ),
    )
    # Ceph tenant buckets contain ':', which botocore rejects client-side even though
    # the server accepts them.
    client.meta.events.unregister(
        "before-parameter-build.s3", botocore.handlers.validate_bucket_name
    )
    return client


def _dest_client() -> Any:
    """Client for the OVH output bucket (standard AWS_* env + AWS_ENDPOINT_URL)."""
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
        config=Config(max_pool_connections=64, retries={"max_attempts": 5, "mode": "standard"}),
    )


def _list_objects(client: Any, bucket: str, prefix: str) -> dict[str, ObjectMeta]:
    """List ``prefix`` as ``{key: (size, etag)}``."""
    objects: dict[str, ObjectMeta] = {}
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            objects[obj["Key"]] = (obj["Size"], obj["ETag"].strip('"'))
    return objects


def _copy_one(
    source_client: Any,
    dest_client: Any,
    source_bucket: str,
    source_key: str,
    dest_bucket: str,
    dest_key: str,
) -> int:
    """GET one source object and PUT it to the destination. Returns bytes copied."""
    body = source_client.get_object(Bucket=source_bucket, Key=source_key)["Body"].read()
    dest_client.put_object(Bucket=dest_bucket, Key=dest_key, Body=body)
    return len(body)


def _write_outputs(output_dir: str, convert_source_url: str, staged: bool) -> None:
    """Write the Argo output parameters the DAG branches on."""
    Path(output_dir, "convert_source_url").write_text(convert_source_url)
    Path(output_dir, "staged").write_text("true" if staged else "false")
    logger.info("convert_source_url=%s staged=%s", convert_source_url, staged)


def _passthrough(args: argparse.Namespace, reason: str) -> int:
    """Hand convert the original source URL, exactly as before prestage existed."""
    logger.info("Passthrough (%s): %s", reason, args.source_url)
    _write_outputs(args.output_dir, args.source_url, staged=False)
    return 0


def run_prestage(args: argparse.Namespace) -> int:
    """Copy the source Zarr to the output bucket and report the staged s3:// URL."""
    if args.mode == "passthrough":
        return _passthrough(args, "mode=passthrough")

    zarr_url = resolve_zarr_url(args.source_url)
    copyable_hosts = {h.strip().lower() for h in args.copyable_hosts.split(",") if h.strip()}
    host = (urlparse(zarr_url).hostname or "").lower()
    if host not in copyable_hosts:
        # e.g. the nginx-s3-gateway, which cannot ListObjectsV2.
        return _passthrough(args, f"host {host!r} not in {sorted(copyable_hosts)}")

    source = parse_https_s3_href(zarr_url)
    item_id = derive_item_id(args.source_url)
    dest_root = staged_prefix(args.dest_prefix, item_id)
    source_prefix = source.key + "/"

    source_client = _source_client(source.endpoint)
    dest_client = _dest_client()

    source_objects = _list_objects(source_client, source.bucket, source_prefix)
    if not source_objects:
        logger.error("No objects under s3://%s/%s — nothing to stage", source.bucket, source_prefix)
        return 3
    source_bytes = sum(size for size, _ in source_objects.values())
    logger.info(
        "Source: %d objects, %.2f GB → s3://%s/%s",
        len(source_objects),
        source_bytes / 1e9,
        args.dest_bucket,
        dest_root,
    )

    already_staged = _list_objects(dest_client, args.dest_bucket, dest_root)
    pending = {
        key: dest_root + key.removeprefix(source_prefix)
        for key, meta in source_objects.items()
        if already_staged.get(dest_root + key.removeprefix(source_prefix)) != meta
    }
    if skipped := len(source_objects) - len(pending):
        logger.info("Skipping %d object(s) already staged with matching size+ETag", skipped)

    started = time.monotonic()
    copied_bytes = 0
    done = 0
    with ThreadPoolExecutor(max_workers=args.copy_workers) as pool:
        futures = [
            pool.submit(
                _copy_one,
                source_client,
                dest_client,
                source.bucket,
                source_key,
                args.dest_bucket,
                dest_key,
            )
            for source_key, dest_key in pending.items()
        ]
        for future in as_completed(futures):
            copied_bytes += future.result()
            done += 1
            if done % 100 == 0:
                logger.info("  %d/%d objects, %.2f GB", done, len(pending), copied_bytes / 1e9)
    elapsed = time.monotonic() - started
    logger.info(
        "Copied %d objects, %.2f GB in %.1fs (%.0f MB/s)",
        done,
        copied_bytes / 1e9,
        elapsed,
        copied_bytes / elapsed / 1e6 if elapsed else 0,
    )

    # Never report a stage as usable without proving it matches the source.
    staged_objects = _list_objects(dest_client, args.dest_bucket, dest_root)
    staged_bytes = sum(size for size, _ in staged_objects.values())
    if len(staged_objects) != len(source_objects) or staged_bytes != source_bytes:
        logger.error(
            "Verification FAILED: staged %d objects / %d bytes, source %d objects / %d bytes",
            len(staged_objects),
            staged_bytes,
            len(source_objects),
            source_bytes,
        )
        return 2
    logger.info("Verified %d objects, %d bytes", len(staged_objects), staged_bytes)

    _write_outputs(args.output_dir, staged_url(args.dest_bucket, args.dest_prefix, item_id), True)
    return 0


def run_cleanup(args: argparse.Namespace) -> int:
    """Delete one staged copy. Runs after register, so leftovers are cheap, not fatal."""
    prefix = staged_prefix(args.dest_prefix, derive_item_id(args.source_url))
    dest_client = _dest_client()
    keys = sorted(_list_objects(dest_client, args.dest_bucket, prefix))
    if not keys:
        logger.info("Nothing staged under s3://%s/%s — already clean", args.dest_bucket, prefix)
        return 0

    deleted = 0
    for start in range(0, len(keys), BATCH_SIZE):
        batch = keys[start : start + BATCH_SIZE]
        response = dest_client.delete_objects(
            Bucket=args.dest_bucket, Delete={"Objects": [{"Key": key} for key in batch]}
        )
        deleted += len(response.get("Deleted", []))
        for error in response.get("Errors", []):
            logger.warning("Delete failed for %s: %s", error.get("Key"), error.get("Code"))
    logger.info("Deleted %d/%d staged objects under %s", deleted, len(keys), prefix)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-url", required=True, help="STAC item URL or direct Zarr URL")
    parser.add_argument("--dest-bucket", required=True, help="Output bucket to stage into")
    parser.add_argument(
        "--dest-prefix",
        default=DEFAULT_DEST_PREFIX,
        help=f"Prefix for staged copies (default: {DEFAULT_DEST_PREFIX})",
    )
    parser.add_argument(
        "--mode",
        choices=["copy", "passthrough", "cleanup"],
        default="copy",
        help="copy: stage the source; passthrough: echo it; cleanup: delete the staged copy",
    )
    parser.add_argument(
        "--copyable-hosts",
        default=DEFAULT_COPYABLE_HOSTS,
        help=f"Comma-separated S3-capable hosts to copy from; others pass through "
        f"(default: {DEFAULT_COPYABLE_HOSTS})",
    )
    parser.add_argument(
        "--copy-workers",
        type=int,
        default=DEFAULT_COPY_WORKERS,
        help=f"Parallel copy threads (default: {DEFAULT_COPY_WORKERS})",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory for Argo output parameter files (default: {DEFAULT_OUTPUT_DIR})",
    )
    args = parser.parse_args(argv)

    try:
        return run_cleanup(args) if args.mode == "cleanup" else run_prestage(args)
    except ValueError as exc:
        logger.error("%s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
