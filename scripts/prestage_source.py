#!/usr/bin/env python3
"""Pre-stage a source Zarr from EODC to the OVH output bucket.

Mission-agnostic by construction: this copies whatever Zarr store a ``source_url``
resolves to, without knowing or caring which mission produced it. Sentinel-2 is the
first caller; Sentinel-3 (OLCI/SLSTR) needs no change here — its products sit in the
same Ceph tenant buckets on the same host and its STAC items resolve through the same
rules (verified against live ``sentinel-3-olci-l1-efr`` / ``-slstr-l1-rbt`` items; see
``source_url_utils`` for the shared rules). Everything environment-specific is a CLI
argument, so a new mission is a template change, not a code change.

Sentinel-2 is what forced this, but the failure is not S2-specific — any mission read
through fsspec ``simplecache`` has the same exposure. Conversion used to read the
source over HTTPS through ``simplecache`` for the whole 9+ minute convert. That is
fragile under load and — since cpm_v270 gave ``quality/atmosphere/aot``/``wvp`` a
single 10980x10980 chunk — deterministically broken: dask splits that one Zarr chunk
into ~9 concurrent reads of the *same* object key, and
``WholeFileCacheFileSystem._cat_file`` downloads a cache miss straight to its final
filename with no temp+rename, so a second reader gets a truncated file and blosc dies
with ``error during blosc decompression: -1`` (data-pipeline#339).

Copying the source to the output bucket first lets convert read it back over the
native ``s3://`` path, which uses s3fs and no simplecache at all — killing both the
race and the EODC-availability coupling. Measured 1.26 GB / 657 objects in 21.6 s
in-cluster, against 9+ minute converts.

Modes:
- ``copy`` (default): stage the source, then print an ``s3://`` URL for convert.
- ``passthrough``: echo the original source URL, touch nothing (the ``prestage_source``
  feature flag being off).
- ``cleanup``: delete one staged copy after a successful register.

Exit codes are a contract with the Argo template, whose retry expression skips 1 and 3
and retries the rest — so classifying a fault wrongly either burns backoff on something
hopeless or permanently fails something a retry would fix:
- 0: staged (or passed through) and proven usable.
- 1: permanent config/permission fault (bad URL, identity lacks an action). Not retried.
- 2: verification mismatch. Retried — the copy skips keys already present with matching
  size+ETag, so a retry re-copies only what is missing.
- 3: the source prefix listed empty. Not retried.
- 4: transient S3 fault (throttling, 5xx). Retried with backoff; 16 copy threads against
  EODC can draw a SlowDown, and that is fixed by waiting, not by editing a policy.

Safety model (this deletes S3 objects, so the guards are explicit):
- Only hosts on ``--copyable-hosts`` are copied. Anything else passes through: the
  nginx-s3-gateway serves GETs but answers ListObjectsV2 with an HTML index, so
  gateway-hosted sources (``cpm-manual/``) must keep their existing direct path.
- EODC is strictly read-only. Never attempt a write against the source endpoint.
- A stage is not complete until dest object count *and* byte total match the source.
- ``cleanup`` only ever deletes ``<dest_prefix>/<namespace>/<item_id>/``, and refuses to
  build that prefix from an empty dest_prefix, namespace or item segment — a bucket-,
  namespace- or prefix-wide delete must be unreachable, not merely unlikely.
- The namespace segment comes from POD_NAMESPACE (downward API), not a parameter, so two
  namespaces sharing this bucket cannot share a staged key however the template is
  copied. See ``staged_prefix``.
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
from botocore.exceptions import ClientError
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


def staged_prefix(dest_prefix: str, namespace: str, item_id: str) -> str:
    """Build the staged-copy key prefix ``<dest_prefix>/<namespace>/<item_id>/``.

    This is the only place a staged prefix is constructed, so it is also the place that
    refuses to build one wide enough to delete more than a single item — or one that a
    different namespace could share.

    The namespace segment is load-bearing. devseed and devseed-staging share both this
    bucket and s3_output_prefix; converted output escapes collision only because its path
    carries the collection (``.../{collection}/{item_id}.zarr``), while a staged key has
    no such discriminator and item_id is identical across namespaces. Without the
    segment, one namespace's cleanup deletes the other's staged copy out from under a
    running convert — and both crons see the same recent products, so that is routine
    rather than unlucky.

    It is deliberately not a template parameter someone types: the prod convert template
    is a near-copy of staging's, so a hand-set value gets copy-pasted forward and
    silently re-shares the prefix. It comes from the pod's own namespace (downward API),
    which cannot be copied wrong.
    """
    base = dest_prefix.strip("/").strip()
    ns = namespace.strip("/").strip()
    item = item_id.strip("/").strip()
    if not base:
        raise ValueError("dest_prefix must not be empty — refusing to address a whole bucket")
    if not ns:
        raise ValueError(
            "empty namespace segment — refusing to build a staged prefix that another "
            "namespace could share (set POD_NAMESPACE via the downward API, or --namespace)"
        )
    if not item:
        raise ValueError(f"empty item segment — refusing to address all of {base}/{ns}")
    return f"{base}/{ns}/{item}/"


def staged_url(dest_bucket: str, dest_prefix: str, namespace: str, item_id: str) -> str:
    """Native ``s3://`` URL of the staged copy — convert reads this, not https."""
    return f"s3://{dest_bucket}/{staged_prefix(dest_prefix, namespace, item_id).rstrip('/')}"


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


# Exit code for an S3 failure that a retry can plausibly fix. The Argo template's retry
# expression skips exit 1 and 3 (permanent faults) and retries everything else, so a
# transient error must NOT be reported as 1 — 16 copy threads against EODC can draw a
# SlowDown, and that is fixed by backing off, not by editing an IAM policy.
EXIT_TRANSIENT_S3 = 4

# Codes meaning "this identity is not allowed to do this". No amount of retrying helps,
# and only these deserve the identity hints below. Everything else (SlowDown, 5xx,
# RequestTimeout) is transient: boto3 has already exhausted its own fast retries, but the
# template's backoff is minutes, which is a different proposition.
PERMISSION_CODES = frozenset(
    {
        "AccessDenied",
        "AllAccessDisabled",
        "AccountProblem",
        "InvalidAccessKeyId",
        "SignatureDoesNotMatch",
        "UnauthorizedAccess",
    }
)


def _s3_code(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Code", "?"))


# Which identity a failure belongs to. A denial on the source and a denial on the
# destination need completely different fixes, so they must never share a message.
SOURCE_HINT = (
    "This reads EODC, which normally allows anonymous access to the product buckets. "
    "If EODC_ACCESS_KEY_ID is set (eodc-s3-credentials), the key may be wrong or expired "
    "— unsetting it falls back to anonymous."
)
DEST_HINT = (
    "This is the AWS_* identity (geozarr-s3-credentials). It needs ListBucket, PutObject "
    "and GetObject on the staged prefix — convert reads the staged copy with these same "
    "credentials, so a gap here breaks convert too, as an opaque read error."
)


def _list_objects(client: Any, bucket: str, prefix: str, *, hint: str) -> dict[str, ObjectMeta]:
    """List ``prefix`` as ``{key: (size, etag)}``.

    `hint` names the identity this listing uses, so a permission failure reports the
    thing that needs fixing instead of an unattributed traceback.
    """
    objects: dict[str, ObjectMeta] = {}
    try:
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                objects[obj["Key"]] = (obj["Size"], obj["ETag"].strip('"'))
    except ClientError as exc:
        code = _s3_code(exc)
        if code in PERMISSION_CODES:
            raise ValueError(f"Cannot list s3://{bucket}/{prefix} ({code}). {hint}") from exc
        # Not a permission fault — do not attach an identity hint that would send the
        # reader to fix the wrong thing. main() classifies it as retryable.
        raise
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


def _assert_staged_readable(client: Any, bucket: str, key: str) -> None:
    """Prove the staged copy can be GET, not merely counted.

    Staging is verified by listing the destination, which only exercises ListObjectsV2.
    Convert reads the copy with GetObject, using this same identity (the AWS_* env from
    geozarr-s3-credentials — the same credentials that write the converted output, since
    source and destination share one endpoint and one account).

    That identity now reads as well as writes. If it is ever scoped down to
    write-without-read on this prefix, the copy succeeds, the object count matches, this
    step reports success, and the failure surfaces minutes later inside convert as an
    opaque zarr/blosc read error that looks like a conversion bug. One GET here turns
    that into an accurate message at the step that is actually misconfigured.
    """
    try:
        body = client.get_object(Bucket=bucket, Key=key)["Body"]
        try:
            body.read(1)
        finally:
            # Abandoning a partially-read StreamingBody leaves the connection out of the
            # pool; we only ever want the first byte.
            body.close()
    except ClientError as exc:
        code = _s3_code(exc)
        if code not in PERMISSION_CODES:
            raise  # transient — main() reports it as retryable, without an identity hint
        raise ValueError(
            f"Staged the copy but cannot read it back ({code} on s3://{bucket}/{key}). "
            f"Convert reads the staged copy with these same credentials "
            f"(AWS_ACCESS_KEY_ID, geozarr-s3-credentials), so it would fail too — but as "
            f"an opaque read error that looks like a conversion bug. This identity needs "
            f"GetObject on the staged prefix, not just PutObject/ListBucket."
        ) from exc


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
    dest_root = staged_prefix(args.dest_prefix, args.namespace, item_id)
    source_prefix = source.key + "/"

    source_client = _source_client(source.endpoint)
    dest_client = _dest_client()

    source_objects = _list_objects(source_client, source.bucket, source_prefix, hint=SOURCE_HINT)
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

    already_staged = _list_objects(dest_client, args.dest_bucket, dest_root, hint=DEST_HINT)
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

    # Never report a stage as usable without proving it matches the source AND that the
    # thing downstream actually does can be done to it.
    staged_objects = _list_objects(dest_client, args.dest_bucket, dest_root, hint=DEST_HINT)
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

    _assert_staged_readable(dest_client, args.dest_bucket, sorted(staged_objects)[0])

    _write_outputs(
        args.output_dir,
        staged_url(args.dest_bucket, args.dest_prefix, args.namespace, item_id),
        True,
    )
    return 0


def run_cleanup(args: argparse.Namespace) -> int:
    """Delete one staged copy. Runs after register, so leftovers are cheap, not fatal."""
    prefix = staged_prefix(args.dest_prefix, args.namespace, derive_item_id(args.source_url))
    dest_client = _dest_client()
    keys = sorted(_list_objects(dest_client, args.dest_bucket, prefix, hint=DEST_HINT))
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
        "--namespace",
        default=os.getenv("POD_NAMESPACE", ""),
        help="Namespace segment for the staged key. Defaults to POD_NAMESPACE, which the "
        "Argo template sets from the pod's own namespace (downward API) so it cannot be "
        "copy-pasted wrong between devseed and devseed-staging, which share this bucket.",
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
        # Configuration/permission faults, already carrying an actionable message.
        logger.error("%s", exc)
        return 1
    except ClientError as exc:
        code = _s3_code(exc)
        if code in PERMISSION_CODES:
            logger.error("S3 permission error (%s): %s", code, exc)
            return 1
        logger.error(
            "Transient S3 error (%s) — exiting %d so the workflow retries with backoff: %s",
            code,
            EXIT_TRANSIENT_S3,
            exc,
        )
        return EXIT_TRANSIENT_S3


if __name__ == "__main__":
    sys.exit(main())
