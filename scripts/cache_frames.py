#!/usr/bin/env python3
"""In-region S3 frame cache for S1 GRD SAFEs (EOPF input-data caching, Tasks 5 & 6).

A single IW GRD frame (~250x170 km) overlaps ~a dozen adjacent MGRS tiles, so the
per-tile S1Tiling workflows each re-download the same frame from CDSE. This module
caches the *extracted SAFE tree* of each frame as one tar object in an in-region S3
prefix, keyed by product id:

    {prefix}/{prod_id}.tar      <-  tar of  data_raw/{prod_id}/{prod_id}.SAFE/

Operations: pull/populate are wired around s1processor by the Argo template (T7);
evict runs periodically (e.g. a small scheduled step) to bound the cache.

  pull      (pre-step)   for each needed frame present in the cache, download its
                         tar and extract it into data_raw/{prod_id}/{prod_id}.SAFE/
                         so S1Tiling's disk scan skips the CDSE download. Misses are
                         reported (s1processor downloads them as normal).
  populate  (post-step)  tar each SAFE s1processor freshly downloaded (a cache miss)
                         and upload it, so the next tile reuses it. Already-cached
                         frames are skipped.
  evict     (retention)  delete frames whose acquisition date is older than the
                         rolling window (--keep-days), so the cache never grows
                         unbounded; --dry-run lists exactly what would go (T9).

One tar per frame keeps the spike's single-object throughput (a SAFE is hundreds of
small files). A pull failure degrades to a miss (s1processor just downloads it) — it
must never fail the pipeline; an *invalid product id*, however, fails loud (it is a
path-traversal signal, not a transient).

Credentials come from the standard AWS env (a dedicated least-privilege key scoped
to the cache prefix — T4); the endpoint defaults to the in-region OVH S3.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import shutil
import sys
import tarfile
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError

log = logging.getLogger("cache_frames")

DEFAULT_ENDPOINT = "https://s3.de.io.cloud.ovh.net"
DEFAULT_PREFIX = "frame-cache"
DEFAULT_MAX_WORKERS = 8
# Evict frames whose acquisition is older than this; the cron only reprocesses a
# rolling window (lookback_days=7), so a few weeks of margin keeps recently-revisited
# frames while bounding the cache. Tune via --keep-days.
DEFAULT_KEEP_DAYS = 21

# S1 GRD product ids look like S1A_IW_GRDH_1SDV_20240101T... — uppercase letters,
# digits and underscores only. Anchoring + this charset rejects path traversal
# ("..", "/") and any shell/key-injection surface before the id is ever used to
# build an S3 key or a filesystem path.
_PROD_ID_RE = re.compile(r"\AS1[A-F]_[A-Z0-9_]{10,120}\Z")


def validate_prod_id(prod_id: str) -> str:
    """Return prod_id if it is a safe S1 product id, else raise ValueError.

    This is the trust boundary: prod_id flows into both an S3 key and a local
    filesystem path, so anything outside the strict S1 id grammar is rejected.
    """
    if not isinstance(prod_id, str) or not _PROD_ID_RE.match(prod_id):
        raise ValueError(f"unsafe or invalid S1 product id: {prod_id!r}")
    return prod_id


def frame_key(prefix: str, prod_id: str) -> str:
    """S3 key for a frame's cache tar. Validates prod_id first."""
    validate_prod_id(prod_id)
    return f"{prefix.rstrip('/')}/{prod_id}.tar"


def _safe_dir(data_raw: str | Path, prod_id: str) -> Path:
    return Path(data_raw) / prod_id / f"{prod_id}.SAFE"


def _is_present(data_raw: str | Path, prod_id: str) -> bool:
    """True if the extracted SAFE is already on disk (S1Tiling's skip condition:
    a valid manifest.safe under {prod_id}.SAFE/)."""
    return (_safe_dir(data_raw, prod_id) / "manifest.safe").is_file()


def cache_has(s3: Any, bucket: str, key: str) -> bool:
    """True if the object exists; False on 404; re-raise other errors."""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def _safe_extract(tar: tarfile.TarFile, dest: str | Path) -> None:
    """Extract a tar, rejecting any member that would escape dest or is a
    link/device (defends against a malicious or corrupt cache object).

    Members are validated up front, then extracted one at a time with
    ``tar.extract`` (never ``extractall``) so only vetted members are written and
    we don't rely on extractall's own member handling. Each extract also applies
    the stdlib ``data`` filter where available, as defence in depth.
    """
    dest_root = Path(dest).resolve()
    members = tar.getmembers()
    for member in members:
        if member.issym() or member.islnk() or member.isdev():
            raise ValueError(f"unsafe tar member type for {member.name!r}")
        target = (dest_root / member.name).resolve()
        if target != dest_root and dest_root not in target.parents:
            raise ValueError(f"tar member escapes destination: {member.name!r}")
    for member in members:
        try:
            tar.extract(member, dest_root, filter="data")
        except TypeError:  # `filter=` kwarg unavailable on older pythons
            tar.extract(member, dest_root)  # member pre-validated above


def pull_frame(s3: Any, bucket: str, prefix: str, prod_id: str, data_raw: str | Path) -> str:
    """Pull one frame from the cache into data_raw if present.

    Returns one of: "present" (already extracted on disk — idempotent no-op),
    "hit" (downloaded + extracted from cache), "miss" (not in cache). Raises only
    on an invalid id or a tar that does not yield a valid SAFE (integrity failure).
    """
    validate_prod_id(prod_id)
    if _is_present(data_raw, prod_id):
        return "present"
    key = frame_key(prefix, prod_id)
    if not cache_has(s3, bucket, key):
        return "miss"
    target = Path(data_raw) / prod_id
    target.mkdir(parents=True, exist_ok=True)
    # Stage into a temp dir on the SAME volume, verify the manifest, then swap the
    # SAFE in atomically. A failed or truncated pull must NOT leave a partial SAFE in
    # data_raw — s1processor would then see a manifest-less tree and mishandle it. The
    # tar buffer and the staging tree both live under data_raw (sized for SAFEs), not
    # /tmp (a small emptyDir/overlay that 8-way concurrency could exhaust).
    staging = Path(tempfile.mkdtemp(dir=target, prefix=".cache-stage-"))
    try:
        with tempfile.TemporaryFile(dir=target) as buf:
            s3.download_fileobj(bucket, key, buf)
            buf.seek(0)
            with tarfile.open(fileobj=buf, mode="r:*") as tar:
                _safe_extract(tar, staging)
        staged_safe = staging / f"{prod_id}.SAFE"
        if not (staged_safe / "manifest.safe").is_file():
            raise RuntimeError(
                f"cache tar for {prod_id} did not yield {prod_id}.SAFE/manifest.safe"
            )
        final = target / f"{prod_id}.SAFE"
        if final.exists():
            shutil.rmtree(final)
        os.replace(staged_safe, final)  # atomic on the same filesystem
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return "hit"


def pull_frames(
    s3: Any,
    bucket: str,
    prefix: str,
    prod_ids: list[str],
    data_raw: str | Path,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> dict[str, str]:
    """Pull frames in parallel. Returns {prod_id: status}. A per-frame failure
    (network/tar) degrades to "miss" so s1processor downloads it; invalid ids fail
    fast and loud before any I/O."""
    for pid in prod_ids:
        validate_prod_id(pid)  # fail fast on the whole batch
    results: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {
            ex.submit(pull_frame, s3, bucket, prefix, pid, data_raw): pid
            for pid in prod_ids
        }
        for fut in as_completed(futs):
            pid = futs[fut]
            try:
                results[pid] = fut.result()
            except Exception as exc:  # noqa: BLE001 - resilience: a bad cache obj must not fail the run
                log.warning("cache pull failed for %s (%s); treating as miss", pid, exc)
                results[pid] = "miss"
    return results


def populate_frame(
    s3: Any,
    bucket: str,
    prefix: str,
    prod_id: str,
    data_raw: str | Path,
    overwrite: bool = False,
) -> str:
    """Tar a freshly-downloaded SAFE and upload it to the cache.

    Returns: "absent" (no SAFE on disk to upload), "cached" (already in cache,
    skipped), "uploaded" (tar'd + uploaded, size-verified). Raises on invalid id or
    an upload size mismatch (silent partial — the failure mode of the old
    csi-rclone writeback this design replaces)."""
    validate_prod_id(prod_id)
    safe_dir = _safe_dir(data_raw, prod_id)
    if not (safe_dir / "manifest.safe").is_file():
        return "absent"
    key = frame_key(prefix, prod_id)
    if not overwrite and cache_has(s3, bucket, key):
        return "cached"
    with tempfile.TemporaryFile() as buf:
        with tarfile.open(fileobj=buf, mode="w") as tar:
            # arcname starts at {prod_id}.SAFE so a pull extracts into
            # data_raw/{prod_id}/{prod_id}.SAFE/ — symmetric with pull_frame.
            tar.add(safe_dir, arcname=f"{prod_id}.SAFE")
        size = buf.tell()
        buf.seek(0)
        s3.upload_fileobj(buf, bucket, key)
    remote = s3.head_object(Bucket=bucket, Key=key).get("ContentLength")
    if remote != size:
        raise RuntimeError(
            f"cache upload size mismatch for {prod_id}: local {size} != remote {remote}"
        )
    return "uploaded"


def populate_frames(
    s3: Any,
    bucket: str,
    prefix: str,
    prod_ids: list[str],
    data_raw: str | Path,
    max_workers: int = DEFAULT_MAX_WORKERS,
    overwrite: bool = False,
) -> dict[str, str]:
    """Upload freshly-downloaded SAFEs in parallel. Returns {prod_id: status}."""
    for pid in prod_ids:
        validate_prod_id(pid)
    results: dict[str, str] = {}
    errors: list[str] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {
            ex.submit(populate_frame, s3, bucket, prefix, pid, data_raw, overwrite): pid
            for pid in prod_ids
        }
        for fut in as_completed(futs):
            pid = futs[fut]
            try:
                results[pid] = fut.result()
            except Exception as exc:  # noqa: BLE001 - finish the batch, then surface
                results[pid] = "error"
                errors.append(f"{pid}: {exc}")
    if errors:
        # good frames are still cached; the run fails loud on any integrity error so a
        # silent partial upload can't masquerade as success.
        raise RuntimeError(f"cache populate failed for {len(errors)} frame(s): " + "; ".join(errors))
    return results


def discover_downloaded_frames(data_raw: str | Path) -> list[str]:
    """List prod_ids that have a valid extracted SAFE on disk (the populate input:
    everything in data_raw, whether cache-pulled or freshly CDSE-downloaded). The
    caller decides what to skip; populate_frame skips already-cached frames."""
    out = []
    root = Path(data_raw)
    if not root.is_dir():
        return out
    for child in sorted(root.iterdir()):
        if child.is_dir() and (child / f"{child.name}.SAFE" / "manifest.safe").is_file():
            try:
                out.append(validate_prod_id(child.name))
            except ValueError:
                log.warning("skipping non-S1 dir in data_raw: %s", child.name)
    return out


def _acq_date(prod_id: str) -> date:
    """Acquisition (start) date parsed from the product id, e.g.
    S1A_IW_GRDH_1SDV_20240101T060000_... -> 2024-01-01. The first `YYYYMMDDThhmmss`
    field is the sensing start. Raises ValueError if absent (unexpected id shape)."""
    m = re.search(r"_(\d{8})T\d{6}_", prod_id)
    if not m:
        raise ValueError(f"no acquisition timestamp in product id: {prod_id!r}")
    return datetime.strptime(m.group(1), "%Y%m%d").date()


def list_cached_frames(s3: Any, bucket: str, prefix: str) -> list[str]:
    """List the product ids of frames currently in the cache (paginated). Keys that
    aren't `{prefix}/{prod_id}.tar` with a valid id are skipped with a warning."""
    pfx = prefix.rstrip("/") + "/"
    out: list[str] = []
    token: str | None = None
    while True:
        kw: dict[str, Any] = {"Bucket": bucket, "Prefix": pfx}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            if not key.startswith(pfx) or not key.endswith(".tar"):
                continue
            pid = key[len(pfx):-len(".tar")]
            try:
                out.append(validate_prod_id(pid))
            except ValueError:
                log.warning("skipping unrecognised cache key: %s", key)
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return out


def evict_stale(
    s3: Any,
    bucket: str,
    prefix: str,
    keep_days: int = DEFAULT_KEEP_DAYS,
    today: date | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Remove cache frames whose acquisition date is older than `today - keep_days`,
    so the cache tracks the rolling reprocessing window and never grows unbounded.

    Keyed to the *acquisition* date (from the product id), not S3 LastModified — a
    frame reused (pulled) many times never has its mtime refreshed, so an mtime rule
    would wrongly evict still-in-window frames. Conservative: a frame whose date
    can't be parsed is KEPT (never delete what we can't classify). With dry_run the
    stale set is computed and returned but nothing is deleted.
    """
    today = today or date.today()
    cutoff = today - timedelta(days=keep_days)
    stale: list[str] = []
    kept = 0
    for pid in list_cached_frames(s3, bucket, prefix):
        try:
            acq = _acq_date(pid)
        except ValueError:
            log.warning("cannot parse acquisition date from %s; keeping it", pid)
            kept += 1
            continue
        if acq < cutoff:
            stale.append(pid)
        else:
            kept += 1
    removed: list[str] = []
    if not dry_run:
        for pid in stale:
            s3.delete_object(Bucket=bucket, Key=frame_key(prefix, pid))
            removed.append(pid)
    return {
        "cutoff": cutoff.isoformat(),
        "stale": sorted(stale),
        "kept": kept,
        "removed": sorted(removed),
        "dry_run": dry_run,
    }


def make_s3_client(endpoint: str | None) -> Any:
    """boto3 S3 client (matches the repo convention: explicit endpoint, else the
    AWS_ENDPOINT_URL env, else AWS default). Credentials via the standard chain.

    boto3 low-level clients are thread-safe, so one client is shared across the
    ThreadPoolExecutor workers in pull_frames/populate_frames."""
    config: dict[str, Any] = {}
    if endpoint:
        config["endpoint_url"] = endpoint
    elif os.getenv("AWS_ENDPOINT_URL"):
        config["endpoint_url"] = os.environ["AWS_ENDPOINT_URL"]
    return boto3.client("s3", **config)


def _read_frames(args: argparse.Namespace) -> list[str]:
    if args.frames:
        raw = args.frames
    elif args.frames_file:
        raw = Path(args.frames_file).read_text()
    else:
        raw = sys.stdin.read()
    return [f.strip() for f in re.split(r"[,\s]+", raw) if f.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("op", choices=("pull", "populate", "evict"))
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", default=DEFAULT_PREFIX)
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--data-raw", help="S1Tiling data_raw directory (pull/populate)")
    parser.add_argument("--frames", help="comma/space-separated product ids")
    parser.add_argument("--frames-file", help="file with one product id per line")
    parser.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS)
    parser.add_argument("--overwrite", action="store_true", help="(populate) re-upload cached frames")
    parser.add_argument("--keep-days", type=int, default=DEFAULT_KEEP_DAYS,
                        help="(evict) keep frames acquired within this many days")
    parser.add_argument("--today", help="(evict) override 'today' as YYYY-MM-DD (testing/repeatable runs)")
    parser.add_argument("--dry-run", action="store_true", help="(evict) list stale frames without deleting")
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s", stream=sys.stderr)
    args = build_parser().parse_args(argv)
    s3 = make_s3_client(args.endpoint)

    if args.op == "evict":
        today = date.fromisoformat(args.today) if args.today else None
        res = evict_stale(s3, args.bucket, args.prefix, args.keep_days, today, args.dry_run)
        log.info("frame cache evict (keep %dd, cutoff %s): %d stale, %d kept%s",
                 args.keep_days, res["cutoff"], len(res["stale"]), res["kept"],
                 " — dry-run, nothing deleted" if args.dry_run else f", {len(res['removed'])} removed")
        for pid in res["stale"]:  # stdout = exactly what is (or would be) removed
            print(pid)
        return 0

    if not args.data_raw:
        build_parser().error(f"--data-raw is required for '{args.op}'")

    if args.op == "pull":
        frames = _read_frames(args)
        results = pull_frames(s3, args.bucket, args.prefix, frames, args.data_raw, args.max_workers)
        misses = sorted(pid for pid, st in results.items() if st == "miss")
        hit = sum(v == "hit" for v in results.values())
        present = sum(v == "present" for v in results.values())
        log.info("frame cache pull: %d available (%d hits, %d already present), %d misses",
                 hit + present, hit, present, len(misses))
        # misses to stdout so the template/operator can see what CDSE must fetch
        for pid in misses:
            print(pid)
        return 0

    # populate: upload everything on disk not already cached
    frames = _read_frames(args) if (args.frames or args.frames_file) else discover_downloaded_frames(args.data_raw)
    results = populate_frames(s3, args.bucket, args.prefix, frames, args.data_raw, args.max_workers, args.overwrite)
    up = sum(v == "uploaded" for v in results.values())
    log.info("frame cache populate: %d uploaded, %d already cached, %d absent",
             up, sum(v == "cached" for v in results.values()), sum(v == "absent" for v in results.values()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
