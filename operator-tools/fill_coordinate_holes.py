"""Fill fill-value holes in 1-D zarr v3 arrays on S3 from a reviewed plan, and change nothing else.

Written for the S1 RTC per-slice coordinates (``r10m/platform``, ``r10m/relative_orbit``) that
``ingest_v1_s1_rtc._sync_tree`` lost on S3: a rewritten chunk whose compressed size had not changed
was not uploaded, so S3 kept the fill value. The values come from a plan file built and reviewed
separately; this tool only writes them.

For each planned array it reads ``zarr.json`` and the single chunk, decodes the chunk with zarr in a
temp dir, fills the planned indices, and re-encodes. A store is written only if, for every array:
- it is a 1-D, unsharded zarr v3 array held in one chunk, with exactly the planned length and fill;
- every planned index holds the fill value now, and no other value changes;
- no fill value is left afterwards (the plan covers every hole);
- ``zarr.json`` is untouched (only the chunk ``c/0`` is written).
An array whose planned indices already hold the planned values is skipped, so a re-run is a no-op.

Safety properties (as in ``consolidate_zarr_groups.py``):
- dry-run by default; ``--apply`` needs ``--backup-dir`` and an explicit ``--s3-endpoint``
- ``--max-writes`` is a hard bound on PUTs, checked before a store is started, so a bounded run
  always stops between stores
- every chunk is backed up (base64, with the hash of its replacement) and fsync'd before its store's
  first PUT; ``--restore`` puts them back, and refuses a chunk changed since the repair unless
  ``--force``
- each chunk's ETag is re-checked before writing, every PUT is read back, any failed or uncertain
  PUT stops the run, and 3 consecutive or 10 total failures abort it
"""

import argparse
import base64
import fcntl
import hashlib
import json
import logging
import os
import sys
import tempfile
import time
from dataclasses import dataclass, field
from itertools import groupby
from pathlib import Path
from typing import Any, TextIO

import numpy as np
import zarr
from botocore.exceptions import BotoCoreError, ClientError
from urllib3.exceptions import HTTPError as Urllib3Error

logger = logging.getLogger(__name__)

PLAN_FORMAT = "fill-coordinate-holes/1"
BACKUP_FORMAT = "fill-coordinate-holes-backup/1"
MAX_CONSECUTIVE_FAILURES = 3
MAX_TOTAL_FAILURES = 10
# What an S3 call can raise: service errors, transport errors, and a TLS failure while a body
# streams (urllib3 raises that one unwrapped). As in consolidate_zarr_groups.py.
S3_ERRORS = (ClientError, BotoCoreError, Urllib3Error)


class PlanError(Exception):
    """A store cannot be repaired safely; nothing is written to it."""


@dataclass
class ChunkWrite:
    key: str
    etag: str
    old: bytes
    new: bytes


@dataclass
class StorePlan:
    bucket: str
    prefix: str
    writes: list[ChunkWrite] = field(default_factory=list)
    already_filled: list[str] = field(default_factory=list)


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """``s3://bucket/a/b.zarr`` -> ``("bucket", "a/b.zarr")``."""
    bucket, _, prefix = uri.removeprefix("s3://").partition("/")
    prefix = prefix.strip("/")
    if not uri.startswith("s3://") or not bucket or not prefix:
        raise ValueError(f"expected s3://bucket/prefix, got {uri!r}")
    return bucket, prefix


def _get(s3: Any, bucket: str, key: str) -> tuple[bytes, str] | None:
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return None
        raise
    return resp["Body"].read(), resp["ETag"]


def chunk_key(prefix: str, path: str, meta: dict[str, Any]) -> str:
    """The key of the array's only chunk; PlanError for any layout this tool does not handle."""
    where = f"{prefix}/{path}"
    if meta.get("zarr_format") != 3 or meta.get("node_type") != "array":
        raise PlanError(f"{where}: not a zarr v3 array")
    shape = meta.get("shape", [])
    chunks = meta.get("chunk_grid", {}).get("configuration", {}).get("chunk_shape", [])
    if len(shape) != 1 or len(chunks) != 1 or shape[0] > chunks[0]:
        raise PlanError(f"{where}: not a 1-D array in one chunk (shape {shape}, chunks {chunks})")
    if any(c.get("name") == "sharding_indexed" for c in meta.get("codecs", [])):
        raise PlanError(f"{where}: sharded")
    encoding = meta.get("chunk_key_encoding", {})
    separator = encoding.get("configuration", {}).get("separator", "/")
    if encoding.get("name") != "default" or separator != "/":
        raise PlanError(f"{where}: chunk key encoding {encoding} is not handled")
    return f"{prefix}/{path}/c/0"


def plan_array(
    s3: Any, bucket: str, prefix: str, spec: dict[str, Any], workdir: Path
) -> ChunkWrite | None:
    """The chunk write that fills ``spec``'s holes, or None when they are already filled."""
    path = spec["path"].strip("/")
    where = f"s3://{bucket}/{prefix}/{path}"
    got = _get(s3, bucket, f"{prefix}/{path}/zarr.json")
    if got is None:
        raise PlanError(f"{where}: no zarr.json")
    meta_body, _ = got
    meta = json.loads(meta_body)
    key = chunk_key(prefix, path, meta)
    if meta["shape"][0] != spec["length"]:
        raise PlanError(f"{where}: length {meta['shape'][0]}, the plan expects {spec['length']}")
    got = _get(s3, bucket, key)
    if got is None:
        raise PlanError(f"{where}: no chunk c/0")
    old, etag = got

    local = workdir / path
    (local / "c").mkdir(parents=True)
    (local / "zarr.json").write_bytes(meta_body)
    (local / "c" / "0").write_bytes(old)
    array = zarr.open_array(str(local), mode="r+", zarr_format=3)
    fill = np.asarray(array.fill_value).tolist()
    if fill != spec["fill"]:
        raise PlanError(f"{where}: fill value {fill!r}, the plan expects {spec['fill']!r}")
    before = np.asarray(array[:])
    planned = {int(i): v for i, v in spec["values"].items()}
    if not planned or not all(0 <= i < len(before) for i in planned):
        raise PlanError(f"{where}: planned indices {sorted(planned)} are empty or out of range")
    if all(before[i].tolist() == v for i, v in planned.items()) and not (before == fill).any():
        return None  # already filled by an earlier run
    for i in planned:
        if before[i].tolist() != fill:
            raise PlanError(f"{where}: index {i} holds {before[i].tolist()!r}, not the fill value")

    expected = before.copy()
    for i, value in planned.items():
        expected[i] = value
    if (expected == fill).any():
        missing = np.flatnonzero(expected == fill).tolist()
        raise PlanError(f"{where}: the plan leaves fill values at {missing}")
    array[:] = expected

    # `expected` is `before` with only the planned holes filled, so this also proves that nothing
    # outside the plan changes.
    after = np.asarray(zarr.open_array(str(local), mode="r", zarr_format=3)[:])
    if not np.array_equal(after, expected):
        raise PlanError(f"{where}: the re-encoded chunk does not decode to the planned values")
    if (local / "zarr.json").read_bytes() != meta_body:
        raise PlanError(f"{where}: zarr rewrote zarr.json")
    files = sorted(p.relative_to(local).as_posix() for p in local.rglob("*") if p.is_file())
    if files != ["c/0", "zarr.json"]:
        raise PlanError(f"{where}: unexpected files after filling: {files}")
    return ChunkWrite(key, etag, old, (local / "c" / "0").read_bytes())


def plan_store(s3: Any, entry: dict[str, Any], workdir: Path) -> StorePlan:
    """Work out every chunk write for one store, without writing anything to S3."""
    bucket, prefix = parse_s3_uri(entry["store"])
    paths = [a["path"].strip("/") for a in entry["arrays"]]
    if len(set(paths)) != len(paths):
        raise PlanError(f"{entry['store']}: an array is planned twice")
    plan = StorePlan(bucket=bucket, prefix=prefix)
    for spec in entry["arrays"]:
        write = plan_array(s3, bucket, prefix, spec, workdir)
        if write is None:
            plan.already_filled.append(spec["path"])
        else:
            plan.writes.append(write)
    return plan


def load_plan(path: Path) -> list[dict[str, Any]]:
    plan = json.loads(path.read_text())
    if plan.get("format") != PLAN_FORMAT:
        raise ValueError(f"plan format {plan.get('format')!r}, this tool reads {PLAN_FORMAT}")
    stores = [entry["store"].rstrip("/") for entry in plan["stores"]]
    if len(set(stores)) != len(stores):
        raise ValueError("a store appears twice in the plan")
    return list(plan["stores"])


class FillRun:
    """One bounded repair (or restore) run."""

    def __init__(self, s3: Any, max_writes: int, apply: bool, backup_dir: Path | None) -> None:
        if max_writes < 1:
            raise ValueError(f"max_writes must be >= 1, got {max_writes}")
        if apply and backup_dir is None:
            raise ValueError("a real run needs a backup_dir")
        self.s3 = s3
        self.max_writes = max_writes
        self.apply = apply
        self.backup_dir = backup_dir
        self.scanned = 0
        self.skipped_clean = 0
        self.writes = 0  # PUTs made or, in a dry run, that would be made
        self.verified = 0
        self.failures = 0
        self.consecutive_failures = 0
        self.truncated = False
        self.backup_path: Path | None = None
        self._backup_fh: TextIO | None = None

    def _backup(
        self, bucket: str, store: str, key: str, etag: str, body: bytes, replacement: bytes
    ) -> None:
        """Append the current chunk, and the hash of what replaces it, and fsync them BEFORE the
        chunk is written. The hash lets ``--restore`` recognise this run's own write even when
        its PUT raised after the server had applied it."""
        if self._backup_fh is None:
            if self.backup_dir is None:
                raise RuntimeError("no backup_dir: refusing to write without a backup")
            self.backup_dir.mkdir(parents=True, exist_ok=True)
            # A new file per run ("x" refuses an existing one): a restore can never append to the
            # backup it is restoring from.
            stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
            unique = f"{time.time_ns() % 1_000_000_000:09d}-{os.getpid()}"
            self.backup_path = self.backup_dir / f"fill-coordinate-holes-{stamp}-{unique}.jsonl"
            self._backup_fh = open(self.backup_path, "x", encoding="utf-8")  # noqa: SIM115 — held across stores
            dir_fd = os.open(self.backup_dir, os.O_RDONLY)  # make the new file's name durable too
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
            logger.info("Backups: %s", self.backup_path.resolve())
        record = {
            "format": BACKUP_FORMAT,
            "bucket": bucket,
            "store": store,
            "key": key,
            "etag": etag,
            "sha256": hashlib.sha256(body).hexdigest(),
            "sha256_new": hashlib.sha256(replacement).hexdigest(),
            "body_b64": base64.b64encode(body).decode("ascii"),
        }
        self._backup_fh.write(json.dumps(record) + "\n")
        self._backup_fh.flush()
        _full_fsync(self._backup_fh.fileno())

    def _fail(self, what: str, why: str) -> bool:
        """Record a failure; return True when the run must abort."""
        self.failures += 1
        self.consecutive_failures += 1
        logger.error("FAILED %s: %s", what, why)
        if self.consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            logger.error("Aborting: %d consecutive failures", self.consecutive_failures)
            return True
        if self.failures >= MAX_TOTAL_FAILURES:
            logger.error("Aborting: %d total failures", self.failures)
            return True
        return False

    def _fits(self, n: int, what: str) -> bool:
        """The write bound, checked before any write of a store is made."""
        if self.writes + n <= self.max_writes:
            return True
        self.truncated = True
        logger.warning(
            "--max-writes %d: %s needs %d more write(s) after %d; stopping here",
            self.max_writes,
            what,
            n,
            self.writes,
        )
        return False

    def _put_verified(self, bucket: str, key: str, body: bytes) -> None:
        """PUT ``body`` and read it back. Raises on any mismatch."""
        if not key.endswith("/c/0"):
            raise PlanError(f"refusing to write {key}: not a chunk c/0")
        self.writes += 1
        self.s3.put_object(
            Bucket=bucket, Key=key, Body=body, ContentType="application/octet-stream"
        )
        back = self.s3.get_object(Bucket=bucket, Key=key)
        if back["Body"].read() != body:
            raise PlanError(f"s3://{bucket}/{key}: read-back differs from what was written")
        self.verified += 1
        self.consecutive_failures = 0

    def _write_store(self, store_uri: str, plan: StorePlan) -> bool:
        """Write one store's plan. Returns True when the run must abort."""
        for write in plan.writes:  # staleness guard, before anything is written
            try:
                current = self.s3.head_object(Bucket=plan.bucket, Key=write.key)["ETag"]
            except S3_ERRORS as exc:
                return self._fail(store_uri, f"{write.key}: {exc}")
            if current != write.etag:
                return self._fail(
                    store_uri, f"{write.key} changed since it was read; store skipped"
                )
        for write in plan.writes:
            self._backup(plan.bucket, plan.prefix, write.key, write.etag, write.old, write.new)
        for write in plan.writes:
            try:
                self._put_verified(plan.bucket, write.key, write.new)
            except (*S3_ERRORS, PlanError) as exc:
                # This PUT may have been applied. Stop: a re-run skips what is already filled,
                # and --restore recognises the write by its hash.
                self._fail(store_uri, f"{write.key}: {exc}")
                return True
            logger.info("filled s3://%s/%s", plan.bucket, write.key)
        return False

    def repair(self, entries: list[dict[str, Any]]) -> None:
        with tempfile.TemporaryDirectory(prefix="fill-coordinate-holes-") as tmp:
            for i, entry in enumerate(entries):
                self.scanned += 1
                store_uri = entry["store"]
                try:
                    plan = plan_store(self.s3, entry, Path(tmp) / str(i))
                except (*S3_ERRORS, PlanError, ValueError, KeyError) as exc:
                    if self._fail(store_uri, str(exc)):
                        return
                    continue
                if not (self.apply and plan.writes):
                    self.consecutive_failures = 0
                if plan.already_filled:
                    logger.info("%s: already filled: %s", store_uri, plan.already_filled)
                if not plan.writes:
                    self.skipped_clean += 1
                    continue
                if not self._fits(len(plan.writes), store_uri):
                    return
                if not self.apply:
                    self.writes += len(plan.writes)
                    for write in plan.writes:
                        logger.info("DRY-RUN would fill s3://%s/%s", plan.bucket, write.key)
                    continue
                if self._write_store(store_uri, plan):
                    return

    def restore(self, backup_file: Path, force: bool) -> None:
        """Put each backed-up chunk back, which empties the filled holes again."""
        entries = []
        for n, line in enumerate(backup_file.read_text().splitlines(), 1):
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError as exc:  # e.g. a last line torn by a full disk
                entry = {"format": f"unreadable ({exc})"}
            if entry.get("format") != BACKUP_FORMAT:
                why = f"backup format {entry.get('format')!r}, this tool reads {BACKUP_FORMAT}"
                if self._fail(f"{backup_file}:{n}", f"{why}; line skipped"):
                    return
                continue
            entries.append(entry)
        buckets = sorted({e["bucket"] for e in entries})
        logger.warning(
            "RESTORE MODE: %d chunk(s) in %s from %s", len(entries), buckets, backup_file
        )

        # A repair backs up a store's chunks together, so they are adjacent: check the bound per
        # store, before any of its writes, so a bounded restore never stops mid-store.
        for (bucket, store), batch in groupby(entries, key=lambda e: (e["bucket"], e["store"])):
            todo = []
            for entry in batch:
                abort, current = self._check_restore(entry, force)
                if abort:
                    return
                if current is not None:
                    todo.append((entry, current))
            if todo and not self._fits(len(todo), f"s3://{bucket}/{store}"):
                return
            for entry, (current_body, current_etag) in todo:
                key, body = entry["key"], base64.b64decode(entry["body_b64"])
                if not self.apply:
                    self.writes += 1
                    self.consecutive_failures = 0
                    logger.info("DRY-RUN would restore s3://%s/%s", bucket, key)
                    continue
                self._backup(bucket, store, key, current_etag, current_body, body)  # reversible too
                try:
                    self._put_verified(bucket, key, body)
                except (*S3_ERRORS, PlanError) as exc:
                    self._fail(key, str(exc))  # possibly applied: stop, a re-run picks it up
                    return
                logger.info("restored s3://%s/%s", bucket, key)

    def _check_restore(
        self, entry: dict[str, Any], force: bool
    ) -> tuple[bool, tuple[bytes, str] | None]:
        """Whether the run must abort, and the current (body, etag) if ``entry`` needs a write."""
        self.scanned += 1
        bucket, key = entry["bucket"], entry["key"]
        body = base64.b64decode(entry["body_b64"])
        if hashlib.sha256(body).hexdigest() != entry["sha256"]:
            return self._fail(key, "the backup line does not match its sha256; not restored"), None
        try:
            got = _get(self.s3, bucket, key)
        except S3_ERRORS as exc:
            got = None
            logger.error("%s: %s", key, exc)
        if got is None:
            return self._fail(key, "cannot read the current chunk"), None
        current_body, _ = got
        if current_body == body:
            self.skipped_clean += 1
            self.consecutive_failures = 0
            logger.info("s3://%s/%s is already the backed-up version", bucket, key)
            return False, None
        repaired = hashlib.sha256(current_body).hexdigest() == entry["sha256_new"]
        if not force and not repaired:
            why = "changed since the repair wrote it; refusing without --force"
            return self._fail(key, why), None
        return False, got

    def summary(self) -> str:
        mode = "APPLY" if self.apply else "DRY-RUN"
        return (
            f"[{mode}] scanned={self.scanned} clean-skipped={self.skipped_clean} "
            f"writes={self.writes} verified={self.verified} failed={self.failures} "
            f"truncated={self.truncated}"
        )


def _full_fsync(fd: int) -> None:
    """fsync, and on macOS also flush the drive's write cache, which plain fsync does not."""
    if hasattr(fcntl, "F_FULLFSYNC"):
        fcntl.fcntl(fd, fcntl.F_FULLFSYNC)
    else:
        os.fsync(fd)


def _path(value: str) -> Path:
    return Path(value).expanduser()  # also for `--flag=~/x`, which no shell expands


def make_s3_client(endpoint: str | None) -> Any:
    import boto3

    # None lets botocore resolve the endpoint itself (AWS_ENDPOINT_URL, else AWS); --apply needs one.
    return boto3.client("s3", endpoint_url=endpoint)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--plan", type=_path, help=f"plan JSON ({PLAN_FORMAT})")
    parser.add_argument(
        "--only", action="append", default=[], help="limit the run to this store (repeatable)"
    )
    parser.add_argument(
        "--max-writes", required=True, type=int, help="hard bound on PUTs, enforced in code"
    )
    parser.add_argument("--apply", action="store_true", help="actually write (default: dry-run)")
    parser.add_argument(
        "--backup-dir", type=_path, help="where the JSONL backup goes (with --apply)"
    )
    parser.add_argument("--restore", type=_path, help="rollback mode: replay a whole backup JSONL")
    parser.add_argument("--force", action="store_true", help="restore: skip the staleness guard")
    parser.add_argument("--s3-endpoint", help="S3 endpoint (else AWS_ENDPOINT_URL)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if args.max_writes < 1:
        parser.error("--max-writes must be a positive integer")
    if args.apply and args.backup_dir is None:
        parser.error("--apply needs --backup-dir")
    if args.apply and not args.s3_endpoint:
        parser.error("--apply needs an explicit --s3-endpoint")
    if args.restore and (args.plan or args.only):
        parser.error("--restore replays a whole backup file; it takes no --plan or --only")
    if not args.restore and not args.plan:
        parser.error("a repair needs --plan")

    entries: list[dict[str, Any]] = []
    if args.plan:
        try:
            entries = load_plan(args.plan)
        except (ValueError, KeyError) as exc:
            parser.error(f"--plan: {exc}")
        if args.only:
            wanted = {s.rstrip("/") for s in args.only}
            unknown = wanted - {e["store"].rstrip("/") for e in entries}
            if unknown:
                parser.error(f"--only names stores that are not in the plan: {sorted(unknown)}")
            entries = [e for e in entries if e["store"].rstrip("/") in wanted]

    s3 = make_s3_client(args.s3_endpoint)
    logger.info("Endpoint: %s", s3.meta.endpoint_url)
    run = FillRun(s3, max_writes=args.max_writes, apply=args.apply, backup_dir=args.backup_dir)
    if args.restore:
        run.restore(args.restore, force=args.force)
    else:
        buckets = sorted({parse_s3_uri(e["store"])[0] for e in entries})
        n_arrays = sum(len(e["arrays"]) for e in entries)
        logger.info("%d store(s), %d array(s) in %s", len(entries), n_arrays, buckets)
        run.repair(entries)

    print(f"{run.summary()} endpoint={s3.meta.endpoint_url}")
    return 1 if run.failures else 0


if __name__ == "__main__":
    sys.exit(main())
