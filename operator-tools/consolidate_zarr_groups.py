"""Add consolidated metadata to zarr v3 groups on S3, and change nothing else (#446).

titiler-eopf 0.12 opens the group a STAC asset href points to over HTTPS. Without a
``consolidated_metadata`` block, zarr has to list that group, the gateway refuses the listing
(PROPFIND 405), and the item returns 500. 30 S1 RTC cubes lost their blocks to the no-op ingest
(fixed in ``ingest_v1_s1_rtc.run_ingest``), and the OLCI converter never wrote any.

For each store, this tool copies every node's ``zarr.json`` (metadata only, never chunks) into a
temp dir, runs ``zarr.consolidate_metadata`` there on the requested groups (deepest first, root
last, as the S1 writer does), and uploads only those groups' ``zarr.json``. It does not call
``consolidate_s1_store``, which also rewrites the root's geo attributes.

Safety properties:
- dry-run by default; ``--apply`` is required for any write
- ``--max-writes`` is a hard bound on PUTs, enforced in code. A store is started only if all of
  its writes fit in what is left, so a bounded run always stops between stores
- a store is written only if each new ``zarr.json`` differs from the old one by the added
  ``consolidated_metadata`` key alone, the block lists exactly the group's nodes found on S3, and
  no other node changed in the local copy. A key with an empty path segment (``//``) refuses
  the store
- a group that already carries a block is skipped, so a re-run is a no-op
- every original ``zarr.json`` is appended to a JSONL backup, with the sha256 of the body that
  replaces it, and fsync'd before its store's first PUT. Each run writes a new backup file and
  never appends to an existing one. ``--restore`` puts the originals back, store by store within
  the same bound, refusing an object that is neither the backed-up nor the repaired body unless
  ``--force``, and backs up what it overwrites the same way
- every target's ETag is re-checked just before the store's first PUT, and every PUT is read back
  and compared. A failed or uncertain PUT stops the run; 3 consecutive or 10 total failures of
  any other kind do too
- ``--apply`` needs an explicit ``--s3-endpoint``; the endpoint and buckets are logged up front
- backup lines carry a format version, an unreadable line fails on its own, and ``--restore``
  replays a whole file (it takes no ``--store`` or ``--group``)
"""

import argparse
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

import zarr
from botocore.exceptions import BotoCoreError, ClientError
from urllib3.exceptions import HTTPError as Urllib3Error

logger = logging.getLogger(__name__)

BLOCK = "consolidated_metadata"
BACKUP_FORMAT = 1
MAX_CONSECUTIVE_FAILURES = 3
MAX_TOTAL_FAILURES = 10
# What an S3 call can raise: service errors, transport errors, and a TLS failure while a body
# streams (urllib3 raises that one unwrapped).
S3_ERRORS = (ClientError, BotoCoreError, Urllib3Error)


class PlanError(Exception):
    """A store cannot be repaired safely; nothing is written to it."""


@dataclass
class GroupWrite:
    group: str  # "" is the root
    key: str
    etag: str
    old: bytes
    new: bytes


@dataclass
class StorePlan:
    bucket: str
    prefix: str
    writes: list[GroupWrite] = field(default_factory=list)
    already_consolidated: list[str] = field(default_factory=list)


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """``s3://bucket/a/b.zarr`` -> ``("bucket", "a/b.zarr")``."""
    bucket, _, prefix = uri.removeprefix("s3://").partition("/")
    prefix = prefix.strip("/")
    if not uri.startswith("s3://") or not bucket or not prefix:
        raise ValueError(f"expected s3://bucket/prefix, got {uri!r}")
    return bucket, prefix


def zarr_json_key(prefix: str, group: str) -> str:
    return f"{prefix}/{group}/zarr.json" if group else f"{prefix}/zarr.json"


def check_only_block_added(old: bytes, new: bytes, where: str) -> None:
    """Raise PlanError unless ``new`` is ``old`` plus a non-empty consolidated block.

    ``old`` may carry an empty or null block, which zarr ignores; that is what gets replaced."""
    before, after = json.loads(old), json.loads(new)
    before.pop(BLOCK, None)
    block = after.pop(BLOCK, None)
    # sort_keys and json's NaN spelling make the comparison independent of key order and NaN != NaN.
    if json.dumps(before, sort_keys=True) != json.dumps(after, sort_keys=True):
        changed = sorted(k for k in before.keys() | after.keys() if before.get(k) != after.get(k))
        raise PlanError(f"{where}: consolidation changed more than the block: {changed}")
    if not block or not block.get("metadata"):
        raise PlanError(f"{where}: consolidation produced an empty block")


def _get(s3: Any, bucket: str, key: str) -> tuple[bytes, str] | None:
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return None
        raise
    return resp["Body"].read(), resp["ETag"]


def mirror_metadata(s3: Any, bucket: str, prefix: str, dest: Path) -> dict[str, tuple[bytes, str]]:
    """Copy every node's ``zarr.json`` under ``prefix`` into ``dest``.

    Returns ``{node path: (body, etag)}``, with ``""`` for the root. It walks the hierarchy with
    delimiter listings and never lists inside an array, so chunk keys are never touched.
    """
    nodes: dict[str, tuple[bytes, str]] = {}
    paginator = s3.get_paginator("list_objects_v2")
    pending = [""]
    while pending:
        rel = pending.pop()
        base = f"{prefix}/{rel}/" if rel else f"{prefix}/"
        got = _get(s3, bucket, base + "zarr.json")
        if got is None:
            if not rel:
                raise PlanError(f"s3://{bucket}/{prefix}: no zarr.json at the store root")
            logger.warning("s3://%s/%s has no zarr.json; not a zarr node, skipped", bucket, base)
            continue
        body, etag = got
        meta = json.loads(body)
        if meta.get("zarr_format") != 3:
            raise PlanError(f"s3://{bucket}/{base}zarr.json is not zarr v3")
        local = dest / rel / "zarr.json"
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_bytes(body)
        nodes[rel] = (body, etag)
        if meta.get("node_type") != "group":
            continue
        for page in paginator.paginate(Bucket=bucket, Prefix=base, Delimiter="/"):
            for common in page.get("CommonPrefixes", []):
                child = common["Prefix"][len(base) :].rstrip("/")
                # `//`, `/./` or `/../` in a key: zarr can't address it, the walk would never end
                # or would write outside the temp dir. Refuse before reading anything under it.
                if child in ("", ".", ".."):
                    raise PlanError(
                        f"s3://{bucket}/{base}: a key has a {child or 'empty'!r} segment"
                    )
                pending.append(f"{rel}/{child}" if rel else child)
    return nodes


def plan_store(s3: Any, store_uri: str, groups: list[str], workdir: Path) -> StorePlan:
    """Work out the new ``zarr.json`` of each target group without writing anything to S3."""
    bucket, prefix = parse_s3_uri(store_uri)
    local = workdir / "store.zarr"
    nodes = mirror_metadata(s3, bucket, prefix, local)
    plan = StorePlan(bucket=bucket, prefix=prefix)

    todo: list[str] = []
    for group in sorted(groups, key=lambda g: -len(Path(g).parts)):  # deepest first, root last
        if group not in nodes or json.loads(nodes[group][0]).get("node_type") != "group":
            raise PlanError(f"{store_uri}: no group at {group or '.'!r}")
        if (json.loads(nodes[group][0]).get(BLOCK) or {}).get("metadata"):  # zarr ignores {}
            plan.already_consolidated.append(group)
        else:
            todo.append(group)

    for group in todo:
        zarr.consolidate_metadata(str(local), path=group or None, zarr_format=3)

    for rel, (old, _) in nodes.items():
        if rel not in todo and (local / rel / "zarr.json").read_bytes() != old:
            raise PlanError(f"{store_uri}: consolidation rewrote {rel or '.'!r}, not a target")
    for group in todo:
        old, etag = nodes[group]
        new = (local / group / "zarr.json").read_bytes()
        check_only_block_added(old, new, f"{store_uri} {group or '.'}")
        # The block must list exactly the group's nodes found on S3: a node zarr skipped, or two
        # names a case-insensitive disk folded into one, would otherwise vanish from it unnoticed.
        under = f"{group}/" if group else ""
        found = {r.removeprefix(under) for r in nodes if r != group and r.startswith(under)}
        listed = set(json.loads(new)[BLOCK]["metadata"])
        if listed != found:
            raise PlanError(
                f"{store_uri} {group or '.'}: the block and the store differ on "
                f"{sorted(listed ^ found)}"
            )
        plan.writes.append(GroupWrite(group, zarr_json_key(prefix, group), etag, old, new))
    return plan


class ConsolidateRun:
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
        """Append the current version of an object, and the hash of what replaces it, and fsync
        them BEFORE that object is written. The hash lets ``--restore`` recognise this run's own
        write even when its PUT raised after the server had applied it."""
        if self._backup_fh is None:
            if self.backup_dir is None:
                raise RuntimeError("no backup_dir: refusing to write without a backup")
            self.backup_dir.mkdir(parents=True, exist_ok=True)
            # A new file per run ("x" refuses an existing one): a restore can never append to the
            # backup it is restoring from, even within the same second or from a parallel run.
            stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
            unique = f"{time.time_ns() % 1_000_000_000:09d}-{os.getpid()}"
            self.backup_path = self.backup_dir / f"consolidate-zarr-groups-{stamp}-{unique}.jsonl"
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
            "body": body.decode("utf-8"),
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
        """The write bound, checked before any write of a unit is made."""
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
        if not key.endswith("/zarr.json"):
            raise PlanError(f"refusing to write {key}: not a zarr.json")
        self.writes += 1
        self.s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
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
                # This PUT may have been applied, and the store may be partly written. Stop: a
                # re-run finishes the store, and --restore recognises the write by its hash.
                self._fail(store_uri, f"{write.key}: {exc}")
                return True
            logger.info("consolidated s3://%s/%s", plan.bucket, write.key)
        return False

    def repair(self, store_uris: list[str], groups: list[str]) -> None:
        with tempfile.TemporaryDirectory(prefix="consolidate-zarr-") as tmp:
            for i, store_uri in enumerate(store_uris):
                self.scanned += 1
                try:
                    plan = plan_store(self.s3, store_uri, groups, Path(tmp) / str(i))
                except (*S3_ERRORS, PlanError, ValueError) as exc:
                    if self._fail(store_uri, str(exc)):
                        return
                    continue
                # A store that needs no write, or a dry run's, has succeeded here. An apply's store
                # succeeds only once its PUTs are verified (_put_verified resets the count then).
                if not (self.apply and plan.writes):
                    self.consecutive_failures = 0
                if plan.already_consolidated:
                    logger.info(
                        "%s: already consolidated: %s",
                        store_uri,
                        [g or "." for g in plan.already_consolidated],
                    )
                if not plan.writes:
                    self.skipped_clean += 1
                    continue
                if not self._fits(len(plan.writes), store_uri):
                    return
                if not self.apply:
                    self.writes += len(plan.writes)
                    for write in plan.writes:
                        logger.info("DRY-RUN would consolidate s3://%s/%s", plan.bucket, write.key)
                    continue
                if self._write_store(store_uri, plan):
                    return

    def restore(self, backup_file: Path, force: bool) -> None:
        """Put each backed-up ``zarr.json`` back, which removes the blocks the repair added."""
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
            "RESTORE MODE: %d object(s) in %s from %s", len(entries), buckets, backup_file
        )

        # A repair backs up a store's objects together, so they are adjacent. The bound counts the
        # writes a store still needs and is checked before any of them: a bounded restore never
        # stops in the middle of a store, and a re-run only needs the budget that is left.
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
                key, body = entry["key"], entry["body"].encode("utf-8")
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
        body = entry["body"].encode("utf-8")
        if hashlib.sha256(body).hexdigest() != entry["sha256"]:
            return self._fail(key, "the backup line does not match its sha256; not restored"), None
        try:
            got = _get(self.s3, bucket, key)
        except S3_ERRORS as exc:
            got = None
            logger.error("%s: %s", key, exc)
        if got is None:
            return self._fail(key, "cannot read the current object"), None
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
    parser.add_argument("--store", action="append", default=[], help="s3://bucket/path/x.zarr")
    parser.add_argument("--stores-file", type=_path, help="file of store URIs, one per line")
    parser.add_argument(
        "--group",
        action="append",
        default=[],
        help="group to consolidate, relative to the store root; '.' is the root (repeatable)",
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
    stores = list(args.store)
    if args.stores_file:
        stores += [s.strip() for s in args.stores_file.read_text().splitlines() if s.strip()]
    if args.restore and (stores or args.group):
        parser.error("--restore replays a whole backup file; it takes no --store or --group")
    if not args.restore and (not stores or not args.group):
        parser.error("a repair needs --store/--stores-file and at least one --group")
    # Each store and group once, so a dry run shows exactly what the apply would write.
    stores = list(dict.fromkeys(s.rstrip("/") for s in stores))
    groups = list(dict.fromkeys("" if g in (".", "/") else g.strip("/") for g in args.group))

    s3 = make_s3_client(args.s3_endpoint)
    logger.info("Endpoint: %s", s3.meta.endpoint_url)
    run = ConsolidateRun(
        s3,
        max_writes=args.max_writes,
        apply=args.apply,
        backup_dir=args.backup_dir,
    )
    if args.restore:
        run.restore(args.restore, force=args.force)
    else:
        buckets = sorted({s.removeprefix("s3://").split("/")[0] for s in stores})
        logger.info(
            "%d store(s) in %s, groups %s", len(stores), buckets, [g or "." for g in groups]
        )
        run.repair(stores, groups)

    print(f"{run.summary()} endpoint={s3.meta.endpoint_url}")
    return 1 if run.failures else 0


if __name__ == "__main__":
    sys.exit(main())
