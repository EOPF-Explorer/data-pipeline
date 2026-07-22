"""Repair STAC items whose links were persisted with the corrupted /stac/raster prefix.

During the stac-auth-proxy link-rewrite incident (2026-07-21T11:15Z -> 2026-07-22T~14:30Z,
platform-deploy#343), read-modify-write pipeline jobs persisted proxy-corrupted hrefs
(``https://<host>/stac/raster/...``) into item ``links`` arrays (rels viewer/xyz/tilejson).
This operator tool rewrites the corrupted prefix back to ``/raster/`` — and nothing else.
Tracked in #371; recovery plan + gates live out-of-repo (session project dir).

Safety properties (per the reviewed recovery plan):
- dry-run by default; ``--apply`` required for any write
- ``--max-items`` is a hard bound on WRITES, enforced in code before each PUT
- every touched item is backed up (JSONL, fsync'd) BEFORE its PUT; ``--restore`` replays
  a backup verbatim (loudly — a restore re-installs the corrupted links by design) with a
  staleness guard against clobbering items re-registered since the repair
- 404 on PUT is logged and skipped, never downgraded to a POST (no resurrection)
- per-item verify (re-GET) after each PUT; 3 consecutive or 10 total failures abort
"""

from __future__ import annotations

import argparse
import copy
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

import requests

sys.path.insert(0, str(Path(__file__).parent))
import stac_auth  # noqa: E402

logger = logging.getLogger(__name__)

CORRUPT_PREFIX = "https://api.explorer.eopf.copernicus.eu/stac/raster/"
CLEAN_PREFIX = "https://api.explorer.eopf.copernicus.eu/raster/"

# Default backup location: durable (survives worktree/session cleanup), never a repo path.
DEFAULT_BACKUP_DIR = (
    Path.home() / ".claude/projects/-Users-lhoupert-DevDS-EOPF-data-pipeline/backups"
)

MAX_CONSECUTIVE_FAILURES = 3
MAX_TOTAL_FAILURES = 10


def repair_links(item: dict[str, Any]) -> tuple[dict[str, Any], int]:
    """Return (repaired copy, number of hrefs changed). Idempotent; touches only links[].href.

    Only an href that starts with the exact corrupted prefix is rewritten; assets and all
    other link fields are untouched. 0 changes means the item is clean (no-op guarantee).
    """
    repaired = copy.deepcopy(item)
    changed = 0
    for link in repaired.get("links", []):
        href = link.get("href")
        if isinstance(href, str) and href.startswith(CORRUPT_PREFIX):
            link["href"] = CLEAN_PREFIX + href[len(CORRUPT_PREFIX) :]
            changed += 1
    return repaired, changed


def is_corrupted(item: dict[str, Any]) -> bool:
    """True when any link href carries the corrupted prefix."""
    return any(
        isinstance(link.get("href"), str) and link["href"].startswith(CORRUPT_PREFIX)
        for link in item.get("links", [])
    )


def discover_corrupted_ids(
    session: requests.Session, api_url: str, collection: str, updated_since: str
) -> list[str]:
    """Search the collection for corrupted items (string comparison on `updated` —
    pgstac's `updated` is text; TIMESTAMP() comparison errors)."""
    body: dict[str, Any] = {
        "collections": [collection],
        "limit": 500,
        "fields": {
            "include": ["id", "links", "properties.updated"],
            "exclude": ["assets", "geometry", "bbox"],
        },
        "filter-lang": "cql2-json",
        "filter": {"op": ">=", "args": [{"property": "updated"}, updated_since]},
    }
    ids: list[str] = []
    url = f"{api_url}/search"
    while True:
        resp = session.post(url, json=body, timeout=60)
        resp.raise_for_status()
        page = resp.json()
        for feature in page.get("features", []):
            if is_corrupted(feature):
                ids.append(feature["id"])
        next_link = next(
            (link for link in page.get("links", []) if link.get("rel") == "next"),
            None,
        )
        if next_link is None:
            return ids
        body = next_link.get("body", body)
        url = next_link.get("href", url)


class RepairRun:
    """One bounded repair (or restore) run against a single collection."""

    def __init__(
        self,
        session: requests.Session,
        api_url: str,
        collection: str,
        max_items: int,
        apply: bool,
        backup_dir: Path,
    ) -> None:
        self.session = session
        self.api_url = api_url
        self.collection = collection
        self.max_items = max_items
        self.apply = apply
        self.backup_dir = Path(backup_dir)
        self.scanned = 0
        self.skipped_clean = 0
        self.written = 0
        self.verified = 0
        self.failures = 0
        self.consecutive_failures = 0
        self.truncated = False
        self._backup_path: Path | None = None
        self._results_path: Path | None = None
        self._backup_fh = None

    def item_url(self, item_id: str) -> str:
        return f"{self.api_url}/collections/{self.collection}/items/{item_id}"

    def _open_backup(self) -> None:
        if self._backup_fh is not None:
            return
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        self._backup_path = self.backup_dir / f"raster-link-repair-{self.collection}-{stamp}.jsonl"
        self._results_path = self._backup_path.with_suffix(".results.jsonl")
        self._backup_fh = open(  # noqa: SIM115 — held across items, fsync'd per line
            self._backup_path, "a", encoding="utf-8"
        )
        logger.info("Backups: %s", self._backup_path.resolve())

    def _backup(self, item: dict[str, Any]) -> None:
        """Append the pre-write doc and fsync BEFORE any write goes out."""
        self._open_backup()
        line = json.dumps({"collection": self.collection, "id": item["id"], "item": item})
        self._backup_fh.write(line + "\n")
        self._backup_fh.flush()
        os.fsync(self._backup_fh.fileno())

    def _record_result(self, item_id: str, updated_after: str | None) -> None:
        with open(self._results_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps({"id": item_id, "updated_after": updated_after}) + "\n")
            fh.flush()
            os.fsync(fh.fileno())

    def _fail(self, item_id: str, why: str) -> bool:
        """Record a failure; return True when the run must abort."""
        self.failures += 1
        self.consecutive_failures += 1
        logger.error("FAILED %s: %s", item_id, why)
        if self.consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            logger.error("Aborting: %d consecutive failures", self.consecutive_failures)
            return True
        if self.failures >= MAX_TOTAL_FAILURES:
            logger.error("Aborting: %d total failures", self.failures)
            return True
        return False

    def _put_and_verify(self, item_id: str, doc: dict[str, Any]) -> bool:
        """PUT one doc and re-GET verify. Returns True when the run must abort."""
        self.written += 1
        resp = self.session.put(self.item_url(item_id), json=doc, timeout=30)
        if resp.status_code == 404:
            # Item deleted between discovery and write: skip. NEVER fall back to a
            # POST — resurrecting a deleted item is worse than leaving it missing.
            logger.warning("PUT 404 for %s — deleted mid-run, skipping (no POST)", item_id)
            return self._fail(item_id, "PUT returned 404")
        resp.raise_for_status()

        check = self.session.get(self.item_url(item_id), timeout=30)
        check.raise_for_status()
        after = check.json()
        if is_corrupted(after):
            return self._fail(item_id, "still corrupted after PUT")
        self.verified += 1
        self.consecutive_failures = 0
        self._record_result(item_id, after.get("properties", {}).get("updated"))
        logger.info("repaired %s", item_id)
        return False

    def repair(self, item_ids: list[str]) -> None:
        """Repair the given items, honoring the write bound and dry-run default."""
        for item_id in item_ids:
            self.scanned += 1
            resp = self.session.get(self.item_url(item_id), timeout=30)
            resp.raise_for_status()
            original = resp.json()

            repaired, changed = repair_links(original)
            if changed == 0:
                self.skipped_clean += 1
                logger.info("clean, skipping %s", item_id)
                continue

            if not self.apply:
                logger.info("DRY-RUN would repair %s (%d links)", item_id, changed)
                continue

            # Hard write bound, checked BEFORE each PUT.
            if self.written >= self.max_items:
                self.truncated = True
                logger.warning(
                    "--max-items %d reached; stopping with items remaining", self.max_items
                )
                return

            self._backup(original)
            if self._put_and_verify(item_id, repaired):
                return

    def restore(self, backup_file: Path, force: bool) -> None:
        """PUT each backed-up doc verbatim (returns items to their pre-repair state).

        Loud by design: the backup contains the CORRUPTED links. A staleness guard
        refuses to clobber an item whose current `updated` differs from the value
        recorded after our repair PUT (i.e. something else wrote it since) unless
        --force is given.
        """
        entries = [json.loads(line) for line in backup_file.read_text().splitlines() if line]
        n_corrupt = sum(1 for e in entries if is_corrupted(e["item"]))
        logger.warning(
            "RESTORE MODE: %d items from %s — %d contain corrupted /stac/raster links "
            "which will be re-installed verbatim",
            len(entries),
            backup_file,
            n_corrupt,
        )
        results_path = backup_file.with_suffix(".results.jsonl")
        expected: dict[str, str | None] = {}
        if results_path.exists():
            for line in results_path.read_text().splitlines():
                if line:
                    rec = json.loads(line)
                    expected[rec["id"]] = rec.get("updated_after")

        for entry in entries:
            item_id = entry["id"]
            self.scanned += 1
            if not self.apply:
                logger.info("DRY-RUN would restore %s", item_id)
                continue
            if self.written >= self.max_items:
                self.truncated = True
                logger.warning("--max-items %d reached; stopping", self.max_items)
                return

            if not force:
                current = self.session.get(self.item_url(item_id), timeout=30)
                current.raise_for_status()
                current_updated = current.json().get("properties", {}).get("updated")
                if item_id not in expected or expected[item_id] != current_updated:
                    logger.error(
                        "STALE %s: current updated=%r != recorded %r — item changed "
                        "since the repair; refusing without --force",
                        item_id,
                        current_updated,
                        expected.get(item_id),
                    )
                    if self._fail(item_id, "stale restore refused"):
                        return
                    continue

            self.written += 1
            resp = self.session.put(self.item_url(item_id), json=entry["item"], timeout=30)
            if resp.status_code == 404:
                logger.warning("PUT 404 for %s — skipping (no POST)", item_id)
                if self._fail(item_id, "PUT returned 404"):
                    return
                continue
            resp.raise_for_status()
            self.verified += 1
            self.consecutive_failures = 0
            logger.info("restored %s", item_id)

    def summary(self) -> str:
        mode = "APPLY" if self.apply else "DRY-RUN"
        return (
            f"[{mode}] scanned={self.scanned} clean-skipped={self.skipped_clean} "
            f"writes={self.written} verified={self.verified} failed={self.failures} "
            f"truncated={self.truncated}"
        )


def make_session() -> requests.Session:
    """A session whose every request carries a fresh bearer (no-op without OIDC env)."""
    session = requests.Session()
    session.auth = stac_auth.bearer_auth
    return session


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--collection", required=True, help="single target collection")
    parser.add_argument(
        "--max-items",
        required=True,
        type=int,
        help="hard bound on WRITES (enforced before each PUT)",
    )
    parser.add_argument("--stac-api-url", default="https://api.explorer.eopf.copernicus.eu/stac")
    parser.add_argument("--ids", nargs="+", help="explicit item ids (canary path)")
    parser.add_argument(
        "--updated-since",
        default="2026-07-21T11:15:00Z",
        help="discovery filter lower bound (string comparison)",
    )
    parser.add_argument("--apply", action="store_true", help="actually write (default: dry-run)")
    parser.add_argument("--backup-dir", type=Path, default=DEFAULT_BACKUP_DIR)
    parser.add_argument("--restore", type=Path, help="rollback mode: replay a backup JSONL")
    parser.add_argument(
        "--force", action="store_true", help="restore mode: override the staleness guard"
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if args.max_items <= 0:
        parser.error("--max-items must be a positive integer")

    session = make_session()
    run = RepairRun(
        session=session,
        api_url=args.stac_api_url.rstrip("/"),
        collection=args.collection,
        max_items=args.max_items,
        apply=args.apply,
        backup_dir=args.backup_dir,
    )
    logger.info("Backup dir: %s", Path(args.backup_dir).resolve())

    if args.restore:
        run.restore(args.restore, force=args.force)
    else:
        ids = args.ids or discover_corrupted_ids(
            session, run.api_url, args.collection, args.updated_since
        )
        logger.info("%d target item(s)", len(ids))
        run.repair(ids)

    print(run.summary())
    return 1 if run.failures else 0


if __name__ == "__main__":
    sys.exit(main())
