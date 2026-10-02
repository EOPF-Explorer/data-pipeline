"""Set `platform` on S1 RTC acquisition items that lost it, from a reviewed plan, and change nothing else.

``ingest_v1_s1_rtc._sync_tree`` lost per-slice `platform` values on S3 (#451), so the per-acquisition
STAC items built from those cubes carry no `platform`. With the cubes filled
(fill_coordinate_holes.py), this sets each item's `platform` from a plan built from the filled cubes
and reviewed separately.

It runs repair_stac_raster_links' RepairRun, so it has the same safety properties: dry-run by
default, ``--max-items`` bounds the PUTs and is checked before each one, every item is backed up
(fsync'd) before its PUT, a PUT is never turned into a POST, every PUT is re-read, 3 consecutive or
10 total failures abort the run, and ``--restore`` puts the backups back (with a staleness guard).

- an item is written only if it has no `platform`; one already holding the planned value is
  skipped, and one holding another value is refused;
- after the PUT, the re-read item must equal the PUT doc apart from `properties.updated` and the
  order of `links`.
"""

import argparse
import copy
import json
import logging
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import repair_stac_raster_links as rsrl

PLAN_FORMAT = "fill-stac-item-platform/1"


def load_plan(path: Path) -> tuple[str, dict[str, str]]:
    """Return (collection, {item id: platform}) from a plan file, or raise ValueError."""
    plan = json.loads(path.read_text())
    if plan.get("format") != PLAN_FORMAT:
        raise ValueError(f"not a {PLAN_FORMAT} plan: format={plan.get('format')!r}")
    collection, platforms = plan.get("collection"), plan.get("platform")
    if not isinstance(collection, str) or not collection:
        raise ValueError("plan 'collection' must be a non-empty string")
    if not isinstance(platforms, dict) or not platforms:
        raise ValueError("plan 'platform' must map item ids to platforms")
    bad = {
        k: v
        for k, v in platforms.items()
        if not (isinstance(v, str) and v.startswith("sentinel-1"))
    }
    if bad:
        raise ValueError(f"plan platforms must be sentinel-1* strings: {bad}")
    return collection, platforms


def make_fix(platforms: dict[str, str]) -> Callable[[dict[str, Any]], tuple[dict[str, Any], int]]:
    """The RepairRun fix: set the planned `platform` where it is missing, refuse a different one."""

    def fix(item: dict[str, Any]) -> tuple[dict[str, Any], int]:
        want = platforms[item["id"]]
        have = item.get("properties", {}).get("platform")
        if have == want:
            return item, 0
        if have not in (None, ""):
            raise ValueError(f"platform is {have!r} but the plan says {want!r}; refusing")
        doc = copy.deepcopy(item)
        doc["properties"]["platform"] = want
        return doc, 1

    return fix


def _comparable(item: dict[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(item)
    out.get("properties", {}).pop("updated", None)
    out["links"] = sorted(out.get("links", []), key=lambda link: json.dumps(link, sort_keys=True))
    return out


def check(after: dict[str, Any], doc: dict[str, Any]) -> str | None:
    """The re-read item must be the PUT doc, apart from `updated` and the order of `links`."""
    if _comparable(after) != _comparable(doc):
        return "re-read item differs from the PUT doc beyond properties.updated"
    return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--plan", type=Path, help=f"plan JSON ({PLAN_FORMAT})")
    parser.add_argument("--ids", nargs="+", help="limit the run to these planned item ids (canary)")
    parser.add_argument(
        "--max-items", required=True, type=int, help="hard bound on PUTs, enforced in code"
    )
    parser.add_argument("--apply", action="store_true", help="actually write (default: dry-run)")
    parser.add_argument(
        "--backup-dir", type=Path, required=True, help="where the JSONL backup goes"
    )
    parser.add_argument("--restore", type=Path, help="rollback mode: replay a whole backup JSONL")
    parser.add_argument("--force", action="store_true", help="restore: skip the staleness guard")
    parser.add_argument("--stac-api-url", default="https://api.explorer.eopf.copernicus.eu/stac")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if args.max_items < 1:
        parser.error("--max-items must be a positive integer")
    if args.restore and (args.plan or args.ids):
        parser.error("--restore replays a whole backup file; it takes no --plan or --ids")
    if not args.restore and not args.plan:
        parser.error("a repair needs --plan")

    platforms: dict[str, str] = {}
    if args.restore:
        collections = {
            json.loads(line)["collection"] for line in args.restore.read_text().splitlines() if line
        }
        if len(collections) != 1:
            parser.error(
                f"--restore: the backup must hold exactly one collection, got {collections}"
            )
        collection = collections.pop()
    else:
        try:
            collection, platforms = load_plan(args.plan)
        except ValueError as exc:
            parser.error(f"--plan: {exc}")
        unknown = set(args.ids or []) - set(platforms)
        if unknown:
            parser.error(f"--ids names items that are not in the plan: {sorted(unknown)}")

    run = rsrl.RepairRun(
        session=rsrl.make_session(),
        api_url=args.stac_api_url.rstrip("/"),
        collection=collection,
        max_items=args.max_items,
        apply=args.apply,
        backup_dir=args.backup_dir,
        fix=make_fix(platforms),
        check=check,
        label="item-platform-fill",
    )
    if args.restore:
        run.restore(args.restore, force=args.force)
    else:
        ids = args.ids or sorted(platforms)
        logging.getLogger(__name__).info("%d item(s) in %s", len(ids), collection)
        run.repair(ids)

    print(run.summary())
    return 1 if run.failures else 0


if __name__ == "__main__":
    sys.exit(main())
