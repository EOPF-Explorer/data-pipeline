"""Re-register every existing S1 RTC staging item with the new asset model (data-model #196).

For each cube item in ``--cube-collection`` this:
  1. re-registers the **cube** via ``register_v1_s1_rtc.py`` (new asset model + decoration), and
  2. for each orbit group present in its store, re-registers the **per-acquisition** items via
     ``register_per_acquisition.py --reregister-all`` into ``--acq-collection``.

The register scripts DELETE-then-POST, so the migration is **idempotent and resumable** — re-running
re-registers cleanly, and a failure part-way can simply be re-run. Staging-only by default.

``--dry-run`` prints the commands without running (safe verification). ``--tile`` / ``--limit`` scope a
smoke run. Rollback: re-run the prior pipeline at the previous pinned data-model SHA (same idempotent
upsert restores the old items).

Usage:
    uv run python scripts/migrate_s1_rtc_stac.py --stac-api-url <url> --raster-api-url <url> \
      --s3-endpoint <url> [--dry-run] [--tile 30TWN] [--limit N]
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess  # nosec B404 -- composes the trusted register scripts with fixed argv (no shell)
import sys
import urllib.request
from pathlib import Path

log = logging.getLogger("migrate_s1_rtc")

SCRIPTS = Path(__file__).parent
_ORBITS = ("ascending", "descending")
DEFAULT_CUBE_COLLECTION = "sentinel-1-grd-rtc-staging"
DEFAULT_ACQ_COLLECTION = "sentinel-1-grd-rtc-acquisitions-staging"


def list_cube_items(stac_api_url: str, cube_collection: str) -> list[tuple[str, str]]:
    """``(item_id, store_href)`` for every cube item, following pagination.

    The store href is the ``zarr-store`` asset's href (the register scripts accept the https gateway
    URI and rewrite to s3 for the alternate-assets block).
    """
    out: list[tuple[str, str]] = []
    url: str | None = f"{stac_api_url.rstrip('/')}/collections/{cube_collection}/items?limit=100"
    while url:
        with urllib.request.urlopen(url, timeout=60) as resp:  # noqa: S310  # nosec B310 -- https STAC API
            page = json.load(resp)
        for feat in page.get("features", []):
            href = feat.get("assets", {}).get("zarr-store", {}).get("href")
            if href:
                out.append((feat["id"], href))
        url = next((lk["href"] for lk in page.get("links", []) if lk.get("rel") == "next"), None)
    return out


def store_orbits(store: str) -> list[str]:
    """Orbit groups present in a cube store (read-only; prefers consolidated metadata)."""
    import zarr

    try:
        root = zarr.open_consolidated(store, zarr_format=3)
    except Exception as exc:  # noqa: BLE001 -- only the consolidated-metadata absence is expected
        if "consolidated metadata" not in str(exc).lower():
            raise
        root = zarr.open_group(store, mode="r", zarr_format=3)
    return [o for o in _ORBITS if o in root]


def _cube_cmd(store: str, args: argparse.Namespace) -> list[str]:
    return [
        sys.executable,
        str(SCRIPTS / "register_v1_s1_rtc.py"),
        "--store", store,
        "--collection", args.cube_collection,
        "--stac-api-url", args.stac_api_url,
        "--raster-api-url", args.raster_api_url,
        "--s3-endpoint", args.s3_endpoint,
    ]  # fmt: skip


def _peracq_cmd(store: str, tile: str, orbit: str, args: argparse.Namespace) -> list[str]:
    return [
        sys.executable,
        str(SCRIPTS / "register_per_acquisition.py"),
        "--store", store,
        "--tile-id", tile,
        "--orbit-direction", orbit,
        "--collection", args.acq_collection,
        "--cube-collection", args.cube_collection,
        "--stac-api-url", args.stac_api_url,
        "--raster-api-url", args.raster_api_url,
        "--s3-endpoint", args.s3_endpoint,
        "--reregister-all",
    ]  # fmt: skip


def _run(cmd: list[str], dry_run: bool) -> int:
    if dry_run:
        print("DRY-RUN:", " ".join(cmd))
        return 0
    return subprocess.run(cmd).returncode  # noqa: S603  # nosec B603 -- fixed argv, no shell


def migrate(args: argparse.Namespace) -> int:
    items = list_cube_items(args.stac_api_url, args.cube_collection)
    if args.tile:
        items = [(i, s) for (i, s) in items if i == f"s1-rtc-{args.tile}"]
    if args.limit:
        items = items[: args.limit]
    log.info("migrating %d cube tile(s) from %s", len(items), args.cube_collection)

    failures = 0
    for item_id, store in items:
        tile = item_id.removeprefix("s1-rtc-")
        if _run(_cube_cmd(store, args), args.dry_run) != 0:
            log.error("cube re-register failed for %s", item_id)
            failures += 1
            continue
        try:
            orbits = [args.orbit_direction] if args.orbit_direction else store_orbits(store)
        except Exception:
            log.exception("could not read orbit groups for %s; skipping per-acq", store)
            failures += 1
            continue
        for orbit in orbits:
            if _run(_peracq_cmd(store, tile, orbit, args), args.dry_run) != 0:
                log.error("per-acq re-register failed for %s %s", item_id, orbit)
                failures += 1

    log.info("migration done: %d tile(s), %d failure(s)", len(items), failures)
    return 1 if failures else 0


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--stac-api-url", required=True)
    p.add_argument("--raster-api-url", required=True)
    p.add_argument("--s3-endpoint", required=True)
    p.add_argument("--cube-collection", default=DEFAULT_CUBE_COLLECTION)
    p.add_argument("--acq-collection", default=DEFAULT_ACQ_COLLECTION)
    p.add_argument("--tile", help="migrate only this MGRS tile (smoke), e.g. 30TWN")
    p.add_argument("--limit", type=int, help="migrate only the first N tiles (smoke)")
    p.add_argument(
        "--orbit-direction",
        choices=["ascending", "descending"],
        help="restrict per-acq re-registration to one orbit (default: every orbit in the store)",
    )
    p.add_argument("--dry-run", action="store_true", help="print the commands without running them")
    p.add_argument(
        "--allow-nonstaging",
        action="store_true",
        help="permit non-…-staging collections (guard against accidental prod writes)",
    )
    return p


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    args = _build_parser().parse_args()
    if not args.allow_nonstaging and not (
        args.cube_collection.endswith("-staging") and args.acq_collection.endswith("-staging")
    ):
        raise SystemExit(
            "Refusing to migrate non-…-staging collections without --allow-nonstaging "
            f"(cube={args.cube_collection}, acq={args.acq_collection})"
        )
    sys.exit(migrate(args))


if __name__ == "__main__":
    main()
