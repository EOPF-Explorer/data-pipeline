"""In-place re-derive of legacy S1 RTC cubes to the current data-model — no re-download/reprocess.

A legacy `sentinel-1-grd-rtc-staging` cube predates three writer fixes and the cron never self-heals
existing stores (append doesn't recreate arrays):

  - #201 — CF `_FillValue`/`standard_name`/`units` on vv/vh (and conditions);
  - #202 — out-of-swath nodata stored as `NaN` (not `0.0`) so titiler masks it transparent;
  - #203 — consolidated metadata on *every* orbit group, not just the last-ingested one.

`redrive_store` reproduces, in place, exactly what a fresh re-ingest would write — by re-deriving each
present orbit's vv/vh from the cube's own native band masked by its own `border_mask`, regenerating the
overviews with the writer's `np.nanmean` downsample, restoring the CF attrs, and consolidating every
orbit. The overview math is the data-model writer's own private `_downsample_2d`/`OVERVIEW_CHAIN`, so
the result is value-identical to a re-ingest only at the pinned writer — `assert_writer_pinned()` (R5)
refuses to run otherwise. See plan `the-migration-of-the-adaptive-wolf.md`.
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path

import eopf_geozarr
import numpy as np
import s1_store_meta
import zarr
from eopf_geozarr.conversion.s1_ingest import (
    BACKSCATTER_CF_ATTRS,
    FLOAT32_NAN_FILL_VALUE,
    OVERVIEW_CHAIN,
    _downsample_2d,
    consolidate_s1_store,
)
from migrate_s1_rtc_stac import list_cube_items
from register_v1 import https_to_s3

log = logging.getLogger("migrate_s1_rtc_datamodel")

# Per-store completion marker (root attr): the migration is idempotent on the *store*, keyed by the
# writer the re-derive ran against. Writes within a store are not atomic across objects, so a crash
# mid-store leaves no marker and the store is re-derived in full on the next run (R2).
MIGRATION_MARKER_KEY = "datamodel_migrated"


@dataclass
class RedriveReport:
    """Outcome of re-deriving one store."""

    store: str
    orbits: list[str] = field(default_factory=list)
    bands_rewritten: int = 0  # count of (orbit, band) pairs re-derived
    conditions_fill_value_set: int = 0  # conditions data arrays given the CF _FillValue attr (#201)
    already_current: bool = False  # marker already at the current writer → no-op
    skipped_no_border_mask: list[str] = field(
        default_factory=list
    )  # orbits lacking border_mask (R6)


def _marker_value() -> str:
    """The writer the migration runs against (the completion-marker value).

    Deliberately version-granular (``eopf_geozarr.__version__``), not the exact git rev — two ``0.10.1``
    builds share a marker. The precise-behaviour guarantee comes from ``assert_writer_pinned`` at run
    time (version + fill value + OVERVIEW_CHAIN), not from the marker; the marker only answers "did *a*
    pinned writer already migrate this store" (the option-(b) R5 scope).
    """
    return str(eopf_geozarr.__version__)


def redrive_store(store_path: str | Path, *, dry_run: bool = False) -> RedriveReport:
    """Re-derive one cube store in place to the current data-model; return what changed.

    Idempotent via the per-store completion marker: a store already migrated at the current writer is
    a no-op. Follows the consolidated-metadata dance (drop → reopen ``r+`` → re-derive → marker →
    ``consolidate_s1_store``) so the writer sees fresh per-array metadata. ``dry_run`` reports what
    would happen (already-current / which orbits, which lack ``border_mask``) and writes **nothing**.
    """
    s1_store_meta.assert_writer_pinned()  # R5 — refuse to run on a drifted writer
    store_path = Path(store_path)
    report = RedriveReport(store=str(store_path))

    root_ro = zarr.open_group(str(store_path), mode="r", zarr_format=3)
    report.orbits = [name for name, _ in root_ro.groups()]
    if dict(root_ro.attrs).get(MIGRATION_MARKER_KEY) == _marker_value():
        report.already_current = True
        return report

    if dry_run:  # report-only: no drop-consolidated, no r+, no writes
        for orbit_name, orbit in root_ro.groups():
            if "border_mask" not in orbit["r10m"]:
                report.skipped_no_border_mask.append(orbit_name)
        return report

    # C1: a consolidated store serves stale array metadata to writers — drop it before reopening r+.
    s1_store_meta.drop_consolidated_metadata(store_path)
    root = zarr.open_group(str(store_path), mode="r+", zarr_format=3)

    overview_levels = OVERVIEW_CHAIN[1:]  # (level, parent, factor) below native r10m
    for orbit_name, orbit in root.groups():
        r10m = orbit["r10m"]
        if "border_mask" not in r10m:
            # R6: border_mask is the authoritative valid-data mask; a cube lacking it cannot be
            # re-derived in place. Flag it (the store stays un-marked → revisited / handled separately)
            # rather than crash the fleet driver.
            report.skipped_no_border_mask.append(orbit_name)
            continue
        # Re-derive ONE time-slice at a time (mirrors the writer's per-acquisition 2-D path) so peak
        # memory stays ~one (y, x) slice + its overview chain, not the whole (T, y, x) band — real tiles
        # are 10980² × T, multiple GB per band. Mask the native by border_mask (#202), then walk the
        # writer's 2-D overview chain, writing each level's slice directly (no full-band buffering).
        for t in range(int(r10m["vv"].shape[0])):
            mask_t = np.asarray(r10m["border_mask"][t])  # (y, x) — authoritative, untouched
            for band in ("vv", "vh"):
                slc = np.where(mask_t == 0, np.nan, np.asarray(r10m[band][t])).astype("float32")
                r10m[band][t] = slc
                prev = slc
                for level_name, _parent, factor in overview_levels:
                    prev = _downsample_2d(prev, factor, "average")
                    orbit[level_name][band][t] = prev
        for band in ("vv", "vh"):  # #201 CF attrs at every level, once per band
            for level_name, _parent, _factor in OVERVIEW_CHAIN:
                orbit[level_name][band].attrs.update(dict(BACKSCATTER_CF_ATTRS))
            report.bands_rewritten += 1

        # #201 — set the CF `_FillValue` attr (only) on conditions DATA arrays (2-D float gamma_area/
        # lia). No data re-mask (R4): conditions nodata derives from the GeoTIFF's declared nodata,
        # which the cube doesn't retain. Coordinate arrays (ndim <= 1) are left untouched.
        if "conditions" in orbit:
            for _name, cond_arr in orbit["conditions"].arrays():
                if cond_arr.ndim >= 2 and np.issubdtype(cond_arr.dtype, np.floating):
                    cond_arr.attrs["_FillValue"] = FLOAT32_NAN_FILL_VALUE
                    report.conditions_fill_value_set += 1

    # Consolidate first (#203 — every orbit + root), then write the completion marker LAST and only if
    # every orbit was re-derived. Ordering is crash-safety (R2): a crash before the marker leaves the
    # store consolidated-but-unmarked → re-derived idempotently next run, never marked-but-unconsolidated
    # (which idempotency would then skip forever). `set_root_attr` edits the root zarr.json directly so
    # the marker doesn't clobber the consolidated metadata just written.
    if report.orbits:
        consolidate_s1_store(str(store_path), report.orbits[0])
    if not report.skipped_no_border_mask:
        s1_store_meta.set_root_attr(str(store_path), MIGRATION_MARKER_KEY, _marker_value())
    return report


# =============================================================================
# Fleet driver — enumerate the cube collection and redrive every store
# =============================================================================


@dataclass
class FleetReport:
    """Aggregate outcome across the collection — one item id in exactly one bucket (or ``failed``)."""

    derived: list[str] = field(default_factory=list)
    already_current: list[str] = field(default_factory=list)
    skipped_no_border_mask: list[str] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)  # (item_id, error message)


def _backup_path(backup_prefix: str, item_id: str) -> str:
    """Per-store backup location under ``backup_prefix`` (e.g. ``s3://bucket/migration-backup``)."""
    return f"{backup_prefix.rstrip('/')}/{item_id}.zarr"


def run_fleet(
    stac_api_url: str,
    cube_collection: str,
    *,
    dry_run: bool = False,
    only_item: str | None = None,
    skip_tiles: tuple[str, ...] = (),
    backup_prefix: str | None = None,
) -> FleetReport:
    """Redrive every cube in ``cube_collection``; one bad store is logged and skipped, never aborting.

    Resumable: a store already at the current writer reports ``already_current`` (the marker) and is a
    no-op, so a re-run only touches the stores that still need it. When ``backup_prefix`` is set (the
    no-versioning fallback), each store is copied there *before* it is re-derived; a backup failure marks
    that store failed and it is not re-derived.
    """
    fleet = FleetReport()
    for item_id, href in list_cube_items(stac_api_url, cube_collection):
        if only_item is not None and item_id != only_item:
            continue
        if any(tile in item_id for tile in skip_tiles):
            continue
        store = https_to_s3(href)  # the STAC asset is the https gateway URI; redrive needs s3://
        if store is None:
            log.error("unresolvable store href for %s: %s", item_id, href)
            fleet.failed.append((item_id, f"unresolvable store href: {href}"))
            continue
        try:
            if backup_prefix and not dry_run:
                s1_store_meta.backup_store(store, _backup_path(backup_prefix, item_id))
            rpt = redrive_store(store, dry_run=dry_run)
        except Exception as exc:  # noqa: BLE001 -- the fleet must continue past one unreadable store
            log.exception("redrive failed for %s (%s)", item_id, store)
            fleet.failed.append((item_id, str(exc)))
            continue
        if rpt.already_current:
            fleet.already_current.append(item_id)
        elif rpt.skipped_no_border_mask:
            fleet.skipped_no_border_mask.append(item_id)
        else:
            fleet.derived.append(item_id)
    return fleet


def run_rollback(
    stac_api_url: str,
    cube_collection: str,
    backup_prefix: str,
    *,
    only_item: str | None = None,
    skip_tiles: tuple[str, ...] = (),
) -> FleetReport:
    """Restore every cube from its ``backup_prefix`` copy (``--rollback`` of an unversioned-bucket run).

    Per-store try/except like ``run_fleet``; the returned report's ``derived`` lists the restored items.
    """
    fleet = FleetReport()
    for item_id, href in list_cube_items(stac_api_url, cube_collection):
        if only_item is not None and item_id != only_item:
            continue
        if any(tile in item_id for tile in skip_tiles):
            continue
        store = https_to_s3(href)
        if store is None:
            log.error("unresolvable store href for %s: %s", item_id, href)
            fleet.failed.append((item_id, f"unresolvable store href: {href}"))
            continue
        try:
            s1_store_meta.restore_store(_backup_path(backup_prefix, item_id), store)
        except Exception as exc:  # noqa: BLE001 -- restore one store; keep going
            log.exception("rollback failed for %s (%s)", item_id, store)
            fleet.failed.append((item_id, str(exc)))
            continue
        fleet.derived.append(item_id)  # "derived" == restored in the rollback report
    return fleet


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Redrive S1 RTC cubes to the current data-model.")
    parser.add_argument("--stac-api-url", required=True)
    parser.add_argument("--cube-collection", required=True)
    parser.add_argument(
        "--list",
        action="store_true",
        dest="list_only",
        help="enumerate items + resolved s3 store (STAC only, no S3 open); for laptop verification",
    )
    parser.add_argument("--dry-run", action="store_true", help="report would-change; write nothing")
    parser.add_argument("--item", default=None, help="redrive a single item id only")
    parser.add_argument(
        "--skip-tiles", nargs="*", default=[], help="tile substrings to exclude (e.g. hard-defect)"
    )
    parser.add_argument(
        "--bucket", default=None, help="cube bucket, for the S3-versioning pre-flight"
    )
    parser.add_argument(
        "--backup-prefix",
        default=None,
        help="s3 prefix for pre-write backups (no-versioning fallback) and the --rollback source",
    )
    parser.add_argument(
        "--rollback", action="store_true", help="restore every store from --backup-prefix"
    )
    parser.add_argument(
        "--s3-endpoint",
        default=None,
        help="S3 endpoint for the versioning check (else AWS_ENDPOINT_URL)",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

    if args.list_only:
        for item_id, href in list_cube_items(args.stac_api_url, args.cube_collection):
            print(f"{item_id}\t{https_to_s3(href)}")  # noqa: T201 -- CLI output
        return 0

    if args.rollback:
        if not args.backup_prefix:
            parser.error("--rollback requires --backup-prefix")
        rb = run_rollback(
            args.stac_api_url,
            args.cube_collection,
            args.backup_prefix,
            only_item=args.item,
            skip_tiles=tuple(args.skip_tiles),
        )
        log.info("rollback: restored=%d failed=%d", len(rb.derived), len(rb.failed))
        for item_id, err in rb.failed:
            log.error("  FAILED %s: %s", item_id, err)
        return 1 if rb.failed else 0

    # Pre-flight rollback safety (C2): a real run must be reversible — either the bucket has S3
    # versioning (per-object restore) or a --backup-prefix is given (each store copied before its first
    # write). Refuse otherwise. Dry-run writes nothing, so it skips the gate.
    if not args.dry_run and not args.backup_prefix:
        if not args.bucket:
            parser.error("a real run needs --bucket (for the versioning check) or --backup-prefix")
        if not s1_store_meta.s3_versioning_enabled(args.bucket, s3_endpoint=args.s3_endpoint):
            log.error(
                "refusing: bucket %s has S3 versioning OFF and no --backup-prefix (C2 rollback safety)",
                args.bucket,
            )
            return 2

    fleet = run_fleet(
        args.stac_api_url,
        args.cube_collection,
        dry_run=args.dry_run,
        only_item=args.item,
        skip_tiles=tuple(args.skip_tiles),
        backup_prefix=args.backup_prefix,
    )
    log.info(
        "fleet %s: derived=%d already-current=%d skipped-no-border_mask=%d failed=%d",
        "DRY-RUN" if args.dry_run else "RUN",
        len(fleet.derived),
        len(fleet.already_current),
        len(fleet.skipped_no_border_mask),
        len(fleet.failed),
    )
    for item_id, err in fleet.failed:
        log.error("  FAILED %s: %s", item_id, err)
    return 1 if fleet.failed else 0


if __name__ == "__main__":
    sys.exit(main())
