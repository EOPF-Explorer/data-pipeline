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
    """The writer revision the migration runs against (the completion-marker value)."""
    return str(eopf_geozarr.__version__)


def redrive_store(store_path: str | Path) -> RedriveReport:
    """Re-derive one cube store in place to the current data-model; return what changed.

    Idempotent via the per-store completion marker: a store already migrated at the current writer is
    a no-op. Follows the consolidated-metadata dance (drop → reopen ``r+`` → re-derive → marker →
    ``consolidate_s1_store``) so the writer sees fresh per-array metadata.
    """
    s1_store_meta.assert_writer_pinned()  # R5 — refuse to run on a drifted writer
    store_path = Path(store_path)
    report = RedriveReport(store=str(store_path))

    root_ro = zarr.open_group(str(store_path), mode="r", zarr_format=3)
    report.orbits = [name for name, _ in root_ro.groups()]
    if dict(root_ro.attrs).get(MIGRATION_MARKER_KEY) == _marker_value():
        report.already_current = True
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
        border_mask = np.asarray(r10m["border_mask"][:])  # (T, y, x) — authoritative, untouched
        for band in ("vv", "vh"):
            native = np.asarray(r10m[band][:])  # (T, y, x); legacy: 0.0 out of swath
            masked = np.empty_like(native, dtype="float32")
            overviews: dict[str, list[np.ndarray]] = {lvl: [] for lvl, _, _ in overview_levels}
            # Per time-slice: mask by border_mask (#202), then walk the writer's 2-D overview chain.
            for t in range(native.shape[0]):
                slc = np.where(border_mask[t] == 0, np.nan, native[t]).astype("float32")
                masked[t] = slc
                prev = slc
                for level_name, _parent, factor in overview_levels:
                    prev = _downsample_2d(prev, factor, "average")
                    overviews[level_name].append(prev)
            r10m[band][:] = masked
            for level_name, slices in overviews.items():
                orbit[level_name][band][:] = np.stack(slices)
            for level_name, _parent, _factor in OVERVIEW_CHAIN:  # #201 CF attrs at every level
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

    # Mark complete only if every orbit was re-derived — a store with a skipped (no-border_mask) orbit
    # stays un-marked so it is revisited rather than recorded as done (R2/R6).
    if not report.skipped_no_border_mask:
        root.attrs[MIGRATION_MARKER_KEY] = _marker_value()  # before consolidation so it's captured
    if report.orbits:
        consolidate_s1_store(str(store_path), report.orbits[0])  # #203 — consolidates ALL orbits
    return report
