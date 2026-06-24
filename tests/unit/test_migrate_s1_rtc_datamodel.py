"""Unit tests for scripts/migrate_s1_rtc_datamodel.py — `redrive_store` core (Task 1, slice 2).

Oracle = the data-model writer itself (criterion 2 "value-identical to a fresh re-ingest"): build a
**fresh** cube via `ingest_s1tiling_acquisition`, **de-migrate** it back to a legacy cube (vv/vh nodata
as 0.0, stale overviews, no CF `_FillValue`, un-consolidated), then `redrive_store` it and assert the
result reproduces the fresh cube exactly (NaN-aware values + CF attrs + standalone-consolidated orbits).
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

import numpy as np
import pytest
import rasterio
import zarr
from eopf_geozarr.conversion.s1_ingest import (
    BACKSCATTER_CF_ATTRS,
    OVERVIEW_CHAIN,
    consolidate_s1_store,
    ingest_s1tiling_acquisition,
)
from rasterio.transform import from_bounds

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import migrate_s1_rtc_datamodel as migrate  # noqa: E402
import s1_store_meta  # noqa: E402

# --- synthetic-GeoTIFF fixture constants (mirrors data-model tests/test_s1_rtc_ingest.py) ---
SIZE = 36
CRS = "EPSG:32633"
TRANSFORM = from_bounds(500000.0, 4997440.0, 502560.0, 5000000.0, SIZE, SIZE)
BORDER_ROWS = 10  # rows 0..9 are out-of-swath (border_mask == 0)


def _tags(stamp_compact: str, orbit_num: str) -> dict[str, str]:
    dt = (
        f"{stamp_compact[0:4]}:{stamp_compact[4:6]}:{stamp_compact[6:8]}"
        f"T{stamp_compact[9:11]}:{stamp_compact[11:13]}:{stamp_compact[13:15]}Z"
    )
    return {
        "ACQUISITION_DATETIME": dt,
        "ORBIT_NUMBER": orbit_num,
        "RELATIVE_ORBIT_NUMBER": "037",
        "FLYING_UNIT_CODE": "S1A",
        "CALIBRATION": "gamma_naught",
    }


def _write_geotiff(path: Path, data: np.ndarray, tags: dict[str, str]) -> None:
    with rasterio.open(
        str(path),
        "w",
        driver="GTiff",
        height=data.shape[0],
        width=data.shape[1],
        count=1,
        dtype=data.dtype,
        crs=CRS,
        transform=TRANSFORM,
    ) as dst:
        dst.update_tags(**tags)
        dst.write(data, 1)


def _ingest_one(
    tmp: Path, store: Path, orbit_dir: str, stamp: str, orbit_num: str, seed: int
) -> None:
    """Write vv/vh/mask GeoTIFFs for one acquisition and append it to the store via the writer."""
    rng = np.random.default_rng(seed)
    tag = _tags(stamp, orbit_num)
    code = "ASC" if orbit_dir == "ascending" else "DES"
    mask = np.ones((SIZE, SIZE), dtype=np.uint8)
    mask[:BORDER_ROWS, :] = 0
    paths = {}
    for pol, lo, hi in (("vv", 0.0, 1.0), ("vh", 0.0, 0.5)):
        arr = rng.uniform(lo, hi, (SIZE, SIZE)).astype(np.float32)
        p = tmp / f"s1a_32TQM_{pol}_{code}_037_{stamp}_GammaNaughtRTC.tif"
        _write_geotiff(p, arr, tag)
        paths[pol] = p
    mpath = tmp / f"mask_{code}_{stamp}.tif"
    _write_geotiff(mpath, mask, tag)
    ingest_s1tiling_acquisition(paths["vv"], paths["vh"], mpath, str(store), orbit_dir)


@pytest.fixture
def fresh_cube(tmp_path: Path) -> Path:
    """A fresh new-datamodel cube: ascending (2 acquisitions) + descending (1), consolidated."""
    store = tmp_path / "s1-rtc-36TEST.zarr"
    gt = tmp_path / "gt"
    gt.mkdir()
    _ingest_one(gt, store, "ascending", "20230115t061234", "47001", seed=1)
    _ingest_one(gt, store, "ascending", "20230127t061235", "47177", seed=2)
    _ingest_one(gt, store, "descending", "20230120t180511", "47090", seed=3)
    consolidate_s1_store(str(store), "ascending")
    return store


def _all_levels() -> list[str]:
    return [lvl for lvl, _, _ in OVERVIEW_CHAIN]


def _snapshot_bands(store: Path) -> dict[tuple[str, str, str], np.ndarray]:
    """{(orbit, level, band): values} for vv/vh across every orbit/level (read un-consolidated)."""
    root = zarr.open_group(str(store), mode="r", zarr_format=3)
    out: dict[tuple[str, str, str], np.ndarray] = {}
    for orbit, og in root.groups():
        for level in _all_levels():
            for band in ("vv", "vh"):
                out[(orbit, level, band)] = og[level][band][:]
    return out


def _demigrate(store: Path, *, mask_native: bool = True) -> None:
    """Turn a fresh cube into a legacy one: nodata back to 0.0 at native, ZERO the overviews, strip
    the CF backscatter attrs, drop consolidated metadata. border_mask / time / coords untouched.

    ``mask_native=False`` leaves the native level already NaN-masked (overviews still stale, no marker):
    a cube left half-migrated by a crash between native and overviews — redrive must re-derive it.
    """
    s1_store_meta.drop_consolidated_metadata(store)
    root = zarr.open_group(str(store), mode="r+", zarr_format=3)
    for _orbit, og in root.groups():
        for level in _all_levels():
            for band in ("vv", "vh"):
                arr = og[level][band]
                if level == "r10m":
                    if mask_native:
                        arr[:] = np.nan_to_num(arr[:], nan=0.0)  # legacy stored 0.0 out of swath
                else:
                    arr[:] = 0.0  # stale overviews — redrive must recompute these
                for k in ("_FillValue", "standard_name", "units"):
                    arr.attrs.pop(k, None)


def test_redrive_reproduces_the_fresh_writer_output(fresh_cube: Path) -> None:
    """criterion 2: migrated vv/vh values equal a fresh re-ingest at every orbit/level (NaN-aware)."""
    golden = _snapshot_bands(fresh_cube)
    _demigrate(fresh_cube)

    report = migrate.redrive_store(fresh_cube)

    assert set(report.orbits) == {"ascending", "descending"}
    assert report.already_current is False
    migrated = _snapshot_bands(fresh_cube)
    for key, expected in golden.items():
        np.testing.assert_array_equal(migrated[key], expected, err_msg=f"mismatch at {key}")


def test_native_nan_iff_border_mask_zero(fresh_cube: Path) -> None:
    """criterion 1 (native level): vv/vh are NaN exactly where border_mask == 0, valid pixels kept."""
    _demigrate(fresh_cube)
    migrate.redrive_store(fresh_cube)

    root = zarr.open_group(str(fresh_cube), mode="r", zarr_format=3)
    for _orbit, og in root.groups():
        r10m = og["r10m"]
        bm = r10m["border_mask"][:]
        for band in ("vv", "vh"):
            data = r10m[band][:]
            assert np.all(np.isnan(data) == (bm == 0)), f"{band}: NaN pattern != border_mask"


def test_sets_cf_attrs_and_leaves_border_mask_untouched(fresh_cube: Path) -> None:
    """criterion 3 (vv/vh): backscatter CF attrs restored at every level; border_mask unchanged."""
    bm_before = {
        orbit: og["r10m"]["border_mask"][:]
        for orbit, og in zarr.open_group(str(fresh_cube), mode="r", zarr_format=3).groups()
    }
    _demigrate(fresh_cube)
    migrate.redrive_store(fresh_cube)

    root = zarr.open_group(str(fresh_cube), mode="r", zarr_format=3)
    for orbit, og in root.groups():
        for level in _all_levels():
            for band in ("vv", "vh"):
                attrs = dict(og[level][band].attrs)
                for k, v in BACKSCATTER_CF_ATTRS.items():
                    assert attrs.get(k) == v, f"{orbit}/{level}/{band} missing CF attr {k}"
        np.testing.assert_array_equal(og["r10m"]["border_mask"][:], bm_before[orbit])


def test_both_orbits_consolidated_standalone(fresh_cube: Path) -> None:
    """criterion 4: every orbit group is consolidated, openable standalone (not via the root)."""
    _demigrate(fresh_cube)
    migrate.redrive_store(fresh_cube)

    for orbit in ("ascending", "descending"):
        meta = (fresh_cube / orbit / "zarr.json").read_text()
        assert "consolidated_metadata" in meta, f"{orbit} not standalone-consolidated"


def test_idempotent_second_run_is_a_noop(fresh_cube: Path) -> None:
    """criterion 5: a second redrive on a current store rewrites nothing (completion marker)."""
    _demigrate(fresh_cube)
    migrate.redrive_store(fresh_cube)
    after_first = _snapshot_bands(fresh_cube)

    report2 = migrate.redrive_store(fresh_cube)

    assert report2.already_current is True
    for key, vals in _snapshot_bands(fresh_cube).items():
        np.testing.assert_array_equal(vals, after_first[key])


def test_crash_safety_redrives_a_half_migrated_store(fresh_cube: Path) -> None:
    """criterion 6: native already masked but overviews stale + no marker → still fully re-derived.

    "native is NaN-masked" is not a safe skip key (a crash between native and overviews leaves stale
    overviews); only the completion marker is. Redrive must reproduce the writer regardless.
    """
    golden = _snapshot_bands(fresh_cube)
    _demigrate(fresh_cube, mask_native=False)  # native left masked, overviews zeroed, no marker

    report = migrate.redrive_store(fresh_cube)

    assert report.already_current is False
    migrated = _snapshot_bands(fresh_cube)
    for key, expected in golden.items():
        np.testing.assert_array_equal(migrated[key], expected, err_msg=f"mismatch at {key}")


def test_missing_border_mask_is_skipped_not_crashed(fresh_cube: Path) -> None:
    """criterion 7 (R6): an orbit lacking border_mask is flagged + skipped; the store is not marked."""
    _demigrate(fresh_cube)
    s1_store_meta.drop_consolidated_metadata(
        fresh_cube
    )  # so the member listing reflects the rmtree
    shutil.rmtree(fresh_cube / "descending" / "r10m" / "border_mask")

    report = migrate.redrive_store(fresh_cube)  # must not raise

    assert report.skipped_no_border_mask == ["descending"]
    # the store stays un-marked → a re-run still re-derives (never recorded as complete)
    assert migrate.redrive_store(fresh_cube).already_current is False
    # the present-border_mask orbit was still re-derived (CF attrs restored)
    asc_vv = zarr.open_group(str(fresh_cube), mode="r", zarr_format=3)["ascending"]["r10m"]["vv"]
    assert dict(asc_vv.attrs).get("_FillValue") == BACKSCATTER_CF_ATTRS["_FillValue"]
