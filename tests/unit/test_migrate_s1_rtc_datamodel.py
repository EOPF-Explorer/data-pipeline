"""Unit tests for scripts/migrate_s1_rtc_datamodel.py — `redrive_store` core (Task 1, slice 2).

Oracle = the data-model writer itself (criterion 2 "value-identical to a fresh re-ingest"): build a
**fresh** cube via `ingest_s1tiling_acquisition`, **de-migrate** it back to a legacy cube (vv/vh nodata
as 0.0, stale overviews, no CF `_FillValue`, un-consolidated), then `redrive_store` it and assert the
result reproduces the fresh cube exactly (NaN-aware values + CF attrs + standalone-consolidated orbits).
"""

from __future__ import annotations

import json
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
    """criterion 4: every orbit group is consolidated, openable standalone (not via the root).

    Also asserts the completion marker COEXISTS with consolidation (I1) — the marker is written after
    consolidation via set_root_attr, which must not clobber the consolidated metadata.
    """
    _demigrate(fresh_cube)
    migrate.redrive_store(fresh_cube)

    for orbit in ("ascending", "descending"):
        meta = (fresh_cube / orbit / "zarr.json").read_text()
        assert "consolidated_metadata" in meta, f"{orbit} not standalone-consolidated"
    root = zarr.open_group(str(fresh_cube), mode="r", zarr_format=3)
    assert dict(root.attrs).get(migrate.MIGRATION_MARKER_KEY) is not None  # marker + consolidation


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


def _add_legacy_conditions(store: Path, orbit_name: str, suffix: str = "037") -> None:
    """Add a legacy `conditions` group: gamma_area/lia 2-D float32 data arrays WITHOUT `_FillValue`,
    plus a 1-D float64 `y` coordinate (which must NOT receive `_FillValue`)."""
    s1_store_meta.drop_consolidated_metadata(store)
    cond = zarr.open_group(str(store), mode="r+", zarr_format=3)[orbit_name].create_group(
        "conditions"
    )
    rng = np.random.default_rng(99)
    for name in (f"gamma_area_{suffix}", f"lia_{suffix}"):
        arr = cond.create_array(
            name, shape=(SIZE, SIZE), dtype="float32", dimension_names=["y", "x"]
        )
        arr[:, :] = rng.uniform(0.0, 10.0, (SIZE, SIZE)).astype(np.float32)  # legacy: no _FillValue
    ycoord = cond.create_array("y", shape=(SIZE,), dtype="float64", dimension_names=["y"])
    ycoord[:] = np.arange(SIZE, dtype="float64")


def test_conditions_get_fill_value_attr_only(fresh_cube: Path) -> None:
    """Task 1b: redrive sets `_FillValue` on conditions DATA arrays (2-D float) without touching their
    values, and leaves coordinate arrays (1-D) alone."""
    _add_legacy_conditions(fresh_cube, "ascending")
    cond_ro = zarr.open_group(str(fresh_cube), mode="r", zarr_format=3)["ascending"]["conditions"]
    before = {name: arr[:] for name, arr in cond_ro.arrays()}

    migrate.redrive_store(fresh_cube)

    cond = zarr.open_group(str(fresh_cube), mode="r", zarr_format=3)["ascending"]["conditions"]
    for name in ("gamma_area_037", "lia_037"):
        assert dict(cond[name].attrs).get("_FillValue") == BACKSCATTER_CF_ATTRS["_FillValue"]
        np.testing.assert_array_equal(cond[name][:], before[name])  # data unchanged (R4: attr only)
    assert "_FillValue" not in dict(cond["y"].attrs)  # 1-D coord must be left alone


def test_dry_run_reports_without_writing(fresh_cube: Path) -> None:
    """Task 2: `dry_run=True` reports the plan (orbits, not-already-current) and writes nothing."""
    _demigrate(fresh_cube)
    before = _snapshot_bands(fresh_cube)

    report = migrate.redrive_store(fresh_cube, dry_run=True)

    assert report.already_current is False
    assert set(report.orbits) == {"ascending", "descending"}
    for key, vals in _snapshot_bands(fresh_cube).items():  # nothing re-derived
        np.testing.assert_array_equal(vals, before[key])
    root = zarr.open_group(str(fresh_cube), mode="r", zarr_format=3)
    assert migrate.MIGRATION_MARKER_KEY not in dict(root.attrs)  # not marked complete


def test_redrive_does_not_mangle_an_s3_uri(monkeypatch) -> None:
    """Regression (real-S3 only): `Path('s3://b/x')` collapses `//` → `s3:/b/x` and breaks the store
    URL. redrive must pass the URI through unchanged to zarr. Local-path unit fixtures can't catch this."""
    seen: list[str] = []

    class _FakeRoot:
        attrs: dict = {}

        def groups(self):
            return iter(())

    monkeypatch.setattr(migrate.zarr, "open_group", lambda p, **_k: seen.append(p) or _FakeRoot())

    migrate.redrive_store("s3://bucket/sentinel-1-grd-rtc-staging/s1-rtc-X.zarr", dry_run=True)

    assert seen == ["s3://bucket/sentinel-1-grd-rtc-staging/s1-rtc-X.zarr"]  # not s3:/bucket/...


# =============================================================================
# The v0.11.0 pin bump: the completion marker must not be coupled to the writer version, and the
# library's new root rewrite must not happen as a side effect of a "re-derive the bands" run.
# =============================================================================


def test_marker_is_stable_across_writer_versions(monkeypatch: pytest.MonkeyPatch) -> None:
    """The marker must NOT be the running release's version.

    This is the regression that made the 0.10.2 -> 0.11.0 bump dangerous: a version-valued marker
    stops matching the moment the pin moves, so every already-migrated cube in the fleet gets its
    bulk vv/vh re-derived from scratch for no reason.
    """
    import eopf_geozarr

    before = migrate._marker_value()
    monkeypatch.setattr(eopf_geozarr, "__version__", "99.9.9")
    assert migrate._marker_value() == before


@pytest.mark.parametrize("legacy", ["0.10.1", "0.10.2"])
def test_a_store_marked_by_an_older_run_is_not_re_derived(fresh_cube: Path, legacy: str) -> None:
    """A cube migrated under 0.10.1/0.10.2 carries a version-valued marker; it must still count as
    migrated, or the pin bump silently re-derives the whole fleet."""
    _demigrate(fresh_cube)
    s1_store_meta.set_root_attr(str(fresh_cube), migrate.MIGRATION_MARKER_KEY, legacy)
    before = _snapshot_bands(fresh_cube)

    report = migrate.redrive_store(fresh_cube)

    assert report.already_current is True
    for key, vals in _snapshot_bands(fresh_cube).items():
        np.testing.assert_array_equal(vals, before[key])


def test_an_unknown_marker_value_is_migrated(fresh_cube: Path) -> None:
    """Only the known markers mean "done"; anything else must be treated as needing the re-derive."""
    _demigrate(fresh_cube)
    s1_store_meta.set_root_attr(str(fresh_cube), migrate.MIGRATION_MARKER_KEY, "something-else")

    report = migrate.redrive_store(fresh_cube)

    assert report.already_current is False
    assert report.bands_rewritten > 0


def test_default_run_consolidates_but_does_not_rewrite_the_root(
    fresh_cube: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """#203 still satisfied, root metadata untouched.

    From 0.11.0 the library's `consolidate_s1_store` also rewrites the store ROOT (proj:code ->
    EPSG:4326, lon/lat spatial:bbox). This migration runs against s3:// production cubes, so that
    must not happen unless asked for.
    """
    calls: list[tuple] = []
    monkeypatch.setattr(migrate, "consolidate_s1_store", lambda *a, **k: calls.append(a))
    _demigrate(fresh_cube)

    migrate.redrive_store(fresh_cube)

    assert calls == []  # the library's root-rewriting helper was never reached
    for orbit in ("ascending", "descending"):  # but #203 consolidation still happened
        assert "consolidated_metadata" in (fresh_cube / orbit / "zarr.json").read_text()
    assert "consolidated_metadata" in (fresh_cube / "zarr.json").read_text()


def test_rewrite_root_geo_opt_in_calls_the_library(
    fresh_cube: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The opt-in flag is what routes consolidation through the library's root refinement."""
    calls: list[tuple] = []
    monkeypatch.setattr(migrate, "consolidate_s1_store", lambda *a, **k: calls.append(a))
    _demigrate(fresh_cube)

    migrate.redrive_store(fresh_cube, rewrite_root_geo=True)

    assert len(calls) == 1


# --- enumeration must not run through a stale consolidated root -------------------------------


def _legacy_bands_keeping_consolidated(store: Path) -> None:
    """Legacy-ise vv/vh but LEAVE the consolidated metadata in place (unlike `_demigrate`).

    `_demigrate` drops it, so every other test enters `redrive_store` with no consolidated block at
    all — which is exactly the condition under which a stale one cannot be noticed.
    """
    root = zarr.open_group(str(store), mode="r+", zarr_format=3, use_consolidated=False)
    for _orbit, og in root.groups():
        for level in _all_levels():
            for band in ("vv", "vh"):
                arr = og[level][band]
                arr[:] = np.nan_to_num(arr[:], nan=0.0) if level == "r10m" else 0.0
                for k in ("_FillValue", "standard_name", "units"):
                    arr.attrs.pop(k, None)


def _drop_from_consolidated_root(store: Path, orbit: str) -> None:
    """Remove `orbit` from the ROOT's consolidated block, leaving the group itself on disk.

    This is the state the library's `consolidate_s1_store` docstring warns about — "an orbit group
    created since the last consolidation is absent from a stale root block". It is written by hand
    here because the local writer path happens to refresh the root when it appends a new orbit
    group; the invariant under test is our own enumeration, not how a store came to be stale.
    """
    root_json = store / "zarr.json"
    meta = json.loads(root_json.read_text())
    members = meta["consolidated_metadata"]["metadata"]
    # The block is a FLAT map ("descending", "descending/r10m", "descending/r10m/vv", ...); leaving
    # the children behind would orphan them and zarr refuses to open the store at all.
    for key in [k for k in members if k == orbit or k.startswith(f"{orbit}/")]:
        del members[key]
    root_json.write_text(json.dumps(meta))


def test_consolidates_an_orbit_absent_from_the_stale_root(fresh_cube: Path) -> None:
    """#203 for real: an orbit missing from the consolidated root still gets consolidated.

    Enumerating orbits through a stale block re-derives that orbit anyway (the re-derive walks a
    post-drop handle) and then silently skips consolidating it — readers opening it standalone fall
    back to a listing. The library re-enumerates with `use_consolidated=False` for exactly this
    reason; so must this script.
    """
    _drop_from_consolidated_root(fresh_cube, "descending")
    stale = zarr.open_group(str(fresh_cube), mode="r", zarr_format=3)
    assert [name for name, _ in stale.groups()] == ["ascending"], "fixture: root is not stale"

    _legacy_bands_keeping_consolidated(fresh_cube)
    report = migrate.redrive_store(fresh_cube)

    assert set(report.orbits) == {"ascending", "descending"}, "the newest orbit was not enumerated"
    for orbit in ("ascending", "descending"):
        assert (
            "consolidated_metadata" in (fresh_cube / orbit / "zarr.json").read_text()
        ), f"{orbit} was re-derived but left unconsolidated"


def test_rewrite_root_geo_on_an_already_migrated_store_is_reported_not_silent(
    fresh_cube: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """The marker short-circuits the store, so --rewrite-root-geo cannot take effect — say so.

    Once the fleet has been migrated every store carries the marker, and an operator passing
    --rewrite-root-geo gets `already-current=N, derived=0` with no root rewrite. Silently ignoring
    an explicitly-passed flag on a production run is how an operator concludes it happened.
    """
    s1_store_meta.set_root_attr(str(fresh_cube), migrate.MIGRATION_MARKER_KEY, "1")

    with caplog.at_level("WARNING"):
        report = migrate.redrive_store(fresh_cube, rewrite_root_geo=True)

    assert report.already_current is True
    assert any(
        "rewrite-root-geo" in rec.getMessage() and "IGNORED" in rec.getMessage()
        for rec in caplog.records
    ), f"no warning about the ignored flag; got: {[r.getMessage() for r in caplog.records]}"
