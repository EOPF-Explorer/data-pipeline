"""Unit tests for ingest_v1_s1_rtc.py -- ingest_all."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import call, patch

import zarr

scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

from ingest_v1_s1_rtc import (  # noqa: E402
    _S3_CONCURRENCY,
    _get_tree,
    _patch_cf_grid_mapping,
    _sync_tree,
    acq_time_ns,
    ingest_all,
    new_acquisitions,
    run_ingest,
    store_times_ns,
)

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_ACQ = {
    "platform": "s1a",
    "tile": "31TCH",
    "orbit_dir": "ASC",
    "rel_orbit": "037",
    "acq_stamp": "20230115t061234",
    "vv": Path("/data/s1a_31TCH_vv_ASC_037_20230115t061234_GammaNaughtRTC.tif"),
    "vh": Path("/data/s1a_31TCH_vh_ASC_037_20230115t061234_GammaNaughtRTC.tif"),
    "vv_mask": Path("/data/s1a_31TCH_vv_ASC_037_20230115t061234_GammaNaughtRTC_BorderMask.tif"),
    "vh_mask": Path("/data/s1a_31TCH_vh_ASC_037_20230115t061234_GammaNaughtRTC_BorderMask.tif"),
}
_COND = {
    "tile": "31TCH",
    "orbit": "037",
    "gamma_area": Path("/data/GAMMA_AREA_31TCH_037.tif"),
    "lia": Path("/data/LIA_31TCH_037.tif"),
}

_MOD = "ingest_v1_s1_rtc"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_exits_0_on_success() -> None:
    """Happy path: 2 acquisitions + 1 condition group -> all 5 steps called -> exit 0."""
    with (
        patch(f"{_MOD}.discover_s1tiling_acquisitions", return_value=[_ACQ, _ACQ]) as mock_disc,
        patch(f"{_MOD}._acquisition_has_data", return_value=True),
        patch(f"{_MOD}.ingest_s1tiling_acquisition", return_value=0) as mock_ing,
        patch(f"{_MOD}.discover_s1tiling_conditions", return_value=[_COND]) as mock_disc_cond,
        patch(f"{_MOD}.ingest_s1tiling_conditions") as mock_ing_cond,
        patch(f"{_MOD}.consolidate_s1_store") as mock_cons,
        patch(f"{_MOD}._patch_cf_grid_mapping") as mock_cf,
    ):
        result = ingest_all("/input", "/store.zarr", "ascending")

    assert result == 0
    mock_cf.assert_called_once_with("/store.zarr", "ascending")
    mock_disc.assert_called_once_with("/input")
    assert mock_ing.call_count == 2
    mock_ing.assert_called_with(
        vv_path=_ACQ["vv"],
        vh_path=_ACQ["vh"],
        border_mask_path=_ACQ["vv_mask"],
        store_path="/store.zarr",
        orbit_direction="ascending",
    )
    mock_disc_cond.assert_called_once_with("/input")
    mock_ing_cond.assert_called_once_with(
        store_path="/store.zarr",
        orbit_direction="ascending",
        relative_orbit=37,
        gamma_area_path=_COND["gamma_area"],
        lia_path=_COND["lia"],
    )
    # consolidated twice: once after ingest (step 5), once after the patches (step 6)
    assert mock_cons.call_count == 2
    mock_cons.assert_has_calls([call("/store.zarr", "ascending"), call("/store.zarr", "ascending")])


def test_exits_2_on_empty_prefix() -> None:
    """Empty discovery result -> exit 2, ingest never called."""
    with (
        patch(f"{_MOD}.discover_s1tiling_acquisitions", return_value=[]),
        patch(f"{_MOD}.ingest_s1tiling_acquisition") as mock_ing,
        patch(f"{_MOD}.consolidate_s1_store") as mock_cons,
    ):
        result = ingest_all("/input", "/store.zarr", "ascending")

    assert result == 2
    mock_ing.assert_not_called()
    mock_cons.assert_not_called()


def test_exits_1_on_ingest_error() -> None:
    """First ingest failure -> exit 1; consolidate_s1_store must not be called."""
    with (
        patch(f"{_MOD}.discover_s1tiling_acquisitions", return_value=[_ACQ]),
        patch(f"{_MOD}._acquisition_has_data", return_value=True),
        patch(f"{_MOD}.ingest_s1tiling_acquisition", side_effect=OSError("disk full")),
        patch(f"{_MOD}.consolidate_s1_store") as mock_cons,
    ):
        result = ingest_all("/input", "/store.zarr", "descending")

    assert result == 1
    mock_cons.assert_not_called()


def test_conditions_non_fatal_if_empty() -> None:
    """Empty conditions -> consolidate_s1_store still called (non-fatal)."""
    with (
        patch(f"{_MOD}.discover_s1tiling_acquisitions", return_value=[_ACQ]),
        patch(f"{_MOD}._acquisition_has_data", return_value=True),
        patch(f"{_MOD}.ingest_s1tiling_acquisition", return_value=0),
        patch(f"{_MOD}.discover_s1tiling_conditions", return_value=[]),
        patch(f"{_MOD}.ingest_s1tiling_conditions") as mock_ing_cond,
        patch(f"{_MOD}.consolidate_s1_store") as mock_cons,
        patch(f"{_MOD}._patch_cf_grid_mapping"),
    ):
        result = ingest_all("/input", "/store.zarr", "ascending")

    assert result == 0
    mock_ing_cond.assert_not_called()
    assert mock_cons.call_count == 2


# ---------------------------------------------------------------------------
# empty-slice skip -- the produced data, not the trigger footprint, decides
# ---------------------------------------------------------------------------


def _write_tif(path: Path, data) -> Path:
    """Write a tiny single-band float32 GeoTIFF (for the has-data probe)."""
    import rasterio
    from rasterio.transform import from_origin

    h, w = data.shape
    with rasterio.open(
        str(path), "w", driver="GTiff", height=h, width=w, count=1,
        dtype="float32", crs="EPSG:32631", transform=from_origin(0, 0, 10, 10),
    ) as dst:  # fmt: skip
        dst.write(data.astype("float32"), 1)
    return path


def test_band_has_data_false_for_all_nodata(tmp_path) -> None:
    import numpy as np
    from ingest_v1_s1_rtc import _band_has_data

    assert _band_has_data(_write_tif(tmp_path / "z.tif", np.zeros((80, 80)))) is False


def test_band_has_data_true_for_sparse_data(tmp_path) -> None:
    """A small real-data patch (well above the 0.01% floor) is detected through the decimated probe."""
    import numpy as np
    from ingest_v1_s1_rtc import _band_has_data

    arr = np.zeros((80, 80))
    arr[16:32, 16:32] = 0.42  # 4% of the tile -> reliably sampled by the 8x probe
    assert _band_has_data(_write_tif(tmp_path / "d.tif", arr)) is True


def test_acquisition_has_data_falls_back_to_vh(tmp_path) -> None:
    import numpy as np
    from ingest_v1_s1_rtc import _acquisition_has_data

    empty = _write_tif(tmp_path / "vv.tif", np.zeros((64, 64)))
    data = np.zeros((64, 64))
    data[8:40, 8:40] = 1.0
    vh = _write_tif(tmp_path / "vh.tif", data)
    assert _acquisition_has_data({"vv": empty, "vh": vh}) is True
    assert _acquisition_has_data({"vv": empty, "vh": empty}) is False


def test_ingest_all_skips_empty_acquisition() -> None:
    """An all-nodata acquisition is dropped; a data-bearing one on another date is still ingested."""
    acq_empty = {**_ACQ, "acq_stamp": "20230116t061234"}
    acq_data = {**_ACQ, "acq_stamp": "20230117t061234"}
    with (
        patch(f"{_MOD}.discover_s1tiling_acquisitions", return_value=[acq_empty, acq_data]),
        patch(f"{_MOD}._acquisition_has_data", side_effect=[False, True]),
        patch(f"{_MOD}.ingest_s1tiling_acquisition", return_value=0) as mock_ing,
        patch(f"{_MOD}.discover_s1tiling_conditions", return_value=[]),
        patch(f"{_MOD}.consolidate_s1_store"),
        patch(f"{_MOD}._patch_cf_grid_mapping"),
    ):
        result = ingest_all("/input", "/store.zarr", "ascending")
    assert result == 0
    assert mock_ing.call_count == 1
    assert mock_ing.call_args.kwargs["vv_path"] == acq_data["vv"]


def test_ingest_all_all_empty_fresh_tile_returns_2_skips_register() -> None:
    """A fresh tile (no existing cube) whose only new scenes are all-nodata builds no store and must
    return 2 (no acquisitions) so the workflow skips register — returning 0 would run register against
    a never-built store (the 30TWQ edge-tile failure: 'No acquisitions found' / 'Orbit group not found')."""
    with (
        patch(f"{_MOD}.discover_s1tiling_acquisitions", return_value=[_ACQ]),
        patch(f"{_MOD}._acquisition_has_data", return_value=False),
        patch(f"{_MOD}.ingest_s1tiling_acquisition") as mock_ing,
        patch(f"{_MOD}.consolidate_s1_store") as mock_cons,
    ):
        result = ingest_all(
            "/input", "/store.zarr", "ascending"
        )  # no existing store -> present empty
    assert result == 2
    mock_ing.assert_not_called()
    mock_cons.assert_not_called()


def test_ingest_all_all_empty_with_existing_cube_returns_0() -> None:
    """All-nodata new scenes but the cube already has slices -> return 0 (re-register the existing cube,
    idempotent). Only a *fresh* tile with nothing to register returns 2."""
    with (
        patch(f"{_MOD}.discover_s1tiling_acquisitions", return_value=[_ACQ]),
        patch(
            f"{_MOD}.store_times_ns", return_value={123}
        ),  # existing cube already has a time slice
        patch(f"{_MOD}._acquisition_has_data", return_value=False),
        patch(f"{_MOD}.ingest_s1tiling_acquisition") as mock_ing,
        patch(f"{_MOD}.consolidate_s1_store"),
    ):
        result = ingest_all("/input", "/store.zarr", "ascending")
    assert result == 0
    mock_ing.assert_not_called()


# ---------------------------------------------------------------------------
# _patch_cf_grid_mapping -- exercised against a real (tiny) local zarr store
# ---------------------------------------------------------------------------


def _build_minimal_s1_store(store_path: str) -> None:
    """Create a minimal S1-shaped GeoZarr V3 store: descending/r10m with a 3D
    vh array (time, y, x) and a geozarr proj:code but NO CF spatial_ref --
    mirroring what eopf_geozarr.s1_ingest produces (and what TiTiler rejects)."""
    import numpy as np

    root = zarr.open_group(store_path, mode="w-", zarr_format=3)
    orbit = root.create_group("descending")
    orbit.attrs["proj:code"] = "EPSG:32631"
    r10m = orbit.create_group("r10m")
    vh = r10m.create_array(
        "vh", shape=(2, 4, 4), dtype="float32", dimension_names=("time", "y", "x")
    )
    vh[...] = np.zeros((2, 4, 4), dtype="float32")
    # coordinate arrays (no y/x data semantics individually)
    r10m.create_array("time", shape=(2,), dtype="int64", dimension_names=("time",))[...] = [0, 1]
    r10m.create_array("y", shape=(4,), dtype="float64", dimension_names=("y",))[...] = range(4)
    r10m.create_array("x", shape=(4,), dtype="float64", dimension_names=("x",))[...] = range(4)


def test_patch_cf_grid_mapping_adds_spatial_ref_and_crs(tmp_path) -> None:
    """After the patch, the r10m group has a spatial_ref coord, vh carries
    grid_mapping=spatial_ref, and rioxarray can resolve the CRS (the condition
    titiler-eopf v0.5.0's _validate_zarr requires)."""
    import xarray as xr

    store = str(tmp_path / "s1.zarr")
    _build_minimal_s1_store(store)

    patched = _patch_cf_grid_mapping(store, "descending")

    assert patched == ["descending/r10m"]
    r10m = zarr.open_group(store, mode="r", zarr_format=3)["descending"]["r10m"]
    assert "spatial_ref" in list(r10m.array_keys())
    assert dict(r10m["vh"].attrs).get("grid_mapping") == "spatial_ref"

    # The decisive check: rioxarray resolves the CRS with decode_coords="all"
    ds = xr.open_zarr(f"{store}/descending/r10m", consolidated=False, decode_coords="all")
    assert ds.rio.crs is not None
    assert ds.rio.crs.to_epsg() == 32631


def test_patch_cf_grid_mapping_idempotent(tmp_path) -> None:
    """Running the patch twice does not error or duplicate the coordinate."""
    store = str(tmp_path / "s1.zarr")
    _build_minimal_s1_store(store)
    _patch_cf_grid_mapping(store, "descending")
    patched = _patch_cf_grid_mapping(store, "descending")
    assert patched == ["descending/r10m"]
    r10m = zarr.open_group(store, mode="r", zarr_format=3)["descending"]["r10m"]
    assert list(r10m.array_keys()).count("spatial_ref") == 1


# ---------------------------------------------------------------------------
# run_ingest -- s3:// routing (container-native upload for Argo)
# ---------------------------------------------------------------------------


def test_run_ingest_local_passthrough(tmp_path) -> None:
    """A local store path goes straight to ingest_all; no temp dir, no upload."""
    local_store = str(tmp_path / "s1-grd-rtc-31TCH.zarr")
    with (
        patch(f"{_MOD}.ingest_all", return_value=0) as mock_ingest,
        patch(f"{_MOD}._upload_store_to_s3") as mock_upload,
    ):
        rc = run_ingest("s3://bucket/in/", local_store, "descending")
    assert rc == 0
    mock_ingest.assert_called_once_with("s3://bucket/in/", local_store, "descending")
    mock_upload.assert_not_called()


def test_run_ingest_s3_uploads_on_success() -> None:
    """An s3:// store ingests into a local temp store, then uploads it to S3."""
    s3_store = "s3://out-bucket/sentinel-1-grd-rtc-staging/s1-grd-rtc-31TCH.zarr"
    with (
        patch(f"{_MOD}._fetch_store_from_s3") as mock_fetch,
        patch(f"{_MOD}.ingest_all", return_value=0) as mock_ingest,
        patch(f"{_MOD}._upload_store_to_s3") as mock_upload,
    ):
        rc = run_ingest("s3://bucket/in/", s3_store, "descending")
    assert rc == 0
    # ingest_all was given a LOCAL temp path named after the store basename
    local_store = mock_ingest.call_args.args[1]
    assert local_store.endswith("s1-grd-rtc-31TCH.zarr")
    assert not local_store.startswith("s3://")
    # the existing cube is fetched into the temp store BEFORE ingest, so a new
    # acquisition appends instead of replacing (cross-run cube accumulation, T4)
    mock_fetch.assert_called_once_with(s3_store, local_store)
    mock_upload.assert_called_once_with(local_store, s3_store)


def test_run_ingest_s3_skips_upload_on_failure() -> None:
    """If ingest_all fails (exit 1 or 2), the upload is not attempted."""
    s3_store = "s3://out-bucket/coll/s1-grd-rtc-31TCH.zarr"
    for code in (1, 2):
        with (
            patch(f"{_MOD}._fetch_store_from_s3"),
            patch(f"{_MOD}.ingest_all", return_value=code),
            patch(f"{_MOD}._upload_store_to_s3") as mock_upload,
        ):
            rc = run_ingest("s3://bucket/in/", s3_store, "descending")
        assert rc == code
        mock_upload.assert_not_called()


def test_sync_tree_lands_at_dest_without_nesting(tmp_path) -> None:
    """_sync_tree (the sole upload path) maps each file to dest/<relpath> — the store lands AT
    dest, not nested under dest/<store-basename>/ (the fsspec put recursive footgun)."""
    import fsspec

    store = tmp_path / "src" / "s1-grd-rtc-31TCH.zarr"
    (store / "descending" / "r10m").mkdir(parents=True)
    (store / "zarr.json").write_text("{}")
    (store / "descending" / "r10m" / "c" / "0.0").parent.mkdir(parents=True)
    (store / "descending" / "r10m" / "c" / "0.0").write_text("chunk")

    dest_root = tmp_path / "dst" / "s1-grd-rtc-31TCH.zarr"
    _sync_tree(fsspec.filesystem("file"), str(store), str(dest_root))

    assert (dest_root / "zarr.json").is_file()
    assert (dest_root / "descending" / "r10m" / "c" / "0.0").is_file()
    # the basename must NOT be nested a second time under dest
    assert not (dest_root / "s1-grd-rtc-31TCH.zarr").exists()


# ---------------------------------------------------------------------------
# T4 -- datacube append + idempotency (skip a `time` already in the cube)
# ---------------------------------------------------------------------------


def _store_with_times(store_path: str, times_ns: list[int], orbit: str = "descending") -> None:
    """Minimal cube whose r10m level carries the given `time` values (ns since epoch)."""
    import numpy as np

    root = zarr.open_group(store_path, mode="w-", zarr_format=3)
    r10m = root.create_group(orbit).create_group("r10m")
    t = r10m.create_array("time", shape=(len(times_ns),), dtype="int64", dimension_names=("time",))
    t[...] = np.asarray(times_ns, dtype="int64")


def test_acq_time_ns_matches_cube_datetime_encoding() -> None:
    """`acq_stamp` -> ns equals how the cube stores `time` (np.datetime64 of the ISO datetime)."""
    import numpy as np

    # acq_stamp 'YYYYMMDDtHHMMSS' is the same instant the GeoTIFF ACQUISITION_DATETIME tag carries
    expected = int(np.datetime64("2023-01-15T06:12:34").astype("datetime64[ns]").astype("int64"))
    assert acq_time_ns("20230115t061234") == expected


def test_store_times_ns_reads_existing_and_handles_absent(tmp_path) -> None:
    t0 = acq_time_ns("20230115t061234")
    t1 = acq_time_ns("20230127t061230")
    store = str(tmp_path / "cube.zarr")
    _store_with_times(store, [t0, t1])
    assert store_times_ns(store, "descending") == {t0, t1}
    # absent store / absent orbit group -> empty (no crash)
    assert store_times_ns(str(tmp_path / "missing.zarr"), "descending") == set()
    assert store_times_ns(store, "ascending") == set()


def test_new_acquisitions_filters_present_times() -> None:
    a0 = {**_ACQ, "acq_stamp": "20230115t061234"}
    a1 = {**_ACQ, "acq_stamp": "20230127t061230"}
    present = {acq_time_ns("20230115t061234")}
    assert new_acquisitions([a0, a1], present) == [a1]  # a0 already in cube -> dropped


def test_ingest_all_skips_acquisition_already_in_cube(tmp_path) -> None:
    """Re-ingesting an acquisition whose `time` is already present is a no-op (exit 0, no ingest)."""
    store = str(tmp_path / "cube.zarr")
    _store_with_times(store, [acq_time_ns(_ACQ["acq_stamp"])])  # _ACQ already present
    with (
        patch(f"{_MOD}.discover_s1tiling_acquisitions", return_value=[_ACQ]),
        patch(f"{_MOD}.ingest_s1tiling_acquisition") as mock_ing,
        patch(f"{_MOD}.discover_s1tiling_conditions", return_value=[]),
        patch(f"{_MOD}.consolidate_s1_store") as mock_cons,
        patch(f"{_MOD}._patch_cf_grid_mapping"),
    ):
        rc = ingest_all("/input", store, "descending")
    assert rc == 0
    mock_ing.assert_not_called()  # nothing to append
    mock_cons.assert_not_called()  # no write, no consolidation


def test_ingest_all_appends_only_new_acquisition(tmp_path) -> None:
    """A 2nd, distinct acquisition appends (ingest called once for the new time only)."""
    store = str(tmp_path / "cube.zarr")
    _store_with_times(store, [acq_time_ns("20230115t061234")])  # one existing time
    present_acq = {**_ACQ, "acq_stamp": "20230115t061234"}
    new_acq = {**_ACQ, "acq_stamp": "20230127t061230"}
    with (
        patch(f"{_MOD}.discover_s1tiling_acquisitions", return_value=[present_acq, new_acq]),
        patch(f"{_MOD}._acquisition_has_data", return_value=True),
        patch(f"{_MOD}.ingest_s1tiling_acquisition", return_value=1) as mock_ing,
        patch(f"{_MOD}.discover_s1tiling_conditions", return_value=[]),
        patch(f"{_MOD}.consolidate_s1_store"),
        patch(f"{_MOD}._patch_cf_grid_mapping"),
    ):
        rc = ingest_all("/input", store, "descending")
    assert rc == 0
    mock_ing.assert_called_once()  # only the new acquisition is appended
    assert mock_ing.call_args.kwargs["vh_path"] == new_acq["vh"]


def test_run_ingest_local_does_not_fetch(tmp_path) -> None:
    """The cross-run fetch is an S3-only concern; a local destination never fetches."""
    local_store = str(tmp_path / "cube.zarr")
    with (
        patch(f"{_MOD}._fetch_store_from_s3") as mock_fetch,
        patch(f"{_MOD}.ingest_all", return_value=0),
    ):
        run_ingest("/input", local_store, "descending")
    mock_fetch.assert_not_called()


# ---------------------------------------------------------------------------
# T4 fix -- appending to a *consolidated* fetched cube must resize `time`
# ---------------------------------------------------------------------------


def test_drop_consolidated_metadata_enables_resize(tmp_path) -> None:
    """A cube consolidated at the *orbit-group* level (as eopf_geozarr does) can't be grown via
    resize+write on reopen — the group serves the stale length-1 shape. Stripping consolidated
    metadata from *every* group (not just the root) restores the append (the cross-run path, T4)."""
    import numpy as np
    import pytest
    from ingest_v1_s1_rtc import _drop_consolidated_metadata

    store = str(tmp_path / "cube.zarr")
    root = zarr.open_group(store, mode="w", zarr_format=3)
    root.create_group("descending").create_group("r10m").create_array(
        "vv", shape=(1, 4, 4), dtype="float32", dimension_names=("time", "y", "x")
    )[0] = 1
    # eopf_geozarr consolidates the orbit group, not the root.
    zarr.consolidate_metadata(store, path="descending")
    import json

    assert "consolidated_metadata" in json.loads(
        (tmp_path / "cube.zarr" / "descending" / "zarr.json").read_text()
    )

    # Bug: with the orbit group consolidated, resize is not seen on re-navigation -> out of bounds.
    r = zarr.open_group(store, mode="r+", zarr_format=3)["descending"]["r10m"]
    r["vv"].resize((2, 4, 4))
    with pytest.raises(Exception):  # noqa: B017 -- zarr BoundsCheckError on the consolidated group
        r["vv"][1, :, :] = np.zeros((4, 4), dtype="float32")

    # Fix: a root-only strip would miss descending/; the recursive strip clears it everywhere.
    _drop_consolidated_metadata(store)
    r2 = zarr.open_group(store, mode="r+", zarr_format=3)["descending"]["r10m"]
    r2["vv"].resize((2, 4, 4))
    r2["vv"][1, :, :] = np.zeros((4, 4), dtype="float32")
    assert r2["vv"].shape == (2, 4, 4)


# ---------------------------------------------------------------------------
# T2 -- self-heal: backfill a per-level `time` missing on r20m/r60m before append
# (an inconsistent/legacy cube where r10m has `time` but a coarser level does not -> the eopf_geozarr
# per-level resize raises KeyError 'time'; the guard makes the append convergent).
# ---------------------------------------------------------------------------

_TIME_CF_ATTRS = {
    "units": "nanoseconds since 1970-01-01",
    "calendar": "proleptic_gregorian",
    "standard_name": "time",
    "_ARRAY_DIMENSIONS": ["time"],
}


def _build_multilevel_cube(
    store_path: str,
    times_ns: list[int],
    *,
    levels_with_time: tuple[str, ...] = ("r10m",),
    orbit: str = "ascending",
    level_lens: dict[str, int] | None = None,
) -> None:
    """A cube with r10m/r20m/r60m, each level carrying a (time, y, x) `vv` of length ``level_lens``
    (default = len(times_ns)), but a CF `time` coordinate only on ``levels_with_time``."""
    import numpy as np

    root = zarr.open_group(store_path, mode="w-", zarr_format=3)
    og = root.create_group(orbit)
    n = len(times_ns)
    for lvl in ("r10m", "r20m", "r60m"):
        g = og.create_group(lvl)
        ln = (level_lens or {}).get(lvl, n)
        vv = g.create_array(
            "vv", shape=(ln, 4, 4), dtype="float32", dimension_names=("time", "y", "x")
        )
        vv[...] = np.zeros((ln, 4, 4), dtype="float32")
        if lvl in levels_with_time:
            t = g.create_array(
                "time", shape=(n,), dtype="int64", chunks=(512,), dimension_names=("time",)
            )
            t[...] = np.asarray(times_ns, dtype="int64")
            t.attrs.update(_TIME_CF_ATTRS)


def test_ensure_level_time_coords_backfills_missing_levels(tmp_path) -> None:
    """r10m has `time`, r20m/r60m do not -> the guard backfills both with values + dtype + CF attrs
    identical to r10m/time, and the per-level resize the append does then succeeds."""
    from ingest_v1_s1_rtc import _ensure_level_time_coords

    t0, t1 = acq_time_ns("20230115t061234"), acq_time_ns("20230127t061230")
    store = str(tmp_path / "cube.zarr")
    _build_multilevel_cube(store, [t0, t1], levels_with_time=("r10m",))

    _ensure_level_time_coords(store, "ascending")

    og = zarr.open_group(store, mode="r", zarr_format=3)["ascending"]
    src = og["r10m"]["time"]
    for lvl in ("r10m", "r20m", "r60m"):
        t = og[lvl]["time"]
        assert list(t[...]) == [t0, t1]
        assert t.dtype == src.dtype
        assert list(t.metadata.dimension_names) == ["time"]
        assert dict(t.attrs) == dict(src.attrs)
    # the resize that crashed before (KeyError 'time' on r20m) now succeeds
    r20 = zarr.open_group(store, mode="r+", zarr_format=3)["ascending"]["r20m"]
    r20["time"].resize((3,))
    assert r20["time"].shape == (3,)


def test_ensure_level_time_coords_noop_when_all_present(tmp_path) -> None:
    """Every level already has `time` -> idempotent no-op (no raise, values untouched)."""
    from ingest_v1_s1_rtc import _ensure_level_time_coords

    store = str(tmp_path / "cube.zarr")
    _build_multilevel_cube(store, [1, 2, 3], levels_with_time=("r10m", "r20m", "r60m"))
    _ensure_level_time_coords(store, "ascending")
    og = zarr.open_group(store, mode="r", zarr_format=3)["ascending"]
    for lvl in ("r10m", "r20m", "r60m"):
        assert list(og[lvl]["time"][...]) == [1, 2, 3]


def test_ensure_level_time_coords_raises_on_length_mismatch(tmp_path) -> None:
    """A half-built cube (r20m/vv shorter than r10m/time) must fail loudly, not mis-heal with a
    wrong-length coordinate."""
    import pytest
    from ingest_v1_s1_rtc import _ensure_level_time_coords

    store = str(tmp_path / "cube.zarr")
    _build_multilevel_cube(store, [1, 2], levels_with_time=("r10m",), level_lens={"r20m": 1})
    with pytest.raises(ValueError, match="half-built"):
        _ensure_level_time_coords(store, "ascending")


def test_ensure_level_time_coords_raises_when_r10m_time_missing(tmp_path) -> None:
    """r10m holds slices but no `time` -> no backfill source -> raise (wipe required), not a silent
    invention of a coordinate."""
    import pytest
    from ingest_v1_s1_rtc import _ensure_level_time_coords

    store = str(tmp_path / "cube.zarr")
    _build_multilevel_cube(store, [1, 2], levels_with_time=())  # no level has `time`, incl. r10m
    with pytest.raises(ValueError, match="backfill source"):
        _ensure_level_time_coords(store, "ascending")


def test_ensure_level_time_coords_skips_conditions_group(tmp_path) -> None:
    """The `conditions` group (arrays are (y, x) only) is not a multiscale level and must never gain
    a `time` coordinate."""
    import numpy as np
    from ingest_v1_s1_rtc import _ensure_level_time_coords

    store = str(tmp_path / "cube.zarr")
    _build_multilevel_cube(store, [1, 2], levels_with_time=("r10m", "r20m", "r60m"))
    cond = zarr.open_group(store, mode="r+", zarr_format=3)["ascending"].create_group("conditions")
    cond.create_array("gamma_area_008", shape=(4, 4), dtype="float32", dimension_names=("y", "x"))[
        ...
    ] = np.ones((4, 4), dtype="float32")

    _ensure_level_time_coords(store, "ascending")

    cond_r = zarr.open_group(store, mode="r", zarr_format=3)["ascending"]["conditions"]
    assert "time" not in list(cond_r.array_keys())


# ---------------------------------------------------------------------------
# F1 -- multi-frame "daily" products whose time is masked (…txxxxxx…) are resolved upstream
# (data-model #184); the local _normalize_masked_stamps workaround (#237) has been removed.
# ---------------------------------------------------------------------------


def test_discover_resolves_masked_multiframe_stamp(tmp_path: Path) -> None:
    """Durable fix (data-model #184, pinned) resolves multi-frame masked stamps with no on-disk
    rename: discover_s1tiling_acquisitions parses a '…txxxxxx' product and derives acq_stamp from
    the GeoTIFF ACQUISITION_DATETIME tag. This is the safety net for removing _normalize_masked_stamps
    (#237) -- green on the pinned data-model, red on any pin predating #184."""
    import numpy as np
    import rasterio
    from eopf_geozarr.conversion.s1_ingest import discover_s1tiling_acquisitions
    from rasterio.transform import from_bounds

    d = tmp_path / "out"
    d.mkdir()
    transform = from_bounds(500000.0, 4997440.0, 502560.0, 5000000.0, 16, 16)
    tags = {
        "ACQUISITION_DATETIME": "2026:06:02T05:43:23Z",
        "ORBIT_NUMBER": "12345",
        "RELATIVE_ORBIT_NUMBER": "139",
        "FLYING_UNIT_CODE": "S1A",
    }
    for pol in ("vv", "vh"):
        base = f"s1a_32TLR_{pol}_DES_139_20260602txxxxxx_GammaNaughtRTC"
        for name in (f"{base}.tif", f"{base}_BorderMask.tif"):
            with rasterio.open(
                d / name,
                "w",
                driver="GTiff",
                height=16,
                width=16,
                count=1,
                dtype="float32",
                crs="EPSG:32632",
                transform=transform,
            ) as dst:
                dst.update_tags(**tags)
                dst.write(np.ones((16, 16), dtype="float32"), 1)

    acqs = discover_s1tiling_acquisitions(str(d))

    assert len(acqs) == 1
    # ACQUISITION_DATETIME "2026:06:02T05:43:23Z" -> resolved stamp (no rename)
    assert acqs[0]["acq_stamp"] == "20260602t054323"
    for k in ("vv", "vh", "vv_mask", "vh_mask"):
        assert k in acqs[0]


# ---------------------------------------------------------------------------
# T1/T2 -- concurrent transfer: the upload (via _put_files) and _get_tree issue
# ONE batched call, not a per-file put_file/get_file loop (capped at _S3_CONCURRENCY).
# (the upload side is asserted on _sync_tree below, the sole upload path.)
# ---------------------------------------------------------------------------


def test_get_tree_issues_single_concurrent_batch(tmp_path) -> None:
    """_get_tree fetches the existing cube via one batched fs.get, pre-creating local parents."""
    from unittest.mock import MagicMock

    fs = MagicMock()
    fs.find.return_value = ["bucket/src/zarr.json", "bucket/src/g/0.0"]
    local = tmp_path / "local"

    _get_tree(fs, "bucket/src", str(local))

    fs.get_file.assert_not_called()
    assert fs.get.call_count == 1
    args, kwargs = fs.get.call_args
    assert args[0] == ["bucket/src/zarr.json", "bucket/src/g/0.0"]
    assert kwargs["batch_size"] == _S3_CONCURRENCY
    assert (local / "g").is_dir()  # local parent created before the batched get


def test_get_tree_empty_src_is_noop(tmp_path) -> None:
    """An empty source (no keys) issues no transfer."""
    from unittest.mock import MagicMock

    fs = MagicMock()
    fs.find.return_value = []
    _get_tree(fs, "bucket/empty", str(tmp_path / "local"))
    fs.get.assert_not_called()


# ---------------------------------------------------------------------------
# T3 -- incremental upload: _sync_tree sends only new/changed objects, always
# re-pushes zarr.json, deletes vanished keys, and never rm's the whole cube.
# ---------------------------------------------------------------------------


def test_sync_tree_uploads_only_new_changed_and_metadata(tmp_path) -> None:
    """Append uploads only new + size-changed chunks, every zarr.json, and deletes vanished keys —
    no rm(recursive) of the live cube."""
    from unittest.mock import MagicMock

    store = tmp_path / "cube.zarr"
    (store / "g").mkdir(parents=True)
    (store / "zarr.json").write_text('{"meta":1}')  # metadata -> always re-upload
    (store / "g" / "zarr.json").write_text('{"m":2}')  # metadata -> always re-upload
    (store / "g" / "new.0").write_text("brand new shard")  # absent remotely -> upload
    (store / "g" / "same.0").write_text("unchanged")  # same size remote -> skip
    (store / "g" / "changed.0").write_text("now much bigger")  # size differs -> upload

    fs = MagicMock()
    fs.exists.return_value = True
    fs.find.return_value = {
        "bucket/c/zarr.json": {"size": 99},
        "bucket/c/g/zarr.json": {"size": 99},
        "bucket/c/g/same.0": {"size": len("unchanged")},
        "bucket/c/g/changed.0": {"size": 3},  # local is larger -> changed
        "bucket/c/g/gone.0": {"size": 5},  # not local -> delete
    }

    _sync_tree(fs, str(store), "bucket/c")

    # one batched, capped fs.put — not a serial put_file loop (T1 concurrency on the upload path)
    fs.put_file.assert_not_called()
    assert fs.put.call_count == 1
    assert fs.put.call_args.kwargs["batch_size"] == _S3_CONCURRENCY
    sent = set(fs.put.call_args[0][1])
    assert sent == {
        "bucket/c/zarr.json",
        "bucket/c/g/zarr.json",  # metadata always
        "bucket/c/g/new.0",
        "bucket/c/g/changed.0",  # new + size-changed
    }
    assert "bucket/c/g/same.0" not in sent  # unchanged chunk skipped
    fs.rm.assert_called_once_with(["bucket/c/g/gone.0"])  # vanished key deleted
    # never an rm(recursive) of the whole cube
    for c in fs.rm.call_args_list:
        assert c.kwargs.get("recursive") is not True


def test_sync_tree_fresh_cube_uploads_everything(tmp_path) -> None:
    """A first-ingest cube (dest absent) uploads all objects, lists nothing, deletes nothing."""
    from unittest.mock import MagicMock

    store = tmp_path / "cube.zarr"
    (store / "g").mkdir(parents=True)
    (store / "zarr.json").write_text("{}")
    (store / "g" / "0.0").write_text("x")

    fs = MagicMock()
    fs.exists.return_value = False

    _sync_tree(fs, str(store), "bucket/c")

    assert set(fs.put.call_args[0][1]) == {"bucket/c/zarr.json", "bucket/c/g/0.0"}
    fs.find.assert_not_called()
    fs.rm.assert_not_called()


def test_sync_tree_local_roundtrip_converges(tmp_path) -> None:
    """End-to-end on a real local fs: an incremental re-sync converges the 'remote' to the local
    store — metadata refreshed, grown chunk replaced, new chunk added, removed chunk deleted."""
    import fsspec

    fs = fsspec.filesystem("file")
    store = tmp_path / "local.zarr"
    remote = tmp_path / "remote.zarr"
    (store / "g").mkdir(parents=True)
    (store / "zarr.json").write_text('{"v":1}')
    (store / "g" / "0.0").write_text("aaa")
    (store / "g" / "1.0").write_text("bbb")

    _sync_tree(fs, str(store), str(remote))
    assert (remote / "zarr.json").read_text() == '{"v":1}'
    assert (remote / "g" / "0.0").read_text() == "aaa"
    assert (remote / "g" / "1.0").read_text() == "bbb"

    # mutate locally: metadata edit, chunk grows, new chunk, one chunk removed
    (store / "zarr.json").write_text('{"v":2}')
    (store / "g" / "0.0").write_text("aaaaaa")  # size change
    (store / "g" / "2.0").write_text("ccc")  # new
    (store / "g" / "1.0").unlink()  # removed

    _sync_tree(fs, str(store), str(remote))
    assert (remote / "zarr.json").read_text() == '{"v":2}'  # metadata refreshed
    assert (remote / "g" / "0.0").read_text() == "aaaaaa"  # grown chunk replaced
    assert (remote / "g" / "2.0").read_text() == "ccc"  # new chunk added
    assert not (remote / "g" / "1.0").exists()  # removed chunk deleted


def test_sync_tree_result_opens_as_zarr(tmp_path) -> None:
    """The incrementally-synced 'remote' is a valid zarr store the reader can open."""
    import fsspec

    fs = fsspec.filesystem("file")
    store = str(tmp_path / "s.zarr")
    _store_with_times(store, [0, 1])  # builds descending/r10m/time
    remote = str(tmp_path / "r.zarr")

    _sync_tree(fs, store, remote)

    g = zarr.open_group(remote, mode="r", zarr_format=3)["descending"]["r10m"]
    assert list(g["time"][...]) == [0, 1]
