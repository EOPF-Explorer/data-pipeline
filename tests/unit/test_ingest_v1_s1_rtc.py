"""Unit tests for ingest_v1_s1_rtc.py -- ingest_all."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import call, patch

import zarr

scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

from ingest_v1_s1_rtc import (  # noqa: E402
    _patch_cf_grid_mapping,
    _put_tree,
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


def test_put_tree_lands_at_dest_without_nesting(tmp_path) -> None:
    """_put_tree maps each file to dest/<relpath> — the store lands AT dest, not
    nested under dest/<store-basename>/ (the fsspec put recursive footgun)."""
    import fsspec

    store = tmp_path / "src" / "s1-grd-rtc-31TCH.zarr"
    (store / "descending" / "r10m").mkdir(parents=True)
    (store / "zarr.json").write_text("{}")
    (store / "descending" / "r10m" / "c" / "0.0").parent.mkdir(parents=True)
    (store / "descending" / "r10m" / "c" / "0.0").write_text("chunk")

    dest_root = tmp_path / "dst" / "s1-grd-rtc-31TCH.zarr"
    _put_tree(fsspec.filesystem("file"), str(store), str(dest_root))

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
    """A consolidated v3 store can't be grown via resize+write on reopen; dropping the root
    consolidated_metadata restores it (the cross-run append path, T4)."""
    import numpy as np
    from ingest_v1_s1_rtc import _drop_consolidated_metadata

    store = str(tmp_path / "cube.zarr")
    root = zarr.open_group(store, mode="w", zarr_format=3)
    g = root.create_group("descending").create_group("r10m")
    g.create_array("vv", shape=(1, 4, 4), dtype="float32", dimension_names=("time", "y", "x"))[
        0
    ] = 1
    zarr.consolidate_metadata(store)

    # Without the fix, resize-then-write on the consolidated store is out of bounds.
    import pytest

    r = zarr.open_group(store, mode="r+", zarr_format=3)["descending"]["r10m"]
    r["vv"].resize((2, 4, 4))
    with pytest.raises(Exception):  # noqa: B017 -- zarr BoundsCheckError on consolidated store
        r["vv"][1, :, :] = np.zeros((4, 4), dtype="float32")

    # After dropping consolidated metadata, the same append succeeds.
    _drop_consolidated_metadata(store)
    r2 = zarr.open_group(store, mode="r+", zarr_format=3)["descending"]["r10m"]
    r2["vv"].resize((2, 4, 4))
    r2["vv"][1, :, :] = np.zeros((4, 4), dtype="float32")
    assert r2["vv"].shape == (2, 4, 4)
