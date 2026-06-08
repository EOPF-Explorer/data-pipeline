"""Unit tests for scripts/validate_s1_rtc.py — the S1 GRD RTC quality-gate checks.

Mirrors the validate_s1_grd_rtc notebook's checks as pure functions (testable with tiny
synthetic inputs); the structural S1RtcRoot check + store I/O live in main() (integration).
"""

import sys
from pathlib import Path

import numpy as np
import xarray as xr

SCRIPT = Path(__file__).parent.parent.parent / "scripts" / "validate_s1_rtc.py"


def _mod():
    sys.path.insert(0, str(SCRIPT.parent))
    import validate_s1_rtc

    return validate_s1_rtc


def _r10m(vv=None, vh=None, *, vv_dtype="float32", with_crs=True, ntime=1, n=4):
    """Build a minimal r10m-level dataset (vv/vh/border_mask, dims time,y,x)."""
    shape = (ntime, n, n)
    vv = np.ones(shape, dtype=vv_dtype) if vv is None else vv.astype(vv_dtype)
    vh = np.ones(shape, dtype="float32") if vh is None else vh
    ds = xr.Dataset(
        {
            "vv": (("time", "y", "x"), vv),
            "vh": (("time", "y", "x"), vh.astype("float32")),
            "border_mask": (("time", "y", "x"), np.ones(shape, dtype="uint8")),
        },
        coords={"time": np.arange(ntime), "y": np.arange(n), "x": np.arange(n)},
    )
    if with_crs:
        import rioxarray  # noqa: F401

        ds = ds.rio.write_crs("EPSG:32631")
    return ds


# --- Level / aggregation -----------------------------------------------------


def test_level_ordering_and_exit_codes():
    m = _mod()
    assert int(m.Level.PASS) == 0
    assert int(m.Level.WARN) == 1
    assert int(m.Level.FAIL) == 2


def test_overall_is_worst_level():
    m = _mod()
    checks = [
        m.Check(m.Level.PASS, "a", ""),
        m.Check(m.Level.WARN, "b", ""),
        m.Check(m.Level.PASS, "c", ""),
    ]
    assert m.overall(checks) == m.Level.WARN
    checks.append(m.Check(m.Level.FAIL, "d", ""))
    assert m.overall(checks) == m.Level.FAIL


def test_overall_empty_is_pass():
    m = _mod()
    assert m.overall([]) == m.Level.PASS


# --- check_finite ------------------------------------------------------------


def test_check_finite_all_finite_passes():
    m = _mod()
    assert m.check_finite("vv", np.ones((4, 4), dtype="float32")).level == m.Level.PASS


def test_check_finite_mostly_nan_fails():
    m = _mod()
    arr = np.full((10, 10), np.nan, dtype="float32")
    arr[0, :] = 1.0  # 10% finite
    assert m.check_finite("vv", arr).level == m.Level.FAIL


# --- check_dtype_dims --------------------------------------------------------


def test_check_dtype_dims_correct_passes():
    m = _mod()
    ds = _r10m()
    assert m.check_dtype_dims(ds, "vv", "float32").level == m.Level.PASS


def test_check_dtype_dims_wrong_dtype_fails():
    m = _mod()
    ds = _r10m(vv_dtype="float64")
    assert m.check_dtype_dims(ds, "vv", "float32").level == m.Level.FAIL


def test_check_dtype_dims_missing_var_fails():
    m = _mod()
    ds = _r10m().drop_vars("vv")
    assert m.check_dtype_dims(ds, "vv", "float32").level == m.Level.FAIL


# --- check_crs ---------------------------------------------------------------


def test_check_crs_present_passes():
    m = _mod()
    assert m.check_crs(_r10m(with_crs=True)).level == m.Level.PASS


def test_check_crs_absent_warns():
    m = _mod()
    assert m.check_crs(_r10m(with_crs=False)).level == m.Level.WARN


# --- check_db_range ----------------------------------------------------------


def test_check_db_range_plausible_passes():
    m = _mod()
    # gamma0 ~ 0.05 linear -> ~ -13 dB, well within bounds
    arr = np.full((20, 20), 0.05, dtype="float32")
    assert m.check_db_range("vv", arr).level == m.Level.PASS


def test_check_db_range_absurd_warns_or_fails():
    m = _mod()
    arr = np.full((20, 20), 1e6, dtype="float32")  # ~ +60 dB, absurd
    assert m.check_db_range("vv", arr).level >= m.Level.WARN


# --- validate_dataset (orchestration over a level) ---------------------------


def test_validate_dataset_good_all_pass():
    m = _mod()
    checks = m.validate_dataset(_r10m(ntime=1, n=8))
    assert m.overall(checks) == m.Level.PASS, [(c.label, c.detail) for c in checks]


def test_validate_dataset_corrupt_fails():
    m = _mod()
    bad = _r10m(vv=np.full((1, 4, 4), np.nan, dtype="float32"), with_crs=False)  # all-NaN vv
    assert m.overall(m.validate_dataset(bad)) == m.Level.FAIL
