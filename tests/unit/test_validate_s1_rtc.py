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


def test_validate_schema_is_metadata_only():
    """Schema checks (dtype/dims/crs) don't include the finite/dB data checks."""
    m = _mod()
    labels = [c.label for c in m.validate_schema(_r10m())]
    assert any("dtype/dims" in label for label in labels)
    assert not any("finite" in label or "dB" in label for label in labels)


def test_validate_data_catches_bad_slice():
    """validate_data on a single corrupt acquisition slice FAILs (the per-time gate path)."""
    m = _mod()
    bad = _r10m(vv=np.full((1, 4, 4), np.nan, dtype="float32"))
    assert m.overall(m.validate_data(bad)) == m.Level.FAIL


def test_time_index_picks_nearest():
    """time_index resolves the nearest acquisition position from the native time coord."""
    m = _mod()
    times = np.array(["2026-06-05T06:09:07", "2026-06-07T05:52:48"], dtype="datetime64[ns]")
    native = xr.Dataset(coords={"time": ("time", times)})
    assert m.time_index(native, "2026-06-07") == 1
    assert m.time_index(native, "2026-06-05T06:09:07") == 0


# --- Structural drift classification -----------------------------------------
#
# The error dicts below were captured from real pydantic runs against real stores: one built by the
# currently pinned writer (data-model 9ede8c3) and one with `time` stripped from the overview levels
# (the shape of a cube written before data-model #192, i.e. the published archive).

# Pre-#216 model rejecting the writer's own coordinate arrays. Before `time` joined the tolerated
# names, these five made every current-generation store report FAIL.
_EXTRA_FORBIDDEN = [
    {
        "type": "extra_forbidden",
        "loc": ("members", "descending", "members", lvl, "members", "time"),
        "msg": "Extra inputs are not permitted",
    }
    for lvl in ("r20m", "r60m", "r120m", "r360m", "r720m")
]

# v0.11.0 model on a pre-#192 cube: no `time` at the overview levels.
_MISSING_TIME = [
    {
        "type": "value_error",
        "loc": ("members", "descending", "members", lvl),
        "msg": (
            "Value error, Overview resolution dataset must contain coordinate arrays ['time'] "
            "(expected ['time', 'x', 'y', 'spatial_ref']; see data-model #192)"
        ),
    }
    for lvl in ("r20m", "r60m", "r120m", "r360m", "r720m")
]

# Pre-#216 writers wrote only `spatial_ref` into a conditions group, never `x`/`y`.
_MISSING_CONDITION_COORDS = [
    {
        "type": "value_error",
        "loc": ("members", "descending", "members", "conditions"),
        "msg": (
            "Value error, Conditions group must contain coordinate arrays ['x', 'y'] "
            "(expected ['x', 'y', 'spatial_ref']; see data-model #192)"
        ),
    }
]

# Same wording, NOT drift: a native level without coordinates has no georeferencing at all —
# rioxarray opens it with an identity transform and TiTiler renders it in the wrong place.
_NO_GEOREFERENCING = [
    {
        "type": "value_error",
        "loc": ("members", "descending", "members", "r10m"),
        "msg": (
            "Value error, Native resolution dataset must contain coordinate arrays "
            "['x', 'y', 'spatial_ref'] (expected ['time', 'x', 'y', 'spatial_ref']; "
            "see data-model #192)"
        ),
    }
]

_REAL_DEFECT = [
    {
        "type": "value_error",
        "loc": ("members", "descending", "members", "r10m"),
        "msg": "Value error, Native resolution dataset must contain 'vv' array",
    }
]


def test_unstamped_store_drift_warns():
    """An unstamped (pre-#216) cube reporting only known drift is WARN, not FAIL, under either pin."""
    m = _mod()
    for errs in (_EXTRA_FORBIDDEN, _MISSING_TIME, _MISSING_CONDITION_COORDS):
        c = m.classify_structural_errors(errs, stamped=False)
        assert c.level == m.Level.WARN
        assert "known coord drift" in c.detail


def test_stamped_store_gets_no_tolerance():
    """`eopf:writer_schema` means the writer emits all of this — the same errors are then real."""
    m = _mod()
    for errs in (_EXTRA_FORBIDDEN, _MISSING_TIME, _MISSING_CONDITION_COORDS):
        assert m.classify_structural_errors(errs, stamped=True).level == m.Level.FAIL


def test_missing_native_coordinates_is_never_drift():
    """The library uses one wording for four group kinds; only two of them are archive drift.

    A substring match on "must contain coordinate arrays" also swallows the native-level failure —
    a store with no georeferencing at all — and passes it as WARN on every unstamped cube, which is
    the entire published archive.
    """
    m = _mod()
    assert m.classify_structural_errors(_NO_GEOREFERENCING, stamped=False).level == m.Level.FAIL


def test_stamp_read_from_root_attrs():
    """The stamp is an int at the store root; anything else (absent, a string) reads as unstamped."""
    m = _mod()

    class _Root:
        def __init__(self, attrs):
            self.attrs = attrs

    assert m._is_stamped(_Root({"eopf:writer_schema": 2}))
    assert not m._is_stamped(_Root({}))
    assert not m._is_stamped(_Root({"eopf:writer_schema": "2"}))


def test_real_defect_fails_even_unstamped():
    """Tolerating archive drift must not blind the gate to an actually broken store."""
    m = _mod()
    c = m.classify_structural_errors(_MISSING_TIME + _REAL_DEFECT, stamped=False)
    assert c.level == m.Level.FAIL
    assert "1 error(s)" in c.detail
