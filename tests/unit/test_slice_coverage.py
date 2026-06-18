"""Unit tests for scripts/register_v1_s1_rtc.py::slice_coverages — per-slice tile coverage from the cube.

Reads ``border_mask`` at the cheap ``r720m`` level (valid = non-zero; ``fill_value=0`` = border) for each
orbit group and pairs it with that level's ``time``. Mirrors ``build_s1_rtc_stac_item``'s open pattern
(``zarr.open_*`` + raw int64-ns ``time``). Exercised against a tiny synthetic zarr-v3 cube — this also
**confirms the border_mask polarity** (T0): a known-full mask must read as coverage 1.0.
"""

import datetime as dt
import sys
from pathlib import Path

import numpy as np
import zarr

SCRIPT = Path(__file__).parent.parent.parent / "scripts" / "register_v1_s1_rtc.py"


def _mod():
    sys.path.insert(0, str(SCRIPT.parent))
    import register_v1_s1_rtc

    return register_v1_s1_rtc


def _ns(day: int) -> int:
    return int(dt.datetime(2026, 6, day, 6, 0, tzinfo=dt.UTC).timestamp() * 1e9)


def _write_orbit(root, orbit: str, masks: list[np.ndarray], days: list[int]) -> None:
    """Write {orbit}/r720m with a border_mask (time,y,x) + time (int64 ns), like the real cube."""
    lvl = root.create_group(orbit).create_group("r720m")
    n = len(masks)
    y, x = masks[0].shape
    bm = lvl.create_array("border_mask", shape=(n, y, x), dtype="uint8", fill_value=0)
    bm[:] = np.stack(masks).astype("uint8")
    t = lvl.create_array("time", shape=(n,), dtype="int64")
    t[:] = np.array([_ns(d) for d in days], dtype="int64")


def _make_cube(tmp_path) -> str:
    store = str(tmp_path / "cube.zarr")
    root = zarr.open_group(store, mode="w", zarr_format=3)
    full = np.ones((4, 4))  # coverage 1.0
    half = np.zeros((4, 4))
    half[:2, :] = 1  # coverage 0.5
    quarter = np.zeros((4, 4))
    quarter[0, :] = 1  # coverage 0.25
    _write_orbit(root, "ascending", [full, half], [4, 6])
    _write_orbit(root, "descending", [quarter], [5])
    return store


def test_slice_coverages_reads_both_orbits_with_correct_fractions(tmp_path):
    m = _mod()
    store = _make_cube(tmp_path)
    by_key = {(s.orbit, s.dt.day): s.coverage for s in m.slice_coverages(store)}
    assert by_key == {
        ("ascending", 4): 1.0,  # full mask -> polarity check (1 = valid)
        ("ascending", 6): 0.5,
        ("descending", 5): 0.25,
    }


def test_slice_coverages_times_are_utc_datetimes(tmp_path):
    m = _mod()
    store = _make_cube(tmp_path)
    s = next(s for s in m.slice_coverages(store) if s.orbit == "descending")
    assert s.dt == dt.datetime(2026, 6, 5, 6, 0, tzinfo=dt.UTC)


def test_slice_coverages_skips_missing_orbit(tmp_path):
    m = _mod()
    store = str(tmp_path / "asc_only.zarr")
    root = zarr.open_group(store, mode="w", zarr_format=3)
    _write_orbit(root, "ascending", [np.ones((4, 4))], [7])
    orbits = {s.orbit for s in m.slice_coverages(store)}
    assert orbits == {"ascending"}
