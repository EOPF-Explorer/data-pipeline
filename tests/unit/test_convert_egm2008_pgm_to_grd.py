"""Unit tests for scripts/convert_egm2008_pgm_to_grd.py (issue #306 geoid longitude fix).

The bug: the converter emitted a 0..360E geoid grid, so OTB Superimpose returned nodata over every
negative longitude (all of western Europe) -> dead DEM+GEOID -> the gamma0 near-range wedge. The fix
emits a -180..180E grid. These tests guard the longitude span and prove the western hemisphere maps to
the correct source column (a -L W node reads the 360-L E source node) — the exact mechanism the 0..360
grid got wrong.

No real PGM is used (the 1-arcmin source is ~466 MB). A synthetic full-width (21601-col), few-row PGM
whose pixel value encodes its own column index lets us assert the western-hemisphere remapping exactly:
since height(col) is monotonic, the geoid value at an output node uniquely identifies the source column
it was sampled from.
"""

from __future__ import annotations

import struct
import sys
from pathlib import Path

import numpy as np
import pytest

scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

import convert_egm2008_pgm_to_grd as c  # noqa: E402

PGM_WIDTH = (
    21601  # 1-arcmin globe incl. the 0==360 wrap column; the converter assumes this resolution
)
SCALE = 0.001
OFFSET = 29.0  # height(col) = col*SCALE + OFFSET -> unique per source column, ~50 m at 356.25E


def _write_synthetic_pgm(path: Path, *, height: int) -> None:
    """A GeographicLib-style PGM spanning 0..360E whose pixel value == its column index.

    Latitude is irrelevant to the longitude-wrap bug, so the grid is only `height` rows tall and
    constant down each column.
    """
    cols = np.arange(PGM_WIDTH, dtype=np.uint16)  # 0..21600 fits in uint16
    pixels = np.broadcast_to(cols, (height, PGM_WIDTH))
    header = (
        b"P5\n"
        b"# Description synthetic EGM-like geoid for tests\n"
        b"# Origin 90N 0E\n"
        + f"# Scale {SCALE}\n".encode("ascii")
        + f"# Offset {OFFSET}\n".encode("ascii")
        + f"{PGM_WIDTH} {height}\n".encode("ascii")
        + b"65535\n"
    )
    with open(path, "wb") as f:
        f.write(header)
        f.write(pixels.astype(">u2").tobytes())  # big-endian, matches read_pgm_data


def _read_grd(path: Path):
    with open(path, "rb") as f:
        lat_min, lat_max, lon_min, lon_max, dlat, dlon = struct.unpack(">6f", f.read(24))
    n_lat = round((lat_max - lat_min) / dlat) + 1
    n_lon = round((lon_max - lon_min) / dlon) + 1
    grid = np.memmap(path, dtype=">f4", mode="r", offset=24).reshape(n_lat, n_lon)
    return (lat_min, lat_max, lon_min, lon_max, dlat, dlon), grid


@pytest.fixture
def converted(tmp_path: Path):
    pgm = tmp_path / "egm.pgm"
    out = tmp_path / "egm.grd"
    _write_synthetic_pgm(pgm, height=16)
    c.convert(pgm, out, step=15)
    return _read_grd(out)


def test_output_spans_minus180_to_180(converted):
    """The fix: the grid must span -180..180E (the 0..360 grid was the #306 bug)."""
    (_, _, lon_min, lon_max, _, _), _ = converted
    assert lon_min == pytest.approx(-180.0, abs=1e-3)
    assert lon_max == pytest.approx(180.0, abs=1e-3)


def test_western_hemisphere_is_finite_and_realistic(converted):
    """Western-hemisphere node (-3.75E) is finite and a plausible geoid height — not nodata.

    On the 0..360 grid this longitude was off the grid (nodata in OTB Superimpose); here it must read
    the matching 356.25E source node (~50 m).
    """
    (_, _, lon_min, _, _, dlon), grid = converted
    col = round((-3.75 - lon_min) / dlon)
    assert col >= 0  # a 0..360 grid (lon_min=0) would put this western node off the grid
    val = float(grid[0, col])
    assert np.isfinite(val)
    assert 48.0 < val < 51.0  # 356.25E source: 21375*0.001 + 29 = 50.375


def test_negative_longitude_reads_the_wrapped_source_node(converted):
    """-L W must sample the 360-L E source column (the exact remap the 0..360 grid got wrong)."""
    (_, _, lon_min, _, _, dlon), grid = converted
    # -3.75E -> source col 356.25*60 = 21375 -> height 21375*SCALE + OFFSET
    west = float(grid[0, round((-3.75 - lon_min) / dlon)])
    assert west == pytest.approx(21375 * SCALE + OFFSET, abs=1e-3)
    # +3.75E (eastern, unaffected by the bug) -> source col 3.75*60 = 225
    east = float(grid[0, round((3.75 - lon_min) / dlon)])
    assert east == pytest.approx(225 * SCALE + OFFSET, abs=1e-3)
