"""Unit tests for scripts/ensure_dem.py — Copernicus GLO-30 DEM auto-fetch (plan T3).

Covers the pure derivation core: tile → swath bbox → integer GLO-30 cells → `Product10` stems /
public-bucket COG keys, plus the idempotent + ocean-skip fetch list (candidates ∩ gpkg − present).
The anon-S3 download + COG→Product10 rename + eotile gpkg copy live in main() (integration; cluster).

Naming + the 31TCH expectations below are ground-truthed against the real `eotile` `DEM_Union.gpkg`:
a lon±4°/lat±1.5° swath margin around 31TCH yields 50 integer cells, of which 41 are real land
tiles (incl. N44W001/N44W002, observed needed in phase-5) and 9 are absent ocean cells.
"""

import sqlite3
import sys
from pathlib import Path

SCRIPT = Path(__file__).parent.parent.parent / "scripts" / "ensure_dem.py"


def _mod():
    sys.path.insert(0, str(SCRIPT.parent))
    import ensure_dem

    return ensure_dem


# --- product10_stem / cog_key (naming) --------------------------------------


def test_product10_stem_north_east():
    m = _mod()
    assert m.product10_stem(42, 1) == "Copernicus_DSM_10_N42_00_E001_00"


def test_product10_stem_north_west_zero_pads():
    m = _mod()
    assert m.product10_stem(44, -1) == "Copernicus_DSM_10_N44_00_W001_00"
    assert m.product10_stem(9, 0) == "Copernicus_DSM_10_N09_00_E000_00"


def test_product10_stem_southern_hemisphere():
    m = _mod()
    assert m.product10_stem(-34, -59) == "Copernicus_DSM_10_S34_00_W059_00"


def test_cog_key_matches_public_bucket_layout():
    """Public `copernicus-dem-30m` stores each tile as <dir>/<dir>.tif with the COG/_DEM variant."""
    m = _mod()
    key = m.cog_key(44, -1)
    assert key == (
        "Copernicus_DSM_COG_10_N44_00_W001_00_DEM/Copernicus_DSM_COG_10_N44_00_W001_00_DEM.tif"
    )


# --- tiles_for_bbox (swath margin → integer cells) ---------------------------


def test_tiles_for_bbox_covers_margin_and_observed_needed_cells():
    m = _mod()
    bbox = [0.533, 42.427, 1.784, 43.346]  # 31TCH
    cells = m.tiles_for_bbox(bbox)  # default margin lon±4, lat±1.5
    assert len(cells) == 50  # lon -4..5 (10) × lat 40..44 (5)
    assert (44, -1) in cells and (44, -2) in cells  # phase-5: swath needed these
    assert (40, -4) in cells and (44, 5) in cells  # corners
    assert cells == sorted(cells)  # deterministic order


def test_tiles_for_bbox_margin_is_configurable():
    m = _mod()
    cells = m.tiles_for_bbox([0.5, 42.4, 0.6, 42.6], margin_lon=0.0, margin_lat=0.0)
    assert cells == [(42, 0)]  # single cell, no margin


# --- read_gpkg_product10 (sqlite, no GDAL) -----------------------------------


def test_read_gpkg_product10_reads_the_product10_column(tmp_path):
    m = _mod()
    gpkg = tmp_path / "DEM_Union.gpkg"
    con = sqlite3.connect(gpkg)
    con.execute("CREATE TABLE dem (id INTEGER, Product10 TEXT)")
    con.executemany(
        "INSERT INTO dem VALUES (?, ?)",
        [(1, "Copernicus_DSM_10_N42_00_E001_00"), (2, "Copernicus_DSM_10_N44_00_W001_00")],
    )
    con.commit()
    con.close()
    prods = m.read_gpkg_product10(gpkg)
    assert prods == {
        "Copernicus_DSM_10_N42_00_E001_00",
        "Copernicus_DSM_10_N44_00_W001_00",
    }


# --- tiles_to_fetch (idempotent + ocean-skip orchestration) ------------------


def test_tiles_to_fetch_skips_ocean_and_already_present():
    """Fetch list = (swath cells ∩ gpkg land tiles) − tiles already on disk."""
    m = _mod()
    s = m.product10_stem
    gpkg_products = {s(42, 1), s(43, 1), s(44, -1)}  # land tiles known to the gpkg
    present = {s(42, 1)}  # already downloaded
    fetch = m.tiles_to_fetch("31TCH", gpkg_products, present)
    # (42,1) present → skipped; everything not in gpkg (ocean / nonexistent) → skipped
    assert fetch == [(43, 1), (44, -1)]


def test_tiles_to_fetch_idempotent_when_all_present():
    m = _mod()
    s = m.product10_stem
    gpkg_products = {s(42, 1), s(43, 1)}
    present = {s(42, 1), s(43, 1)}
    assert m.tiles_to_fetch("31TCH", gpkg_products, present) == []


def test_tiles_to_fetch_rejects_malformed_tile():
    m = _mod()
    import pytest

    with pytest.raises(ValueError):
        m.tiles_to_fetch("not-a-tile", set(), set())
