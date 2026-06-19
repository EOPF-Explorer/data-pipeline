"""Unit tests for scripts/gen_aoi_tiles.py — region bbox → land/DEM MGRS tile list (T7 Phase 2).

Covers the pure selection core: a region bbox is sampled on a grid into MGRS 100 km tile ids, then
filtered to tiles that are both a real S2 granule (in the `eotile` S2 tiling grid) and actually
overlap DEM/land cells (a tile with no `Product10` cell in the `eotile` `DEM_Union.gpkg` is ocean /
out-of-coverage and is dropped). The gpkg reads + anon staging live in ensure_dem / read_s2_tile_ids
(integration); here the land set and the valid-S2 set are injected so the tests are hermetic and need
neither eotile nor network.
"""

import sys
from pathlib import Path

SCRIPT = Path(__file__).parent.parent.parent / "scripts" / "gen_aoi_tiles.py"


def _mod():
    sys.path.insert(0, str(SCRIPT.parent))
    import gen_aoi_tiles

    return gen_aoi_tiles


def _stem():
    import ensure_dem

    return ensure_dem.product10_stem


# --- mgrs_tiles_in_bbox (bbox → MGRS 100 km tile ids) ------------------------


def test_mgrs_tiles_in_bbox_small_bbox_single_tile():
    """A bbox well inside one 100 km square resolves to just that tile."""
    m = _mod()
    tiles = m.mgrs_tiles_in_bbox([0.95, 42.45, 1.05, 42.55])  # near 31TCH centre
    assert tiles == {"31TCH"}


def test_mgrs_tiles_in_bbox_spans_multiple_tiles():
    m = _mod()
    tiles = m.mgrs_tiles_in_bbox([0.3, 42.4, 1.6, 43.2])  # straddles the C column / G-H rows
    assert {"31TCH", "31TBH"} <= tiles
    assert all(len(t) == 5 for t in tiles)


def test_mgrs_tiles_in_bbox_is_a_set_no_duplicates():
    m = _mod()
    tiles = m.mgrs_tiles_in_bbox([0.0, 42.0, 2.0, 43.0], step=0.05)
    assert isinstance(tiles, set)


# --- tile_is_land (gpkg land filter) ----------------------------------------


def test_tile_is_land_true_when_a_covered_cell_is_in_gpkg():
    m = _mod()
    s = _stem()
    # 31TCH covers integer cells (42,0),(42,1),(43,0),(43,1); the gpkg knowing any one ⇒ land.
    assert m.tile_is_land("31TCH", {s(43, 1)}) is True


def test_tile_is_land_false_when_no_covered_cell_in_gpkg():
    m = _mod()
    s = _stem()
    # only an unrelated cell present ⇒ the tile has no DEM coverage (treat as ocean / out-of-area).
    assert m.tile_is_land("31TCH", {s(10, 10)}) is False


def test_tile_is_land_uses_tile_footprint_not_swath_margin():
    """Selection must test the tile's own footprint (margin 0), not the wide DEM swath margin —
    otherwise a coastal/ocean tile would be kept just because its swath reaches distant land."""
    m = _mod()
    s = _stem()
    # a cell ~3° away (inside the swath margin, outside the tile footprint) must NOT count as land.
    assert m.tile_is_land("31TCH", {s(45, 4)}) is False


# --- tiles_for_region (sorted, deduped, land-only) --------------------------


def test_tiles_for_region_is_sorted_deduped_and_land_only():
    m = _mod()
    s = _stem()
    bbox = [0.3, 42.4, 1.6, 43.2]
    gpkg = {s(42, 0), s(42, 1), s(43, 0), s(43, 1)}  # land around 31T B/C H
    valid = m.mgrs_tiles_in_bbox(bbox)  # S2 filter a no-op here: isolate the land assertion
    tiles = m.tiles_for_region(bbox, gpkg, valid)
    assert tiles == sorted(tiles)
    assert len(tiles) == len(set(tiles))
    assert "31TCH" in tiles
    assert all(m.tile_is_land(t, gpkg) for t in tiles)


def test_tiles_for_region_empty_when_gpkg_has_no_land():
    """An all-ocean region (no covered cell in the gpkg) yields an empty list, not a crash."""
    m = _mod()
    bbox = [0.3, 42.4, 1.6, 43.2]
    assert m.tiles_for_region(bbox, set(), m.mgrs_tiles_in_bbox(bbox)) == []


def test_tiles_for_region_drops_tile_absent_from_s2_grid():
    """A math-valid square that is land but NOT a defined S2 granule must be dropped (the 31TBH bug).

    31TBH straddles the UTM zone-30/31 boundary; its ground is served by 30TYN, so the S2 grid omits
    it. It IS land (the DEM filter keeps it), so only the S2-grid gate can remove it — otherwise
    s1tiling exits 73 on the non-existent tile and fails the whole run.
    """
    m = _mod()
    s = _stem()
    bbox = [0.3, 42.4, 1.6, 43.2]
    gpkg = {s(42, 0), s(42, 1), s(43, 0), s(43, 1)}  # land under both 31TBH and 31TCH
    assert "31TBH" in m.mgrs_tiles_in_bbox(bbox)  # the sampler still produces it
    assert m.tile_is_land("31TBH", gpkg)  # ... and it passes the land filter
    valid = m.mgrs_tiles_in_bbox(bbox) - {"31TBH"}  # but the S2 grid omits it
    tiles = m.tiles_for_region(bbox, gpkg, valid)
    assert "31TBH" not in tiles
    assert "31TCH" in tiles


def test_tiles_for_region_drops_excluded_tiles():
    """The ocean denylist removes a tile that otherwise passes both the S2 and land filters — the
    case the coarse 1° DEM filter can't catch (an all-sea footprint inside a part-land 1° cell)."""
    m = _mod()
    s = _stem()
    bbox = [0.3, 42.4, 1.6, 43.2]
    gpkg = {s(42, 0), s(42, 1), s(43, 0), s(43, 1)}
    valid = m.mgrs_tiles_in_bbox(bbox)
    assert "31TCH" in m.tiles_for_region(bbox, gpkg, valid)
    assert "31TCH" not in m.tiles_for_region(bbox, gpkg, valid, exclude=frozenset({"31TCH"}))


def test_exclude_keys_are_known_regions():
    """Every denylist key must be a defined region, else its tiles silently never apply."""
    m = _mod()
    assert set(m.EXCLUDE) <= set(m.REGIONS)


def test_tiles_for_region_is_deterministic():
    m = _mod()
    s = _stem()
    gpkg = {s(42, 1), s(43, 1)}
    bbox = [0.3, 42.4, 1.6, 43.2]
    valid = m.mgrs_tiles_in_bbox(bbox)
    assert m.tiles_for_region(bbox, gpkg, valid) == m.tiles_for_region(bbox, gpkg, valid)


# --- REGIONS (committed bboxes) ---------------------------------------------


def test_regions_defines_france_with_valid_bbox():
    m = _mod()
    assert set(m.REGIONS) == {"france"}
    lon0, lat0, lon1, lat1 = m.REGIONS["france"]
    assert lon0 < lon1 and lat0 < lat1
    assert lon0 < -5.0  # west bound reaches Brittany (Ushant ~-5.1°W)
    assert lon1 == 9.2  # east bound stops at the longitude of Stuttgart (~9.18°E)
    assert lat1 == 49.0  # north bound ~49°N
