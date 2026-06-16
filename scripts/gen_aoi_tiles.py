"""Generate the land/DEM MGRS tile list for a region AOI (T7 Phase 2).

Scaling the S1 RTC pipeline to a mountain arc (Pyrenees, Alps) needs a deterministic, reviewable
list of the MGRS 100 km tiles to process. This takes a region bbox, samples it on a grid into the
MGRS tiles it overlaps, and keeps only the tiles that actually have DEM/land coverage — a tile whose
footprint touches no `Product10` cell in the `eotile` `DEM_Union.gpkg` is ocean / out-of-coverage and
is dropped (no S1 land data to process). Output is sorted + deduped so the committed list is stable.

The selection is intentionally pure: the gpkg land set is read once (reusing ensure_dem's stdlib
sqlite reader) and the rest is integer-cell geometry, so it's testable without eotile or network.
A region bbox over-includes some non-mountain land (e.g. the Ebro/Po basins); that's accepted for
v1 — those are valid land tiles. A precise mountain polygon is a later refinement.

Usage:
    uv run python scripts/gen_aoi_tiles.py --region pyrenees \
      --gpkg $WORKDIR/DEM/dem_db/DEM_Union.gpkg --out aoi/pyrenees.txt
"""

from __future__ import annotations

import argparse
from pathlib import Path

import mgrs

# ensure_dem / watch_cdse_and_process are sibling scripts (not a package); import them inside the
# functions that need them — as ensure_dem itself does — so this module imports cleanly regardless
# of whether scripts/ is on sys.path at import time (pytest's full-suite collection isn't).

# Region AOIs as committed WGS84 bboxes [lon_min, lat_min, lon_max, lat_max]. Bbox per arc (v1):
# simplest + deterministic, over-includes some basin/foothill land (accepted — valid land tiles).
REGIONS: dict[str, list[float]] = {
    # Atlantic (~-1.8°W) to Mediterranean (~3.2°E), the full Pyrenean chain + immediate foothills.
    "pyrenees": [-1.8, 42.3, 3.2, 43.3],
    # The Alpine arc from the French Alps (~5°E) to the eastern Austrian Alps (~16°E).
    "alps": [5.0, 43.5, 16.0, 48.0],
}


def mgrs_tiles_in_bbox(bbox: list[float], *, step: float = 0.1) -> set[str]:
    """The MGRS 100 km tile ids (5-char, e.g. ``31TCH``) overlapping ``bbox``.

    Samples the bbox on a ``step``° grid and reverse-geocodes each point to its MGRS tile. ``step``
    must stay well under a tile's ~0.9° span so no overlapped tile is skipped; 0.1° (~11 km) is safe.
    """
    lon0, lat0, lon1, lat1 = bbox
    m = mgrs.MGRS()
    tiles: set[str] = set()
    lat = lat0
    while lat <= lat1:
        lon = lon0
        while lon <= lon1:
            tiles.add(m.toMGRS(lat, lon, MGRSPrecision=0))
            lon += step
        lat += step
    return tiles


def tile_is_land(tile_id: str, gpkg_products: set[str]) -> bool:
    """True iff the tile's own footprint overlaps a DEM/land cell known to the gpkg.

    Uses the tile footprint (no swath margin) so selection isn't fooled by a swath reaching distant
    land — that wide margin is for DEM *provisioning*, not for deciding a tile is worth processing.
    """
    from ensure_dem import product10_stem, tiles_for_bbox
    from watch_cdse_and_process import tile_bbox

    cells = tiles_for_bbox(tile_bbox(tile_id), margin_lon=0.0, margin_lat=0.0)
    return any(product10_stem(lat, lon) in gpkg_products for lat, lon in cells)


def tiles_for_region(bbox: list[float], gpkg_products: set[str], *, step: float = 0.1) -> list[str]:
    """Sorted, deduped MGRS tile ids overlapping ``bbox`` that have DEM/land coverage."""
    return sorted(t for t in mgrs_tiles_in_bbox(bbox, step=step) if tile_is_land(t, gpkg_products))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--region", required=True, choices=sorted(REGIONS), help="named region AOI")
    ap.add_argument("--gpkg", required=True, type=Path, help="dem_db/DEM_Union.gpkg (eotile index)")
    ap.add_argument("--step", type=float, default=0.1, help="sampling grid step in degrees")
    ap.add_argument("--out", type=Path, help="write the tile list here (default: stdout)")
    args = ap.parse_args()

    from ensure_dem import ensure_gpkg, read_gpkg_product10

    ensure_gpkg(args.gpkg)
    products = read_gpkg_product10(args.gpkg)
    tiles = tiles_for_region(REGIONS[args.region], products, step=args.step)

    text = "\n".join(tiles) + "\n" if tiles else ""
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text)
        print(f"{args.region}: {len(tiles)} tile(s) → {args.out}")
    else:
        print(text, end="")


if __name__ == "__main__":
    main()
