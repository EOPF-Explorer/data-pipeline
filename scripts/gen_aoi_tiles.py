"""Generate the land/DEM MGRS tile list for a region AOI (T7 Phase 2).

Scaling the S1 RTC pipeline to a region (western Europe — France + Alps) needs a deterministic, reviewable
list of the MGRS 100 km tiles to process. This takes a region bbox, samples it on a grid into the
MGRS tiles it overlaps, then keeps a tile only if it is BOTH (a) a real Sentinel-2 granule in the
authoritative `eotile` S2 tiling grid AND (b) actually has DEM/land coverage. A tile whose footprint
touches no `Product10` cell in the `eotile` `DEM_Union.gpkg` is ocean / out-of-coverage and is dropped
(no S1 land data to process). Output is sorted + deduped so the committed list is stable.

The S2-grid gate matters because `mgrs.toMGRS` is pure coordinate math: it labels every sampled point
with a 100 km square, but not every math-valid square is a defined S2 granule. Squares straddling a UTM
zone boundary are served by the neighbouring zone's tile (e.g. 31TBH's ground is covered by 30TYN), so
the S2 grid omits the redundant one. Such a tile is still land — the DEM filter alone keeps it — but
s1tiling has no granule for it and exits 73, failing the whole cron run. Validating against the S2 grid
(the same ESA grid s1tiling's bundled MGRS.gpkg is derived from) is the only reliable filter for this.

The selection is intentionally pure: the gpkg land set + the S2 tile-id set are read once (stdlib
sqlite) and injected, so the core is testable without eotile or network. A region bbox over-includes
some neighbouring land within the box (e.g. S England, Belgium/Luxembourg, W Germany/Switzerland,
Austria, NW Italy); that's accepted for v1 — those are valid land tiles. A precise polygon is a later
refinement.

The single home for the committed list is the platform-deploy `s1rtc-aoi-tiles` ConfigMap (the cron
reads its comma-separated `western-europe:` value). Generate to a temp file, then splice it in:

Usage:
    uv run python scripts/gen_aoi_tiles.py --region western-europe \
      --gpkg $WORKDIR/DEM/dem_db/DEM_Union.gpkg --out /tmp/western-europe.txt
    paste -sd ',' /tmp/western-europe.txt   # → the ConfigMap `western-europe:` value (single line)
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import mgrs

# ensure_dem / watch_cdse_and_process are sibling scripts (not a package); import them inside the
# functions that need them — as ensure_dem itself does — so this module imports cleanly regardless
# of whether scripts/ is on sys.path at import time (pytest's full-suite collection isn't).

# Region AOI as a committed WGS84 bbox [lon_min, lat_min, lon_max, lat_max] (v1): simplest +
# deterministic, over-includes some neighbouring land within the box (accepted — valid land tiles).
REGIONS: dict[str, list[float]] = {
    # western-europe — France + the Alps (the box spans well beyond France, hence the broad name).
    # W -5.2° (Ushant / west Brittany) → E 13.5° (Salzburg / eastern Dolomites); S 42.0° (northern
    # Spain) → N 51.2° (covers Hauts-de-France: Lille ~50.6°N, Dunkirk ~51.1°N). The bbox includes
    # neighbouring land: S England, Belgium/Luxembourg, W Germany/Switzerland, Austria, NW Italy —
    # all valid land tiles, S2-/DEM-filtered; a precise polygon is a later refinement.
    "western-europe": [-5.2, 42.0, 13.5, 51.2],
}

# Ocean denylist: real S2 granules inside a region bbox that the 1° DEM land filter keeps, but whose
# own footprint is essentially open sea. The filter keeps them because a neighbouring part of the same
# coarse 1° cell is land; it can't see that the tile itself has ~no land. Drop them so the pipeline
# doesn't download + process all-ocean scenes. Each tile is annotated with its land fraction
# (share of the footprint over land, ~1 km global-land-mask); threshold for this list is < 2%.
EXCLUDE: dict[str, set[str]] = {
    "western-europe": {
        "30TWQ",  # 0.0%  Bay of Biscay
        "30TWR",  # 0.0%  Bay of Biscay
        "30UUV",  # 0.0%  Channel approaches, N of Brittany
        "31TGG",  # 0.0%  Gulf of Lion
        "32TKM",  # 0.0%  Gulf of Lion
        "32TLM",  # 0.0%  Ligurian Sea
        "30TUT",  # 0.1%  offshore SW Brittany
        "31TFH",  # 1.1%  Gulf of Lion
        "32TLN",  # 1.7%  Ligurian Sea
        # Surfaced when tile_bbox gained the true 109.8 km S2 extent (their bbox now touches the
        # part-land N46/W003 cell); the tiles themselves end at −2.87°E, all open sea.
        "30TVR",  # 0.0%  Bay of Biscay
        "30TVS",  # 0.0%  Bay of Biscay
    },
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


def read_s2_tile_ids(s2_gpkg_path: Path) -> set[str]:
    """Read the set of valid Sentinel-2 MGRS tile ids from the `eotile` S2 tiling-grid GeoPackage.

    This is the authoritative grid s1tiling accepts (its bundled `MGRS.gpkg` is the same ESA S2 grid):
    a 100 km square produced by `mgrs.toMGRS` math but absent here is not a defined S2 granule (e.g. a
    UTM-zone-boundary square served by the neighbouring zone) and s1tiling rejects it (exit 73).
    Stdlib sqlite only (no GDAL): the feature table + id column are discovered from the gpkg's own
    `gpkg_contents` / schema — never user input.
    """
    con = sqlite3.connect(str(s2_gpkg_path))
    try:
        row = con.execute("SELECT table_name FROM gpkg_contents LIMIT 1").fetchone()
        if row is None:
            raise ValueError(f"no feature table in {s2_gpkg_path}")
        table = row[0]
        cols = [c[1] for c in con.execute(f'PRAGMA table_info("{table}")')]
        id_col = next((c for c in cols if c.lower() == "id"), None)
        if id_col is None:
            raise ValueError(f"no id column in table {table!r} of {s2_gpkg_path}")
        # `table`/`id_col` are identifiers discovered from the gpkg's own schema (gpkg_contents +
        # PRAGMA) — never user input. SQLite cannot parameterise an identifier, so interpolation is
        # required and safe for this trusted static gpkg; flagged as a false positive by S608 / B608.
        return {r[0] for r in con.execute(f'SELECT "{id_col}" FROM "{table}"') if r[0]}  # noqa: S608  # nosec B608
    finally:
        con.close()


def resolve_s2_gpkg(s2_gpkg: Path | None) -> Path:
    """Return the S2 tiling-grid gpkg: the explicit ``--s2-gpkg`` path, else the eotile-bundled one."""
    if s2_gpkg is not None:
        return s2_gpkg
    try:
        import eotile
    except ImportError as exc:  # eotile is data-only; not a runtime dep of the pure core
        raise SystemExit(
            "no --s2-gpkg given and eotile not installed — `uv pip install eotile --no-deps` "
            "or pass the S2 tiling-grid gpkg explicitly"
        ) from exc
    src = next(Path(eotile.__file__).parent.rglob("s2_no_overlap.gpkg"), None)
    if src is None:
        raise SystemExit("eotile installed but bundled s2_no_overlap.gpkg not found")
    return src


def tiles_for_region(
    bbox: list[float],
    gpkg_products: set[str],
    valid_s2_tiles: set[str],
    *,
    exclude: frozenset[str] = frozenset(),
    step: float = 0.1,
) -> list[str]:
    """Sorted, deduped tile ids overlapping ``bbox`` that are real S2 granules AND have DEM/land cover.

    The S2-grid membership test (``valid_s2_tiles``) drops math-valid-but-non-existent squares that the
    land filter alone would keep (e.g. UTM-zone-boundary duplicates); the land filter drops ocean /
    out-of-DEM-coverage tiles. A tile must pass both. Tiles in ``exclude`` (a curated ocean denylist
    for footprints the coarse 1° land filter can't tell are ~all-sea) are then removed.
    """
    return sorted(
        t
        for t in mgrs_tiles_in_bbox(bbox, step=step)
        if t in valid_s2_tiles and t not in exclude and tile_is_land(t, gpkg_products)
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--region", required=True, choices=sorted(REGIONS), help="named region AOI")
    ap.add_argument("--gpkg", required=True, type=Path, help="dem_db/DEM_Union.gpkg (eotile index)")
    ap.add_argument(
        "--s2-gpkg",
        type=Path,
        help="eotile S2 tiling-grid gpkg (default: the one bundled with eotile)",
    )
    ap.add_argument("--step", type=float, default=0.1, help="sampling grid step in degrees")
    ap.add_argument("--out", type=Path, help="write the tile list here (default: stdout)")
    args = ap.parse_args()

    from ensure_dem import ensure_gpkg, read_gpkg_product10

    ensure_gpkg(args.gpkg)
    products = read_gpkg_product10(args.gpkg)
    valid_s2 = read_s2_tile_ids(resolve_s2_gpkg(args.s2_gpkg))
    tiles = tiles_for_region(
        REGIONS[args.region],
        products,
        valid_s2,
        exclude=frozenset(EXCLUDE.get(args.region, set())),
        step=args.step,
    )

    text = "\n".join(tiles) + "\n" if tiles else ""
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text)
        print(f"{args.region}: {len(tiles)} tile(s) → {args.out}")
    else:
        print(text, end="")


if __name__ == "__main__":
    main()
