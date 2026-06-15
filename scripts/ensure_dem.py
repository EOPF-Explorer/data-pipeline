"""Auto-provision the Copernicus GLO-30 DEM for an MGRS tile's S1 swath (plan T3).

s1tiling needs, per tile, the DEM cells covering the **whole SAR swath** (not just the tile —
phase-5 showed 31TCH's swath reaching N44/W002), plus the static `eotile` `DEM_Union.gpkg` index
that maps cell geometries to a `Product10` filename stem. This derives the integer GLO-30 cells for
the tile's swath bbox (lon±4°/lat±1.5° margin), keeps only the land cells the gpkg knows about
(ocean cells simply don't exist in GLO-30), skips cells already on disk, and downloads the rest from
the public anon `copernicus-dem-30m` bucket — renaming each COG to the `Product10` stem s1tiling
matches. Idempotent: a re-run with the tiles present fetches nothing.

Usage:
    uv run python scripts/ensure_dem.py --tile-id 31TCH \
      --dem-dir $WORKDIR/DEM/COP_DEM_GLO30 --gpkg $WORKDIR/DEM/dem_db/DEM_Union.gpkg
"""

from __future__ import annotations

import argparse
import math
import shutil
import sqlite3
from pathlib import Path
from typing import Any

DEM_BUCKET = "copernicus-dem-30m"  # AWS Open Data, anonymous
DEM_REGION = "eu-central-1"
MARGIN_LON = 4.0  # SAR (IW) swath reaches ~3° E/W of the tile at mid-latitudes
# The swath spans further N/S than the tile: a descending pass over 31TCG (tile top ~42.45°N) still
# reaches N44 (~1.5° above the tile), so lat±1.5 fell short and AgglomerateDEM aborted (2026-06-15).
# 3.0° covers the full swath for tiles at the southern edge of a pass, with an N45 row of headroom.
MARGIN_LAT = 3.0


def product10_stem(lat: int, lon: int) -> str:
    """s1tiling/`eotile` `Product10` filename stem for an integer 1°×1° cell (its SW corner)."""
    ns, ew = ("N" if lat >= 0 else "S"), ("E" if lon >= 0 else "W")
    return f"Copernicus_DSM_10_{ns}{abs(lat):02d}_00_{ew}{abs(lon):03d}_00"


def cog_key(lat: int, lon: int) -> str:
    """Object key in `copernicus-dem-30m` (`<dir>/<dir>.tif`, the COG/`_DEM` naming variant)."""
    d = f"Copernicus_DSM_COG_10_{'N' if lat >= 0 else 'S'}{abs(lat):02d}_00_"
    d += f"{'E' if lon >= 0 else 'W'}{abs(lon):03d}_00_DEM"
    return f"{d}/{d}.tif"


def tiles_for_bbox(
    bbox: list[float], *, margin_lon: float = MARGIN_LON, margin_lat: float = MARGIN_LAT
) -> list[tuple[int, int]]:
    """Integer (lat, lon) SW corners of the 1°×1° cells covering ``bbox`` grown by the margin.

    Returns them in ascending (lat, lon) order — deterministic regardless of input.
    """
    lon0, lat0, lon1, lat1 = bbox
    lats = range(math.floor(lat0 - margin_lat), math.floor(lat1 + margin_lat) + 1)
    lons = range(math.floor(lon0 - margin_lon), math.floor(lon1 + margin_lon) + 1)
    return [(la, lo) for la in lats for lo in lons]


def read_gpkg_product10(gpkg_path: Path) -> set[str]:
    """Read the set of `Product10` stems from the `eotile` `DEM_Union.gpkg` (stdlib sqlite, no GDAL)."""
    con = sqlite3.connect(str(gpkg_path))
    try:
        tables = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        table = next(
            (
                t
                for t in tables
                if any(c[1] == "Product10" for c in con.execute(f"PRAGMA table_info({t})"))
            ),
            None,
        )
        if table is None:
            raise ValueError(f"no table with a Product10 column in {gpkg_path}")
        return {r[0] for r in con.execute(f"SELECT Product10 FROM {table}") if r[0]}  # noqa: S608
    finally:
        con.close()


def tiles_to_fetch(
    tile_id: str,
    gpkg_products: set[str],
    present_stems: set[str],
    *,
    margin_lon: float = MARGIN_LON,
    margin_lat: float = MARGIN_LAT,
) -> list[tuple[int, int]]:
    """Cells to download: (swath cells ∩ gpkg land tiles) − tiles already on disk.

    Raises ``ValueError`` for a malformed/unknown tile id (via ``tile_bbox``).
    """
    from watch_cdse_and_process import tile_bbox

    bbox = tile_bbox(tile_id)
    out: list[tuple[int, int]] = []
    for lat, lon in tiles_for_bbox(bbox, margin_lon=margin_lon, margin_lat=margin_lat):
        stem = product10_stem(lat, lon)
        if stem in gpkg_products and stem not in present_stems:
            out.append((lat, lon))
    return out


# --- gpkg + anon download (integration; exercised in main / on cluster) ------


def ensure_gpkg(gpkg_path: Path) -> None:
    """Stage the static global `DEM_Union.gpkg` from the `eotile` package if it isn't present."""
    if gpkg_path.exists():
        return
    try:
        import eotile
    except ImportError as exc:  # eotile is data-only; not a runtime dep of the pure core
        raise SystemExit(
            f"{gpkg_path} missing and eotile not installed — `uv pip install eotile --no-deps` "
            "or stage the gpkg on the PVC"
        ) from exc
    src = next(Path(eotile.__file__).parent.rglob("DEM_Union.gpkg"), None)
    if src is None:
        raise SystemExit("eotile installed but bundled DEM_Union.gpkg not found")
    gpkg_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(src, gpkg_path)
    print(f"staged DEM_Union.gpkg from eotile: {src}")


def _anon_s3(region: str) -> Any:
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config

    # Pin the public AWS S3 endpoint explicitly: `copernicus-dem-30m` lives on AWS, but boto3 would
    # otherwise inherit an ambient `AWS_ENDPOINT_URL` (e.g. the OVH endpoint used for the output
    # bucket) and 400 the anonymous DEM fetch (HeadObject Bad Request). Pinning it makes the fetch
    # independent of whatever endpoint the surrounding pipeline set.
    return boto3.client(
        "s3",
        region_name=region,
        endpoint_url=f"https://s3.{region}.amazonaws.com",
        config=Config(signature_version=UNSIGNED),
    )


def _download(s3: Any, bucket: str, key: str, dest: Path) -> None:
    """Download to a temp sibling then rename, so a failure never leaves a partial Product10 .tif."""
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    s3.download_file(bucket, key, str(tmp))
    tmp.replace(dest)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tile-id", required=True)
    ap.add_argument(
        "--dem-dir", required=True, type=Path, help="COP_DEM_GLO30 dir (Product10 .tif)"
    )
    ap.add_argument("--gpkg", required=True, type=Path, help="dem_db/DEM_Union.gpkg (eotile index)")
    ap.add_argument("--margin-lon", type=float, default=MARGIN_LON)
    ap.add_argument("--margin-lat", type=float, default=MARGIN_LAT)
    ap.add_argument("--bucket", default=DEM_BUCKET)
    ap.add_argument("--region", default=DEM_REGION)
    args = ap.parse_args()

    ensure_gpkg(args.gpkg)
    products = read_gpkg_product10(args.gpkg)
    args.dem_dir.mkdir(parents=True, exist_ok=True)
    present = {p.stem for p in args.dem_dir.glob("Copernicus_DSM_10_*.tif")}
    fetch = tiles_to_fetch(
        args.tile_id, products, present, margin_lon=args.margin_lon, margin_lat=args.margin_lat
    )
    if not fetch:
        print(f"DEM already complete for {args.tile_id}: {len(present)} tile(s) present")
        return
    s3 = _anon_s3(args.region)
    for lat, lon in fetch:
        dest = args.dem_dir / f"{product10_stem(lat, lon)}.tif"
        _download(s3, args.bucket, cog_key(lat, lon), dest)
        print(f"fetched {dest.name}")
    print(
        f"ensured DEM for {args.tile_id}: +{len(fetch)} tile(s) ({len(present) + len(fetch)} total)"
    )


if __name__ == "__main__":
    main()
