"""Convert GeographicLib EGM2008 PGM to OTB binary .grd format.

Usage:
  python scripts/convert_egm2008_pgm_to_grd.py \
      --pgm ~/Downloads/geoids/egm2008-1.pgm \
      --out ~/s1tiling/geoid/egm2008.grd \
      [--step 15]

--step controls subsampling of the 1-arcmin PGM grid:
  15 → 15-arcmin output (0.25°), same resolution as the bundled egm96.grd  [default]
   5 → 5-arcmin output (~37 MB)
   1 → full 1-arcmin (no subsampling, ~932 MB — very large)
"""

import argparse
import pathlib
import struct
import sys

import numpy as np


def parse_pgm_header(path: pathlib.Path) -> tuple[int, int, int, dict[str, str]]:
    """Return (data_offset, width, height, metadata) from a GeographicLib PGM header."""
    meta = {}
    width = height = None
    with open(path, "rb") as f:
        while True:
            line = f.readline().decode("ascii").strip()
            if line in ("P5", ""):
                continue
            elif line.startswith("# "):
                parts = line[2:].split(None, 1)
                if len(parts) == 2:
                    meta[parts[0]] = parts[1]
            elif line == "65535":
                data_offset = f.tell()
                break
            elif width is None:
                w, h = line.split()
                width, height = int(w), int(h)
    if width is None or height is None:
        raise ValueError(f"Invalid PGM header in {path}: missing width/height before maxval")
    return data_offset, width, height, meta


def read_pgm_data(path: pathlib.Path, data_offset: int, height: int, width: int) -> np.ndarray:
    with open(path, "rb") as f:
        f.seek(data_offset)
        raw = np.frombuffer(f.read(width * height * 2), dtype=">u2")
    return raw.reshape(height, width)


def convert(pgm: pathlib.Path, out: pathlib.Path, step: int) -> None:
    print(f"Reading {pgm} ...")
    data_offset, pgm_width, pgm_height, meta = parse_pgm_header(pgm)
    scale = float(meta["Scale"])
    offset = float(meta["Offset"])
    print(f"  origin={meta.get('Origin')}, description={meta.get('Description')}")

    pixels = read_pgm_data(pgm, data_offset, pgm_height, pgm_width)

    # Subsample: rows 0..pgm_height-1 step=step, cols 0..pgm_width step=step
    # Col pgm_width wraps to 0 (360E == 0E) — duplicate the first column
    row_idx = np.arange(0, pgm_height, step)
    col_idx = np.arange(0, pgm_width + 1, step)  # +1 to include 360E = 0E wrap
    col_idx_wrapped = col_idx % pgm_width

    sub = pixels[np.ix_(row_idx, col_idx_wrapped)].astype(np.float32) * scale + offset

    n_lat, n_lon = sub.shape
    dlat = dlon = step / 60.0
    lat_max = 90.0
    lat_min = lat_max - (n_lat - 1) * dlat
    lon_min, lon_max = 0.0, (n_lon - 1) * dlon

    print(f"  output grid: {n_lat} lat × {n_lon} lon, dlat=dlon={dlat:.4f}°")
    print(f"  lat [{lat_min}, {lat_max}]  lon [{lon_min}, {lon_max}]")

    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "wb") as f:
        # 6-float big-endian header (same layout as egm96.grd)
        f.write(struct.pack(">6f", lat_min, lat_max, lon_min, lon_max, dlat, dlon))
        f.write(sub.astype(">f4").tobytes())

    # ENVI sidecar so GDAL/rasterio can open the .grd directly
    # pixel center convention: map_info origin is top-left pixel center
    hdr = out.parent / (out.name + ".hdr")
    hdr.write_text(
        f"ENVI\n"
        f"samples = {n_lon}\n"
        f"lines   = {n_lat}\n"
        f"bands   = 1\n"
        f"header offset = 24\n"
        f"file type = ENVI Standard\n"
        f"data type = 4\n"
        f"interleave = bsq\n"
        f"byte order = 1\n"
        f"map info = {{Geographic Lat/Lon, 1, 1, {lon_min - dlon / 2:.6f}, {lat_max + dlat / 2:.6f},"
        f" {dlon:.6f}, {dlat:.6f}, WGS-84}}\n"
        f"band names = {{\nBand 1}}\n"
    )

    print(f"Written: {out}  ({out.stat().st_size:,} bytes)")
    print(f"Written: {hdr}")


def validate(out: pathlib.Path, step: int, egm96_grd: pathlib.Path | None) -> bool:
    ok = True

    # Reference values derived from the 1-arcmin PGM source at exact 15-arcmin grid points.
    # All points are multiples of 0.25° so subsampling produces an exact match (no interpolation).
    KNOWN = [
        (0.0, 0.0, 17.226, "equator / prime meridian"),
        (48.75, 2.25, 44.694, "near Paris (48.75N 2.25E)"),
        (51.5, 0.0, 45.921, "near London (51.5N 0E)"),
        (-33.75, 151.25, 22.776, "near Sydney (-33.75N 151.25E)"),
        (35.75, 139.75, 36.945, "near Tokyo (35.75N 139.75E)"),
    ]

    with open(out, "rb") as f:
        lat_min, lat_max, lon_min, lon_max, dlat, dlon = struct.unpack(">6f", f.read(24))

    n_lat = round((lat_max - lat_min) / dlat) + 1
    n_lon = round((lon_max - lon_min) / dlon) + 1
    # memmap avoids loading the full grid — matters for --step 1 (~932 MB)
    grid = np.memmap(out, dtype=">f4", mode="r", offset=24).reshape(n_lat, n_lon)

    if not all(
        abs(round(lat / dlat) - lat / dlat) < 1e-9 and abs(round(lon / dlon) - lon / dlon) < 1e-9
        for lat, lon, *_ in KNOWN
    ):
        raise ValueError(
            "KNOWN reference points must fall on exact grid multiples for the given --step"
        )

    print("\nSpot-check (exact subsampling — tolerance ±0.001 m):")
    for lat, lon, expected, label in KNOWN:
        # grid is north→south: row 0 = lat_max
        row = round((lat_max - lat) / dlat)
        col = round((lon - lon_min) / dlon) % n_lon
        got = float(grid[row, col])
        diff = abs(got - expected)
        status = "OK" if diff <= 0.001 else "FAIL"
        if status == "FAIL":
            ok = False
        print(
            f"  [{status}] {label:35s} expected={expected:7.3f}  got={got:7.3f}  diff={diff:.3f} m"
        )

    if egm96_grd and egm96_grd.exists() and step == 15:
        expected_size = egm96_grd.stat().st_size
        actual_size = out.stat().st_size
        size_ok = actual_size == expected_size
        if not size_ok:
            ok = False
        print(
            f"\n  [{'OK' if size_ok else 'FAIL'}] File size: got {actual_size:,}  expected {expected_size:,} (egm96.grd)"
        )

    return ok


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--pgm", required=True, type=pathlib.Path)
    ap.add_argument("--out", required=True, type=pathlib.Path)
    ap.add_argument("--step", type=int, default=15, help="subsampling step in arcmin (default 15)")
    ap.add_argument(
        "--egm96-grd",
        type=pathlib.Path,
        default=None,
        help="path to egm96.grd for size comparison (optional, only meaningful with --step 15)",
    )
    args = ap.parse_args()

    if not args.pgm.exists():
        sys.exit(f"Error: PGM file not found: {args.pgm}")

    convert(args.pgm, args.out, args.step)
    ok = validate(args.out, args.step, args.egm96_grd)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
