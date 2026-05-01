#!/usr/bin/env python3
"""Convert a local .zarr store to GeoZarr with output stored locally.

Calls convert_s2_optimized() directly (no S3 credentials required for output). With
``--quick``, runs a fast codec probe: opens only measurements/reflectance/r10m, slices
to the first QUICK_TILE_SIZE×QUICK_TILE_SIZE pixels, writes a minimal zarr via
create_measurements_encoding(), and prints the zarr.json codec chain (seconds instead
of minutes).

Usage
-----
# Fast codec probe (seconds; writes ./codec_probe.zarr):
    uv run python operator-tools/codec/run_local_conversion.py \
        S2C_MSIL2A_20260427T101021_N0512_R022_T33UWT_20260427T151616.zarr \
        --quick

# Fast probe with a custom output directory:
    uv run python operator-tools/codec/run_local_conversion.py path/to/scene.zarr \
        --quick --output-dir /tmp

# Full local conversion (writes ./<stem>_converted.zarr):
    uv run python operator-tools/codec/run_local_conversion.py \
        S2C_MSIL2A_20260427T101021_N0512_R022_T33UWT_20260427T151616.zarr

# Full conversion with more workers and a custom output directory:
    uv run python operator-tools/codec/run_local_conversion.py path/to/scene.zarr \
        --n-workers 4 --memory-limit 16Gi --output-dir /tmp
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import pathlib
import shutil
import sys

import xarray as xr
from eopf_geozarr.conversion.fs_utils import get_storage_options
from eopf_geozarr.s2_optimization.s2_converter import convert_s2_optimized
from eopf_geozarr.s2_optimization.s2_multiscale import create_measurements_encoding

# Allow imports from this folder (check_zarr_codecs) and from scripts/ (convert_v1_s2).
_REPO_ROOT = pathlib.Path(__file__).parent.parent.parent
sys.path.insert(0, str(pathlib.Path(__file__).parent))  # operator-tools/codec/
sys.path.insert(0, str(_REPO_ROOT / "scripts"))  # scripts/

from check_zarr_codecs import print_codec_summary  # noqa: E402
from convert_v1_s2 import setup_dask_cluster  # noqa: E402

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_N_WORKERS = 2
DEFAULT_MEMORY_LIMIT = "12Gi"
DEFAULT_OUTPUT_DIR = "."
QUICK_TILE_SIZE = 512  # spatial pixels used for the --quick codec probe


def _quick_codec_probe(source_path: pathlib.Path, output_dir: pathlib.Path) -> int:
    """Write a tiny test zarr using only b02 r10m to verify the codec pipeline."""
    probe_path = output_dir / "codec_probe.zarr"
    if probe_path.exists():
        shutil.rmtree(probe_path)

    logger.info(f"📥 Opening measurements/reflectance/r10m from {source_path}")
    storage_options = get_storage_options(str(source_path))
    dt = xr.open_datatree(
        str(source_path),
        engine="zarr",
        chunks="auto",
        storage_options=storage_options,
    )

    ds = dt["/measurements/reflectance/r10m"].to_dataset()

    # Slice to a small tile so the write is near-instant
    t = QUICK_TILE_SIZE
    sliced_ds = ds.isel({d: slice(0, t) for d in ds.dims if d not in ("time", "band")})
    logger.info(f"   Sliced to {dict(sliced_ds.dims)}")

    encoding = create_measurements_encoding(
        sliced_ds,
        spatial_chunk=256,
        enable_sharding=True,
        keep_scale_offset=False,
        experimental_scale_offset_codec=True,
    )

    logger.info(f"📤 Writing probe zarr → {probe_path}")
    sliced_ds.to_zarr(
        str(probe_path), mode="w", zarr_format=3, encoding=encoding, consolidated=False
    )

    print()
    print(f"Probe output: {probe_path}")
    print("Verifying codec on b02 ...")
    zarr_json_path = probe_path / "b02" / "zarr.json"
    if not zarr_json_path.exists():
        print(f"WARNING: zarr.json not found at {zarr_json_path}", file=sys.stderr)
        return 0
    meta = json.loads(zarr_json_path.read_text())
    print_codec_summary(meta)
    return 0


def _print_summary(
    source_path: pathlib.Path, output_path: pathlib.Path, args: argparse.Namespace
) -> None:
    print("=" * 60)
    print("Local conversion summary")
    print("=" * 60)
    print(f"  source (local):     {source_path}")
    print(f"  output (local):     {output_path}")
    print(f"  n_workers:          {args.n_workers}")
    print(f"  memory_limit:       {args.memory_limit}")
    print("  enable_sharding:    True")
    print("  scale_offset_codec: True")
    print("=" * 60)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convert a local .zarr store to GeoZarr format and write output locally."
    )
    parser.add_argument("source_zarr", help="Path to the locally downloaded .zarr store")
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory for the output .zarr store (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument("--n-workers", type=int, default=DEFAULT_N_WORKERS)
    parser.add_argument("--memory-limit", default=DEFAULT_MEMORY_LIMIT)
    parser.add_argument(
        "--quick",
        action="store_true",
        help=(
            f"Fast codec probe: slice measurements/reflectance/r10m to "
            f"{QUICK_TILE_SIZE}×{QUICK_TILE_SIZE} px, write a minimal zarr, "
            "and print the codec chain. Skips full conversion."
        ),
    )
    args = parser.parse_args()

    source_path = pathlib.Path(args.source_zarr).resolve()
    if not source_path.exists():
        print(f"ERROR: source path does not exist: {source_path}", file=sys.stderr)
        return 1

    output_dir = pathlib.Path(args.output_dir).resolve()

    if args.quick:
        return _quick_codec_probe(source_path, output_dir)

    item_id = source_path.stem  # strips trailing .zarr extension
    output_path = output_dir / f"{item_id}_converted.zarr"

    _print_summary(source_path, output_path, args)

    if output_path.exists():
        logger.info(f"🧹 Removing existing output: {output_path}")
        shutil.rmtree(output_path)

    setup_dask_cluster(
        enable_dask=True, verbose=True, n_workers=args.n_workers, memory_limit=args.memory_limit
    )

    logger.info(f"📥 Loading input dataset: {source_path}")
    storage_options = get_storage_options(str(source_path))
    dt_input = xr.open_datatree(
        str(source_path),
        engine="zarr",
        chunks="auto",
        storage_options=storage_options,
    )

    convert_s2_optimized(
        dt_input=dt_input,
        output_path=str(output_path),
        spatial_chunk=256,
        compression_level=3,
        enable_sharding=True,
        validate_output=True,
        keep_scale_offset=False,
        experimental_scale_offset_codec=True,
    )

    logger.info(f"✅ Conversion complete → {output_path}")

    # ── Codec verification ──────────────────────────────────────────────────
    print()
    print("Verifying codec on measurements/reflectance/r10m/b02 ...")
    zarr_json_path = output_path / "measurements" / "reflectance" / "r10m" / "b02" / "zarr.json"
    if not zarr_json_path.exists():
        print(f"WARNING: zarr.json not found at {zarr_json_path}", file=sys.stderr)
        return 0
    meta = json.loads(zarr_json_path.read_text())
    print_codec_summary(meta)
    return 0


if __name__ == "__main__":
    sys.exit(main())
