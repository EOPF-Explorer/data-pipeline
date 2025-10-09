#!/usr/bin/env python3
"""Benchmark GeoZarr vs EOPF Zarr random access performance.

Compares real-world access patterns:
- EOPF: xr.open_datatree with eopf-zarr engine (hierarchical)
- GeoZarr: xr.open_dataset with zarr v3 engine (individual bands)

Both use the same access pattern: load RGB composite (b04, b03, b02) windows.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import time
from datetime import UTC

import fsspec
import numpy as np
import xarray as xr

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)


def open_eopf_datatree(url: str) -> xr.Dataset:
    """Open EOPF zarr using datatree (production access method).

    Returns a Dataset with multiple bands (b02, b03, b04, etc.)
    """
    logger.info(f"Opening EOPF datatree: {url}")
    dt = xr.open_datatree(url, engine="zarr", consolidated=False)

    # Navigate to the measurement group
    for node in dt.subtree:
        if node.ds and len(node.ds.data_vars) > 0:
            logger.info(f"Found node: {node.path}, variables: {list(node.ds.data_vars.keys())}")
            return node.ds

    raise ValueError(f"No data variables found in {url}")


def open_geozarr_bands(base_url: str, bands: list[str]) -> xr.Dataset:
    """Open GeoZarr individual band arrays and combine into Dataset.

    Args:
        base_url: Base S3 URL to the measurement group (e.g., .../r10m)
        bands: List of band names to load (e.g., ['b02', 'b03', 'b04'])

    Returns:
        Dataset with requested bands as data variables
    """
    logger.info(f"Opening GeoZarr bands: {bands}")

    # Setup S3 filesystem
    endpoint = os.getenv("AWS_ENDPOINT_URL", "https://s3.de.cloud.ovh.net")
    fs = fsspec.filesystem(
        "s3",
        key=os.getenv("AWS_ACCESS_KEY_ID"),
        secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
        endpoint_url=endpoint,
    )

    data_vars = {}
    for band in bands:
        band_url = f"{base_url}/{band}"
        logger.info(f"  Loading band: {band} from {band_url}")
        store = fs.get_mapper(band_url)

        # Open as xarray DataArray directly (zarr v3 array)
        da = xr.open_dataarray(store, engine="zarr", consolidated=False)
        data_vars[band] = da

    # Combine into single Dataset
    combined = xr.Dataset(data_vars)
    logger.info(f"Combined GeoZarr dataset: {list(combined.data_vars.keys())}")
    return combined


def open_zarr(url: str, is_geozarr: bool = False) -> xr.Dataset:
    """Open zarr dataset using appropriate method based on format.

    Args:
        url: URL to zarr store
        is_geozarr: If True, treats as GeoZarr (individual bands), else EOPF

    Returns:
        xarray Dataset with data variables
    """
    if is_geozarr:
        # GeoZarr: individual band arrays at base_url/b02, base_url/b03, etc.
        # Extract base URL (remove band name if present)
        base_url = url.rsplit("/", 1)[0] if url.endswith(("/b02", "/b03", "/b04", "/b08")) else url

        # Load RGB bands for typical tile request
        return open_geozarr_bands(base_url, ["b02", "b03", "b04"])
    else:
        # EOPF: hierarchical datatree with eopf-zarr engine
        return open_eopf_datatree(url)


def benchmark(
    url: str, is_geozarr: bool = False, num_windows: int = 5, window_size: int = 512
) -> dict:
    """Benchmark random window access on zarr dataset.

    Simulates typical map tile requests by reading RGB composite windows.
    For GeoZarr, loads 3 bands (b02, b03, b04). For EOPF, uses same bands from dataset.
    """
    ds = open_zarr(url, is_geozarr=is_geozarr)

    # Get dimensions and bands
    bands = ["b02", "b03", "b04"]
    available_bands = [b for b in bands if b in ds.data_vars]

    if not available_bands:
        # Fallback: use first 3 variables
        available_bands = list(ds.data_vars.keys())[:3]
        logger.warning(f"RGB bands not found, using: {available_bands}")

    logger.info(f"Benchmarking bands: {available_bands}")

    # Get spatial dimensions from first band
    first_var = ds[available_bands[0]]
    # Find y and x dimensions (usually last two dims)
    dims = list(first_var.dims)
    y_dim, x_dim = dims[-2], dims[-1]
    y_size, x_size = first_var.sizes[y_dim], first_var.sizes[x_dim]

    logger.info(f"Array dimensions: {y_size}×{x_size} ({y_dim}, {x_dim})")

    if y_size < window_size or x_size < window_size:
        raise ValueError(f"Array too small: {y_size}×{x_size} < {window_size}")

    times = []
    for i in range(num_windows):
        y = random.randint(0, y_size - window_size)
        x = random.randint(0, x_size - window_size)

        start = time.perf_counter()

        # Read all RGB bands for this window (typical tile request)
        for band in available_bands:
            data = ds[band]
            window = data.isel({y_dim: slice(y, y + window_size), x_dim: slice(x, x + window_size)})
            _ = window.compute()  # Force evaluation

        elapsed = time.perf_counter() - start
        times.append(elapsed)
        logger.info(f"  Window {i+1}: {elapsed:.3f}s ({len(available_bands)} bands)")

    return {
        "avg": float(np.mean(times)),
        "std": float(np.std(times)),
        "min": float(np.min(times)),
        "max": float(np.max(times)),
        "times": [float(t) for t in times],
        "bands_per_window": len(available_bands),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark GeoZarr vs EOPF zarr access using real-world patterns"
    )
    parser.add_argument("--geozarr-url", required=True, help="GeoZarr measurement group URL")
    parser.add_argument("--eopf-url", required=True, help="EOPF measurement group URL")
    parser.add_argument("--item-id", required=True)
    parser.add_argument("--windows", type=int, default=5)
    parser.add_argument("--window-size", type=int, default=512)
    args = parser.parse_args()

    logger.info("=== GeoZarr (individual band arrays) ===")
    geo = benchmark(
        args.geozarr_url, is_geozarr=True, num_windows=args.windows, window_size=args.window_size
    )

    logger.info("=== EOPF (datatree with eopf-zarr engine) ===")
    eopf = benchmark(
        args.eopf_url, is_geozarr=False, num_windows=args.windows, window_size=args.window_size
    )

    speedup = eopf["avg"] / geo["avg"]
    from datetime import datetime

    results = {
        "timestamp": datetime.now(UTC).isoformat(),
        "item_id": args.item_id,
        "config": {
            "windows": args.windows,
            "window_size": args.window_size,
            "access_pattern": "RGB composite (3 bands)",
        },
        "geozarr": {"url": args.geozarr_url, **geo},
        "eopf": {"url": args.eopf_url, **eopf},
        "speedup": round(speedup, 2),
        "geozarr_faster": speedup > 1.0,
    }
    logger.info(f"\n{'='*60}")
    logger.info(
        f"GeoZarr: {geo['avg']:.3f}s ± {geo['std']:.3f}s ({geo['bands_per_window']} bands/window)"
    )
    logger.info(
        f"EOPF:    {eopf['avg']:.3f}s ± {eopf['std']:.3f}s ({eopf['bands_per_window']} bands/window)"
    )
    logger.info(f"Speedup: {speedup:.2f}× ({'GeoZarr' if speedup > 1 else 'EOPF'} faster)")
    logger.info(f"{'='*60}\n")
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
