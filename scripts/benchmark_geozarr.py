#!/usr/bin/env python3
"""Automated GeoZarr vs EOPF performance comparison.

Measures load time and memory usage comparing original EOPF Zarr format
against optimized GeoZarr format.

Usage:
    benchmark_geozarr.py --eopf-url s3://... --geozarr-url s3://... --output results.json
"""

import argparse
import json
import logging
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path

import xarray as xr

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class BenchmarkResult:
    """Performance measurement result."""

    format_type: str  # "eopf" or "geozarr"
    dataset_url: str
    load_time_seconds: float
    dataset_size_mb: float
    num_variables: int
    chunk_sizes: dict[str, tuple[int, ...]]


def benchmark_load_time(dataset_url: str, format_type: str) -> BenchmarkResult:
    """Measure dataset load time and basic metrics."""
    logger.info(f"Benchmarking {format_type}: {dataset_url}")

    start = time.perf_counter()
    ds = xr.open_zarr(dataset_url, consolidated=True)
    load_time = time.perf_counter() - start

    # Collect metrics
    chunks = {var: ds[var].chunks for var in list(ds.data_vars)[:3]}  # Sample 3 vars
    size_mb = sum(var.nbytes for var in ds.data_vars.values()) / 1024 / 1024

    result = BenchmarkResult(
        format_type=format_type,
        dataset_url=dataset_url,
        load_time_seconds=round(load_time, 3),
        dataset_size_mb=round(size_mb, 2),
        num_variables=len(ds.data_vars),
        chunk_sizes=chunks,
    )

    ds.close()
    logger.info(f"✓ {format_type} load time: {load_time:.3f}s")
    return result


def compare_results(eopf: BenchmarkResult, geozarr: BenchmarkResult) -> dict:
    """Generate comparison summary."""
    speedup = (
        eopf.load_time_seconds / geozarr.load_time_seconds if geozarr.load_time_seconds > 0 else 0
    )

    return {
        "eopf": asdict(eopf),
        "geozarr": asdict(geozarr),
        "comparison": {
            "speedup_factor": round(speedup, 2),
            "time_saved_seconds": round(eopf.load_time_seconds - geozarr.load_time_seconds, 3),
            "faster_format": "geozarr" if speedup > 1 else "eopf",
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Benchmark GeoZarr vs EOPF performance")
    parser.add_argument("--eopf-url", required=True, help="URL to EOPF Zarr dataset")
    parser.add_argument("--geozarr-url", required=True, help="URL to GeoZarr dataset")
    parser.add_argument("--output", type=Path, help="Output JSON file path")
    parser.add_argument("--verbose", action="store_true")

    args = parser.parse_args(argv)

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        # Run benchmarks
        eopf_result = benchmark_load_time(args.eopf_url, "eopf")
        geozarr_result = benchmark_load_time(args.geozarr_url, "geozarr")

        # Generate comparison
        results = compare_results(eopf_result, geozarr_result)

        # Write output
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(results, indent=2))
            logger.info(f"Results written to: {args.output}")

        # Print summary
        print(json.dumps(results, indent=2))

        speedup = results["comparison"]["speedup_factor"]
        if speedup > 1:
            logger.info(f"✅ GeoZarr is {speedup}x faster than EOPF")
        else:
            logger.warning(f"⚠️  EOPF is {1 / speedup:.2f}x faster than GeoZarr")

        return 0

    except Exception as e:
        logger.error(f"Benchmark failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
