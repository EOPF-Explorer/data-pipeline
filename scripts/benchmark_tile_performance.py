#!/usr/bin/env python3
"""Benchmark tile generation performance for GeoZarr datasets.

This script measures end-to-end tile generation latency via the titiler-eopf
raster API. It demonstrates the actual user-facing performance improvements
of GeoZarr over direct EOPF access.

Usage:
    python benchmark_tile_performance.py \\
        --stac-api https://api.explorer.eopf.copernicus.eu/stac \\
        --raster-api https://api.explorer.eopf.copernicus.eu/raster \\
        --collection sentinel-2-l2a \\
        --item-id S2A_MSIL2A_... \\
        --num-tiles 20 \\
        --zoom-levels 10,11,12
"""

import argparse
import json
import logging
import random
import sys
import time
from typing import Any, cast
from urllib.parse import urlencode

import requests  # type: ignore[import-untyped]

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def fetch_item(stac_api: str, collection: str, item_id: str) -> dict[str, Any]:
    """Fetch STAC item from API."""
    url = f"{stac_api}/collections/{collection}/items/{item_id}"
    logger.info(f"Fetching STAC item: {url}")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()  # type: ignore[no-any-return]


def get_tile_url(raster_api: str, collection: str, item_id: str, z: int, x: int, y: int) -> str:
    """Construct tile URL for given z/x/y coordinates."""
    base = f"{raster_api}/collections/{collection}/items/{item_id}"
    return f"{base}/tiles/WebMercatorQuad/{z}/{x}/{y}.png"


def generate_tile_coordinates(zoom: int, num_tiles: int) -> list[tuple[int, int, int]]:
    """Generate random tile coordinates for a given zoom level.

    Args:
        zoom: Zoom level (0-20)
        num_tiles: Number of random tiles to generate

    Returns:
        List of (z, x, y) tuples
    """
    max_coord = 2**zoom
    coords = []
    for _ in range(num_tiles):
        x = random.randint(0, max_coord - 1)
        y = random.randint(0, max_coord - 1)
        coords.append((zoom, x, y))
    return coords


def benchmark_tile(
    raster_api: str,
    collection: str,
    item_id: str,
    z: int,
    x: int,
    y: int,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Fetch a single tile and measure latency.

    Args:
        raster_api: Base raster API URL
        collection: Collection ID
        item_id: Item ID
        z, x, y: Tile coordinates
        params: Optional query parameters (e.g., assets, rescale)

    Returns:
        Dictionary with timing metrics and response info
    """
    url = get_tile_url(raster_api, collection, item_id, z, x, y)
    if params:
        url = f"{url}?{urlencode(params)}"

    start = time.perf_counter()
    try:
        resp = requests.get(url, timeout=60)
        elapsed = time.perf_counter() - start

        success = resp.status_code == 200
        size_bytes = len(resp.content) if success else 0

        return {
            "z": z,
            "x": x,
            "y": y,
            "url": url,
            "success": success,
            "status_code": resp.status_code,
            "latency_ms": elapsed * 1000,
            "size_bytes": size_bytes,
            "error": None if success else resp.text[:200],
        }
    except Exception as e:
        elapsed = time.perf_counter() - start
        return {
            "z": z,
            "x": x,
            "y": y,
            "url": url,
            "success": False,
            "status_code": None,
            "latency_ms": elapsed * 1000,
            "size_bytes": 0,
            "error": str(e)[:200],
        }


def benchmark_zoom_level(
    raster_api: str,
    collection: str,
    item_id: str,
    zoom: int,
    num_tiles: int,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Benchmark multiple tiles at a specific zoom level.

    Args:
        raster_api: Base raster API URL
        collection: Collection ID
        item_id: Item ID
        zoom: Zoom level
        num_tiles: Number of tiles to test
        params: Optional query parameters

    Returns:
        Aggregated statistics for this zoom level
    """
    logger.info(f"Benchmarking zoom level {zoom} ({num_tiles} tiles)")
    coords = generate_tile_coordinates(zoom, num_tiles)

    results = []
    for z, x, y in coords:
        result = benchmark_tile(raster_api, collection, item_id, z, x, y, params)
        results.append(result)
        status = "✓" if result["success"] else "✗"
        logger.debug(
            f"  {status} z{z}/{x}/{y}: {result['latency_ms']:.1f}ms "
            f"({result['size_bytes']/1024:.1f}KB)"
        )

    # Calculate statistics
    successful = [r for r in results if r["success"]]
    if not successful:
        logger.warning(f"All tiles failed at zoom {zoom}")
        return {
            "zoom": zoom,
            "num_tiles": num_tiles,
            "num_successful": 0,
            "success_rate": 0.0,
            "latency_ms": None,
            "results": results,
        }

    latencies = [r["latency_ms"] for r in successful]
    sizes = [r["size_bytes"] for r in successful]

    stats = {
        "zoom": zoom,
        "num_tiles": num_tiles,
        "num_successful": len(successful),
        "success_rate": len(successful) / num_tiles,
        "latency_ms": {
            "mean": sum(latencies) / len(latencies),
            "min": min(latencies),
            "max": max(latencies),
            "p50": sorted(latencies)[len(latencies) // 2],
            "p95": sorted(latencies)[int(len(latencies) * 0.95)],
        },
        "size_bytes": {
            "mean": sum(sizes) / len(sizes),
            "min": min(sizes),
            "max": max(sizes),
        },
        "results": results,
    }

    latency_stats = cast(dict[str, float], stats["latency_ms"])
    logger.info(
        f"  Zoom {zoom}: {latency_stats['mean']:.1f}ms avg, "
        f"{latency_stats['p95']:.1f}ms p95, "
        f"{stats['success_rate']:.1%} success"
    )

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark tile generation performance")
    parser.add_argument(
        "--stac-api",
        required=True,
        help="STAC API base URL",
    )
    parser.add_argument(
        "--raster-api",
        required=True,
        help="Raster API base URL (titiler-eopf)",
    )
    parser.add_argument(
        "--collection",
        required=True,
        help="Collection ID",
    )
    parser.add_argument(
        "--item-id",
        required=True,
        help="Item ID to benchmark",
    )
    parser.add_argument(
        "--num-tiles",
        type=int,
        default=20,
        help="Number of tiles to test per zoom level (default: 20)",
    )
    parser.add_argument(
        "--zoom-levels",
        default="10,11,12",
        help="Comma-separated zoom levels to test (default: 10,11,12)",
    )
    parser.add_argument(
        "--assets",
        help="Comma-separated asset keys to visualize (e.g., b04,b03,b02)",
    )
    parser.add_argument(
        "--rescale",
        help="Rescale values (e.g., 0,3000)",
    )
    parser.add_argument(
        "--output",
        help="Output JSON file for results",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    # Parse zoom levels
    try:
        zoom_levels = [int(z.strip()) for z in args.zoom_levels.split(",")]
    except ValueError:
        logger.error(f"Invalid zoom levels: {args.zoom_levels}")
        sys.exit(1)

    # Fetch item metadata
    try:
        item = fetch_item(args.stac_api, args.collection, args.item_id)
        logger.info(f"Item found: {item['id']} in {item['collection']}")
    except Exception as e:
        logger.error(f"Failed to fetch item: {e}")
        sys.exit(1)

    # Build query parameters
    params: dict[str, Any] = {}
    if args.assets:
        params["assets"] = args.assets
    elif args.collection.startswith("sentinel-2"):
        # Default to RGB composite for S2
        params["assets"] = "SR_10m"
        params["asset_as_band"] = "true"
        params["bidx"] = "4,3,2"  # R,G,B bands from SR_10m
        logger.info("Using default S2 RGB assets: SR_10m (bands 4,3,2)")
    elif args.collection.startswith("sentinel-1"):
        # Default to VV/VH for S1
        params["assets"] = "vv,vh"
        logger.info("Using default S1 assets: vv,vh")

    if args.rescale:
        params["rescale"] = args.rescale
    elif "sentinel-2" in args.collection:
        # Default rescale for S2
        params["rescale"] = "0,3000"
        logger.info("Using default S2 rescale: 0,3000")

    logger.info(f"Query parameters: {params}")

    # Benchmark each zoom level
    all_results = []
    total_start = time.perf_counter()

    for zoom in zoom_levels:
        stats = benchmark_zoom_level(
            args.raster_api,
            args.collection,
            args.item_id,
            zoom,
            args.num_tiles,
            params,
        )
        all_results.append(stats)

    total_elapsed = time.perf_counter() - total_start

    # Calculate overall statistics
    all_successful = [r for stats in all_results for r in stats["results"] if r["success"]]
    all_latencies = [r["latency_ms"] for r in all_successful]

    summary = {
        "item_id": args.item_id,
        "collection": args.collection,
        "raster_api": args.raster_api,
        "zoom_levels": zoom_levels,
        "num_tiles_per_zoom": args.num_tiles,
        "total_tiles": len(zoom_levels) * args.num_tiles,
        "total_successful": len(all_successful),
        "overall_success_rate": len(all_successful) / (len(zoom_levels) * args.num_tiles),
        "total_duration_sec": total_elapsed,
        "overall_latency_ms": {
            "mean": sum(all_latencies) / len(all_latencies) if all_latencies else None,
            "min": min(all_latencies) if all_latencies else None,
            "max": max(all_latencies) if all_latencies else None,
            "p50": sorted(all_latencies)[len(all_latencies) // 2] if all_latencies else None,
            "p95": sorted(all_latencies)[int(len(all_latencies) * 0.95)] if all_latencies else None,
        },
        "zoom_level_results": all_results,
    }

    # Print summary
    print("\n" + "=" * 70)
    print("TILE PERFORMANCE BENCHMARK SUMMARY")
    print("=" * 70)
    print(f"Item:              {summary['item_id']}")
    print(f"Collection:        {summary['collection']}")
    print(f"Zoom levels:       {', '.join(map(str, zoom_levels))}")
    print(f"Tiles per zoom:    {args.num_tiles}")
    print(f"Total tiles:       {summary['total_tiles']}")
    print(
        f"Successful:        {summary['total_successful']} ({summary['overall_success_rate']:.1%})"
    )
    print(f"Total duration:    {summary['total_duration_sec']:.2f}s")
    print()
    if all_latencies:
        print("Overall Latency:")
        print(f"  Mean:            {summary['overall_latency_ms']['mean']:.1f}ms")
        print(f"  Median (p50):    {summary['overall_latency_ms']['p50']:.1f}ms")
        print(f"  95th percentile: {summary['overall_latency_ms']['p95']:.1f}ms")
        print(f"  Min:             {summary['overall_latency_ms']['min']:.1f}ms")
        print(f"  Max:             {summary['overall_latency_ms']['max']:.1f}ms")
    print()
    print("Per-Zoom Results:")
    for stats in all_results:
        if stats["latency_ms"]:
            print(
                f"  z{stats['zoom']:2d}: "
                f"{stats['latency_ms']['mean']:6.1f}ms avg, "
                f"{stats['latency_ms']['p95']:6.1f}ms p95, "
                f"{stats['success_rate']:5.1%} success"
            )
        else:
            print(f"  z{stats['zoom']:2d}: All tiles failed")
    print("=" * 70)

    # Save to file if requested
    if args.output:
        with open(args.output, "w") as f:
            json.dump(summary, f, indent=2)
        logger.info(f"Results saved to {args.output}")


if __name__ == "__main__":
    main()
