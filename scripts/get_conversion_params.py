#!/usr/bin/env python3
"""GeoZarr conversion parameters for satellite collections.

Provides conversion parameters (groups, flags, chunks) for different
satellite collections. Supports Sentinel-1 and Sentinel-2 with simple
prefix matching.

Usage:
    python3 get_conversion_params.py --collection sentinel-1-l1-grd
    python3 get_conversion_params.py --collection sentinel-2-l2a --format json
    python3 get_conversion_params.py --collection sentinel-2-l2a --param groups
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

# Conversion parameters by mission
CONFIGS: dict[str, dict[str, Any]] = {
    "sentinel-1": {
        "groups": "/measurements",
        "extra_flags": "--gcp-group /conditions/gcp",
        "spatial_chunk": 4096,
        "tile_width": 512,
        "enable_sharding": False,
    },
    "sentinel-2": {
        "groups": [
            "/measurements/reflectance/r10m",
            "/measurements/reflectance/r20m",
            "/measurements/reflectance/r60m",
            "/quality/l2a_quicklook/r10m",
        ],
        "extra_flags": "--crs-groups /conditions/geometry",
        "spatial_chunk": 1024,
        "tile_width": 256,
        "enable_sharding": True,
    },
}


def get_conversion_params(collection_id: str) -> dict[str, Any]:
    """Get conversion parameters for collection. Defaults to Sentinel-2 if unrecognized."""
    parts = collection_id.lower().split("-")
    if len(parts) >= 2:
        prefix = f"{parts[0]}-{parts[1]}"
        if prefix in CONFIGS:
            return CONFIGS[prefix]
    return CONFIGS["sentinel-2"]


def main(argv: list[str] | None = None) -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Get GeoZarr conversion parameters for satellite collections"
    )
    parser.add_argument(
        "--collection",
        required=True,
        help="Collection ID (e.g., sentinel-1-l1-grd, sentinel-2-l2a)",
    )
    parser.add_argument(
        "--format",
        choices=["shell", "json"],
        default="shell",
        help="Output format (shell vars or JSON)",
    )
    parser.add_argument(
        "--param",
        choices=["groups", "extra_flags", "spatial_chunk", "tile_width", "enable_sharding"],
        help="Get single parameter (for shell scripts)",
    )

    args = parser.parse_args(argv)
    params = get_conversion_params(args.collection)

    if args.param:
        # Output single parameter (for shell variable assignment)
        value = params.get(args.param, "")
        # Convert boolean to shell-friendly format
        if isinstance(value, bool):
            print("true" if value else "false")
        else:
            print(value if value is not None else "")
    elif args.format == "json":
        # Output JSON (for parsing with jq)
        print(json.dumps(params, indent=2))
    else:
        # Output shell variables (for eval/source)
        print(f"ZARR_GROUPS='{params['groups']}'")
        print(f"EXTRA_FLAGS='{params['extra_flags']}'")
        print(f"CHUNK={params['spatial_chunk']}")
        print(f"TILE_WIDTH={params['tile_width']}")
        print(f"ENABLE_SHARDING={'true' if params['enable_sharding'] else 'false'}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
