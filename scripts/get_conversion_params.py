#!/usr/bin/env python3
"""Generate GeoZarr conversion parameters from collection registry.

This script exports conversion parameters (groups, flags, chunks) for
different satellite collections, enabling the workflow template to use
data-driven configuration instead of hard-coded bash conditionals.

Usage:
    python3 get_conversion_params.py --collection sentinel-1-l1-grd
    python3 get_conversion_params.py --collection sentinel-2-l2a --format json
    python3 get_conversion_params.py --collection sentinel-2-l2a --param groups
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, cast

# Import collection configs from augment_stac_item
# In production, this would be a shared module
_COLLECTION_CONFIGS: dict[str, dict[str, Any]] = {
    "sentinel-1-l1-grd": {
        "pattern": "sentinel-1-l1-grd*",
        "conversion": {
            "groups": "/measurements",
            "extra_flags": "--gcp-group /conditions/gcp",
            "spatial_chunk": 4096,  # Increased from 2048 for faster I/O
            "tile_width": 512,
        },
    },
    "sentinel-2-l2a": {
        "pattern": "sentinel-2-l2a*",
        "conversion": {
            "groups": "/quality/l2a_quicklook/r10m",
            "extra_flags": "--crs-groups /quality/l2a_quicklook/r10m",
            "spatial_chunk": 4096,
            "tile_width": 512,
        },
    },
}

_DEFAULT_COLLECTION = "sentinel-2-l2a"


def _match_collection_config(collection_id: str) -> dict[str, Any] | None:
    """Match collection ID to configuration using pattern matching."""
    for _key, config in _COLLECTION_CONFIGS.items():
        # mypy needs help understanding .items() returns dict values
        cfg = cast(dict[str, Any], config)  # type: ignore[redundant-cast]
        pattern = str(cfg.get("pattern", ""))
        if collection_id.startswith(pattern.rstrip("*")):
            return cfg
    return None


def get_conversion_params(collection_id: str) -> dict[str, Any]:
    """Get conversion parameters for collection.

    Args:
        collection_id: Collection identifier (e.g., sentinel-1-l1-grd-dp-test)

    Returns:
        Dict of conversion parameters (groups, extra_flags, spatial_chunk, tile_width)

    Raises:
        ValueError: If collection not found in registry
    """
    config = _match_collection_config(collection_id)
    if not config:
        # Fallback to default - mypy needs help with dict.get() return type
        default_config = cast(dict[str, Any] | None, _COLLECTION_CONFIGS.get(_DEFAULT_COLLECTION))  # type: ignore[redundant-cast]
        if not default_config:
            raise ValueError(f"No config for collection {collection_id}")
        config = default_config

    return cast(dict[str, Any], config.get("conversion", {}))


def main(argv: list[str] | None = None) -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Get GeoZarr conversion parameters from collection registry"
    )
    parser.add_argument(
        "--collection",
        required=True,
        help="Collection ID (e.g., sentinel-1-l1-grd, sentinel-2-l2a-dp-test)",
    )
    parser.add_argument(
        "--format",
        choices=["shell", "json"],
        default="shell",
        help="Output format (shell vars or JSON)",
    )
    parser.add_argument(
        "--param",
        choices=["groups", "extra_flags", "spatial_chunk", "tile_width"],
        help="Get single parameter (for shell scripts)",
    )

    args = parser.parse_args(argv)

    try:
        params = get_conversion_params(args.collection)
    except ValueError as exc:
        # Use print for CLI output, not logging
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    if args.param:
        # Output single parameter (for shell variable assignment)
        value = params.get(args.param, "")
        print(value)
    elif args.format == "json":
        # Output JSON (for parsing with jq)
        print(json.dumps(params, indent=2))
    else:
        # Output shell variables (for eval/source)
        print(f"ZARR_GROUPS='{params.get('groups', '')}'")
        print(f"EXTRA_FLAGS='{params.get('extra_flags', '')}'")
        print(f"CHUNK={params.get('spatial_chunk', 4096)}")
        print(f"TILE_WIDTH={params.get('tile_width', 512)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
