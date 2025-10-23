#!/usr/bin/env python3
"""Validate GeoZarr compliance and generate quality metrics.

Validates:
- GeoZarr spec 0.4 compliance (via eopf-geozarr CLI)
- STAC item spec compliance (via pystac)
- TileMatrixSet OGC compliance (via morecantile)
- CF-conventions compliance (via cf-xarray)
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)


def validate_geozarr(dataset_path: str, verbose: bool = False) -> dict:
    """Run eopf-geozarr validate and parse results.

    Returns:
        dict with validation status and any errors/warnings
    """
    logger.info(f"Validating: {dataset_path}")

    cmd = ["eopf-geozarr", "validate", dataset_path]
    if verbose:
        cmd.append("--verbose")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,  # 5 minute timeout
        )

        validation_result = {
            "valid": result.returncode == 0,
            "exit_code": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }

        if result.returncode == 0:
            logger.info("✅ Validation passed")
        else:
            logger.error(f"❌ Validation failed (exit code {result.returncode})")
            if result.stderr:
                logger.error(f"Errors:\n{result.stderr}")

        return validation_result

    except subprocess.TimeoutExpired:
        logger.error("❌ Validation timeout (>5 minutes)")
        return {
            "valid": False,
            "exit_code": -1,
            "error": "Validation timeout",
        }
    except Exception as e:
        logger.error(f"❌ Validation error: {e}")
        return {
            "valid": False,
            "exit_code": -1,
            "error": str(e),
        }


def validate_stac_item(item_path: str | Path) -> dict:
    """Validate STAC item against spec.

    Args:
        item_path: Path to STAC item JSON file

    Returns:
        dict with validation status
    """
    try:
        import pystac

        logger.info(f"Validating STAC item: {item_path}")
        item = pystac.Item.from_file(str(item_path))
        item.validate()

        logger.info("✅ STAC item valid")
        return {"valid": True, "item_id": item.id, "collection": item.collection_id}

    except Exception as e:
        logger.error(f"❌ STAC validation failed: {e}")
        return {"valid": False, "error": str(e)}


def validate_tile_matrix_set(zarr_path: str) -> dict:
    """Validate TileMatrixSet against OGC spec.

    Args:
        zarr_path: Path to GeoZarr dataset

    Returns:
        dict with validation status
    """
    try:
        import zarr
        from morecantile import TileMatrixSet

        logger.info("Validating TileMatrixSet...")
        store = zarr.open(zarr_path, mode="r")
        attrs = store.attrs.asdict()

        if "tile_matrix_set" not in attrs:
            logger.warning("⚠️  No tile_matrix_set found in attributes")
            return {"valid": False, "error": "Missing tile_matrix_set attribute"}

        # Parse and validate TMS
        tms = TileMatrixSet(**attrs["tile_matrix_set"])
        # morecantile validates on instantiation

        logger.info("✅ TileMatrixSet valid")
        return {
            "valid": True,
            "tms_id": tms.id,
            "crs": str(tms.crs),
            "num_levels": len(tms.tileMatrices),
        }

    except Exception as e:
        logger.error(f"❌ TMS validation failed: {e}")
        return {"valid": False, "error": str(e)}


def validate_cf_conventions(zarr_path: str) -> dict:
    """Validate CF-conventions compliance.

    Args:
        zarr_path: Path to GeoZarr dataset

    Returns:
        dict with validation status
    """
    try:
        import cf_xarray  # noqa: F401
        import xarray as xr

        logger.info("Validating CF-conventions...")
        ds = xr.open_zarr(zarr_path, consolidated=False)

        # Attempt CF decoding (raises if non-compliant)
        ds.cf.decode()

        # Check for required CF attributes
        issues = []
        for var_name in ds.data_vars:
            var = ds[var_name]
            if "standard_name" not in var.attrs and "long_name" not in var.attrs:
                issues.append(f"Variable {var_name} missing standard_name/long_name")

        if issues:
            logger.warning(f"⚠️  CF compliance warnings: {len(issues)}")
            for issue in issues[:5]:  # Show first 5
                logger.warning(f"  - {issue}")
            return {"valid": True, "warnings": issues}

        logger.info("✅ CF-conventions valid")
        return {"valid": True}

    except Exception as e:
        logger.error(f"❌ CF validation failed: {e}")
        return {"valid": False, "error": str(e)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate GeoZarr compliance")
    parser.add_argument("dataset_path", help="Path to GeoZarr dataset (S3 or local)")
    parser.add_argument("--item-id", help="STAC item ID for tracking")
    parser.add_argument("--stac-item", help="Path to STAC item JSON for validation")
    parser.add_argument("--output", help="Output JSON file path")
    parser.add_argument("--skip-cf", action="store_true", help="Skip CF-conventions check")
    parser.add_argument("--skip-tms", action="store_true", help="Skip TileMatrixSet check")
    parser.add_argument("--verbose", action="store_true", help="Verbose validation output")
    args = parser.parse_args()

    # Run all validations
    validations = {}

    # 1. GeoZarr spec compliance (via eopf-geozarr CLI)
    validations["geozarr"] = validate_geozarr(args.dataset_path, args.verbose)

    # 2. STAC item validation (if provided)
    if args.stac_item:
        validations["stac_item"] = validate_stac_item(args.stac_item)

    # 3. TileMatrixSet validation
    if not args.skip_tms:
        validations["tile_matrix_set"] = validate_tile_matrix_set(args.dataset_path)

    # 4. CF-conventions validation
    if not args.skip_cf:
        validations["cf_conventions"] = validate_cf_conventions(args.dataset_path)

    # Determine overall validity
    all_valid = all(v.get("valid", False) for v in validations.values())

    # Build complete result
    result = {
        "timestamp": datetime.now(UTC).isoformat(),
        "dataset_path": args.dataset_path,
        "item_id": args.item_id,
        "valid": all_valid,
        "validations": validations,
    }

    # Write to file if requested
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(result, f, indent=2)
        logger.info(f"Results written to: {output_path}")

    # Print summary
    logger.info("\n" + "=" * 60)
    logger.info(f"Dataset: {args.dataset_path}")
    logger.info(f"Overall Valid: {all_valid}")
    for check_name, check_result in validations.items():
        status = "✅" if check_result.get("valid") else "❌"
        logger.info(f"  {status} {check_name}: {check_result.get('valid')}")
    if args.item_id:
        logger.info(f"Item ID: {args.item_id}")
    logger.info("=" * 60 + "\n")

    # Output JSON for workflow
    print(json.dumps(result, indent=2))

    # Exit with validation status
    sys.exit(0 if all_valid else 1)


if __name__ == "__main__":
    main()
