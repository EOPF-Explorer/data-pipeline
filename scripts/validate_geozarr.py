#!/usr/bin/env python3
"""Validate GeoZarr compliance and generate quality metrics."""

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


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate GeoZarr compliance")
    parser.add_argument("dataset_path", help="Path to GeoZarr dataset (S3 or local)")
    parser.add_argument("--item-id", help="STAC item ID for tracking")
    parser.add_argument("--output", help="Output JSON file path")
    parser.add_argument("--verbose", action="store_true", help="Verbose validation output")
    args = parser.parse_args()

    # Run validation
    validation = validate_geozarr(args.dataset_path, args.verbose)

    # Build complete result
    result = {
        "timestamp": datetime.now(UTC).isoformat(),
        "dataset_path": args.dataset_path,
        "item_id": args.item_id,
        "validation": validation,
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
    logger.info(f"Valid: {validation['valid']}")
    if args.item_id:
        logger.info(f"Item ID: {args.item_id}")
    logger.info("=" * 60 + "\n")

    # Output JSON for workflow
    print(json.dumps(result, indent=2))

    # Exit with validation status
    sys.exit(0 if validation["valid"] else 1)


if __name__ == "__main__":
    main()
