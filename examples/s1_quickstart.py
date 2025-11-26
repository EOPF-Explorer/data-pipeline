#!/usr/bin/env python3
"""Quick S1 GRD to GeoZarr conversion example.

Demonstrates end-to-end S1 pipeline:
1. Fetch S1 item from STAC
2. Convert to GeoZarr
3. Register in STAC catalog
4. Augment with preview links
"""

import subprocess
import sys
from pathlib import Path


def run_s1_pipeline(
    stac_url: str = "https://stac.core.eopf.eodc.eu",
    item_id: str = "S1C_IW_GRDH_1SDV_20251008T163126_20251008T163151_004473_008DBA_9AB4",
    output_dir: Path = Path("./s1_output"),
) -> int:
    """Run S1 GRD pipeline locally."""

    output_dir.mkdir(exist_ok=True)
    geozarr_path = output_dir / f"{item_id}_geozarr.zarr"

    print(f"🛰️  Processing S1 item: {item_id}")

    # Step 1: Get source URL
    print("\n1️⃣  Fetching STAC item...")
    cmd = [
        "python",
        "scripts/get_zarr_url.py",
        f"{stac_url}/collections/sentinel-1-l1-grd/items/{item_id}",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    source_url = result.stdout.strip()
    print(f"   Source: {source_url}")

    # Step 2: Convert to GeoZarr
    print("\n2️⃣  Converting to GeoZarr...")
    cmd = [
        "eopf-geozarr",
        "convert",
        source_url,
        str(geozarr_path),
        "--groups",
        "/measurements",
        "--gcp-group",
        "/conditions/gcp",
        "--spatial-chunk",
        "2048",
        "--verbose",
    ]
    subprocess.run(cmd, check=True)
    print(f"   ✓ Created: {geozarr_path}")

    # Step 3: Validate
    print("\n3️⃣  Validating GeoZarr...")
    cmd = ["eopf-geozarr", "validate", str(geozarr_path)]
    subprocess.run(cmd, check=True)
    print("   ✓ Valid GeoZarr")

    print("\n✅ S1 pipeline complete!")
    print(f"   Output: {geozarr_path}")
    print("\n   Next steps:")
    print("   - Upload to S3")
    print("   - Register in STAC catalog")
    print("   - View in titiler-eopf")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(run_s1_pipeline())
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Pipeline failed: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted", file=sys.stderr)
        sys.exit(130)
