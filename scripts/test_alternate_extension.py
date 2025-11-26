#!/usr/bin/env python3
"""Test script to demonstrate alternate S3 URLs in STAC items."""

import json
from pathlib import Path

from pystac import Item


def https_to_s3(https_url: str, endpoint: str) -> str | None:
    """Convert https:// URL back to s3:// URL if it matches the S3 endpoint pattern."""
    if not https_url.startswith("https://"):
        return None

    from urllib.parse import urlparse

    endpoint_host = urlparse(endpoint).netloc or urlparse(endpoint).path
    parsed = urlparse(https_url)

    if endpoint_host in parsed.netloc:
        bucket = parsed.netloc.replace(f".{endpoint_host}", "")
        return f"s3://{bucket}{parsed.path}"

    return None


def add_alternate_s3_to_item(item_dict: dict, s3_endpoint: str) -> dict:
    """Add alternate S3 URLs to a STAC item dictionary."""
    # Add extensions
    extensions = [
        "https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json",
        "https://stac-extensions.github.io/storage/v2.0.0/schema.json",
    ]

    if "stac_extensions" not in item_dict:
        item_dict["stac_extensions"] = []

    for ext in extensions:
        if ext not in item_dict["stac_extensions"]:
            item_dict["stac_extensions"].append(ext)

    # Extract region from endpoint
    from urllib.parse import urlparse

    endpoint_host = urlparse(s3_endpoint).netloc or urlparse(s3_endpoint).path
    region = "unknown"
    if ".de." in endpoint_host:
        region = "de"
    elif ".gra." in endpoint_host:
        region = "gra"
    elif ".sbg." in endpoint_host:
        region = "sbg"

    # Add alternate to each asset
    modified_count = 0
    for asset_key, asset in item_dict.get("assets", {}).items():
        href = asset.get("href", "")

        # Skip non-HTTPS URLs or thumbnails
        if not href.startswith("https://"):
            continue
        if "thumbnail" in asset.get("roles", []):
            continue

        # Convert to S3 URL
        s3_url = https_to_s3(href, s3_endpoint)
        if not s3_url:
            continue

        # Add alternate
        asset["alternate"] = {
            "s3": {
                "href": s3_url,
                "storage:platform": "OVHcloud",
                "storage:region": region,
                "storage:requester_pays": False,
            }
        }
        modified_count += 1

    print(f"✅ Added S3 alternates to {modified_count} asset(s)")
    return item_dict


def main():
    """Test the alternate extension with the sample JSON."""
    # Read the sample JSON
    sample_file = Path(__file__).parent / "S2A_MSIL2A_20250831T103701_N0511_R008_T31TFL_20250831T145420.json"

    if not sample_file.exists():
        print(f"❌ Sample file not found: {sample_file}")
        return 1

    print(f"📖 Reading sample file: {sample_file.name}")
    with open(sample_file) as f:
        item_dict = json.load(f)

    # Apply the transformation
    s3_endpoint = "https://s3.de.io.cloud.ovh.net"
    print(f"🔧 Adding alternate S3 URLs (endpoint: {s3_endpoint})")
    modified_item = add_alternate_s3_to_item(item_dict, s3_endpoint)

    # Show a sample asset with alternate
    print("\n📋 Sample asset with alternate:")
    for asset_key in ["AOT_10m", "reflectance"]:
        if asset_key in modified_item["assets"]:
            asset = modified_item["assets"][asset_key]
            print(f"\n  Asset: {asset_key}")
            print(f"  HTTPS href: {asset['href']}")
            if "alternate" in asset:
                s3_alt = asset["alternate"]["s3"]
                print(f"  S3 href: {s3_alt['href']}")
                print(f"  Storage platform: {s3_alt['storage:platform']}")
                print(f"  Storage region: {s3_alt['storage:region']}")
            break

    # Show extensions
    print("\n📚 STAC Extensions:")
    for ext in modified_item["stac_extensions"]:
        if "alternate" in ext or "storage" in ext:
            print(f"  ✨ {ext}")

    # Write output
    output_file = sample_file.with_name(f"{sample_file.stem}_with_alternate.json")
    with open(output_file, "w") as f:
        json.dump(modified_item, f, indent=2)
    print(f"\n💾 Saved modified item to: {output_file.name}")

    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
