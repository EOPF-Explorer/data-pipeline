#!/usr/bin/env python3
"""Test script to demonstrate the complete workflow with new gateway format and alternate extension."""

import json
from pathlib import Path
from urllib.parse import urlparse


def s3_to_https(s3_url: str, gateway_url: str = "https://s3.explorer.eopf.copernicus.eu") -> str:
    """Convert s3:// URL to https:// using S3 gateway."""
    if not s3_url.startswith("s3://"):
        return s3_url

    parsed = urlparse(s3_url)
    bucket = parsed.netloc
    path = parsed.path

    gateway_base = gateway_url.rstrip("/")
    return f"{gateway_base}/{bucket}{path}"


def https_to_s3(
    https_url: str, gateway_url: str = "https://s3.explorer.eopf.copernicus.eu"
) -> str | None:
    """Convert https:// URL back to s3:// URL."""
    if not https_url.startswith("https://"):
        return None

    parsed = urlparse(https_url)
    gateway_parsed = urlparse(gateway_url)
    gateway_host = gateway_parsed.netloc

    # Check if URL matches the new gateway format: gateway-host/bucket/path
    if parsed.netloc == gateway_host:
        # Extract bucket from path (first component)
        path_parts = parsed.path.lstrip("/").split("/", 1)
        if len(path_parts) >= 1:
            bucket = path_parts[0]
            remaining_path = "/" + path_parts[1] if len(path_parts) > 1 else ""
            return f"s3://{bucket}{remaining_path}"

    # Check if URL matches old S3 endpoint pattern: bucket.endpoint-host/path
    if ".s3." in parsed.netloc or "s3." in parsed.netloc:
        parts = parsed.netloc.split(".s3.")
        if len(parts) == 2:
            bucket = parts[0]
            return f"s3://{bucket}{parsed.path}"

    return None


def process_item_with_gateway(item_dict: dict, s3_endpoint: str) -> dict:
    """Process STAC item to use new gateway format and add alternate extension."""

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
    endpoint_host = urlparse(s3_endpoint).netloc or urlparse(s3_endpoint).path
    region = "unknown"
    if ".de." in endpoint_host:
        region = "de"
    elif ".gra." in endpoint_host:
        region = "gra"
    elif ".sbg." in endpoint_host:
        region = "sbg"

    # Process assets
    processed_count = 0
    for asset_key, asset in item_dict.get("assets", {}).items():
        href = asset.get("href", "")

        # Skip non-HTTPS URLs or thumbnails
        if not href.startswith("https://"):
            continue
        if "thumbnail" in asset.get("roles", []):
            continue

        # Convert old format to new gateway format if needed
        old_format_patterns = [".s3.de.", ".s3.gra.", ".s3.sbg."]
        is_old_format = any(pattern in href for pattern in old_format_patterns)

        if is_old_format:
            # Convert to S3 first, then to new gateway format
            s3_url = https_to_s3(href)
            if s3_url:
                # Update href to use new gateway
                new_href = s3_to_https(s3_url)
                asset["href"] = new_href
                print(f"  🔄 Converted {asset_key}:")
                print(f"     From: {href}")
                print(f"     To:   {new_href}")

        # Add alternate S3 URL
        s3_url = https_to_s3(asset["href"])
        if s3_url:
            asset["alternate"] = {
                "s3": {
                    "href": s3_url,
                    "storage:platform": "OVHcloud",
                    "storage:region": region,
                    "storage:requester_pays": False,
                }
            }
            processed_count += 1

    # Update store link
    for link in item_dict.get("links", []):
        if link.get("rel") == "store":
            old_href = link["href"]
            if ".s3." in old_href:
                s3_url = https_to_s3(old_href)
                if s3_url:
                    new_href = s3_to_https(s3_url)
                    link["href"] = new_href
                    print("  🔄 Converted store link:")
                    print(f"     From: {old_href}")
                    print(f"     To:   {new_href}")

    print(f"\n✅ Processed {processed_count} assets with gateway format and alternate S3 URLs")
    return item_dict


def main() -> int:
    """Test the complete workflow."""
    # Read the sample JSON
    sample_file = (
        Path(__file__).parent.parent
        / "stac"
        / "S2A_MSIL2A_20250831T103701_N0511_R008_T31TFL_20250831T145420.json"
    )

    if not sample_file.exists():
        print(f"❌ Sample file not found: {sample_file}")
        return 1

    print("=" * 80)
    print("Complete Workflow Test: New Gateway Format + Alternate Extension")
    print("=" * 80)
    print(f"\n📖 Reading sample file: {sample_file.name}\n")

    with open(sample_file) as f:
        item_dict = json.load(f)

    # Show original format
    print("📋 Original asset format (first data asset):")
    for asset_key in ["AOT_10m", "reflectance"]:
        if asset_key in item_dict["assets"]:
            asset = item_dict["assets"][asset_key]
            print(f"  Asset: {asset_key}")
            print(f"  href: {asset['href']}")
            break

    # Process with new gateway format
    print("\n🔧 Processing with new gateway format...\n")
    s3_endpoint = "https://s3.de.io.cloud.ovh.net"
    modified_item = process_item_with_gateway(item_dict, s3_endpoint)

    # Show results
    print("\n📋 New asset format (after processing):")
    for asset_key in ["AOT_10m", "reflectance"]:
        if asset_key in modified_item["assets"]:
            asset = modified_item["assets"][asset_key]
            print(f"\n  Asset: {asset_key}")
            print(f"  HTTPS href (gateway): {asset['href']}")
            if "alternate" in asset:
                s3_alt = asset["alternate"]["s3"]
                print(f"  S3 alternate href: {s3_alt['href']}")
                print(f"  Storage platform: {s3_alt['storage:platform']}")
                print(f"  Storage region: {s3_alt['storage:region']}")
            break

    # Show store link
    print("\n📋 Store link:")
    for link in modified_item.get("links", []):
        if link.get("rel") == "store":
            print(f"  href: {link['href']}")
            break

    # Show extensions
    print("\n📚 STAC Extensions (new ones added):")
    for ext in modified_item["stac_extensions"]:
        if "alternate" in ext or "storage" in ext:
            print(f"  ✨ {ext}")

    # Write output
    output_file = sample_file.with_name(f"{sample_file.stem}_new_gateway.json")
    with open(output_file, "w") as f:
        json.dump(modified_item, f, indent=2)
    print(f"\n💾 Saved modified item to: {output_file.name}")

    print("\n" + "=" * 80)
    print("✅ Complete workflow test PASSED!")
    print("=" * 80)
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
