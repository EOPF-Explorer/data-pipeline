#!/usr/bin/env python3
"""Test script to verify the new S3 gateway format with alternate extension."""

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
    # This is for backwards compatibility
    if ".s3." in parsed.netloc or "s3." in parsed.netloc:
        # Try to extract bucket name (everything before .s3.)
        parts = parsed.netloc.split(".s3.")
        if len(parts) == 2:
            bucket = parts[0]
            return f"s3://{bucket}{parsed.path}"

    return None


def main() -> int:
    """Test the gateway format conversions."""
    print("Testing S3 Gateway Format Conversions")
    print("=" * 70)

    # Test cases
    test_cases = [
        {
            "name": "Simple path",
            "s3": "s3://esa-zarr-sentinel-explorer-fra/tests-output/file.zarr",
            "expected_https": "https://s3.explorer.eopf.copernicus.eu/esa-zarr-sentinel-explorer-fra/tests-output/file.zarr",
        },
        {
            "name": "Deep nested path",
            "s3": "s3://esa-zarr-sentinel-explorer-fra/tests-output/sentinel-2-l2a-staging/S2A_MSIL2A_20250831T103701_N0511_R008_T31TFL_20250831T145420.zarr/quality/atmosphere/r10m/aot",
            "expected_https": "https://s3.explorer.eopf.copernicus.eu/esa-zarr-sentinel-explorer-fra/tests-output/sentinel-2-l2a-staging/S2A_MSIL2A_20250831T103701_N0511_R008_T31TFL_20250831T145420.zarr/quality/atmosphere/r10m/aot",
        },
        {
            "name": "Reflectance asset",
            "s3": "s3://esa-zarr-sentinel-explorer-fra/tests-output/sentinel-2-l2a-staging/S2A_MSIL2A_20250831T103701_N0511_R008_T31TFL_20250831T145420.zarr/measurements/reflectance",
            "expected_https": "https://s3.explorer.eopf.copernicus.eu/esa-zarr-sentinel-explorer-fra/tests-output/sentinel-2-l2a-staging/S2A_MSIL2A_20250831T103701_N0511_R008_T31TFL_20250831T145420.zarr/measurements/reflectance",
        },
    ]

    print("\n1. Testing S3 to HTTPS conversion")
    print("-" * 70)
    all_passed = True
    for test in test_cases:
        result = s3_to_https(test["s3"])
        passed = result == test["expected_https"]
        all_passed = all_passed and passed
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"\n{status} - {test['name']}")
        print(f"  Input:    {test['s3']}")
        print(f"  Expected: {test['expected_https']}")
        print(f"  Got:      {result}")

    print("\n\n2. Testing HTTPS to S3 conversion (round-trip)")
    print("-" * 70)
    for test in test_cases:
        https_url = test["expected_https"]
        s3_result = https_to_s3(https_url)
        passed = s3_result == test["s3"]
        all_passed = all_passed and passed
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"\n{status} - {test['name']}")
        print(f"  Input:    {https_url}")
        print(f"  Expected: {test['s3']}")
        print(f"  Got:      {s3_result}")

    # Test backwards compatibility with old format
    print("\n\n3. Testing backwards compatibility with old S3 format")
    print("-" * 70)
    old_format_url = (
        "https://esa-zarr-sentinel-explorer-fra.s3.de.io.cloud.ovh.net/tests-output/file.zarr"
    )
    expected_s3 = "s3://esa-zarr-sentinel-explorer-fra/tests-output/file.zarr"
    old_result = https_to_s3(old_format_url)
    passed = old_result == expected_s3
    all_passed = all_passed and passed
    status = "✅ PASS" if passed else "❌ FAIL"
    print(f"\n{status} - Old S3 subdomain format")
    print(f"  Input:    {old_format_url}")
    print(f"  Expected: {expected_s3}")
    print(f"  Got:      {old_result}")

    # Summary
    print("\n" + "=" * 70)
    if all_passed:
        print("✅ All tests PASSED!")
        return 0
    else:
        print("❌ Some tests FAILED!")
        return 1


if __name__ == "__main__":
    import sys

    sys.exit(main())
