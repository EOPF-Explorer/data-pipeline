#!/usr/bin/env python3
"""Verify the dtype and codec chain of a Zarr V3 array (local or S3).

Usage
-----
# Local path (e.g. --quick probe output or full local conversion):
    python scripts/check_zarr_codecs.py \
        codec_probe.zarr/b02

    python scripts/check_zarr_codecs.py \
        S2C_MSIL2A_20260427T101021_N0512_R022_T33UWT_20260427T151616_converted.zarr/measurements/reflectance/r10m/b02

# S3 path:
    python scripts/check_zarr_codecs.py \
        s3://esa-zarr-sentinel-explorer-s2-l2a-staging/converted/sentinel-2-l2a-staging-codecs/S2B_MSIL2A_20260427T105619_N0512_R094_T31UCU_20260427T133501.zarr/measurements/reflectance/r10m/b02 \
        --endpoint-url https://s3.de.io.cloud.ovh.net

    python scripts/check_zarr_codecs.py \
        s3://esa-zarr-sentinel-explorer-s2-l2a-staging/converted/sentinel-2-l2a-staging-codecs/S2B_MSIL2A_20260427T105619_N0512_R094_T31UCU_20260427T133501.zarr/measurements/reflectance/r10m/b02 \
        --endpoint-url https://s3.de.io.cloud.ovh.net --profile eopfexplorer
"""

import argparse
import json
import pathlib
import sys
from io import BytesIO
from typing import Any, cast
from urllib.parse import urlparse

import boto3

OVH_ENDPOINT = "https://s3.de.io.cloud.ovh.net"


def fetch_zarr_json_local(array_path: str) -> dict[str, Any]:
    p = pathlib.Path(array_path.rstrip("/")) / "zarr.json"
    if not p.exists():
        raise FileNotFoundError(f"zarr.json not found at {p}")
    return cast(dict[str, Any], json.loads(p.read_text()))


def fetch_zarr_json(
    bucket: str, key: str, endpoint_url: str, profile: str | None = None
) -> dict[str, Any]:
    session = boto3.Session(profile_name=profile)
    s3 = session.client("s3", endpoint_url=endpoint_url)
    buf = BytesIO()
    s3.download_fileobj(bucket, key, buf)
    return cast(dict[str, Any], json.loads(buf.getvalue()))


def print_codec_summary(meta: dict) -> None:
    dtype = meta.get("data_type", "unknown")
    codecs = meta.get("codecs", [])

    print(f"dtype:  {dtype}")
    print(f"codecs: {[c.get('name') for c in codecs]}")

    # Walk into sharding_indexed inner codecs if present
    for codec in codecs:
        if codec.get("name") == "sharding_indexed":
            inner = codec.get("configuration", {}).get("codecs", [])
            print(f"  └─ inner codecs: {[c.get('name') for c in inner]}")

    has_scale_offset = any(c.get("name") == "scale_offset" for c in codecs)
    has_cast_value = any(c.get("name") == "cast_value" for c in codecs)
    print()
    print(f"scale_offset codec: {'✓ YES' if has_scale_offset else '✗ NO'}")
    print(f"cast_value codec:   {'✓ YES' if has_cast_value else '✗ NO'}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check Zarr V3 array dtype and codecs (local path or s3://)."
    )
    parser.add_argument(
        "array_url",
        help="Path to the Zarr array: local (path/to/array) or S3 (s3://bucket/path/to/array)",
    )
    parser.add_argument(
        "--endpoint-url", default=OVH_ENDPOINT, help=f"S3 endpoint URL (default: {OVH_ENDPOINT})"
    )
    parser.add_argument("--profile", default=None, help="AWS CLI profile name (~/.aws/credentials)")
    args = parser.parse_args()

    parsed = urlparse(args.array_url)

    if parsed.scheme == "s3":
        bucket = parsed.netloc
        key = parsed.path.lstrip("/").rstrip("/") + "/zarr.json"
        print(f"Checking: s3://{bucket}/{key}\n")
        meta = fetch_zarr_json(bucket, key, args.endpoint_url, args.profile)
    else:
        local_path = pathlib.Path(args.array_url)
        print(f"Checking: {local_path / 'zarr.json'}\n")
        meta = fetch_zarr_json_local(args.array_url)

    print_codec_summary(meta)
    return 0


if __name__ == "__main__":
    sys.exit(main())
