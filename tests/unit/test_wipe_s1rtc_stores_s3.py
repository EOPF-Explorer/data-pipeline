"""Unit tests for scripts/wipe_s1rtc_stores_s3.py (A1 of the geoid-fix rollout).

Covers the two pure, testable functions:
- backup_key(): maps a source object key to the backup prefix
- _chunk(): splits a list into batches of a given size (delete_objects limit)
No network; no boto3 needed for these tests.
"""

from __future__ import annotations

import sys
from pathlib import Path

scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

import wipe_s1rtc_stores_s3 as w  # noqa: E402

BUCKET = "esa-zarr-sentinel-explorer-fra"
STORE_PREFIX = "tests-output/sentinel-1-grd-rtc-staging"
BACKUP_PREFIX = "backups/s1-306-geoid-20260626"


def test_backup_key_strips_store_prefix_and_maps_to_tile():
    source_key = f"{STORE_PREFIX}/s1-rtc-30TWM.zarr/r10m/vv/c0"
    expected = f"{BACKUP_PREFIX}/30TWM/r10m/vv/c0"
    assert w.backup_key(source_key, "30TWM", STORE_PREFIX, BACKUP_PREFIX) == expected


def test_backup_key_preserves_nested_path():
    source_key = f"{STORE_PREFIX}/s1-rtc-31TCG.zarr/.zattrs"
    expected = f"{BACKUP_PREFIX}/31TCG/.zattrs"
    assert w.backup_key(source_key, "31TCG", STORE_PREFIX, BACKUP_PREFIX) == expected


def test_backup_key_root_zarr_json():
    source_key = f"{STORE_PREFIX}/s1-rtc-32TLP.zarr/zarr.json"
    expected = f"{BACKUP_PREFIX}/32TLP/zarr.json"
    assert w.backup_key(source_key, "32TLP", STORE_PREFIX, BACKUP_PREFIX) == expected


def test_chunk_splits_evenly():
    items = list(range(10))
    chunks = list(w._chunk(items, 3))
    assert chunks == [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]


def test_chunk_exact_multiple():
    items = list(range(6))
    chunks = list(w._chunk(items, 3))
    assert chunks == [[0, 1, 2], [3, 4, 5]]


def test_chunk_empty():
    assert list(w._chunk([], 1000)) == []


def test_chunk_smaller_than_size():
    items = ["a", "b"]
    assert list(w._chunk(items, 1000)) == [["a", "b"]]
