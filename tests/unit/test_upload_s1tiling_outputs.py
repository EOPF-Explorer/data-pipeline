"""Unit tests for scripts/upload_s1tiling_outputs.py.

Uses a real local fsspec filesystem (no S3) to exercise the upload + verification
end to end, mirroring tests/unit/test_ingest_v1_s1_rtc.py::test_put_tree_*.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import fsspec
from upload_s1tiling_outputs import (
    collect_local_tifs,
    s1_output_prefix,
    upload_outputs,
)

TILE = "31TCH"
ORBIT = "descending"
DATE = "2025-02-01"

# Representative s1tiling output: 3 acquisitions x (vv, vh) GammaNaughtRTC + BorderMask
# = 12 acquisition tifs, plus 3 GAMMA_AREA condition tifs = 15 files.
_ACQ_NAMES = [
    f"s1a_{TILE}_{pol}_DES_{orb}_{stamp}_GammaNaughtRTC{suffix}.tif"
    for (orb, stamp) in [
        ("008", "20250210t060920"),
        ("037", "20250212t055301"),
        ("110", "20250205t060110"),
    ]
    for pol in ("vv", "vh")
    for suffix in ("", "_BorderMask")
]
_GAMMA_NAMES = [f"GAMMA_AREA_{TILE}_{orb}.tif" for orb in ("008", "037", "110")]


def _make_inputs(tmp_path: Path) -> Path:
    """Create data_out/<tile>/*.tif and data_gamma_area/*.tif with unique content."""
    data_dir = tmp_path / "data"
    acq_dir = data_dir / "data_out" / TILE
    gamma_dir = data_dir / "data_gamma_area"
    acq_dir.mkdir(parents=True)
    gamma_dir.mkdir(parents=True)
    for i, name in enumerate(_ACQ_NAMES):
        (acq_dir / name).write_bytes(b"a" * (100 + i))  # distinct sizes
    for i, name in enumerate(_GAMMA_NAMES):
        (gamma_dir / name).write_bytes(b"g" * (50 + i))
    return data_dir


def test_s1_output_prefix() -> None:
    assert (
        s1_output_prefix("mybucket", TILE, ORBIT, DATE)
        == "s3://mybucket/s1tiling-output/31TCH/descending/2025-02-01/"
    )


def test_collect_local_tifs_finds_acquisitions_and_gamma(tmp_path) -> None:
    data_dir = _make_inputs(tmp_path)
    names = {p.name for p in collect_local_tifs(data_dir, TILE)}
    assert names == set(_ACQ_NAMES) | set(_GAMMA_NAMES)
    assert len(names) == 15


def test_upload_outputs_uploads_flat_and_verifies(tmp_path) -> None:
    """All 15 files land flat under the canonical prefix; returns 0."""
    data_dir = _make_inputs(tmp_path)
    bucket_root = tmp_path / "bucket"
    bucket_root.mkdir()
    fs = fsspec.filesystem("file")

    rc = upload_outputs(fs, data_dir, TILE, ORBIT, DATE, str(bucket_root))
    assert rc == 0

    dest = bucket_root / "s1tiling-output" / TILE / ORBIT / DATE
    landed = {p.name for p in dest.glob("*.tif")}
    assert landed == set(_ACQ_NAMES) | set(_GAMMA_NAMES)
    # Flat: no nested subdirectories were created under dest.
    assert not [p for p in dest.iterdir() if p.is_dir()]
    # Sizes match (content preserved).
    for f in collect_local_tifs(data_dir, TILE):
        assert (dest / f.name).stat().st_size == f.stat().st_size


def test_upload_outputs_no_files_returns_0_clean_skip(tmp_path) -> None:
    """No GeoTIFFs = no S1 coverage for this tile/orbit/day (not an error): exit 0 so the
    empty prefix flows to ingest, which no-ops it (exit 2). Returning 1 here would re-fail
    the workflow on a legitimately empty tile (the 30TXT/31TGJ descending case)."""
    data_dir = tmp_path / "data"
    (data_dir / "data_out" / TILE).mkdir(parents=True)
    (data_dir / "data_gamma_area").mkdir(parents=True)
    fs = fsspec.filesystem("file")
    rc = upload_outputs(fs, data_dir, TILE, ORBIT, DATE, str(tmp_path / "bucket"))
    assert rc == 0
    # nothing was written to the destination
    assert not (tmp_path / "bucket").exists() or not any((tmp_path / "bucket").rglob("*.tif"))


def test_upload_outputs_verify_mismatch_returns_1(tmp_path) -> None:
    """If S3 ends up missing a file (partial upload), verification fails -> exit 1."""
    data_dir = _make_inputs(tmp_path)
    fs = MagicMock()
    fs.put_file.return_value = None
    # Simulate only the first 3 files surviving on S3 (the rclone failure mode).
    survivors = collect_local_tifs(data_dir, TILE)[:3]
    fs.ls.return_value = [
        {"name": f"bucket/s1tiling-output/{TILE}/{ORBIT}/{DATE}/{f.name}", "size": f.stat().st_size}
        for f in survivors
    ]
    rc = upload_outputs(fs, data_dir, TILE, ORBIT, DATE, "bucket")
    assert rc == 1
    # It still attempted to upload all 15.
    assert fs.put_file.call_count == 15


def test_upload_outputs_verify_size_mismatch_returns_1(tmp_path) -> None:
    """A truncated upload (wrong size) is caught even if the name is present."""
    data_dir = _make_inputs(tmp_path)
    files = collect_local_tifs(data_dir, TILE)
    fs = MagicMock()
    fs.put_file.return_value = None
    listed = [
        {"name": f"bucket/s1tiling-output/{TILE}/{ORBIT}/{DATE}/{f.name}", "size": f.stat().st_size}
        for f in files
    ]
    listed[0]["size"] = 1  # corrupt one size
    fs.ls.return_value = listed
    rc = upload_outputs(fs, data_dir, TILE, ORBIT, DATE, "bucket")
    assert rc == 1


def test_upload_outputs_permanent_failure_returns_1(tmp_path, monkeypatch) -> None:
    """A retry-exhausted upload fails loudly with a clean exit code, not a traceback."""
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)
    data_dir = _make_inputs(tmp_path)
    fs = MagicMock()
    fs.put_file.side_effect = OSError("backend down")  # always fails
    rc = upload_outputs(fs, data_dir, TILE, ORBIT, DATE, "bucket")
    assert rc == 1
    fs.ls.assert_not_called()  # never claims verification on a failed upload


def test_upload_outputs_duplicate_basename_returns_1(tmp_path) -> None:
    """A basename collision across source dirs is refused (would silently overwrite)."""
    data_dir = _make_inputs(tmp_path)
    # Same basename in both data_out/<tile> and data_gamma_area.
    (data_dir / "data_out" / TILE / "DUP.tif").write_bytes(b"x")
    (data_dir / "data_gamma_area" / "DUP.tif").write_bytes(b"y")
    fs = MagicMock()
    rc = upload_outputs(fs, data_dir, TILE, ORBIT, DATE, "bucket")
    assert rc == 1
    fs.put_file.assert_not_called()  # refused before uploading anything


def test_put_one_retries_transient_errors(tmp_path, monkeypatch) -> None:
    """_put_one retries on transient backend errors (no real waiting)."""
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)
    from upload_s1tiling_outputs import _put_one

    fs = MagicMock()
    fs.put_file.side_effect = [OSError("transient"), OSError("transient"), None]
    _put_one(fs, tmp_path / "x.tif", "bucket/key/x.tif")
    assert fs.put_file.call_count == 3
