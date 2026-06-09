"""Unit tests for run_ingest_register.py -- run_pipeline."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

from run_ingest_register import run_pipeline  # noqa: E402

_MOD = "run_ingest_register"

_KWARGS = {
    "s3_geotiff_prefix": "s3://bucket/input/",
    "tile_id": "31TCH",
    "orbit_direction": "descending",
    "collection": "sentinel-1-grd-rtc-staging",
    "s3_output_bucket": "my-bucket",
    "s3_endpoint": "https://s3.example.com",
    "stac_api_url": "https://stac.example.com",
    "raster_api_url": "https://raster.example.com",
}


def _mock_proc(returncode: int) -> MagicMock:
    m = MagicMock()
    m.returncode = returncode
    return m


def test_exits_0_on_success() -> None:
    """Ingest, upload, cube register, per-acq register all rc=0 -> 0; all 4 subprocesses called."""
    with patch(f"{_MOD}.subprocess.run", side_effect=[_mock_proc(0)] * 4) as mock_run:
        result = run_pipeline(**_KWARGS)
    assert result == 0
    assert mock_run.call_count == 4


def test_registers_both_cube_item_and_per_acquisition_items() -> None:
    """The register stage runs BOTH register_v1_s1_rtc (cube item -> -staging) AND
    register_per_acquisition (per-acq items -> -acquisitions), so neither collection is missed."""
    with patch(f"{_MOD}.subprocess.run", side_effect=[_mock_proc(0)] * 4) as mock_run:
        run_pipeline(**_KWARGS)
    scripts_called = [
        tok
        for call in mock_run.call_args_list
        for tok in call[0][0]
        if tok.endswith("register_v1_s1_rtc.py") or tok.endswith("register_per_acquisition.py")
    ]
    assert any(
        s.endswith("register_v1_s1_rtc.py") for s in scripts_called
    ), "cube item not registered"
    assert any(
        s.endswith("register_per_acquisition.py") for s in scripts_called
    ), "per-acquisition items not registered"
    # per-acq register targets the acquisitions collection with the same s3 store
    peracq_cmd = mock_run.call_args_list[3][0][0]
    assert "sentinel-1-grd-rtc-acquisitions" in peracq_cmd
    assert "s3://my-bucket/sentinel-1-grd-rtc-staging/s1-grd-rtc-31TCH.zarr" in peracq_cmd


def test_per_acquisition_skipped_when_cube_register_fails() -> None:
    """If the cube-item register fails, the per-acquisition register is not attempted."""
    with patch(
        f"{_MOD}.subprocess.run",
        side_effect=[_mock_proc(0), _mock_proc(0), _mock_proc(1)],
    ) as mock_run:
        result = run_pipeline(**_KWARGS)
    assert result == 1
    assert mock_run.call_count == 3  # ingest, upload, failed cube register; no per-acq


def test_exits_0_skips_register_on_empty_prefix() -> None:
    """Ingest rc=2 (no acquisitions) -> upload and register not called, pipeline returns 0."""
    with patch(f"{_MOD}.subprocess.run", return_value=_mock_proc(2)) as mock_run:
        result = run_pipeline(**_KWARGS)
    assert result == 0
    assert mock_run.call_count == 1


def test_exits_1_on_ingest_error() -> None:
    """Ingest rc=1 -> upload and register not called, pipeline returns 1."""
    with patch(f"{_MOD}.subprocess.run", return_value=_mock_proc(1)) as mock_run:
        result = run_pipeline(**_KWARGS)
    assert result == 1
    assert mock_run.call_count == 1


def test_ingest_uses_local_zarr_path() -> None:
    """Ingest subprocess must receive a local (non-s3://) path as --s3-zarr-store."""
    with patch(f"{_MOD}.subprocess.run", side_effect=[_mock_proc(0)] * 4) as mock_run:
        run_pipeline(**_KWARGS)

    ingest_cmd: list[str] = mock_run.call_args_list[0][0][0]
    store_idx = ingest_cmd.index("--s3-zarr-store")
    local_store = ingest_cmd[store_idx + 1]
    assert not local_store.startswith(
        "s3://"
    ), f"ingest must receive a local path, got: {local_store!r}"


def test_upload_called_between_ingest_and_register() -> None:
    """An S3 sync upload must be called after ingest and before register."""
    with patch(f"{_MOD}.subprocess.run", side_effect=[_mock_proc(0)] * 4) as mock_run:
        run_pipeline(**_KWARGS)

    upload_cmd: list[str] = mock_run.call_args_list[1][0][0]
    assert (
        "s3" in upload_cmd and "sync" in upload_cmd
    ), f"second subprocess call must be aws s3 sync, got: {upload_cmd}"
    # upload destination must be the S3 zarr (prefix == collection)
    expected_s3_zarr = "s3://my-bucket/sentinel-1-grd-rtc-staging/s1-grd-rtc-31TCH.zarr"
    assert expected_s3_zarr in upload_cmd, f"upload must target {expected_s3_zarr}"


def test_zarr_store_derived_correctly() -> None:
    """--store arg to register subprocess must equal s3://{bucket}/{collection}/s1-grd-rtc-{tile}.zarr."""
    with patch(f"{_MOD}.subprocess.run", side_effect=[_mock_proc(0)] * 4) as mock_run:
        run_pipeline(**_KWARGS)

    register_cmd: list[str] = mock_run.call_args_list[2][0][0]  # third call = register
    expected_store = "s3://my-bucket/sentinel-1-grd-rtc-staging/s1-grd-rtc-31TCH.zarr"
    store_idx = register_cmd.index("--store")
    assert register_cmd[store_idx + 1] == expected_store


@pytest.mark.parametrize("bad_collection", ["", "has/slash", "a/b/c"])
def test_invalid_collection_rejected(bad_collection: str) -> None:
    """An empty or slash-bearing collection would yield a malformed S3 key -> reject early.

    The guard must fire before any subprocess runs (no store written, no API touched).
    """
    with patch(f"{_MOD}.subprocess.run") as mock_run, pytest.raises(ValueError):
        run_pipeline(**{**_KWARGS, "collection": bad_collection})
    mock_run.assert_not_called()


def test_store_prefix_tracks_collection() -> None:
    """Store key prefix must be derived from --collection, not a fixed value.

    Adversarial: a *different* collection must move the store path accordingly.
    The staging-only assertions above would also pass a hardcoded prefix; this one
    would not.
    """
    kwargs = {**_KWARGS, "collection": "sentinel-1-grd-rtc-tests"}
    with patch(f"{_MOD}.subprocess.run", side_effect=[_mock_proc(0)] * 4) as mock_run:
        run_pipeline(**kwargs)

    register_cmd: list[str] = mock_run.call_args_list[2][0][0]
    expected_store = "s3://my-bucket/sentinel-1-grd-rtc-tests/s1-grd-rtc-31TCH.zarr"
    store_idx = register_cmd.index("--store")
    assert register_cmd[store_idx + 1] == expected_store


def test_upload_failure_stops_pipeline() -> None:
    """Upload rc=1 -> register not called, pipeline returns 1."""
    with patch(f"{_MOD}.subprocess.run", side_effect=[_mock_proc(0), _mock_proc(1)]) as mock_run:
        result = run_pipeline(**_KWARGS)
    assert result == 1
    assert mock_run.call_count == 2  # ingest + failed upload; no register


def test_local_zarr_cleaned_before_ingest() -> None:
    """Stale local zarr must be removed before ingest to avoid appending to old data."""
    with (
        patch(f"{_MOD}.subprocess.run", side_effect=[_mock_proc(0)] * 4),
        patch(f"{_MOD}.shutil.rmtree") as mock_rm,
        patch(f"{_MOD}.os.path.exists", return_value=True),
    ):
        run_pipeline(**_KWARGS)

    mock_rm.assert_called_once()
    cleaned_path = mock_rm.call_args[0][0]
    assert "s1-grd-rtc-31TCH" in cleaned_path
