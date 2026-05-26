"""Unit tests for run_ingest_register.py -- run_pipeline."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

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
    "s3_output_prefix": "my-prefix",
    "s3_endpoint": "https://s3.example.com",
    "stac_api_url": "https://stac.example.com",
    "raster_api_url": "https://raster.example.com",
}


def _mock_proc(returncode: int) -> MagicMock:
    m = MagicMock()
    m.returncode = returncode
    return m


def test_exits_0_on_success() -> None:
    """Ingest rc=0, register rc=0 -> pipeline returns 0; both subprocesses called."""
    with patch(f"{_MOD}.subprocess.run", side_effect=[_mock_proc(0), _mock_proc(0)]) as mock_run:
        result = run_pipeline(**_KWARGS)
    assert result == 0
    assert mock_run.call_count == 2


def test_exits_0_skips_register_on_empty_prefix() -> None:
    """Ingest rc=2 (no acquisitions) -> register not called, pipeline returns 0."""
    with patch(f"{_MOD}.subprocess.run", return_value=_mock_proc(2)) as mock_run:
        result = run_pipeline(**_KWARGS)
    assert result == 0
    assert mock_run.call_count == 1


def test_exits_1_on_ingest_error() -> None:
    """Ingest rc=1 -> register not called, pipeline returns 1."""
    with patch(f"{_MOD}.subprocess.run", return_value=_mock_proc(1)) as mock_run:
        result = run_pipeline(**_KWARGS)
    assert result == 1
    assert mock_run.call_count == 1


def test_zarr_store_derived_correctly() -> None:
    """--store arg to register subprocess must equal s3://{bucket}/{prefix}/s1-grd-rtc-{tile}.zarr."""
    with patch(f"{_MOD}.subprocess.run", side_effect=[_mock_proc(0), _mock_proc(0)]) as mock_run:
        run_pipeline(**_KWARGS)

    register_cmd: list[str] = mock_run.call_args_list[1][0][0]
    expected_store = "s3://my-bucket/my-prefix/s1-grd-rtc-31TCH.zarr"
    store_idx = register_cmd.index("--store")
    assert register_cmd[store_idx + 1] == expected_store
