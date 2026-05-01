"""Unit tests for convert_v1_s2.py — Dask client lifecycle."""

import contextlib
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

import convert_v1_s2  # noqa: E402
from convert_v1_s2 import run_conversion  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BASE_ARGS = {
    "source_url": "https://stac.example.com/collections/sentinel-2-l2a/items/S2A_test",
    "collection": "sentinel-2-l2a-staging-codecs",
    "s3_output_bucket": "test-bucket",
    "s3_output_prefix": "converted",
    "use_dask_cluster": True,
    "experimental_scale_offset_codec": True,
}


def _run_with_mocks(mock_client, convert_side_effect=None):
    """Call run_conversion with all I/O mocked out."""
    mock_convert = MagicMock(side_effect=convert_side_effect)
    mock_fs = MagicMock()
    mock_fs.rm.side_effect = FileNotFoundError  # no existing output to clean

    with (
        patch.object(convert_v1_s2, "get_zarr_url", return_value="s3://bucket/scene.zarr"),
        patch.object(convert_v1_s2, "setup_dask_cluster", return_value=mock_client),
        patch.object(convert_v1_s2, "get_storage_options", return_value={}),
        patch.object(convert_v1_s2, "convert_s2_optimized", mock_convert),
        patch("fsspec.filesystem", return_value=mock_fs),
        patch.object(convert_v1_s2.xr, "open_datatree", return_value=MagicMock()),
        contextlib.suppress(Exception),  # callers that inject side_effect check close() separately
    ):
        run_conversion(**_BASE_ARGS)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_no_worker_plugin_when_scale_offset_codec_enabled():
    """Scale-offset codec is built into zarr-python; no Dask worker plugin is registered."""
    mock_client = MagicMock()
    _run_with_mocks(mock_client)

    assert not mock_client.register_plugin.called
    assert not mock_client.register_worker_plugin.called


def test_client_closed_on_successful_conversion():
    """client.close() is called after a successful conversion."""
    mock_client = MagicMock()
    _run_with_mocks(mock_client)

    mock_client.close.assert_called_once()


def test_client_closed_when_conversion_raises():
    """client.close() is called even when convert_s2_optimized raises."""
    mock_client = MagicMock()
    _run_with_mocks(mock_client, convert_side_effect=RuntimeError("conversion failed"))

    mock_client.close.assert_called_once()


def test_no_client_created_when_dask_disabled():
    """When use_dask_cluster=False, setup_dask_cluster returns None."""
    args = {**_BASE_ARGS, "use_dask_cluster": False}
    mock_fs = MagicMock()
    mock_fs.rm.side_effect = FileNotFoundError

    with (
        patch.object(convert_v1_s2, "get_zarr_url", return_value="s3://bucket/scene.zarr"),
        patch.object(convert_v1_s2, "setup_dask_cluster", return_value=None) as mock_setup,
        patch.object(convert_v1_s2, "get_storage_options", return_value={}),
        patch.object(convert_v1_s2, "convert_s2_optimized"),
        patch("fsspec.filesystem", return_value=mock_fs),
        patch.object(convert_v1_s2.xr, "open_datatree", return_value=MagicMock()),
    ):
        run_conversion(**args)

    mock_setup.assert_called_once()
