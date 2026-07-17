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
        patch.object(convert_v1_s2, "open_source_datatree", return_value=MagicMock()),
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
        patch.object(convert_v1_s2, "open_source_datatree", return_value=MagicMock()),
    ):
        run_conversion(**args)

    mock_setup.assert_called_once()


def _open_source_datatree_call_for(zarr_url: str, get_storage_options_return: dict | None = None):
    """Run a conversion resolving to `zarr_url`; return the open_source_datatree call args.

    Isolates the branch in `run_conversion` that builds `storage_options`/`cache_dir`
    (HTTPS vs. s3://) from everything else the function does.
    """
    mock_open_source_datatree = MagicMock(return_value=MagicMock())
    mock_fs = MagicMock()
    mock_fs.rm.side_effect = FileNotFoundError

    with (
        patch.object(convert_v1_s2, "get_zarr_url", return_value=zarr_url),
        patch.object(convert_v1_s2, "setup_dask_cluster", return_value=None),
        patch.object(convert_v1_s2, "get_storage_options", return_value=get_storage_options_return),
        patch.object(convert_v1_s2, "convert_s2_optimized"),
        patch("fsspec.filesystem", return_value=mock_fs),
        patch.object(convert_v1_s2, "open_source_datatree", mock_open_source_datatree),
    ):
        run_conversion(**_BASE_ARGS)

    return mock_open_source_datatree.call_args


def test_https_source_gets_cache_dir_and_no_simplecache_wrapping():
    """HTTPS sources must get a cache_dir and plain storage_options.

    `open_source_datatree` owns the atomic on-disk cache now; regressing to the old
    hand-rolled `simplecache` wrapper here would silently reopen the #339 concurrency
    race this fix closes (two dask tasks writing the same non-atomic cache file).
    """
    call = _open_source_datatree_call_for("https://source.example.com/scene.zarr")

    assert call.kwargs["cache_dir"] is not None
    storage_options = call.kwargs["storage_options"]
    assert storage_options["asynchronous"] is True
    assert callable(storage_options["get_client"])
    for stale_key in (
        "protocol",
        "target_protocol",
        "cache_storage",
        "expiry_time",
        "target_options",
    ):
        assert stale_key not in storage_options


def test_s3_source_gets_no_cache_dir():
    """Pre-staged s3:// sources skip open_source_datatree's cache_dir.

    #339 was specifically about the HTTPS simplecache race; s3fs reads don't share a
    local cache file, so there's nothing for `open_source_datatree`'s CacheStore to
    protect here (matches the s3:// comment on the CLI entry point below).
    """
    call = _open_source_datatree_call_for(
        "s3://bucket/scene.zarr", get_storage_options_return={"anon": False}
    )

    assert call.kwargs["cache_dir"] is None
    assert call.kwargs["storage_options"] == {"anon": False}
