"""Unit tests for convert_v1_s2.py — Dask client lifecycle and source-store opening."""

import contextlib
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
import xarray as xr

scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

import convert_v1_s2  # noqa: E402
from convert_v1_s2 import open_source_datatree, run_conversion  # noqa: E402

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


# ---------------------------------------------------------------------------
# Source store opening — try default, fall back to Zarr v2 on error (coordination#263)
#
# Upstream EOPF products on data.eodc.eu now ship an empty (0-byte) Zarr v3
# `zarr.json` next to the valid v2 `.zgroup`. Our zarr/xarray stack prefers v3
# and crashes on `json.loads(b"")`. open_source_datatree tries the normal,
# format-autodetecting open first (so genuine v3 stores keep working) and only
# retries pinned to zarr_format=2 when that fails.
# ---------------------------------------------------------------------------


def _write_v2_store_with_empty_v3_stub(store: Path) -> None:
    """Write a valid Zarr v2 store, then drop a 0-byte v3 `zarr.json` beside it.

    Reproduces the exact upstream layout from coordination#263: a v2 `.zgroup`
    plus an empty v3 root-metadata stub that v3-preferring readers choke on.
    """
    ds = xr.Dataset({"b": ("x", np.arange(4))}, coords={"x": np.arange(4)})
    ds.to_zarr(store, zarr_format=2, consolidated=False)
    (store / "zarr.json").write_bytes(b"")  # empty v3 stub, as upstream ships


def test_default_open_reproduces_empty_v3_failure(tmp_path):
    """Documents the #263 repro: the default open path crashes on the empty zarr.json.

    Version-sensitive (zarr is pinned `>=3.2.0`): this pins *current* broken
    behaviour to justify the fallback, not a contract. If a future zarr stops
    preferring the empty v3 stub, this default open would just succeed.
    """
    store = tmp_path / "scene.zarr"
    _write_v2_store_with_empty_v3_stub(store)

    with pytest.raises(json.JSONDecodeError):
        xr.open_datatree(str(store), engine="zarr").close()


def test_open_source_datatree_falls_back_to_v2(tmp_path):
    """End-to-end fallback: a store poisoned with an empty v3 zarr.json still opens.

    Exercises the real try/except against the real JSONDecodeError (no mocks).
    """
    store = tmp_path / "scene.zarr"
    _write_v2_store_with_empty_v3_stub(store)

    dt = open_source_datatree(str(store), storage_options=None)
    assert list(dt.data_vars) == ["b"]
    assert dt["b"].values.tolist() == [0, 1, 2, 3]


def test_open_source_datatree_uses_default_when_it_works():
    """When the default open succeeds, v2 is NOT forced (a genuine v3 store keeps working)."""
    sentinel = MagicMock()
    with patch.object(convert_v1_s2.xr, "open_datatree", return_value=sentinel) as mock_open:
        result = open_source_datatree("s3://bucket/scene.zarr", storage_options={})

    assert result is sentinel
    mock_open.assert_called_once()
    assert "zarr_format" not in mock_open.call_args.kwargs


def test_open_source_datatree_retries_with_zarr_format_2():
    """On a first-open failure, retry pins zarr_format=2 (the #263 fallback)."""
    sentinel = MagicMock()
    with patch.object(
        convert_v1_s2.xr,
        "open_datatree",
        side_effect=[json.JSONDecodeError("Expecting value", "", 0), sentinel],
    ) as mock_open:
        result = open_source_datatree("s3://bucket/scene.zarr", storage_options={})

    assert result is sentinel
    assert mock_open.call_count == 2
    assert "zarr_format" not in mock_open.call_args_list[0].kwargs  # default first
    assert mock_open.call_args_list[1].kwargs.get("zarr_format") == 2  # then v2
