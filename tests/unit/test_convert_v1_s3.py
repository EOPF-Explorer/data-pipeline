"""Unit tests for convert_v1_s3.py — Dask client lifecycle and source-URL handling."""

import contextlib
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

import convert_v1_s3  # noqa: E402
from convert_v1_s3 import run_conversion  # noqa: E402

# Real OLCI L1 EFR id (borrowed from test_source_url_utils.py fixtures).
S3_OLCI_ITEM_ID = (
    "S3A_OL_1_EFR____20260714T222153_20260714T222243_20260715T003629_0050_141_329_1080_PS1_O_NR_004"
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BASE_ARGS = {
    "source_url": (
        f"https://stac.core.eopf.eodc.eu/collections/sentinel-3-olci-l1-efr/items/{S3_OLCI_ITEM_ID}"
    ),
    "collection": "sentinel-3-olci-l1-efr-staging",
    "s3_output_bucket": "test-bucket",
    "s3_output_prefix": "s3-olci-staging",
    "use_dask_cluster": True,
}


def _run_with_mocks(mock_client, convert_side_effect=None, **arg_overrides):
    """Call run_conversion with all I/O mocked out. Returns the output URL."""
    mock_convert = MagicMock(side_effect=convert_side_effect)
    mock_fs = MagicMock()
    mock_fs.rm.side_effect = FileNotFoundError  # no existing output to clean
    output_url = None

    with (
        patch.object(convert_v1_s3, "resolve_zarr_url", return_value="s3://bucket/scene.zarr"),
        patch.object(convert_v1_s3, "setup_dask_cluster", return_value=mock_client),
        patch.object(convert_v1_s3, "get_storage_options", return_value={}),
        patch.object(convert_v1_s3, "convert_olci_optimized", mock_convert),
        patch("fsspec.filesystem", return_value=mock_fs),
        patch.object(convert_v1_s3, "open_source_datatree", return_value=MagicMock()),
        contextlib.suppress(Exception),  # callers that inject side_effect check close() separately
    ):
        output_url = run_conversion(**{**_BASE_ARGS, **arg_overrides})

    return output_url, mock_convert


# ---------------------------------------------------------------------------
# Converter invocation
# ---------------------------------------------------------------------------


def test_invokes_olci_converter_with_expected_kwargs():
    """run_conversion calls convert_olci_optimized with the OLCI parameter set."""
    _, mock_convert = _run_with_mocks(MagicMock(), spatial_chunk=512, min_dimension=128)

    mock_convert.assert_called_once()
    kwargs = mock_convert.call_args.kwargs
    assert kwargs["spatial_chunk"] == 512
    assert kwargs["min_dimension"] == 128
    assert kwargs["enable_sharding"] is False
    assert kwargs["keep_scale_offset"] is False
    assert kwargs["output_path"].endswith(f"{S3_OLCI_ITEM_ID}.zarr")
    # OLCI converter has no validate_output / experimental codec params.
    assert "validate_output" not in kwargs
    assert "experimental_scale_offset_codec" not in kwargs


def test_client_closed_on_successful_conversion():
    """client.close() is called after a successful conversion."""
    mock_client = MagicMock()
    _run_with_mocks(mock_client)

    mock_client.close.assert_called_once()


def test_client_closed_when_conversion_raises():
    """client.close() is called even when convert_olci_optimized raises."""
    mock_client = MagicMock()
    _run_with_mocks(mock_client, convert_side_effect=RuntimeError("conversion failed"))

    mock_client.close.assert_called_once()


def test_no_client_created_when_dask_disabled():
    """When use_dask_cluster=False, setup_dask_cluster returns None."""
    args = {**_BASE_ARGS, "use_dask_cluster": False}
    mock_fs = MagicMock()
    mock_fs.rm.side_effect = FileNotFoundError

    with (
        patch.object(convert_v1_s3, "resolve_zarr_url", return_value="s3://bucket/scene.zarr"),
        patch.object(convert_v1_s3, "setup_dask_cluster", return_value=None) as mock_setup,
        patch.object(convert_v1_s3, "get_storage_options", return_value={}),
        patch.object(convert_v1_s3, "convert_olci_optimized"),
        patch("fsspec.filesystem", return_value=mock_fs),
        patch.object(convert_v1_s3, "open_source_datatree", return_value=MagicMock()),
    ):
        run_conversion(**args)

    mock_setup.assert_called_once()


# ---------------------------------------------------------------------------
# CLI argument handling
# ---------------------------------------------------------------------------


def test_cli_parses_same_core_args_as_s2():
    """The CLI accepts the shared core argument set and forwards them to run_conversion."""
    with patch.object(convert_v1_s3, "run_conversion") as mock_run:
        rc = convert_v1_s3.main(
            [
                "--source-url",
                _BASE_ARGS["source_url"],
                "--collection",
                "sentinel-3-olci-l1-efr-staging",
                "--s3-output-bucket",
                "out",
                "--s3-output-prefix",
                "s3-olci-staging",
                "--spatial-chunk",
                "512",
                "--min-dimension",
                "128",
                "--no-enable-sharding",
            ]
        )

    assert rc == 0
    kwargs = mock_run.call_args.kwargs
    assert kwargs["collection"] == "sentinel-3-olci-l1-efr-staging"
    assert kwargs["spatial_chunk"] == 512
    assert kwargs["min_dimension"] == 128
    assert kwargs["enable_sharding"] is False


@pytest.mark.parametrize(
    "source_url",
    [
        (
            "https://stac.core.eopf.eodc.eu/collections/sentinel-3-olci-l1-efr/items/"
            + S3_OLCI_ITEM_ID
        ),
        f"s3://esa-zarr-sentinel-explorer-fra/source-cache/{S3_OLCI_ITEM_ID}",
    ],
)
def test_cli_accepts_https_and_staged_s3_sources(source_url):
    """prestage hands convert a native s3:// URL, which skips the HTTPS cache branch."""
    with patch.object(convert_v1_s3, "run_conversion") as mock_run:
        rc = convert_v1_s3.main(
            [
                "--source-url",
                source_url,
                "--collection",
                "sentinel-3-olci-l1-efr-staging",
                "--s3-output-bucket",
                "out",
                "--s3-output-prefix",
                "s3-olci-staging",
            ]
        )

    assert rc == 0
    assert mock_run.call_args.kwargs["source_url"] == source_url


@pytest.mark.parametrize("source_url", ["ftp://example.com/x.zarr", "/local/path.zarr"])
def test_cli_rejects_other_schemes(source_url):
    with patch.object(convert_v1_s3, "run_conversion") as mock_run:
        rc = convert_v1_s3.main(
            [
                "--source-url",
                source_url,
                "--collection",
                "sentinel-3-olci-l1-efr-staging",
                "--s3-output-bucket",
                "out",
                "--s3-output-prefix",
                "s3-olci-staging",
            ]
        )

    assert rc == 1
    assert not mock_run.called


def test_cli_returns_2_when_source_dataset_missing():
    """A GroupNotFoundError from the converter path surfaces as exit code 2."""
    import zarr

    with patch.object(
        convert_v1_s3, "run_conversion", side_effect=zarr.errors.GroupNotFoundError("x")
    ):
        rc = convert_v1_s3.main(
            [
                "--source-url",
                _BASE_ARGS["source_url"],
                "--collection",
                "sentinel-3-olci-l1-efr-staging",
                "--s3-output-bucket",
                "out",
                "--s3-output-prefix",
                "s3-olci-staging",
            ]
        )

    assert rc == 2


# ---------------------------------------------------------------------------
# Output path invariants (register depends on these)
# ---------------------------------------------------------------------------


def test_output_path_from_staged_s3_source():
    """A staged source (prefix/<item_id>, no .zarr) yields the item_id-based output path."""
    url, _ = _run_with_mocks(
        MagicMock(),
        source_url=f"s3://esa-zarr-sentinel-explorer-fra/source-cache/{S3_OLCI_ITEM_ID}",
    )

    assert url == (
        f"s3://test-bucket/s3-olci-staging/sentinel-3-olci-l1-efr-staging/{S3_OLCI_ITEM_ID}.zarr"
    )


def test_output_path_strips_zarr_suffix_from_direct_zarr_source():
    """A direct .zarr source_url must not produce <item>.zarr.zarr output."""
    url, _ = _run_with_mocks(
        MagicMock(),
        source_url=f"https://objects.eodc.eu:443/bucket/products/{S3_OLCI_ITEM_ID}.zarr",
    )

    assert url == (
        f"s3://test-bucket/s3-olci-staging/sentinel-3-olci-l1-efr-staging/{S3_OLCI_ITEM_ID}.zarr"
    )


# ---------------------------------------------------------------------------
# Source opening (storage_options / cache_dir wiring — #339, converter-agnostic)
# ---------------------------------------------------------------------------


def _open_source_datatree_call_for(zarr_url: str, get_storage_options_return: dict | None = None):
    """Run a conversion resolving to `zarr_url`; return the open_source_datatree call args."""
    mock_open_source_datatree = MagicMock(return_value=MagicMock())
    mock_fs = MagicMock()
    mock_fs.rm.side_effect = FileNotFoundError

    with (
        patch.object(convert_v1_s3, "resolve_zarr_url", return_value=zarr_url),
        patch.object(convert_v1_s3, "setup_dask_cluster", return_value=None),
        patch.object(convert_v1_s3, "get_storage_options", return_value=get_storage_options_return),
        patch.object(convert_v1_s3, "convert_olci_optimized"),
        patch("fsspec.filesystem", return_value=mock_fs),
        patch.object(convert_v1_s3, "open_source_datatree", mock_open_source_datatree),
    ):
        run_conversion(**_BASE_ARGS)

    return mock_open_source_datatree.call_args


def test_https_source_gets_cache_dir_and_no_simplecache_wrapping():
    """HTTPS sources must get a cache_dir and plain storage_options (#339)."""
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
    """Pre-staged s3:// sources skip open_source_datatree's cache_dir."""
    call = _open_source_datatree_call_for(
        "s3://bucket/scene.zarr", get_storage_options_return={"anon": False}
    )

    assert call.kwargs["cache_dir"] is None
    assert call.kwargs["storage_options"] == {"anon": False}
