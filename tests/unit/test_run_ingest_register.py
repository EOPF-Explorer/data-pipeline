"""Unit tests for run_ingest_register.py -- run_pipeline.

`run_pipeline` orchestrates three subprocesses: (0) ingest into the **s3:// cube** via
`ingest_v1_s1_rtc.py` (which fetches the existing cube and **appends** the new scene — the T4 path),
(1) register the cube item, (2) register the per-acquisition items. These tests prove the
*delegation* contract (the s3:// store is passed; no separate upload step; correct ordering); the
append correctness itself is covered by `test_ingest_v1_s1_rtc.py`.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

from run_ingest_register import check_env_consistency, run_pipeline  # noqa: E402

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

# TEMPORARY (#246): the cube is written at titiler's reconstructed render path
# s3://{bucket}/tests-output/{collection}/s1-rtc-{tile}.zarr (revert when titiler-eopf#108 lands).
_S3_CUBE = "s3://my-bucket/tests-output/sentinel-1-grd-rtc-staging/s1-rtc-31TCH.zarr"


def _mock_proc(returncode: int) -> MagicMock:
    m = MagicMock()
    m.returncode = returncode
    return m


def test_exits_0_on_success() -> None:
    """Ingest, cube register, per-acq register all rc=0 -> 0; exactly 3 subprocesses (no upload)."""
    with patch(f"{_MOD}.subprocess.run", side_effect=[_mock_proc(0)] * 3) as mock_run:
        result = run_pipeline(**_KWARGS)
    assert result == 0
    assert mock_run.call_count == 3


def test_ingest_uses_s3_store_for_append() -> None:
    """The fix: ingest must receive the **s3:// cube** as --s3-zarr-store so run_ingest fetches the
    existing cube and appends (T4) — not a fresh local temp store that would overwrite it."""
    with patch(f"{_MOD}.subprocess.run", side_effect=[_mock_proc(0)] * 3) as mock_run:
        run_pipeline(**_KWARGS)
    ingest_cmd: list[str] = mock_run.call_args_list[0][0][0]
    store = ingest_cmd[ingest_cmd.index("--s3-zarr-store") + 1]
    assert store.startswith("s3://"), f"ingest must append into the s3:// cube, got: {store!r}"
    assert store == _S3_CUBE


def test_no_separate_aws_sync_step() -> None:
    """No standalone `aws s3 sync` step — run_ingest uploads via s3fs after appending, so a separate
    sync (the old overwrite mechanism, also dependent on the aws CLI) must not be invoked."""
    with patch(f"{_MOD}.subprocess.run", side_effect=[_mock_proc(0)] * 3) as mock_run:
        run_pipeline(**_KWARGS)
    for call in mock_run.call_args_list:
        cmd = call[0][0]
        assert not (cmd[0] == "aws" and "sync" in cmd), f"unexpected aws s3 sync: {cmd}"


def test_registers_both_cube_item_and_per_acquisition_items() -> None:
    """The register stage runs BOTH register_v1_s1_rtc (cube item -> -staging) AND
    register_per_acquisition (per-acq items -> -acquisitions), so neither collection is missed."""
    with patch(f"{_MOD}.subprocess.run", side_effect=[_mock_proc(0)] * 3) as mock_run:
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
    # per-acq register (3rd call) targets the acquisitions collection with the same s3 store
    peracq_cmd = mock_run.call_args_list[2][0][0]
    assert "sentinel-1-grd-rtc-acquisitions" in peracq_cmd
    assert _S3_CUBE in peracq_cmd


def test_per_acquisition_skipped_when_cube_register_fails() -> None:
    """If the cube-item register (2nd call) fails, the per-acquisition register is not attempted."""
    with patch(
        f"{_MOD}.subprocess.run",
        side_effect=[_mock_proc(0), _mock_proc(1)],
    ) as mock_run:
        result = run_pipeline(**_KWARGS)
    assert result == 1
    assert mock_run.call_count == 2  # ingest, failed cube register; no per-acq


def test_exits_0_skips_register_on_empty_prefix() -> None:
    """Ingest rc=2 (no acquisitions) -> register not called, pipeline returns 0."""
    with patch(f"{_MOD}.subprocess.run", return_value=_mock_proc(2)) as mock_run:
        result = run_pipeline(**_KWARGS)
    assert result == 0
    assert mock_run.call_count == 1


def test_exits_1_on_ingest_error() -> None:
    """Ingest rc=1 (includes an append/upload failure inside run_ingest) -> register not called."""
    with patch(f"{_MOD}.subprocess.run", return_value=_mock_proc(1)) as mock_run:
        result = run_pipeline(**_KWARGS)
    assert result == 1
    assert mock_run.call_count == 1


def test_zarr_store_derived_correctly() -> None:
    """--store arg to the cube register (2nd call) must equal s3://{bucket}/{collection}/...zarr."""
    with patch(f"{_MOD}.subprocess.run", side_effect=[_mock_proc(0)] * 3) as mock_run:
        run_pipeline(**_KWARGS)
    register_cmd: list[str] = mock_run.call_args_list[1][0][0]  # second call = cube register
    store_idx = register_cmd.index("--store")
    assert register_cmd[store_idx + 1] == _S3_CUBE


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

    Adversarial: a *different* collection must move the store path accordingly (in the ingest store
    and the cube register --store).
    """
    kwargs = {**_KWARGS, "collection": "sentinel-1-grd-rtc-tests"}
    # TEMPORARY (#246): tests-output render path + s1-rtc- filename (revert with titiler-eopf#108).
    expected_store = "s3://my-bucket/tests-output/sentinel-1-grd-rtc-tests/s1-rtc-31TCH.zarr"
    with patch(f"{_MOD}.subprocess.run", side_effect=[_mock_proc(0)] * 3) as mock_run:
        run_pipeline(**kwargs)
    ingest_cmd: list[str] = mock_run.call_args_list[0][0][0]
    assert ingest_cmd[ingest_cmd.index("--s3-zarr-store") + 1] == expected_store
    register_cmd: list[str] = mock_run.call_args_list[1][0][0]
    assert register_cmd[register_cmd.index("--store") + 1] == expected_store


def test_env_mismatch_rejected_before_any_subprocess() -> None:
    """A staging collection paired with the tests bucket is the 32TLR footgun -> reject early.

    The guard must fire before ingest, so no Zarr is written and no API is touched.
    """
    kwargs = {
        **_KWARGS,
        "collection": "sentinel-1-grd-rtc-staging",
        "s3_output_bucket": "esa-zarr-sentinel-explorer-tests",
    }
    with patch(f"{_MOD}.subprocess.run") as mock_run, pytest.raises(ValueError, match="mismatch"):
        run_pipeline(**kwargs)
    mock_run.assert_not_called()


@pytest.mark.parametrize(
    ("collection", "bucket"),
    [
        ("sentinel-1-grd-rtc-staging", "esa-zarr-sentinel-explorer-s1-l1grd-staging"),
        ("sentinel-1-grd-rtc-tests", "esa-zarr-sentinel-explorer-tests"),
        ("sentinel-1-grd-rtc-prod", "esa-zarr-sentinel-explorer-s1-l1grd-prod"),
    ],
)
def test_matched_env_pairs_allowed(collection: str, bucket: str) -> None:
    """Matching per-env bucket/collection pairs run the full pipeline (3 subprocesses)."""
    kwargs = {**_KWARGS, "collection": collection, "s3_output_bucket": bucket}
    with patch(f"{_MOD}.subprocess.run", side_effect=[_mock_proc(0)] * 3) as mock_run:
        assert run_pipeline(**kwargs) == 0
    assert mock_run.call_count == 3


@pytest.mark.parametrize(
    ("collection", "bucket"),
    [
        # Unrecognized bucket: env unknown -> can't judge -> allowed (existing _KWARGS case).
        ("sentinel-1-grd-rtc-staging", "my-bucket"),
        # Unrecognized collection (e.g. the cross-env -acquisitions collection) -> allowed.
        ("sentinel-1-grd-rtc-acquisitions", "esa-zarr-sentinel-explorer-tests"),
    ],
)
def test_unrecognized_names_pass_through(collection: str, bucket: str) -> None:
    """When either name isn't a known per-env value, the guard can't infer env and stays out."""
    check_env_consistency(collection, bucket)  # must not raise
