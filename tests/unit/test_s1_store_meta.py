"""Unit tests for scripts/s1_store_meta.py — the S1 RTC datamodel-migration low-level helpers.

Slice 1 covers the R5 writer-pin guard (`assert_writer_pinned`): the migration re-derives vv/vh +
overviews with the data-model writer's private `_downsample_2d`/`OVERVIEW_CHAIN`, so it must refuse to
run unless the writer is at the pinned, value-identical behavior (eopf-geozarr 0.11.0 == data-model
tag `v0.11.0`). At 0.11.0 the byte-identity argument that carried the earlier pins no longer holds --
`s1_ingest.py` was substantially rewritten -- so the guard also probes `_downsample_2d`'s average
behaviour numerically, which is the only numerical routine the re-derive borrows.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import eopf_geozarr
import pytest
from eopf_geozarr.conversion import s1_ingest

# Load the script module by path (scripts/ is not an importable package).
_SPEC = importlib.util.spec_from_file_location(
    "s1_store_meta", Path(__file__).resolve().parents[2] / "scripts" / "s1_store_meta.py"
)
s1_store_meta = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(s1_store_meta)  # type: ignore[union-attr]


def test_passes_on_the_pinned_env() -> None:
    """The worktree pins v0.11.0 / 0.11.0, so the guard must accept it (no raise)."""
    s1_store_meta.assert_writer_pinned()


def test_rejects_wrong_eopf_geozarr_version(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(eopf_geozarr, "__version__", "9.9.9")
    with pytest.raises(RuntimeError, match="eopf-geozarr"):
        s1_store_meta.assert_writer_pinned()


def test_rejects_changed_float32_nan_fill_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(s1_ingest, "FLOAT32_NAN_FILL_VALUE", "WRONG==")
    with pytest.raises(RuntimeError, match="FLOAT32_NAN_FILL_VALUE"):
        s1_store_meta.assert_writer_pinned()


def test_rejects_changed_overview_chain(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(s1_ingest, "OVERVIEW_CHAIN", s1_ingest.OVERVIEW_CHAIN[:-1])
    with pytest.raises(RuntimeError, match="OVERVIEW_CHAIN"):
        s1_store_meta.assert_writer_pinned()


def test_rejects_changed_downsample_average_behaviour(monkeypatch: pytest.MonkeyPatch) -> None:
    """The version compare cannot see inside `_downsample_2d`, so the guard probes it directly.

    v0.11.0 is the case in point: it rewrote that function (adding a `max` branch and a ValueError)
    while leaving the average path byte-identical. A version bump alone could not tell that apart
    from a change to the average path the re-derive depends on.
    """
    monkeypatch.setattr(s1_ingest, "_downsample_2d", lambda data, factor, method="average": data)
    with pytest.raises(RuntimeError, match="_downsample_2d"):
        s1_store_meta.assert_writer_pinned()


def test_downsample_probe_is_nan_aware() -> None:
    """The probe must pin `nanmean`, not `mean` -- the re-derive masks out-of-swath pixels to NaN
    before downsampling, so a switch to plain `mean` would silently poison every overview.

    The top-right 2x2 block is (NaN, 5, 5, 5): `nanmean` gives 5.0, plain `mean` gives NaN. A probe
    without a NaN could not tell the two apart.
    """
    import numpy as np

    assert np.isnan(np.array(s1_store_meta.DOWNSAMPLE_PROBE_INPUT, dtype="float32")).any()
    assert s1_store_meta.EXPECTED_DOWNSAMPLE_PROBE_OUTPUT[0][1] == 5.0


def test_drop_consolidated_metadata_works_on_a_remote_scheme() -> None:
    """C1: the real fleet run passes `s3://` stores — the drop must be filesystem-agnostic, not local
    `Path.rglob` (which silently no-ops on `s3://`). Proven here with fsspec's in-memory scheme, which
    routes through the same `url_to_fs` path as `s3://`."""
    import json

    import fsspec

    fs = fsspec.filesystem("memory")
    base = "/drop-remote-test.zarr"
    grp = lambda extra: json.dumps({"zarr_format": 3, "node_type": "group", **extra}).encode()  # noqa: E731
    fs.pipe_file(f"{base}/zarr.json", grp({"consolidated_metadata": {"a": 1}}))
    fs.pipe_file(f"{base}/ascending/zarr.json", grp({"consolidated_metadata": {"b": 2}}))
    fs.pipe_file(f"{base}/ascending/r10m/vv/zarr.json", json.dumps({"node_type": "array"}).encode())

    dropped = s1_store_meta.drop_consolidated_metadata(f"memory://{base}")

    assert dropped == 2  # both group nodes stripped; the array node untouched
    assert "consolidated_metadata" not in json.loads(fs.cat_file(f"{base}/zarr.json"))
    assert "consolidated_metadata" not in json.loads(fs.cat_file(f"{base}/ascending/zarr.json"))


def test_set_root_attr_preserves_consolidated_metadata() -> None:
    """I1: the completion marker is written AFTER consolidation; set_root_attr must add it to the root
    group's attributes WITHOUT clobbering the consolidated_metadata block just written."""
    import json

    import fsspec

    fs = fsspec.filesystem("memory")
    base = "/set-attr-test.zarr"
    fs.pipe_file(
        f"{base}/zarr.json",
        json.dumps(
            {
                "zarr_format": 3,
                "node_type": "group",
                "attributes": {"existing": 1},
                "consolidated_metadata": {"kind": "inline"},
            }
        ).encode(),
    )

    s1_store_meta.set_root_attr(f"memory://{base}", "datamodel_migrated", "0.10.1")

    meta = json.loads(fs.cat_file(f"{base}/zarr.json"))
    assert meta["attributes"]["datamodel_migrated"] == "0.10.1"
    assert meta["attributes"]["existing"] == 1  # pre-existing attrs preserved
    assert meta["consolidated_metadata"] == {"kind": "inline"}  # NOT clobbered (I1)


def test_s3_versioning_enabled_reads_bucket_status(monkeypatch: pytest.MonkeyPatch) -> None:
    """Task 3 pre-flight: reflects the bucket's versioning Status (Enabled → True, else False)."""
    import boto3

    class _FakeClient:
        def __init__(self, status: str | None) -> None:
            self._status = status

        def get_bucket_versioning(self, Bucket: str) -> dict:  # noqa: N803 -- boto3 kwarg name
            return {"Status": self._status} if self._status else {}

    monkeypatch.setattr(boto3, "client", lambda *a, **k: _FakeClient("Enabled"))
    assert s1_store_meta.s3_versioning_enabled("a-bucket") is True

    monkeypatch.setattr(boto3, "client", lambda *a, **k: _FakeClient("Suspended"))
    assert s1_store_meta.s3_versioning_enabled("a-bucket") is False

    monkeypatch.setattr(boto3, "client", lambda *a, **k: _FakeClient(None))  # never enabled
    assert s1_store_meta.s3_versioning_enabled("a-bucket") is False


def test_backup_and_restore_round_trip() -> None:
    """Task 3 AC: backup copies every store object; restore brings a mutated store back to its bytes."""

    import fsspec

    fs = fsspec.filesystem("memory")
    store, backup = "/cube.zarr", "/backup/cube.zarr"
    fs.pipe_file(f"{store}/zarr.json", b'{"node_type":"group","attributes":{}}')
    fs.pipe_file(f"{store}/ascending/r10m/vv/c/0.0.0", b"ORIGINAL-SHARD")

    n = s1_store_meta.backup_store(f"memory://{store}", f"memory://{backup}")
    assert n == 2

    # simulate redrive overwriting a vv shard + stamping the completion marker
    fs.pipe_file(f"{store}/ascending/r10m/vv/c/0.0.0", b"MUTATED-SHARD")
    fs.pipe_file(
        f"{store}/zarr.json", b'{"node_type":"group","attributes":{"datamodel_migrated":"0.10.1"}}'
    )

    restored = s1_store_meta.restore_store(f"memory://{backup}", f"memory://{store}")

    assert restored == 2
    assert fs.cat_file(f"{store}/ascending/r10m/vv/c/0.0.0") == b"ORIGINAL-SHARD"
    assert b"datamodel_migrated" not in fs.cat_file(f"{store}/zarr.json")  # marker rolled back
