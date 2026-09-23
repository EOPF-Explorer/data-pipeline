"""Tests for the Track B store copy.

The emphasis is deliberate: most of these exercise the *controls* -- the store
cap, the write confinement, and the digest check -- rather than the happy path.
A bound that has never been fired is not a bound.
"""

import urllib.error
from unittest.mock import MagicMock

import pytest

from scripts.copy_zarr3_samples import (
    MAX_STORES_CEILING,
    CopyError,
    StorePlan,
    assert_writes_confined,
    chunk_keys_for_array,
    copy_object,
    copy_store,
    main,
    parse_confinement,
    plan_store,
)


class TestChunkKeys:
    def test_two_d_grid_is_enumerated_in_full(self):
        meta = {
            "shape": [20, 30],
            "chunk_grid": {"configuration": {"chunk_shape": [10, 10]}},
            "chunk_key_encoding": {"configuration": {"separator": "/"}},
        }
        keys = chunk_keys_for_array("a/b", meta)
        assert len(keys) == 2 * 3
        assert "a/b/c/0/0" in keys
        assert "a/b/c/1/2" in keys

    def test_sharded_band_is_a_single_object(self):
        """A 10980x10980 band under an 11264 shard is one object, not 108."""
        meta = {
            "shape": [10980, 10980],
            "chunk_grid": {"configuration": {"chunk_shape": [11264, 11264]}},
            "chunk_key_encoding": {"configuration": {"separator": "/"}},
        }
        keys = chunk_keys_for_array("m/r10m/b04", meta)
        assert keys == ["m/r10m/b04/c/0/0"]

    def test_scalar_array_has_one_key(self):
        meta = {"shape": [], "chunk_grid": {"configuration": {"chunk_shape": []}}}
        assert chunk_keys_for_array("m/spatial_ref", meta) == ["m/spatial_ref/c"]

    def test_separator_is_honoured(self):
        meta = {
            "shape": [2],
            "chunk_grid": {"configuration": {"chunk_shape": [1]}},
            "chunk_key_encoding": {"configuration": {"separator": "."}},
        }
        keys = chunk_keys_for_array("a", meta)
        assert keys == ["a/c.0", "a/c.1"]

    def test_v2_encoding_has_no_c_prefix_and_dots_by_default(self):
        """A `c/0/0` guess 404s on every v2 chunk, and each 404 would read as fill."""
        meta = {
            "shape": [2, 2],
            "chunk_grid": {"configuration": {"chunk_shape": [1, 2]}},
            "chunk_key_encoding": {"name": "v2"},
        }
        assert chunk_keys_for_array("a", meta) == ["a/0.0", "a/1.0"]
        scalar = {**meta, "shape": [], "chunk_grid": {"configuration": {"chunk_shape": []}}}
        assert chunk_keys_for_array("s", scalar) == ["s/0"]

    def test_unknown_encoding_is_refused(self):
        meta = {
            "shape": [1],
            "chunk_grid": {"configuration": {"chunk_shape": [1]}},
            "chunk_key_encoding": {"name": "future"},
        }
        with pytest.raises(CopyError, match="unknown chunk_key_encoding"):
            chunk_keys_for_array("a", meta)


class TestPlanStore:
    def test_refuses_a_store_without_consolidated_metadata(self, monkeypatch):
        """No consolidated metadata and no listing means no derivable key set."""
        monkeypatch.setattr(
            "scripts.copy_zarr3_samples.fetch_json", lambda url: {"node_type": "group"}
        )
        with pytest.raises(CopyError, match="no consolidated_metadata"):
            plan_store("https://example.test/S2X.zarr")

    def test_plans_metadata_and_chunk_keys(self, monkeypatch):
        root_doc = {
            "consolidated_metadata": {
                "metadata": {
                    "grp": {"node_type": "group"},
                    "grp/arr": {
                        "node_type": "array",
                        "shape": [4],
                        "chunk_grid": {"configuration": {"chunk_shape": [2]}},
                        "chunk_key_encoding": {"configuration": {"separator": "/"}},
                    },
                    "grp/scal": {
                        "node_type": "array",
                        "shape": [],
                        "chunk_grid": {"configuration": {"chunk_shape": []}},
                    },
                }
            }
        }
        monkeypatch.setattr("scripts.copy_zarr3_samples.fetch_json", lambda url: root_doc)
        plan = plan_store("https://example.test/S2X.zarr/")

        assert plan.name == "S2X.zarr"
        assert "zarr.json" in plan.keys
        assert "grp/arr/zarr.json" in plan.keys
        assert {"grp/arr/c/0", "grp/arr/c/1"} <= set(plan.keys)
        assert plan.required_keys == {
            "zarr.json",
            "grp/zarr.json",
            "grp/arr/zarr.json",
            "grp/scal/zarr.json",
        }
        assert "grp/scal/c" not in plan.required_keys


class TestConfinement:
    def test_parse_requires_s3_scheme_and_bucket(self):
        with pytest.raises(ValueError, match="s3:// URL"):
            parse_confinement("https://bucket/prefix")
        with pytest.raises(ValueError, match="missing a bucket"):
            parse_confinement("s3:///prefix")

    def test_parse_appends_trailing_slash(self):
        assert parse_confinement("s3://b/samples-zarr3-proxy") == ("b", "samples-zarr3-proxy/")

    def test_refuses_a_different_bucket(self):
        with pytest.raises(CopyError, match="refusing to write to bucket"):
            assert_writes_confined("other", ["samples/x"], ("mine", "samples/"))

    def test_refuses_a_key_outside_the_prefix(self):
        with pytest.raises(CopyError, match="outside"):
            assert_writes_confined(
                "mine", ["samples/ok", "tests-output/STRAY"], ("mine", "samples/")
            )

    def test_accepts_keys_under_the_prefix(self):
        assert_writes_confined("mine", ["samples/a", "samples/b"], ("mine", "samples/"))


class TestCopyObject:
    def _client(self, etag):
        client = MagicMock()
        client.put_object.return_value = {"ETag": f'"{etag}"'}
        return client

    def test_refuses_when_the_stored_digest_disagrees(self, monkeypatch):
        """The one check that would catch a truncated or corrupted transfer."""
        monkeypatch.setattr(
            "scripts.copy_zarr3_samples.urllib.request.urlopen",
            lambda *a, **k: _resp(b"payload"),
        )
        with pytest.raises(CopyError, match="not the bytes we read"):
            copy_object(self._client("deadbeef"), "https://x/k", "b", "k", optional=False)

    def test_absent_chunk_is_not_an_error(self, monkeypatch):
        """Zarr reads a missing chunk as the fill value; 4 non-scalar chunks and 19
        scalar ones are genuinely absent in a real store, so absence is copied."""
        monkeypatch.setattr("scripts.copy_zarr3_samples.urllib.request.urlopen", _raise_404)
        assert copy_object(MagicMock(), "https://x/k", "b", "k", optional=True) is None

    def test_absent_required_object_fails_the_run(self, monkeypatch):
        """A node's zarr.json is named in the consolidated metadata: it must exist."""
        monkeypatch.setattr("scripts.copy_zarr3_samples.urllib.request.urlopen", _raise_404)
        with pytest.raises(CopyError, match="HTTP 404"):
            copy_object(MagicMock(), "https://x/k", "b", "k", optional=False)


class TestCopyStore:
    PLAN = StorePlan(
        root="https://x/A.zarr",
        name="A.zarr",
        keys=["zarr.json", "arr/zarr.json", "arr/c/0", "arr/c/1"],
        required_keys={"zarr.json", "arr/zarr.json"},
    )

    def test_a_store_whose_every_chunk_is_absent_fails(self, monkeypatch):
        """Metadata over nothing is what a wrong key form produces, reported as success."""
        monkeypatch.setattr(
            "scripts.copy_zarr3_samples.copy_object",
            lambda _c, url, _b, _k, optional: None if optional else 10,
        )
        with pytest.raises(CopyError, match="all 2 chunk keys were absent"):
            copy_store(MagicMock(), self.PLAN, "b", "p/", workers=1, dry_run=False)

    def test_some_absent_chunks_are_fill_values(self, monkeypatch):
        monkeypatch.setattr(
            "scripts.copy_zarr3_samples.copy_object",
            lambda _c, url, _b, _k, optional: None if url.endswith("c/1") else 10,
        )
        assert copy_store(MagicMock(), self.PLAN, "b", "p/", workers=1, dry_run=False) == (
            3,
            1,
            30,
        )


class TestStoreCap:
    """The cap is a feature of the tool, exercised here before any real target."""

    BASE = [
        "--dest",
        "s3://b/samples/",
        "--confine-to",
        "s3://b/samples/",
        "--store-root",
        "https://x/A.zarr",
    ]

    def test_zero_is_rejected(self):
        assert main([*self.BASE, "--max-stores", "0"]) == 2

    def test_above_the_ceiling_is_rejected(self):
        assert main([*self.BASE, "--max-stores", str(MAX_STORES_CEILING + 1)]) == 2

    def test_more_roots_than_the_cap_refuses_before_planning(self, monkeypatch):
        """Refusal must happen before any network call, let alone any write."""
        called = []
        monkeypatch.setattr("scripts.copy_zarr3_samples.plan_store", lambda r: called.append(r))
        code = main(
            [
                "--dest",
                "s3://b/samples/",
                "--confine-to",
                "s3://b/samples/",
                "--store-root",
                "https://x/A.zarr",
                "--store-root",
                "https://x/B.zarr",
                "--max-stores",
                "1",
            ]
        )
        assert code == 2
        assert called == []

    def test_no_roots_is_rejected(self):
        assert main(["--dest", "s3://b/s/", "--confine-to", "s3://b/s/", "--max-stores", "1"]) == 2


class TestDryRun:
    def test_dry_run_plans_and_confines_but_writes_nothing(self, monkeypatch):
        monkeypatch.setattr(
            "scripts.copy_zarr3_samples.plan_store",
            lambda root: StorePlan(root=root, name="A.zarr", keys=["zarr.json"]),
        )
        client = MagicMock()
        monkeypatch.setattr("scripts.copy_zarr3_samples.boto3.client", lambda *a, **k: client)
        code = main(
            [
                "--dest",
                "s3://b/samples/",
                "--confine-to",
                "s3://b/samples/",
                "--store-root",
                "https://x/A.zarr",
                "--max-stores",
                "1",
                "--dry-run",
            ]
        )
        assert code == 0
        client.put_object.assert_not_called()

    def test_confinement_violation_refuses_before_any_write(self, monkeypatch):
        monkeypatch.setattr(
            "scripts.copy_zarr3_samples.plan_store",
            lambda root: StorePlan(root=root, name="A.zarr", keys=["zarr.json"]),
        )
        client = MagicMock()
        monkeypatch.setattr("scripts.copy_zarr3_samples.boto3.client", lambda *a, **k: client)
        with pytest.raises(CopyError, match="outside"):
            main(
                [
                    "--dest",
                    "s3://b/tests-output/",
                    "--confine-to",
                    "s3://b/samples-zarr3-proxy/",
                    "--store-root",
                    "https://x/A.zarr",
                    "--max-stores",
                    "1",
                ]
            )
        client.put_object.assert_not_called()


class _resp:
    def __init__(self, body):
        self._body = body

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _raise_404(*args, **kwargs):
    raise urllib.error.HTTPError("https://x/k", 404, "Not Found", {}, None)
