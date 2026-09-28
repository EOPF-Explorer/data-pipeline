"""Tests for the Track B store copy.

The emphasis is deliberate: most of these exercise the *controls* -- the store
cap, the write confinement, and the digest check -- rather than the happy path.
A bound that has never been fired is not a bound.
"""

import hashlib
import io
import urllib.error
import urllib.request
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from scripts.copy_zarr3_samples import (
    MAX_STORES_CEILING,
    CopyError,
    StorePlan,
    _open,
    _RefuseRedirects,
    assert_writes_confined,
    chunk_keys_for_array,
    copy_object,
    copy_store,
    main,
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
        assert plan.dest_root("samples/") == "samples/S2X.zarr/"

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

    @pytest.mark.parametrize("path", ["grp/../../stray", "/abs", "a//b", "./a", "", "a?x", "a#x"])
    def test_an_unsafe_node_path_is_refused(self, monkeypatch, path):
        """Node paths go verbatim into the source URL and the S3 key; confinement is a
        prefix match that a `..` segment would pass."""
        root_doc = {"consolidated_metadata": {"metadata": {path: {"node_type": "group"}}}}
        monkeypatch.setattr("scripts.copy_zarr3_samples.fetch_json", lambda url: root_doc)
        with pytest.raises(CopyError, match="unsafe node path"):
            plan_store("https://example.test/S2X.zarr")


class TestConfinement:
    # Parsing is s3_item_cleanup.parse_s3_prefix, tested in test_s3_item_cleanup.py.

    def test_refuses_a_different_bucket(self):
        with pytest.raises(CopyError, match="outside s3://mine/samples/"):
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
        """The ETag check verifies the PUT against the bytes read; the length check below
        verifies the GET. Neither alone catches both a bad upload and a short read."""
        monkeypatch.setattr(
            "scripts.copy_zarr3_samples._open",
            lambda *a, **k: _resp(b"payload"),
        )
        with pytest.raises(CopyError, match="not the bytes we read"):
            copy_object(self._client("deadbeef"), "https://x/k", "b", "k", optional=False)

    def test_objects_stream_through_a_spooled_file_not_memory(self, monkeypatch):
        """A whole shard in memory per worker is ~2 GB at 8 workers; stream it instead."""
        payload = bytes(range(256)) * 40
        monkeypatch.setattr("scripts.copy_zarr3_samples.READ_BYTES", 1000)
        monkeypatch.setattr("scripts.copy_zarr3_samples.SPOOL_BYTES", 1024)  # forces rollover
        monkeypatch.setattr("scripts.copy_zarr3_samples._open", lambda *a, **k: _resp(payload))
        seen = {}

        def put_object(**kwargs):
            seen["streamed"] = not isinstance(kwargs["Body"], bytes)
            seen["body"] = kwargs["Body"].read()
            seen["length"] = kwargs["ContentLength"]
            return {"ETag": f'"{hashlib.md5(payload, usedforsecurity=False).hexdigest()}"'}

        client = MagicMock()
        client.put_object.side_effect = put_object
        assert copy_object(client, "https://x/k", "b", "k", optional=False) == len(payload)
        assert seen == {"streamed": True, "body": payload, "length": len(payload)}

    def test_a_get_that_ends_early_is_refused_before_the_put(self, monkeypatch):
        """Reproduced 2026-09-28: 40,000 of 102,400 declared bytes copied with no error. The
        ETag check hashes the same short bytes it read, so it cannot catch this."""
        monkeypatch.setattr(
            "scripts.copy_zarr3_samples._open", lambda *a, **k: _resp(b"x" * 400, declared=1024)
        )
        client = self._client(hashlib.md5(b"x" * 400, usedforsecurity=False).hexdigest())
        with pytest.raises(CopyError, match="ended early: read 400 of 1024 bytes"):
            copy_object(client, "https://x/k", "b", "k", optional=False)
        client.put_object.assert_not_called()

    def test_absent_chunk_is_not_an_error(self, monkeypatch):
        """Zarr reads a missing chunk as the fill value; 4 non-scalar chunks and 19
        scalar ones are genuinely absent in a real store, so absence is copied."""
        monkeypatch.setattr("scripts.copy_zarr3_samples._open", _raise_404)
        assert copy_object(MagicMock(), "https://x/k", "b", "k", optional=True) is None

    def test_absent_required_object_fails_the_run(self, monkeypatch):
        """A node's zarr.json is named in the consolidated metadata: it must exist."""
        monkeypatch.setattr("scripts.copy_zarr3_samples._open", _raise_404)
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

    def test_some_absent_chunks_are_fill_values_and_are_named(self, monkeypatch, caplog):
        """Named, not counted: a vanished band looks exactly like an all-fill array."""
        monkeypatch.setattr(
            "scripts.copy_zarr3_samples.copy_object",
            lambda _c, url, _b, _k, optional: None if url.endswith("c/1") else 10,
        )
        with caplog.at_level("INFO", logger="copy_zarr3_samples"):
            assert copy_store(MagicMock(), self.PLAN, "b", "p/", workers=1, dry_run=False) == (
                3,
                1,
                30,
            )
        assert "absent (404): arr/c/1" in caplog.text

    def test_the_root_is_written_last_as_a_commit_marker(self, monkeypatch):
        """A reader opens the root first; written last, a part-copied store 404s on open
        instead of reading its missing chunks as fill values."""
        calls = []

        def record(_c, url, _b, _k, optional):
            calls.append(url)
            return 10

        monkeypatch.setattr("scripts.copy_zarr3_samples.copy_object", record)
        copy_store(MagicMock(), self.PLAN, "b", "p/", workers=2, dry_run=False)
        assert calls[-1] == "https://x/A.zarr/zarr.json"
        assert calls.count("https://x/A.zarr/zarr.json") == 1

    def test_a_failed_store_never_gets_its_root(self, monkeypatch):
        calls = []

        def failing_chunk(_c, url, _b, _k, optional):
            calls.append(url)
            if url.endswith("c/0"):
                raise CopyError("HTTP 500")
            return 10

        monkeypatch.setattr("scripts.copy_zarr3_samples.copy_object", failing_chunk)
        with pytest.raises(CopyError, match="A.zarr: HTTP 500"):
            copy_store(MagicMock(), self.PLAN, "b", "p/", workers=1, dry_run=False)
        assert "https://x/A.zarr/zarr.json" not in calls


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


class TestSourceSafety:
    ARGS = ["--dest", "s3://b/samples/", "--confine-to", "s3://b/samples/", "--max-stores", "2"]

    @pytest.mark.parametrize("root", ["http://x/A.zarr", "file:///etc/A.zarr"])
    def test_a_non_https_root_is_refused_before_planning(self, monkeypatch, root):
        called = []
        monkeypatch.setattr("scripts.copy_zarr3_samples.plan_store", called.append)
        assert main([*self.ARGS, "--store-root", root]) == 2
        assert called == []

    def test_two_roots_with_one_store_name_are_refused_before_planning(self, monkeypatch):
        """The copy is flat: both would write <prefix>/A.zarr/, mixing two sources."""
        called = []
        monkeypatch.setattr("scripts.copy_zarr3_samples.plan_store", called.append)
        roots = ["--store-root", "https://x/2026/09/A.zarr", "--store-root", "https://x/A.zarr/"]
        assert main([*self.ARGS, *roots]) == 2
        assert called == []

    def test_a_redirect_is_refused_not_followed(self):
        """A 3xx could downgrade to http or change host; its bytes would still be uploaded."""
        request = urllib.request.Request("https://x/A.zarr/zarr.json")
        with pytest.raises(urllib.error.HTTPError, match="redirect to http://elsewhere/"):
            _RefuseRedirects().redirect_request(
                request, None, 302, "Found", {}, "http://elsewhere/"
            )
        # ...and it is the handler every source read goes through.
        assert any(isinstance(h, _RefuseRedirects) for h in _open.__self__.handlers)

    def test_a_redirected_object_fails_the_copy(self, monkeypatch):
        def redirected(*_a, **_k):
            raise urllib.error.HTTPError("https://x/k", 302, "redirect to http://e/", {}, None)

        monkeypatch.setattr("scripts.copy_zarr3_samples._open", redirected)
        with pytest.raises(CopyError, match="HTTP 302"):
            copy_object(MagicMock(), "https://x/k", "b", "k", optional=True)


class TestDryRun:
    def test_dry_run_plans_and_confines_but_writes_nothing(self, monkeypatch):
        monkeypatch.setattr(
            "scripts.copy_zarr3_samples.plan_store",
            lambda root: StorePlan(root=root, name="A.zarr", keys=["zarr.json"]),
        )
        client = _bucket()
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
        code = main(
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
        assert code == 2, "a misaimed destination is an operator error"
        client.put_object.assert_not_called()


class TestFailures:
    ARGS = ["--dest", "s3://b/p/", "--confine-to", "s3://b/p/", "--max-stores", "2"]
    PLAN = StorePlan(
        root="https://x/A.zarr",
        name="A.zarr",
        keys=["zarr.json", *(f"arr/c/{i}" for i in range(6))],
        required_keys={"zarr.json"},
    )

    def test_a_store_stops_at_its_first_failure(self, monkeypatch):
        """pool.map queued every copy; leaving the pool then waited for all of them."""
        calls = []

        def failing(_c, url, _b, _k, optional):
            calls.append(url)
            raise CopyError("HTTP 500")

        monkeypatch.setattr("scripts.copy_zarr3_samples.copy_object", failing)
        with pytest.raises(CopyError, match="A.zarr: HTTP 500"):
            copy_store(MagicMock(), self.PLAN, "b", "p/", workers=1, dry_run=False)
        assert len(calls) <= 2, f"{len(calls)} of 7 copies ran after the first failure"

    def test_a_failed_store_is_reported_partial_and_the_next_store_still_runs(self, monkeypatch):
        monkeypatch.setattr(
            "scripts.copy_zarr3_samples.plan_store",
            lambda root: StorePlan(root=root, name=root.rsplit("/", 1)[-1], keys=["zarr.json"]),
        )
        copied = []

        def copy_store_stub(_client, plan, *_a, **_k):
            if plan.name == "A.zarr":
                raise CopyError("A.zarr: HTTP 500")
            copied.append(plan.name)
            return 1, 0, 10

        monkeypatch.setattr("scripts.copy_zarr3_samples.copy_store", copy_store_stub)
        monkeypatch.setattr("scripts.copy_zarr3_samples.boto3.client", lambda *a, **k: _bucket())
        roots = ["--store-root", "https://x/A.zarr", "--store-root", "https://x/B.zarr"]
        assert main([*self.ARGS, *roots]) == 1
        assert copied == ["B.zarr"]

    def test_ctrl_c_stops_the_run_and_reports_the_store_partial(self, monkeypatch, caplog):
        monkeypatch.setattr(
            "scripts.copy_zarr3_samples.plan_store",
            lambda root: StorePlan(root=root, name=root.rsplit("/", 1)[-1], keys=["zarr.json"]),
        )
        started = []

        def interrupted(_client, plan, *_a, **_k):
            started.append(plan.name)
            raise KeyboardInterrupt

        monkeypatch.setattr("scripts.copy_zarr3_samples.copy_store", interrupted)
        monkeypatch.setattr("scripts.copy_zarr3_samples.boto3.client", lambda *a, **k: _bucket())
        roots = ["--store-root", "https://x/A.zarr", "--store-root", "https://x/B.zarr"]
        try:
            code = main([*self.ARGS, *roots])
        except KeyboardInterrupt:
            pytest.fail("Ctrl-C escaped main: no PARTIAL line, no summary")
        assert code == 130
        assert started == ["A.zarr"], "no further store may start after Ctrl-C"
        assert "PARTIAL" in caplog.text and "s3://b/p/A.zarr/" in caplog.text

    def test_ctrl_c_inside_a_store_cancels_its_queued_copies(self, monkeypatch):
        """Only `Exception` was caught, so Ctrl-C waited for all 41 of 41 queued writes."""
        calls = []

        def interrupted(_c, url, _b, _k, optional):
            calls.append(url)
            raise KeyboardInterrupt

        monkeypatch.setattr("scripts.copy_zarr3_samples.copy_object", interrupted)
        with pytest.raises(KeyboardInterrupt):
            copy_store(MagicMock(), self.PLAN, "b", "p/", workers=1, dry_run=False)
        assert len(calls) <= 2, f"{len(calls)} of 6 chunk copies ran after Ctrl-C"

    def test_an_existing_destination_store_is_refused_unless_overwrite(self, monkeypatch):
        """Copying over a same-named store mixes two copies; its stale objects would hide
        fill-value holes."""
        monkeypatch.setattr(
            "scripts.copy_zarr3_samples.plan_store",
            lambda root: StorePlan(root=root, name="A.zarr", keys=["zarr.json"]),
        )
        client = _bucket("p/A.zarr/zarr.json")
        monkeypatch.setattr("scripts.copy_zarr3_samples.boto3.client", lambda *a, **k: client)
        stores = []
        monkeypatch.setattr(
            "scripts.copy_zarr3_samples.copy_store",
            lambda _c, plan, *_a, **_k: stores.append(plan.name) or (1, 0, 10),
        )
        root = ["--store-root", "https://x/A.zarr"]
        assert main([*self.ARGS, *root]) == 2
        assert stores == []
        assert main([*self.ARGS, *root, "--overwrite"]) == 0
        assert stores == ["A.zarr"]

    def test_every_key_written_is_under_the_confined_prefix(self, monkeypatch):
        """Confinement checks the keys main computes; this ties them to the keys written."""
        plan = StorePlan(
            root="https://x/A.zarr",
            name="A.zarr",
            keys=["zarr.json", "arr/zarr.json", "arr/c/0"],
            required_keys={"zarr.json", "arr/zarr.json"},
        )
        monkeypatch.setattr("scripts.copy_zarr3_samples.plan_store", lambda root: plan)
        monkeypatch.setattr("scripts.copy_zarr3_samples._open", lambda *a, **k: _resp(b"x"))
        client = _bucket()
        client.put_object.return_value = {
            "ETag": f'"{hashlib.md5(b"x", usedforsecurity=False).hexdigest()}"'
        }
        monkeypatch.setattr("scripts.copy_zarr3_samples.boto3.client", lambda *a, **k: client)
        assert main([*self.ARGS, "--store-root", plan.root]) == 0
        written = [call.kwargs for call in client.put_object.call_args_list]
        assert len(written) == 3
        assert all(w["Bucket"] == "b" and w["Key"].startswith("p/A.zarr/") for w in written)

    def test_a_planning_failure_writes_nothing_and_exits_1(self, monkeypatch):
        def unreachable(_url):
            raise urllib.error.URLError("connection refused")

        monkeypatch.setattr("scripts.copy_zarr3_samples.fetch_json", unreachable)
        client = MagicMock()
        monkeypatch.setattr("scripts.copy_zarr3_samples.boto3.client", lambda *a, **k: client)
        assert main([*self.ARGS, "--store-root", "https://x/A.zarr"]) == 1
        client.put_object.assert_not_called()

    @pytest.mark.parametrize(
        "extra",
        [["--workers", "0"], ["--dest", "bucket/p/"], ["--confine-to", "s3:///p/"]],
    )
    def test_operator_errors_exit_2_not_a_traceback(self, extra):
        assert main([*self.ARGS, "--store-root", "https://x/A.zarr", *extra]) == 2


class _resp:
    """A source response. ``declared`` overrides the Content-Length, to fake a GET that
    ends before the whole object has arrived."""

    def __init__(self, body, declared=None):
        self._body = io.BytesIO(body)
        self.headers = {"Content-Length": str(len(body) if declared is None else declared)}

    def read(self, size=-1):
        return self._body.read(size)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _raise_404(*args, **kwargs):
    raise urllib.error.HTTPError("https://x/k", 404, "Not Found", {}, None)


def _bucket(*existing_keys):
    """An S3 client whose bucket holds only ``existing_keys``: every other HEAD is a 404."""
    client = MagicMock()

    def head_object(Bucket, Key):  # noqa: N803 - boto3's keyword names
        if Key not in existing_keys:
            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")
        return {}

    client.head_object.side_effect = head_object
    return client
