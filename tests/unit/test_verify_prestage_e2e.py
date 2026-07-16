"""Unit tests for verify_prestage_e2e.py.

The gate exists to catch a success that isn't one: prestage-source falls back to
passthrough and exits 0 whenever it cannot copy, so every node goes green while convert
reads https:// exactly as before. "The workflow succeeded" is not evidence that anything
was staged. These tests pin the judgements that tell those two apart — including the
Argo node shape the phases are read from, which is an assumption worth freezing.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

import verify_prestage_e2e as v  # noqa: E402
from source_url_utils import derive_item_id  # noqa: E402

ITEM_ID = "S2B_MSIL2A_20260713T104619_N0512_R051_T31UDP_20260713T131840"
STAGED_URL = f"s3://esa-zarr-sentinel-explorer-fra/source-cache/devseed-staging/{ITEM_ID}"
STAC_URL = f"https://stac.core.eopf.eodc.eu/collections/sentinel-2-l2a/items/{ITEM_ID}"


class TestClassifySourceUrl:
    def test_a_real_staged_copy_is_staged(self):
        assert v.classify_source_url(STAGED_URL, ITEM_ID) == v.STAGED

    def test_a_silent_passthrough_is_caught(self):
        """The whole reason this gate exists."""
        assert v.classify_source_url(STAC_URL, ITEM_ID) == v.PASSTHROUGH

    def test_a_gateway_passthrough_is_caught(self):
        url = f"https://s3.explorer.eopf.copernicus.eu/bucket/cpm-manual/{ITEM_ID}.zarr"
        assert v.classify_source_url(url, ITEM_ID) == v.PASSTHROUGH

    def test_missing_output_is_caught(self):
        assert v.classify_source_url("", ITEM_ID) == v.EMPTY

    def test_a_staged_url_for_a_different_item_is_caught(self):
        """Would mean the staged key and the STAC id have drifted apart."""
        other = "s3://bucket/source-cache/devseed-staging/SOME_OTHER_ITEM"
        assert v.classify_source_url(other, ITEM_ID) == v.MISMATCH

    def test_an_s3_url_outside_the_cache_prefix_is_caught(self):
        assert v.classify_source_url(f"s3://bucket/elsewhere/{ITEM_ID}", ITEM_ID) == v.MISMATCH

    @pytest.mark.parametrize("ns", ["devseed-staging", "devseed"])
    def test_either_namespace_prefix_counts_as_staged(self, ns):
        assert v.classify_source_url(f"s3://b/source-cache/{ns}/{ITEM_ID}", ITEM_ID) == v.STAGED


class TestParseS3Url:
    def test_splits_bucket_and_key(self):
        assert v.parse_s3_url(STAGED_URL) == (
            "esa-zarr-sentinel-explorer-fra",
            f"source-cache/devseed-staging/{ITEM_ID}",
        )

    def test_rejects_a_non_s3_url(self):
        with pytest.raises(ValueError):
            v.parse_s3_url(STAC_URL)


class TestArgoNodeReading:
    """Argo's retry shape: a Retry node keeps the task's displayName and carries the
    aggregate phase plus the successful attempt's outputs, while the pod children are
    renamed "task(0)", "task(1)". Exact-name matching must land on the Retry node.
    """

    WF = {
        "status": {
            "phase": "Succeeded",
            "nodes": {
                "a": {
                    "displayName": "prestage-source",
                    "type": "Retry",
                    "phase": "Succeeded",
                    "outputs": {
                        "parameters": [
                            {"name": "staged", "value": "true"},
                            {"name": "convert_source_url", "value": STAGED_URL},
                        ]
                    },
                },
                "b": {"displayName": "prestage-source(0)", "type": "Pod", "phase": "Failed"},
                "c": {"displayName": "prestage-source(1)", "type": "Pod", "phase": "Succeeded"},
                "d": {"displayName": "convert", "type": "Retry", "phase": "Succeeded"},
                "e": {"displayName": "cleanup-source", "type": "Retry", "phase": "Omitted"},
            },
        }
    }

    def test_reads_the_retry_node_not_a_failed_attempt(self):
        assert v.node_phase(self.WF, "prestage-source") == "Succeeded"

    def test_reads_outputs_from_the_retry_node(self):
        assert v.node_output(self.WF, "prestage-source", "staged") == "true"
        assert v.node_output(self.WF, "prestage-source", "convert_source_url") == STAGED_URL

    def test_a_missing_node_is_reported_not_crashed(self):
        assert v.node_phase(self.WF, "register") == "MISSING"

    def test_a_missing_output_is_empty(self):
        assert v.node_output(self.WF, "convert", "staged") == ""

    def test_an_omitted_cleanup_is_not_success(self):
        """Omitted means the `when` was false, i.e. nothing was staged."""
        assert v.node_phase(self.WF, "cleanup-source") == "Omitted"


class TestItemIdIsNotReimplemented:
    def test_the_gate_uses_the_shared_derivation(self):
        """A third implementation would let the staged key and the STAC id drift; the
        gate must judge by the same rule prestage and convert use."""
        assert v.item_id_for(STAC_URL) == derive_item_id(STAC_URL) == ITEM_ID

    @pytest.mark.parametrize(
        "url",
        [
            STAC_URL,
            f"https://objects.eodc.eu:443/b:c/13/products/cpm_v270/{ITEM_ID}.zarr",
            f"https://example.org/items/{ITEM_ID}.json",
        ],
    )
    def test_agrees_with_derive_item_id_on_every_url_shape(self, url):
        assert v.item_id_for(url) == derive_item_id(url)


class TestS3Preflight:
    """Bad S3 credentials must fail before the convert, not after it.

    The cleanup assertion is the last check to run, so without a preflight a credentials
    problem surfaces ~10 minutes in, after a full convert, and takes the registered/renders
    checks down with it — the run is wasted rather than merely failed.
    """

    def test_bad_credentials_fail_before_anything_is_submitted(self, monkeypatch):
        submitted = []
        monkeypatch.setattr(v, "submit", lambda *a, **k: submitted.append(a) or "wf-should-not-run")

        class Boom:
            def list_buckets(self):
                raise RuntimeError("Unable to locate credentials")

        monkeypatch.setattr(v.boto3, "client", lambda *a, **k: Boom())

        rc = v.main([STAC_URL, "--image", "pr-344"])

        assert rc == 1
        assert submitted == [], "submitted a workflow despite unusable S3 credentials"

    def test_working_credentials_do_not_block_the_run(self, monkeypatch):
        """The preflight must not become a second failure mode of its own."""

        class Fine:
            def list_buckets(self):
                return {"Buckets": []}

        monkeypatch.setattr(v.boto3, "client", lambda *a, **k: Fine())
        monkeypatch.setattr(v, "submit", lambda *a, **k: "wf-1")
        monkeypatch.setattr(v, "fetch", lambda *a, **k: {"status": {"nodes": {}}})
        monkeypatch.setattr(v, "verify", lambda *a, **k: None)

        assert v.main([STAC_URL, "--image", "pr-344"]) == 0
