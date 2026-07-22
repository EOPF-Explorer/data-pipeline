"""Unit tests for provision_source_cache_lifecycle.py.

The dangerous part of this script is not the rule it adds, it is the rule set it might
silently drop: PutBucketLifecycleConfiguration REPLACES the whole configuration, and the
S2 output bucket is shared with other rules and with the prod namespace. So the tests
that matter here are the ones about what SURVIVES a run.

No network: the S3 client is an in-memory fake, so read/merge/put/verify run for real.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest
from botocore.exceptions import ClientError

scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

import provision_source_cache_lifecycle as lc  # noqa: E402

BUCKET = "esa-zarr-sentinel-explorer-fra"

# A rule that has nothing to do with us and must never be collateral damage.
UNRELATED_RULE: dict[str, Any] = {
    "ID": "tier-down-converted",
    "Status": "Enabled",
    "Filter": {"Prefix": "tests-output/"},
    "Expiration": {"Days": 180},
}
OTHER_RULE: dict[str, Any] = {
    "ID": "abort-mpu",
    "Status": "Enabled",
    "Filter": {"Prefix": ""},
}


def _client_error(code: str) -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": code}}, "GetBucketLifecycleConfiguration"
    )


class FakeS3:
    """Minimal lifecycle-config surface of an S3 client."""

    def __init__(self, rules: list[dict] | None = None, read_error: str | None = None) -> None:
        self.rules = rules
        self.read_error = read_error
        self.puts: list[list[dict]] = []

    def get_bucket_lifecycle_configuration(self, Bucket: str):  # noqa: N803 (boto3 kwarg)
        if self.read_error:
            raise _client_error(self.read_error)
        if self.rules is None:
            raise _client_error("NoSuchLifecycleConfiguration")
        return {"Rules": self.rules}

    def put_bucket_lifecycle_configuration(  # noqa: N803 (boto3 kwarg)
        self, Bucket: str, LifecycleConfiguration: dict
    ):
        self.puts.append(LifecycleConfiguration["Rules"])
        self.rules = LifecycleConfiguration["Rules"]


def _ids(rules: list[dict]) -> set[str]:
    return {r["ID"] for r in rules}


def test_creates_the_rule_when_the_bucket_has_no_lifecycle_config():
    fake = FakeS3(rules=None)  # NoSuchLifecycleConfiguration
    lc.provision(fake, BUCKET, lc.DEFAULT_PREFIX, lc.DEFAULT_DAYS, apply=True)
    assert _ids(fake.rules) == {lc.RULE_ID}


def test_preserves_unrelated_rules():
    fake = FakeS3(rules=[UNRELATED_RULE, OTHER_RULE])
    lc.provision(fake, BUCKET, lc.DEFAULT_PREFIX, lc.DEFAULT_DAYS, apply=True)
    assert _ids(fake.rules) == {"tier-down-converted", "abort-mpu", lc.RULE_ID}
    # and byte-identical, not merely present
    assert UNRELATED_RULE in fake.rules
    assert OTHER_RULE in fake.rules


def test_rerunning_replaces_our_rule_without_duplicating_it():
    fake = FakeS3(rules=[UNRELATED_RULE])
    lc.provision(fake, BUCKET, lc.DEFAULT_PREFIX, lc.DEFAULT_DAYS, apply=True)
    lc.provision(fake, BUCKET, lc.DEFAULT_PREFIX, lc.DEFAULT_DAYS, apply=True)
    assert len([r for r in fake.rules if r["ID"] == lc.RULE_ID]) == 1
    assert len(fake.rules) == 2


def test_rerunning_with_a_new_expiry_updates_in_place():
    fake = FakeS3(rules=None)
    lc.provision(fake, BUCKET, lc.DEFAULT_PREFIX, 7, apply=True)
    lc.provision(fake, BUCKET, lc.DEFAULT_PREFIX, 14, apply=True)
    ours = [r for r in fake.rules if r["ID"] == lc.RULE_ID]
    assert len(ours) == 1
    assert ours[0]["Expiration"]["Days"] == 14


@pytest.mark.parametrize("code", ["AccessDenied", "NoSuchBucket", "InvalidAccessKeyId"])
def test_a_read_error_is_never_mistaken_for_an_empty_config(code):
    """The wipe scenario: if a 403 were swallowed into "no rules yet", the merge would
    compute a rule set of exactly ours and put it, deleting every real rule on the
    bucket. Only NoSuchLifecycleConfiguration may mean empty."""
    fake = FakeS3(rules=[UNRELATED_RULE], read_error=code)
    with pytest.raises(ClientError):
        lc.provision(fake, BUCKET, lc.DEFAULT_PREFIX, lc.DEFAULT_DAYS, apply=True)
    assert fake.puts == []  # nothing was written


def test_dry_run_writes_nothing():
    fake = FakeS3(rules=[UNRELATED_RULE])
    lc.provision(fake, BUCKET, lc.DEFAULT_PREFIX, lc.DEFAULT_DAYS, apply=False)
    assert fake.puts == []
    assert fake.rules == [UNRELATED_RULE]


def test_the_rule_expires_the_source_cache_prefix():
    fake = FakeS3(rules=None)
    lc.provision(fake, BUCKET, lc.DEFAULT_PREFIX, lc.DEFAULT_DAYS, apply=True)
    (rule,) = fake.rules
    assert rule["Status"] == "Enabled"
    assert rule["Filter"]["Prefix"] == "source-cache/"
    assert rule["Expiration"]["Days"] == 7


@pytest.mark.parametrize(
    "staged_key",
    [
        "source-cache/devseed-staging/S2B_MSIL2A_X/zarr.json",
        "source-cache/devseed/S2B_MSIL2A_X/zarr.json",
    ],
)
def test_one_rule_covers_both_namespaces(staged_key):
    """devseed and devseed-staging share this bucket and stage under namespace-scoped
    prefixes; a single source-cache/ rule must reach both."""
    assert staged_key.startswith(lc.DEFAULT_PREFIX)


def test_verification_fails_when_the_stored_rule_does_not_match():
    """A put that reports success but stores something else is exactly what the read-back
    exists to catch."""

    class LyingS3(FakeS3):
        def put_bucket_lifecycle_configuration(self, Bucket: str, LifecycleConfiguration: dict):  # noqa: N803
            self.puts.append(LifecycleConfiguration["Rules"])
            self.rules = []  # accepted, stored nothing

    fake = LyingS3(rules=None)
    with pytest.raises(RuntimeError, match="[Vv]erif"):
        lc.provision(fake, BUCKET, lc.DEFAULT_PREFIX, lc.DEFAULT_DAYS, apply=True)


def test_refuses_an_empty_prefix():
    """An empty prefix would expire the entire bucket, converted output included."""
    fake = FakeS3(rules=None)
    with pytest.raises(ValueError, match="prefix"):
        lc.provision(fake, BUCKET, "", lc.DEFAULT_DAYS, apply=True)
    assert fake.puts == []


@pytest.mark.parametrize("days", [0, -1])
def test_refuses_a_non_positive_expiry(days):
    fake = FakeS3(rules=None)
    with pytest.raises(ValueError, match="days"):
        lc.provision(fake, BUCKET, lc.DEFAULT_PREFIX, days, apply=True)
    assert fake.puts == []
