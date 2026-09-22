"""Unit tests for provision_source_cache_lifecycle.py.

The dangerous part of this script is not the rule it adds, it is the rule set it might
silently drop: PutBucketLifecycleConfiguration REPLACES the whole configuration, and the
S2 output bucket is shared with other rules and with the prod namespace. So the tests
that matter here are the ones about what SURVIVES a run.

No network: the S3 client is an in-memory fake, so read/merge/put/verify run for real.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from botocore.exceptions import ClientError

scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

import provision_source_cache_lifecycle as lc  # noqa: E402
import provision_tier_down_lifecycle as td  # noqa: E402

# The read-back retries for eventual consistency; an in-memory fake is consistent, so a
# retry here would only make the failure tests sleep.
lc._VERIFY_ATTEMPTS = 1

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
        self.deletes = 0

    def get_bucket_lifecycle_configuration(self, Bucket: str):  # noqa: N803 (boto3 kwarg)
        if self.read_error:
            raise _client_error(self.read_error)
        if self.rules is None:
            raise _client_error("NoSuchLifecycleConfiguration")
        return {"Rules": self.rules}

    def put_bucket_lifecycle_configuration(  # noqa: N803 (boto3 kwarg)
        self, Bucket: str, LifecycleConfiguration: dict
    ):
        # S3 rejects a configuration with zero rules; the fake must too, or the tests
        # would bless a call that fails in production.
        if not LifecycleConfiguration["Rules"]:
            raise _client_error("MalformedXML")
        self.puts.append(LifecycleConfiguration["Rules"])
        self.rules = LifecycleConfiguration["Rules"]

    def delete_bucket_lifecycle(self, Bucket: str):  # noqa: N803 (boto3 kwarg)
        self.deletes += 1
        self.rules = None  # back to NoSuchLifecycleConfiguration


def _ids(rules: list[dict]) -> set[str]:
    return {r["ID"] for r in rules}


def test_creates_the_rule_when_the_bucket_has_no_lifecycle_config():
    fake = FakeS3(rules=None)  # NoSuchLifecycleConfiguration
    lc.provision(fake, BUCKET, lc.expiration_rule(lc.DEFAULT_PREFIX, lc.DEFAULT_DAYS), apply=True)
    assert _ids(fake.rules) == {lc.RULE_ID}


def test_preserves_unrelated_rules():
    fake = FakeS3(rules=[UNRELATED_RULE, OTHER_RULE])
    lc.provision(fake, BUCKET, lc.expiration_rule(lc.DEFAULT_PREFIX, lc.DEFAULT_DAYS), apply=True)
    assert _ids(fake.rules) == {"tier-down-converted", "abort-mpu", lc.RULE_ID}
    # and byte-identical, not merely present
    assert UNRELATED_RULE in fake.rules
    assert OTHER_RULE in fake.rules


def test_rerunning_replaces_our_rule_without_duplicating_it():
    fake = FakeS3(rules=[UNRELATED_RULE])
    lc.provision(fake, BUCKET, lc.expiration_rule(lc.DEFAULT_PREFIX, lc.DEFAULT_DAYS), apply=True)
    lc.provision(fake, BUCKET, lc.expiration_rule(lc.DEFAULT_PREFIX, lc.DEFAULT_DAYS), apply=True)
    assert len([r for r in fake.rules if r["ID"] == lc.RULE_ID]) == 1
    assert len(fake.rules) == 2


def test_rerunning_with_a_new_expiry_updates_in_place():
    fake = FakeS3(rules=None)
    lc.provision(fake, BUCKET, lc.expiration_rule(lc.DEFAULT_PREFIX, 7), apply=True)
    lc.provision(fake, BUCKET, lc.expiration_rule(lc.DEFAULT_PREFIX, 14), apply=True)
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
        lc.provision(
            fake, BUCKET, lc.expiration_rule(lc.DEFAULT_PREFIX, lc.DEFAULT_DAYS), apply=True
        )
    assert fake.puts == []  # nothing was written


def test_dry_run_writes_nothing():
    fake = FakeS3(rules=[UNRELATED_RULE])
    lc.provision(fake, BUCKET, lc.expiration_rule(lc.DEFAULT_PREFIX, lc.DEFAULT_DAYS), apply=False)
    assert fake.puts == []
    assert fake.rules == [UNRELATED_RULE]


def test_the_rule_expires_the_source_cache_prefix():
    fake = FakeS3(rules=None)
    lc.provision(fake, BUCKET, lc.expiration_rule(lc.DEFAULT_PREFIX, lc.DEFAULT_DAYS), apply=True)
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
        lc.provision(
            fake, BUCKET, lc.expiration_rule(lc.DEFAULT_PREFIX, lc.DEFAULT_DAYS), apply=True
        )


def test_refuses_an_empty_prefix():
    """An empty prefix would expire the entire bucket, converted output included."""
    fake = FakeS3(rules=None)
    with pytest.raises(ValueError, match="prefix"):
        lc.provision(fake, BUCKET, lc.expiration_rule("", lc.DEFAULT_DAYS), apply=True)
    assert fake.puts == []


@pytest.mark.parametrize("days", [0, -1])
def test_refuses_a_non_positive_expiry(days):
    fake = FakeS3(rules=None)
    with pytest.raises(ValueError, match="days"):
        lc.provision(fake, BUCKET, lc.expiration_rule(lc.DEFAULT_PREFIX, days), apply=True)
    assert fake.puts == []


# ---------------------------------------------------------------------------
# The High Performance -> STANDARD transition rule (provision_tier_down_lifecycle.py).
#
# The rule's Filter IS its blast radius, and the put replaces the bucket's whole
# configuration, so the tests that matter are: the other rules survive, and what the
# server stored is what we sent.
# ---------------------------------------------------------------------------

# Live on the bucket since 2026-07-16. It must survive every run.
EXPIRE_SOURCE_CACHE: dict[str, Any] = {
    "ID": "expire-source-cache",
    "Status": "Enabled",
    "Filter": {"Prefix": "source-cache/"},
    "Expiration": {"Days": 7},
}
TRANSITION_PREFIX = "tests-output/"


def _transition() -> dict:
    return td.transition_rule(TRANSITION_PREFIX, 1, 1)


def test_transition_merge_preserves_expire_source_cache():
    """The union from the plan, exactly: our rule added, theirs untouched."""
    fake = FakeS3(rules=[dict(EXPIRE_SOURCE_CACHE)])
    lc.provision(fake, BUCKET, _transition(), apply=True)
    assert fake.rules == [
        EXPIRE_SOURCE_CACHE,
        {
            "ID": "tier-down-all-to-standard",
            "Status": "Enabled",
            "Filter": {"And": {"Prefix": "tests-output/", "ObjectSizeGreaterThan": 1}},
            "Transitions": [{"Days": 1, "StorageClass": "STANDARD"}],
        },
    ]


def test_transition_filter_is_an_and_of_prefix_and_size():
    """A prefix plus a size predicate must be wrapped in And. A bare
    {"Prefix", "ObjectSizeGreaterThan"} is not valid S3 and botocore will not catch it."""
    fake = FakeS3(rules=None)
    lc.provision(fake, BUCKET, _transition(), apply=True)
    (rule,) = fake.rules
    assert rule["Filter"] == {"And": {"Prefix": "tests-output/", "ObjectSizeGreaterThan": 1}}
    assert "Prefix" not in rule["Filter"]
    assert rule["Transitions"] == [{"Days": 1, "StorageClass": "STANDARD"}]


def test_transition_dry_run_writes_nothing():
    fake = FakeS3(rules=[EXPIRE_SOURCE_CACHE])
    lc.provision(fake, BUCKET, _transition(), apply=False)
    assert fake.puts == []
    assert fake.rules == [EXPIRE_SOURCE_CACHE]


def test_transition_rerun_does_not_duplicate_the_rule():
    fake = FakeS3(rules=[EXPIRE_SOURCE_CACHE])
    lc.provision(fake, BUCKET, _transition(), apply=True)
    lc.provision(fake, BUCKET, td.transition_rule(TRANSITION_PREFIX, 30, 1), apply=True)
    ours = [r for r in fake.rules if r["ID"] == td.RULE_ID]
    assert len(ours) == 1
    assert ours[0]["Transitions"] == [{"Days": 30, "StorageClass": "STANDARD"}]
    assert len(fake.rules) == 2


class _StoresSomethingElse(FakeS3):
    """A server that accepts the put and then stores a rule set of its own choosing."""

    def __init__(self, stored: list[dict]) -> None:
        super().__init__(rules=[EXPIRE_SOURCE_CACHE])
        self._stored = stored

    def put_bucket_lifecycle_configuration(  # noqa: N803 (boto3 kwarg)
        self, Bucket: str, LifecycleConfiguration: dict
    ):
        self.puts.append(LifecycleConfiguration["Rules"])
        self.rules = self._stored


def test_transition_readback_raises_when_the_size_predicate_was_dropped():
    """OVH may accept the rule and honour only half the filter. Without the size
    predicate every object of 128KB or less stays in High Performance and the
    convergence check never reaches zero, so a flattened filter must fail loudly."""
    flattened = {**_transition(), "Filter": {"Prefix": TRANSITION_PREFIX}}
    fake = _StoresSomethingElse([EXPIRE_SOURCE_CACHE, flattened])
    with pytest.raises(RuntimeError, match="[Vv]erif"):
        lc.provision(fake, BUCKET, _transition(), apply=True)


def test_transition_readback_raises_when_the_target_class_was_altered():
    retargeted = {**_transition(), "Transitions": [{"Days": 1, "StorageClass": "GLACIER"}]}
    fake = _StoresSomethingElse([EXPIRE_SOURCE_CACHE, retargeted])
    with pytest.raises(RuntimeError, match="[Vv]erif"):
        lc.provision(fake, BUCKET, _transition(), apply=True)


def test_readback_raises_when_another_rule_was_dropped():
    """The 1.3GB-per-failed-convert guard. A put that stored our rule and quietly lost
    expire-source-cache looks like success everywhere except here."""
    fake = _StoresSomethingElse([_transition()])
    with pytest.raises(RuntimeError, match="dropped"):
        lc.provision(fake, BUCKET, _transition(), apply=True)


def test_dry_run_line_shows_prefix_size_and_target():
    """The dry run is the in-tool bound: an operator reads this line before typing
    --apply, so the whole filter has to be on it."""
    line = lc._describe(_transition())
    assert "tests-output/" in line
    assert "size>1B" in line
    assert "STANDARD" in line
    assert "<none>" not in line


@pytest.mark.parametrize(
    "kwargs",
    [
        {"prefix": "", "transition_days": 1, "min_object_size": 1},
        {"prefix": TRANSITION_PREFIX, "transition_days": 0, "min_object_size": 1},
        {"prefix": TRANSITION_PREFIX, "transition_days": 1, "min_object_size": -1},
    ],
)
def test_transition_rule_refuses_nonsense(kwargs):
    with pytest.raises(ValueError):
        td.transition_rule(**kwargs)


def test_transition_rule_allows_a_zero_size_floor():
    """0 is legal and meaningful (it reaches 1-byte directory markers); only negatives
    are refused. Whether OVH honours an explicit 0 is for the scratch-bucket probe."""
    rule = td.transition_rule(TRANSITION_PREFIX, 1, 0)
    assert rule["Filter"]["And"]["ObjectSizeGreaterThan"] == 0


# --- removal: the rollback path ------------------------------------------------------


def test_remove_restores_the_prior_config():
    fake = FakeS3(rules=[dict(EXPIRE_SOURCE_CACHE)])
    lc.provision(fake, BUCKET, _transition(), apply=True)
    lc.deprovision(fake, BUCKET, td.RULE_ID, apply=True)
    assert fake.rules == [EXPIRE_SOURCE_CACHE]


def test_remove_is_idempotent():
    """A rollback re-run must not fail: the rule being already gone is the goal, and a
    count check of "one fewer than before" would wrongly raise here."""
    fake = FakeS3(rules=[dict(EXPIRE_SOURCE_CACHE)])
    lc.deprovision(fake, BUCKET, td.RULE_ID, apply=True)
    lc.deprovision(fake, BUCKET, td.RULE_ID, apply=True)
    assert fake.rules == [EXPIRE_SOURCE_CACHE]


def test_remove_dry_run_writes_nothing():
    fake = FakeS3(rules=[EXPIRE_SOURCE_CACHE, _transition()])
    lc.deprovision(fake, BUCKET, td.RULE_ID, apply=False)
    assert fake.puts == []
    assert fake.deletes == 0


def test_removing_the_last_rule_deletes_the_configuration():
    """S3 has no configuration with zero rules: putting an empty Rules list is
    MalformedXML. On a bucket where ours is the only rule — the tests bucket during the
    scratch probe — a put-based rollback fails and leaves the transition rule live."""
    fake = FakeS3(rules=[_transition()])
    lc.deprovision(fake, BUCKET, td.RULE_ID, apply=True)
    assert fake.deletes == 1
    assert fake.puts == []  # never attempted the invalid empty put
    assert lc.read_rules(fake, BUCKET) == []


def test_verification_survives_a_date_based_rule_on_the_bucket():
    """botocore parses Expiration.Date into a datetime. If the failure diagnostic cannot
    serialise it, a real verification failure turns into a TypeError traceback and the
    operator loses the one message that says which rule was dropped."""
    dated: dict[str, Any] = {
        "ID": "archive-by-date",
        "Status": "Enabled",
        "Filter": {"Prefix": "archive/"},
        "Expiration": {"Date": datetime(2027, 1, 1, tzinfo=UTC)},
    }
    fake = _StoresSomethingElse([dated])  # our rule vanished; dated rule survives
    fake.rules = [dated]
    with pytest.raises(RuntimeError, match="dropped"):
        lc.provision(fake, BUCKET, _transition(), apply=True)


def test_verification_tolerates_a_rule_with_no_id():
    """read_rules returns whatever the server sends, and OVH is not guaranteed to assign
    an ID. An ID-less rule must not turn the read-back into a KeyError after the put."""
    anonymous: dict[str, Any] = {"Status": "Enabled", "Filter": {"Prefix": "misc/"}}
    fake = FakeS3(rules=[anonymous])
    stored = lc.provision(fake, BUCKET, _transition(), apply=True)
    assert anonymous in stored
    assert len(stored) == 2


def test_an_empty_prefix_is_not_printed_like_an_absent_one():
    """Filter.Prefix "" matches every object in the bucket. Printing it as <none> would
    understate the blast radius in the one place an operator checks it."""
    whole_bucket = {"ID": "abort-mpu", "Status": "Enabled", "Filter": {"Prefix": ""}}
    assert "whole bucket" in lc._describe(whole_bucket)


# --- the CLI contract ----------------------------------------------------------------


def _no_client(monkeypatch):
    """A test that reaches boto3 has already failed: these all refuse before that."""

    def _boom(*args, **kwargs):
        raise AssertionError("a client must not be built")

    monkeypatch.setattr(td.boto3, "client", _boom)


@pytest.fixture
def no_client(monkeypatch):
    _no_client(monkeypatch)
    monkeypatch.setenv("AWS_ENDPOINT_URL", "https://s3.example.invalid")


@pytest.mark.parametrize("argv", [[], ["--bucket", BUCKET], ["--prefix", "tests-output/"]])
def test_tier_down_cli_requires_an_explicit_target(argv, no_client):
    """--bucket and --prefix have no defaults: this rule rewrites storage classes, so
    nobody runs it against whatever the environment happened to point at."""
    with pytest.raises(SystemExit) as exc:
        td.main(argv)
    assert exc.value.code == 2


def test_tier_down_cli_remove_does_not_need_a_prefix(no_client, monkeypatch):
    """The rollback path must not make an operator type a prefix that does nothing."""
    monkeypatch.setattr(td.boto3, "client", lambda *a, **k: FakeS3(rules=[EXPIRE_SOURCE_CACHE]))
    assert td.main(["--bucket", BUCKET, "--remove"]) == 0


def test_tier_down_cli_refuses_an_unset_endpoint(monkeypatch):
    """Unset, boto3 addresses the bucket name at AWS rather than at OVH. Never infer the
    target: refuse."""
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    _no_client(monkeypatch)
    assert td.main(["--bucket", BUCKET, "--prefix", TRANSITION_PREFIX]) == 1


def test_tier_down_cli_exits_1_on_a_bad_rule(no_client, monkeypatch):
    """A builder ValueError must exit 1 like every other failure, not traceback."""
    monkeypatch.setattr(td.boto3, "client", lambda *a, **k: FakeS3(rules=None))
    argv = ["--bucket", BUCKET, "--prefix", TRANSITION_PREFIX, "--min-object-size", "-1"]
    assert td.main(argv) == 1


def test_tier_down_cli_dry_run_writes_nothing(monkeypatch):
    fake = FakeS3(rules=[EXPIRE_SOURCE_CACHE])
    monkeypatch.setenv("AWS_ENDPOINT_URL", "https://s3.example.invalid")
    monkeypatch.setattr(td.boto3, "client", lambda *a, **k: fake)
    assert td.main(["--bucket", BUCKET, "--prefix", TRANSITION_PREFIX]) == 0
    assert fake.puts == []
