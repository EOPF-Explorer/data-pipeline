#!/usr/bin/env python3
"""Ensure the ``source-cache/`` expiry rule exists on the S2 output bucket.

Why this exists
---------------
The convert DAG stages the source Zarr to ``s3://<bucket>/source-cache/<ns>/<item>/`` and
deletes it in ``cleanup-source`` after a successful register. That cleanup runs with
``continueOn: {failed: true}`` on purpose — a janitorial failure must never mark a
successfully-registered item as a pipeline failure — and it is skipped entirely whenever
convert or register fails, so the copy survives for a cheap retry.

Both behaviours are deliberate and both leak. Without an expiry rule the leak is
permanent: every failed convert strands ~1.3 GB that nothing ever looks at again. The
rule is not an optimisation, it is the required backstop for ``continueOn: failed``
(spec ``claude-docs/specs/prestage_source_s3.md`` §5.6, data-pipeline#339).

The prefix is ``source-cache/`` rather than ``source-cache/<namespace>/`` so one rule
covers both ``devseed`` and ``devseed-staging``, which share this bucket.

Safety
------
``PutBucketLifecycleConfiguration`` REPLACES the entire configuration — it is not a
merge. Putting one rule blindly silently deletes every other rule on the bucket. So this
reads the current configuration, keeps every rule that is not ours, puts the union, and
reads it back to confirm. It writes nothing without ``--apply``.

Only ``NoSuchLifecycleConfiguration`` may be read as "no rules yet". Any other error
(``AccessDenied``, wrong endpoint, ...) is re-raised: treating it as an empty rule set is
precisely how a permissions problem turns into a wiped bucket configuration.

Usage
-----
    uv run python scripts/provision_source_cache_lifecycle.py            # dry run
    uv run python scripts/provision_source_cache_lifecycle.py --apply

Credentials come from the standard ``AWS_*`` environment, as everywhere else in this repo.

The read, merge and read-back verification here are shared with
``provision_tier_down_lifecycle.py``, which installs a storage-class transition rule on
the same buckets. Keep them generic: a rule is just a dict, built by its own script.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from typing import Any

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

RULE_ID = "expire-source-cache"
DEFAULT_BUCKET = "esa-zarr-sentinel-explorer-fra"
DEFAULT_PREFIX = "source-cache/"
DEFAULT_DAYS = 7

# Lifecycle configuration is eventually consistent, so the read-back gets a few tries
# before it calls a write failed. Tests set _VERIFY_ATTEMPTS = 1.
_VERIFY_ATTEMPTS = 3
_VERIFY_DELAY_S = 1.5


def expiration_rule(prefix: str, days: int) -> dict:
    """The ``source-cache/`` expiry rule. Validation lives with the rule it guards."""
    if not prefix.strip():
        raise ValueError("prefix must not be empty — that would expire the whole bucket")
    if days <= 0:
        raise ValueError(f"days must be positive, got {days}")
    return {
        "ID": RULE_ID,
        "Filter": {"Prefix": prefix},
        "Status": "Enabled",
        "Expiration": {"Days": days},
    }


def read_rules(client: Any, bucket: str) -> list[dict]:
    """Current lifecycle rules, or [] only if the bucket genuinely has none."""
    try:
        return list(client.get_bucket_lifecycle_configuration(Bucket=bucket).get("Rules", []))
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "NoSuchLifecycleConfiguration":
            return []
        # Never let a read failure look like "no rules": the merge would then compute a
        # rule set of exactly ours and delete everything else on the bucket.
        raise


def merge_rules(current: list[dict], rule: dict) -> list[dict]:
    """Every rule that is not ours, plus ours. Replacing by ID keeps this idempotent."""
    return [*remove_rule(current, rule["ID"]), rule]


def remove_rule(current: list[dict], rule_id: str) -> list[dict]:
    """Every rule but ``rule_id``. Removing an absent rule is a no-op, not an error."""
    return [rule for rule in current if rule.get("ID") != rule_id]


def _describe(rule: dict) -> str:
    """One line per rule for the dry run. The filter IS the blast radius, so every part
    of it has to be visible here — a rule whose prefix printed as '<none>' would defeat
    the point of the dry run."""
    rule_filter = rule.get("Filter", {})
    scope = rule_filter.get("And", rule_filter) or {"Prefix": rule.get("Prefix")}
    # An empty prefix matches EVERY object. It must not print like an absent one.
    prefix = scope.get("Prefix", None)
    prefix = '"" (whole bucket)' if prefix == "" else (prefix or "<none>")
    line = f"  {rule.get('ID')}  status={rule.get('Status')}  prefix={prefix}"
    if "ObjectSizeGreaterThan" in scope:
        line += f"  size>{scope['ObjectSizeGreaterThan']}B"
    if "Expiration" in rule:
        line += f"  expiry={rule['Expiration'].get('Days', '-')}d"
    for transition in rule.get("Transitions", []):
        line += f"  -> {transition.get('StorageClass')} after {transition.get('Days')}d"
    return line


def _report(
    bucket: str,
    current: list[dict],
    proposed: list[dict],
    rule_id: str,
    endpoint: str | None,
) -> None:
    """Show what is there now and what would replace it — the whole point of the dry run."""
    logger.info("=== target: s3://%s via %s ===", bucket, endpoint or "<default AWS endpoint>")
    logger.info("=== current rules ===")
    for rule in current or [None]:
        logger.info("%s", _describe(rule) if rule else "  (none)")
    logger.info("=== proposed ===")
    for rule in proposed or [None]:
        logger.info("%s", _describe(rule) if rule else "  (none)")
    logger.info("=== keeping %d pre-existing rule(s) ===", len(remove_rule(proposed, rule_id)))


def _verify_stored(client: Any, bucket: str, proposed: list[dict]) -> list[dict]:
    """Read back what the put actually stored, and insist it is exactly what we sent.

    A put that reports success but stores something else — a filter silently stripped of
    its size predicate, a transition retargeted, another rule dropped — is exactly what
    this exists to catch. Rules are compared by ID so a server that reorders them does
    not read as a mismatch.
    """
    want = {rule.get("ID"): rule for rule in proposed}
    # Lifecycle configuration propagates: a GET straight after the PUT can still answer with
    # the old config. Re-read a few times before calling it a failure, so a rollback that
    # actually worked does not report a false alarm at the worst possible moment.
    for attempt in range(_VERIFY_ATTEMPTS):
        stored = read_rules(client, bucket)
        got = {rule.get("ID"): rule for rule in stored}
        if got == want and len(stored) == len(proposed):
            return stored
        if attempt + 1 < _VERIFY_ATTEMPTS:
            time.sleep(_VERIFY_DELAY_S)

    dropped = sorted(k for k in want.keys() - got.keys() if k is not None)
    added = sorted(k for k in got.keys() - want.keys() if k is not None)
    changed = sorted(
        rid for rid in want.keys() & got.keys() if rid is not None and want[rid] != got[rid]
    )
    detail = ", ".join(
        part
        for part in (
            f"rules dropped: {dropped}" if dropped else "",
            f"unexpected rules: {added}" if added else "",
            f"rules stored differently: {changed}" if changed else "",
            f"{len(stored)} rules stored, want {len(proposed)}"
            if len(stored) != len(proposed)
            else "",
        )
        if part
    )
    raise RuntimeError(
        f"verification failed after {_VERIFY_ATTEMPTS} read(s): {detail}\n"
        f"sent:   {json.dumps(proposed, indent=2, sort_keys=True, default=str)}\n"
        f"stored: {json.dumps(stored, indent=2, sort_keys=True, default=str)}"
    )


def _commit(
    client: Any,
    bucket: str,
    current: list[dict],
    proposed: list[dict],
    rule_id: str,
    *,
    apply: bool,
    endpoint: str | None = None,
) -> list[dict]:
    """Report, then put and verify — or report only. The one place that writes."""
    _report(bucket, current, proposed, rule_id, endpoint)
    if not apply:
        logger.info("\nDRY RUN — nothing written. Re-run with --apply to commit.")
        return current

    if proposed:
        client.put_bucket_lifecycle_configuration(
            Bucket=bucket, LifecycleConfiguration={"Rules": proposed}
        )
    else:
        # S3 has no "configuration with zero rules": a put of an empty Rules list is
        # MalformedXML, and botocore does not catch it. Removing the last rule means
        # deleting the configuration. Without this the documented rollback fails exactly
        # when ours is the only rule — leaving the transition rule installed and moving.
        logger.info("no rules left — deleting the bucket's lifecycle configuration")
        client.delete_bucket_lifecycle(Bucket=bucket)
    return _verify_stored(client, bucket, proposed)


def provision(
    client: Any, bucket: str, rule: dict, *, apply: bool, endpoint: str | None = None
) -> list[dict]:
    """Ensure exactly one rule with ``rule``'s ID, exactly as given. Returns the rules."""
    current = read_rules(client, bucket)
    stored = _commit(
        client,
        bucket,
        current,
        merge_rules(current, rule),
        rule["ID"],
        apply=apply,
        endpoint=endpoint,
    )
    if apply:
        logger.info("VERIFIED: '%s' stored as sent; %d rule(s).", rule["ID"], len(stored))
    return stored


def deprovision(
    client: Any, bucket: str, rule_id: str, *, apply: bool, endpoint: str | None = None
) -> list[dict]:
    """Remove ``rule_id``, keeping every other rule. The rollback path — idempotent."""
    current = read_rules(client, bucket)
    stored = _commit(
        client,
        bucket,
        current,
        remove_rule(current, rule_id),
        rule_id,
        apply=apply,
        endpoint=endpoint,
    )
    if apply:
        logger.info("VERIFIED: '%s' absent; %d rule(s) remain.", rule_id, len(stored))
    return stored


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0] if __doc__ else None)
    parser.add_argument("--bucket", default=os.getenv("SOURCE_CACHE_BUCKET", DEFAULT_BUCKET))
    parser.add_argument("--prefix", default=DEFAULT_PREFIX)
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS)
    parser.add_argument("--apply", action="store_true", help="Actually write; omit for a dry run.")
    args = parser.parse_args(argv)

    endpoint = os.getenv("AWS_ENDPOINT_URL")
    client = boto3.client("s3", endpoint_url=endpoint)
    try:
        provision(
            client,
            args.bucket,
            expiration_rule(args.prefix, args.days),
            apply=args.apply,
            endpoint=endpoint,
        )
    except (ClientError, RuntimeError, ValueError) as exc:
        logger.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
