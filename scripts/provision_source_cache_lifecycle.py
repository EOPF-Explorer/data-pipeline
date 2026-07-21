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
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Any

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

RULE_ID = "expire-source-cache"
DEFAULT_BUCKET = "esa-zarr-sentinel-explorer-fra"
DEFAULT_PREFIX = "source-cache/"
DEFAULT_DAYS = 7


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


def merge_rules(current: list[dict], prefix: str, days: int) -> list[dict]:
    """Every rule that is not ours, plus ours. Replacing by ID keeps this idempotent."""
    kept = [rule for rule in current if rule.get("ID") != RULE_ID]
    ours = {
        "ID": RULE_ID,
        "Filter": {"Prefix": prefix},
        "Status": "Enabled",
        "Expiration": {"Days": days},
    }
    return [*kept, ours]


def _describe(rule: dict) -> str:
    prefix = rule.get("Filter", {}).get("Prefix", rule.get("Prefix", "<none>"))
    days = rule.get("Expiration", {}).get("Days", "-")
    return f"  {rule.get('ID')}  status={rule.get('Status')}  prefix={prefix}  expiry={days}d"


def _report(bucket: str, current: list[dict], proposed: list[dict]) -> None:
    """Show what is there now and what would replace it — the whole point of the dry run."""
    logger.info("=== current rules on s3://%s ===", bucket)
    for rule in current:
        logger.info("%s", _describe(rule))
    if not current:
        logger.info("  (none)")
    logger.info("=== proposed ===")
    for rule in proposed:
        logger.info("%s", _describe(rule))
    logger.info("=== preserving %d pre-existing rule(s) ===", len(proposed) - 1)


def _verify_stored(client: Any, bucket: str, prefix: str, days: int, want_count: int) -> list[dict]:
    """Read back what the put actually stored. A put that reports success but stores
    something else — or drops another rule — is exactly what this catches."""
    stored = read_rules(client, bucket)
    ours = [rule for rule in stored if rule.get("ID") == RULE_ID]
    if len(ours) != 1:
        raise RuntimeError(f"verification failed: {len(ours)} '{RULE_ID}' rules stored, want 1")
    if ours[0].get("Expiration", {}).get("Days") != days:
        raise RuntimeError(f"verification failed: stored expiry {ours[0]} != {days}d")
    if ours[0].get("Filter", {}).get("Prefix") != prefix:
        raise RuntimeError(f"verification failed: stored prefix {ours[0]} != {prefix!r}")
    if len(stored) != want_count:
        raise RuntimeError(
            f"verification failed: {len(stored)} rules stored, want {want_count} — rules dropped"
        )
    return stored


def provision(client: Any, bucket: str, prefix: str, days: int, *, apply: bool) -> list[dict]:
    """Ensure exactly one RULE_ID rule expiring `prefix` after `days`. Returns the rules."""
    if not prefix.strip():
        raise ValueError("prefix must not be empty — that would expire the whole bucket")
    if days <= 0:
        raise ValueError(f"days must be positive, got {days}")

    current = read_rules(client, bucket)
    proposed = merge_rules(current, prefix, days)
    _report(bucket, current, proposed)

    if not apply:
        logger.info("\nDRY RUN — nothing written. Re-run with --apply to commit.")
        return current

    client.put_bucket_lifecycle_configuration(
        Bucket=bucket, LifecycleConfiguration={"Rules": proposed}
    )
    stored = _verify_stored(client, bucket, prefix, days, len(proposed))
    logger.info(
        "VERIFIED: '%s' expires %s after %dd; %d rule(s).", RULE_ID, prefix, days, len(stored)
    )
    return stored


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0] if __doc__ else None)
    parser.add_argument("--bucket", default=os.getenv("SOURCE_CACHE_BUCKET", DEFAULT_BUCKET))
    parser.add_argument("--prefix", default=DEFAULT_PREFIX)
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS)
    parser.add_argument("--apply", action="store_true", help="Actually write; omit for a dry run.")
    args = parser.parse_args(argv)

    client = boto3.client("s3", endpoint_url=os.getenv("AWS_ENDPOINT_URL"))
    try:
        provision(client, args.bucket, args.prefix, args.days, apply=args.apply)
    except (ClientError, RuntimeError, ValueError) as exc:
        logger.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
