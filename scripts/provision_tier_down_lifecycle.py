#!/usr/bin/env python3
"""Install the High Performance -> STANDARD lifecycle transition rule on an OVH bucket.

What it does
------------
Adds one rule, ``tier-down-all-to-standard``, that transitions every object under
``--prefix`` from ``EXPRESS_ONEZONE`` (OVH "High Performance") to ``STANDARD`` after
``--transition-days``. S3 applies it retroactively to objects already there, so this is
how a bucket's existing High Performance data is drained without an application walk.

    # always dry-run first: it prints the current rules and the proposed union
    python scripts/provision_tier_down_lifecycle.py --bucket BUCKET --prefix tests-output/
    python scripts/provision_tier_down_lifecycle.py --bucket BUCKET --prefix tests-output/ --apply
    # rollback: drop our rule, keep every other one
    python scripts/provision_tier_down_lifecycle.py --bucket BUCKET --remove --apply

Read this before running it
---------------------------
* **Objects of 128 KB or less are never transitioned** by S3 unless the rule carries an
  ``ObjectSizeGreaterThan`` predicate. Without ``--min-object-size`` every ``zarr.json``
  would stay in High Performance and the convergence check would never reach zero. Note
  the predicate is *strictly* greater-than, so the default of 1 also leaves 0- and 1-byte
  objects (directory markers) behind; ``--min-object-size 0`` includes them, but whether
  OVH honours an explicit 0 is unverified — probe it on a scratch bucket first.
* **``PutBucketLifecycleConfiguration`` replaces the bucket's entire configuration.** It
  is not a merge. This script therefore reads the current rules, keeps every rule that is
  not ours, puts the union, and reads it back to confirm nothing was dropped or altered.
  The bucket's other rules are load-bearing: dropping ``expire-source-cache`` leaks about
  1.3 GB per failed convert.
* **A High Performance -> STANDARD transition cannot be reversed by lifecycle.** Removing
  the rule stops further moves; it does not move anything back. Per-item recovery is
  ``scripts/change_storage_tier.py --stac-item-url ... --storage-class EXPRESS_ONEZONE``.
* **Lifecycle calls need the bucket-owner credential.** The laptop "OVH S3" key is
  object-level and gets ``AccessDenied`` on every lifecycle call, read included. The owner
  credential is the ``geozarr-s3-credentials`` secret in the ``devseed`` namespace, so
  this runs from a one-off in-cluster Pod.
* **This script imports its shared helpers from ``provision_source_cache_lifecycle.py``.**
  Mounting this one file alone into an older image will fail at import — the Pod needs an
  image built from the merge commit, or both files mounted together.
* ``AWS_ENDPOINT_URL`` must be set. Unset, boto3 would silently address the bucket name at
  AWS instead of OVH, so the script refuses rather than guess the target.
"""

import argparse
import logging
import os
import sys

import boto3
from botocore.exceptions import ClientError
from provision_source_cache_lifecycle import deprovision, provision

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

RULE_ID = "tier-down-all-to-standard"
TARGET_STORAGE_CLASS = "STANDARD"
DEFAULT_TRANSITION_DAYS = 1
DEFAULT_MIN_OBJECT_SIZE = 1


def transition_rule(prefix: str, transition_days: int, min_object_size: int) -> dict:
    """The transition rule, in the one shape S3 accepts.

    A prefix *plus* a size predicate must be wrapped in ``Filter.And``. A bare
    ``{"Prefix": ..., "ObjectSizeGreaterThan": ...}`` is not valid S3, and a server may
    accept it while honouring only one half — which is why the read-back compares the
    whole stored rule rather than trusting the put.
    """
    if not prefix.strip():
        raise ValueError("prefix must not be empty — that would re-tier the whole bucket")
    if transition_days < 1:
        raise ValueError(f"transition-days must be at least 1, got {transition_days}")
    if min_object_size < 0:
        raise ValueError(f"min-object-size must not be negative, got {min_object_size}")
    return {
        "ID": RULE_ID,
        "Status": "Enabled",
        "Filter": {"And": {"Prefix": prefix, "ObjectSizeGreaterThan": min_object_size}},
        "Transitions": [{"Days": transition_days, "StorageClass": TARGET_STORAGE_CLASS}],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        epilog=__doc__,  # the whole docstring, landmines included
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    # No defaults for the target: this rule rewrites storage classes, so the bucket and
    # the prefix are always typed out in full by whoever runs it.
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", help="Required unless --remove.")
    parser.add_argument("--transition-days", type=int, default=DEFAULT_TRANSITION_DAYS, dest="days")
    parser.add_argument(
        "--min-object-size", type=int, default=DEFAULT_MIN_OBJECT_SIZE, dest="min_size"
    )
    parser.add_argument("--remove", action="store_true", help=f"Delete the '{RULE_ID}' rule.")
    parser.add_argument("--apply", action="store_true", help="Actually write; omit for a dry run.")
    args = parser.parse_args(argv)
    if args.remove:
        # Removal is by rule ID and is bucket-wide. An operator who typed a prefix or a
        # rule shape believes it is scoped by them; say so rather than silently ignoring.
        shaping = [
            flag
            for flag, given in (
                ("--prefix", args.prefix is not None),
                ("--transition-days", args.days != DEFAULT_TRANSITION_DAYS),
                ("--min-object-size", args.min_size != DEFAULT_MIN_OBJECT_SIZE),
            )
            if given
        ]
        if shaping:
            parser.error(
                f"--remove deletes the '{RULE_ID}' rule from the whole bucket by ID; "
                f"it is not scoped by {', '.join(shaping)}. Drop them and re-run."
            )
    elif not args.prefix:
        parser.error("--prefix is required unless --remove is given")

    endpoint = os.getenv("AWS_ENDPOINT_URL")
    if not endpoint:
        logger.error(
            "AWS_ENDPOINT_URL is not set — refusing to guess the target. "
            "Set it to the OVH endpoint (https://s3.de.io.cloud.ovh.net) and re-run."
        )
        return 1

    client = boto3.client("s3", endpoint_url=endpoint)
    try:
        if args.remove:
            deprovision(client, args.bucket, RULE_ID, apply=args.apply, endpoint=endpoint)
        else:
            rule = transition_rule(args.prefix, args.days, args.min_size)
            provision(client, args.bucket, rule, apply=args.apply, endpoint=endpoint)
    except (ClientError, RuntimeError, ValueError) as exc:
        logger.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
