#!/usr/bin/env python3
"""Drive one scene through the convert DAG with pre-staging ON and prove it worked.

Why this exists
---------------
The prestage path (data-pipeline#339) has a failure mode that looks exactly like success:
``prestage-source`` falls back to passthrough and exits 0 whenever it decides it cannot
copy — flag off, host not in ``--copyable-hosts``, and so on. Every node then goes green
while convert reads ``https://`` exactly as before, testing nothing. **A succeeded
workflow is not evidence that anything was staged.**

So this asserts the things that separate a real stage from a green no-op: ``staged=true``,
a ``convert_source_url`` that is genuinely ``s3://.../source-cache/<item>``, and an empty
staged prefix afterwards (cleanup ran) — then that the item registered and renders.

This is the T7 gate of ``claude-docs/plans/prestage_source_s3_plan.md``, made repeatable.

Usage
-----
    uv run python scripts/verify_prestage_e2e.py <stac-item-url> --image <tag>

``--image`` is required on purpose: a stale default would pass the gate against old code,
"proving" code that never ran — exactly the green-but-meaningless result this exists to
catch.

Before v1.13.0 the only image with these scripts is the #344 PR build. **Use ``pr-344``.**

Do *not* derive the tag from ``gh pr view --json potentialMergeCommit``: that names a sha
CI never built an image from, because GitHub recomputes the potential merge commit whenever
``main`` moves, not just when the PR branch is pushed. (Nor is it the branch head — a
push-event build on a feature branch publishes nothing: ``PUSH_IMAGE`` in
``.github/workflows/build.yml`` is true only for main, tags, workflow_call, or a same-repo
pull_request.) The tag that exists is the merge commit *as of the last build*. Both wrong
answers were tried against the registry on 2026-07-16 before ``pr-344`` was found.

``pr-344`` (``type=ref,event=pr``) always tracks the latest PR build. For an immutable tag,
read it from the build log rather than the PR API::

    gh run list --repo EOPF-Explorer/data-pipeline --branch feat--prestage-source-s3 \
      --workflow build.yml --limit 2 --json databaseId,event
    gh run view <pull_request-run-id> --log | grep -m1 DOCKER_METADATA_OUTPUT_TAGS

Once v1.13.0 exists, just pass the release tag.

Needs the ``argo`` CLI and a kube context on the namespace, plus S3 credentials for the
cleanup assertion (checked up-front — see ``main``).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
from typing import Any
from urllib.parse import urlparse

import boto3
import requests
from source_url_utils import derive_item_id

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

# Verdicts for what convert was actually handed.
STAGED = "staged"
PASSTHROUGH = "passthrough"
EMPTY = "empty"
MISMATCH = "mismatch"

CACHE_MARKER = "/source-cache/"


def item_id_for(source_url: str) -> str:
    """The item id, by the *shared* rule — never a local reimplementation.

    prestage's staged key, convert's output path and register's STAC id all agree only
    because they derive this identically. A gate with its own copy could pass while they
    drift.
    """
    return derive_item_id(source_url)


def classify_source_url(convert_source_url: str, item_id: str) -> str:
    """What convert was actually told to read."""
    if not convert_source_url:
        return EMPTY
    if convert_source_url.startswith("s3://"):
        in_cache = CACHE_MARKER in convert_source_url
        return STAGED if in_cache and convert_source_url.rstrip("/").endswith(item_id) else MISMATCH
    return PASSTHROUGH


def parse_s3_url(url: str) -> tuple[str, str]:
    """``s3://bucket/key`` -> ``(bucket, key)``."""
    parsed = urlparse(url)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"not an s3:// url: {url!r}")
    return parsed.netloc, parsed.path.lstrip("/")


def _nodes(wf: dict) -> list[dict]:
    return list(wf.get("status", {}).get("nodes", {}).values())


def node_phase(wf: dict, name: str) -> str:
    """Phase of the task named `name`.

    Exact displayName matching lands on the Retry node (its pod children are renamed
    "name(0)", "name(1)"), which carries the aggregate phase — so a task that failed once
    and then succeeded reads as Succeeded, not Failed.
    """
    for node in _nodes(wf):
        if node.get("displayName") == name:
            return str(node.get("phase", "MISSING"))
    return "MISSING"


def node_output(wf: dict, name: str, param: str) -> str:
    """Output parameter `param` of task `name`, or "" if absent."""
    for node in _nodes(wf):
        if node.get("displayName") != name:
            continue
        for out in node.get("outputs", {}).get("parameters", []) or []:
            if out.get("name") == param:
                return str(out.get("value", ""))
    return ""


class Checks:
    def __init__(self) -> None:
        self.failures = 0

    def ok(self, msg: str) -> None:
        logger.info("  OK    %s", msg)

    def bad(self, msg: str) -> None:
        logger.error("  FAIL  %s", msg)
        self.failures += 1

    def equal(self, label: str, got: Any, want: Any) -> None:
        if got == want:
            self.ok(f"{label} ({got})")
        else:
            self.bad(f"{label}: got {got!r}, want {want!r}")


def submit(namespace: str, template: str, source_url: str, collection: str, image: str) -> str:
    # Argument list, never a shell string: source_url comes from a STAC catalogue, and
    # list form passes it to execve as one argv entry with no shell to reinterpret it.
    cmd = [  # noqa: S607  # nosec B607 -- argo resolved from PATH, as everywhere else here
        "argo",
        "submit",
        "-n",
        namespace,
        "--from",
        f"workflowtemplate/{template}",
        "-p",
        f"source_url={source_url}",
        "-p",
        f"collection={collection}",
        "-p",
        "prestage_source=true",
        "-p",
        f"pipeline_image_version={image}",
        "-o",
        "name",
    ]
    out = subprocess.run(  # noqa: S603  # nosec B603 -- list form, no shell
        cmd, check=True, capture_output=True, text=True
    )
    return out.stdout.strip()


def fetch(namespace: str, workflow: str) -> dict:
    # A failed workflow still has node data worth reporting, and that report is the point.
    wait_cmd = ["argo", "wait", "-n", namespace, workflow]  # noqa: S607  # nosec B607 -- argo from PATH
    subprocess.run(wait_cmd, capture_output=True, text=True)  # noqa: S603  # nosec B603 -- list form, no shell

    get_cmd = ["argo", "get", "-n", namespace, workflow, "-o", "json"]  # noqa: S607  # nosec B607 -- argo from PATH
    out = subprocess.run(  # noqa: S603  # nosec B603 -- list form, no shell
        get_cmd, check=True, capture_output=True, text=True
    )
    return dict(json.loads(out.stdout))


def verify(wf: dict, item_id: str, collection: str, stac_api: str, s3: Any, c: Checks) -> None:
    logger.info("=== node phases ===")
    c.equal("workflow", wf.get("status", {}).get("phase"), "Succeeded")
    for name in ("prestage-source", "convert", "register", "cleanup-source"):
        c.equal(name, node_phase(wf, name), "Succeeded")

    logger.info("=== staging actually happened (not a green passthrough) ===")
    c.equal("prestage-source staged", node_output(wf, "prestage-source", "staged"), "true")

    convert_url = node_output(wf, "prestage-source", "convert_source_url")
    verdict = classify_source_url(convert_url, item_id)
    if verdict == STAGED:
        c.ok(f"convert read a staged s3:// copy ({convert_url})")
    elif verdict == PASSTHROUGH:
        c.bad(f"convert_source_url is still https:// — passthrough, NOT staged: {convert_url}")
    elif verdict == EMPTY:
        c.bad("convert_source_url is empty — prestage-source produced no output")
    else:
        c.bad(f"convert_source_url is not a staged copy of {item_id}: {convert_url}")

    if verdict == STAGED:
        logger.info("=== cleanup deleted the staged copy ===")
        # The URL convert was handed is the authority on where the copy went; re-deriving
        # it here would only test this script against itself.
        bucket, key = parse_s3_url(convert_url)
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=f"{key}/")
        c.equal(f"objects left under {key}/", resp.get("KeyCount", 0), 0)

    logger.info("=== the item is actually usable ===")
    resp = requests.get(f"{stac_api}/collections/{collection}/items/{item_id}", timeout=30)
    if resp.status_code != 200:
        c.bad(f"STAC item {item_id} not found in {collection} (HTTP {resp.status_code})")
        return
    c.ok("STAC item registered")
    thumb = resp.json().get("assets", {}).get("thumbnail", {}).get("href", "")
    if not thumb:
        c.bad("item has no thumbnail asset")
    else:
        c.equal("thumbnail renders", requests.get(thumb, timeout=60).status_code, 200)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Prove one scene really staged and converted.")
    parser.add_argument("source_url", help="STAC item URL")
    parser.add_argument(
        "--image",
        required=True,
        help="Image tag to test. No default: the pre-release tag is #344's merge sha, "
        "which moves on every push, and a stale default would pass against old code.",
    )
    parser.add_argument("--namespace", default=os.getenv("NAMESPACE", "devseed-staging"))
    parser.add_argument("--template", default="eopf-explorer-convert-v1-s2")
    parser.add_argument("--collection", default="sentinel-2-l2a-staging")
    parser.add_argument(
        "--stac-api-url",
        default=os.getenv("STAC_API_URL", "https://api.explorer.eopf.copernicus.eu/stac"),
    )
    args = parser.parse_args(argv)

    item_id = item_id_for(args.source_url)

    # Prove the S3 credentials authenticate BEFORE submitting. The cleanup assertion is the
    # last thing to run, so a credentials problem would otherwise surface only after a full
    # convert (~10 min) and take the registered/renders checks down with it — the run is
    # wasted, not merely failed. (Observed 2026-07-16.)
    #
    # list_buckets, not list_objects_v2: the bucket is only known once the workflow reports
    # convert_source_url, and hard-coding it here would re-derive what the gate deliberately
    # takes from convert. So this proves "these credentials work against this endpoint", not
    # "they can read that specific bucket" — it catches the unset/expired/wrong-endpoint case
    # that actually bites, and leaves the narrower one to the real assertion.
    s3 = boto3.client("s3", endpoint_url=os.getenv("AWS_ENDPOINT_URL"))
    try:
        s3.list_buckets()
    except Exception as exc:  # noqa: BLE001 -- any S3 failure here is fatal and worth showing
        logger.error(
            "S3 preflight failed: %s\n"
            "The cleanup assertion needs S3. Set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / "
            "AWS_ENDPOINT_URL (object-read rights suffice — bucket-owner is NOT required).",
            exc,
        )
        return 1

    logger.info("=== submitting: %s (image %s, prestage ON) ===", item_id, args.image)
    workflow = submit(args.namespace, args.template, args.source_url, args.collection, args.image)
    logger.info("workflow: %s", workflow)
    logger.info("watch: argo get -n %s %s", args.namespace, workflow)

    wf = fetch(args.namespace, workflow)
    checks = Checks()
    verify(wf, item_id, args.collection, args.stac_api_url, s3, checks)

    if checks.failures:
        logger.error(
            "\nFAIL — %d check(s) failed. Inspect: argo get -n %s %s",
            checks.failures,
            args.namespace,
            workflow,
        )
        return 1
    logger.info(
        "\nPASS — %s staged, converted from s3://, registered, cleaned up, renders.", item_id
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
