#!/usr/bin/env python3
"""
Harbor Registry Cleanup Script

This script cleans up old container images from a Harbor registry based on tag patterns
and retention policies.

Tag Patterns:
- Version tags (v1.0.0, latest, main): Never deleted
- SHA tags (sha-abc123): Deleted after SHA_RETENTION_DAYS
- PR tags (pr-123): Deleted after PR_RETENTION_DAYS
- Untagged images: Deleted after SHA_RETENTION_DAYS
"""

import os
import re
import sys
from datetime import UTC, datetime
from typing import Any

import requests
from dateutil import parser as date_parser  # type: ignore[import-untyped]

# Tag patterns
SHA_PATTERN = re.compile(r"^sha-[a-f0-9]+$")
PR_PATTERN = re.compile(r"^pr-\d+$")
VERSION_PATTERN = re.compile(r"^v?\d+\.\d+(\.\d+)? (-.*)?$|^latest$|^main$")


def get_api_url(harbor_url: str, path: str) -> str:
    """Construct full API URL."""
    # Handle both with and without https://
    base = harbor_url.rstrip("/")
    if not base.startswith("http"):
        base = f"https://{base}"
    return f"{base}/api/v2.0{path}"


def get_artifacts(
    harbor_url: str, username: str, password: str, project_name: str, repository_name: str
) -> list[Any]:
    """Fetch all artifacts from the repository."""
    url = get_api_url(
        harbor_url, f"/projects/{project_name}/repositories/{repository_name}/artifacts"
    )
    params: dict[str, Any] = {"page_size": 100, "with_tag": "true"}

    all_artifacts = []
    page = 1

    while True:
        params["page"] = page
        response = requests.get(url, params=params, auth=(username, password), timeout=30)
        response.raise_for_status()

        artifacts = response.json()
        if not artifacts:
            break

        all_artifacts.extend(artifacts)
        page += 1

    return all_artifacts


def delete_artifact(
    harbor_url: str,
    username: str,
    password: str,
    project_name: str,
    repository_name: str,
    digest: str,
) -> None:
    """Delete an artifact by its digest."""
    url = get_api_url(
        harbor_url, f"/projects/{project_name}/repositories/{repository_name}/artifacts/{digest}"
    )
    response = requests.delete(url, auth=(username, password), timeout=30)
    response.raise_for_status()


def delete_tag(
    harbor_url: str,
    username: str,
    password: str,
    project_name: str,
    repository_name: str,
    reference: str,
    tag_name: str,
) -> None:
    """Delete a specific tag from an artifact."""
    url = get_api_url(
        harbor_url,
        f"/projects/{project_name}/repositories/{repository_name}/artifacts/{reference}/tags/{tag_name}",
    )
    response = requests.delete(url, auth=(username, password), timeout=30)
    response.raise_for_status()


def parse_push_time(push_time: str | datetime) -> datetime:
    """Parse and normalize push time to timezone-aware datetime."""
    pushed_at: datetime = date_parser.parse(push_time) if isinstance(push_time, str) else push_time
    if pushed_at.tzinfo is None:
        pushed_at = pushed_at.replace(tzinfo=UTC)
    return pushed_at


def should_delete_tag(
    tag_name: str, push_time: str | datetime, sha_retention_days: int, pr_retention_days: int
) -> tuple[bool, str]:
    """Determine if a tag should be deleted based on retention policy."""
    age_days = (datetime.now(UTC) - parse_push_time(push_time)).days

    # Version tags (semver, latest, main) - NEVER delete
    if VERSION_PATTERN.match(tag_name):
        return False, "version tag (protected)"

    # SHA tags - delete after SHA_RETENTION_DAYS
    if SHA_PATTERN.match(tag_name):
        if age_days > sha_retention_days:
            return True, f"SHA tag older than {sha_retention_days} days ({age_days} days old)"
        return False, f"SHA tag within retention ({age_days} days old)"

    # PR tags - delete after PR_RETENTION_DAYS
    if PR_PATTERN.match(tag_name):
        if age_days > pr_retention_days:
            return True, f"PR tag older than {pr_retention_days} days ({age_days} days old)"
        return False, f"PR tag within retention ({age_days} days old)"

    # Unknown tag patterns - keep them (safe default)
    return False, "unknown pattern (keeping as precaution)"


def should_delete_untagged_artifact(
    push_time: str | datetime, sha_retention_days: int
) -> tuple[bool, str]:
    """Determine if an untagged artifact should be deleted based on retention policy."""
    age_days = (datetime.now(UTC) - parse_push_time(push_time)).days
    if age_days > sha_retention_days:
        return True, f"untagged artifact older than {sha_retention_days} days ({age_days} days old)"
    return False, f"untagged artifact within retention ({age_days} days old)"


def main() -> int:
    """Main cleanup logic."""
    # Configuration from environment variables
    harbor_url = os.environ["HARBOR_URL"]
    username = os.environ["HARBOR_USERNAME"]
    password = os.environ["HARBOR_PASSWORD"]
    project_name = os.environ["PROJECT_NAME"]
    repository_name = os.environ["REPOSITORY_NAME"]
    sha_retention_days = int(os.environ["SHA_RETENTION_DAYS"])
    pr_retention_days = int(os.environ["PR_RETENTION_DAYS"])
    dry_run = os.environ.get("DRY_RUN", "true").lower() == "true"

    print("=" * 60)
    print("Harbor Registry Cleanup")
    print("=" * 60)
    print(f"Harbor URL: {harbor_url}")
    print(f"Project: {project_name}")
    print(f"Repository: {repository_name}")
    print(f"SHA retention: {sha_retention_days} days")
    print(f"PR retention: {pr_retention_days} days")
    print(f"Untagged retention: {sha_retention_days} days (same as SHA)")
    print(f"Dry run: {dry_run}")
    print("=" * 60)

    try:
        artifacts = get_artifacts(harbor_url, username, password, project_name, repository_name)
        print(f"\nFound {len(artifacts)} artifact(s)\n")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching artifacts: {e}")
        return 1

    tags_to_delete: list[tuple[str, str]] = []
    tags_to_keep: list[str] = []
    artifacts_to_delete: list[tuple[str, list[str], str]] = []  # (digest, tag_names, reason)

    for artifact in artifacts:
        digest = artifact.get("digest", "unknown")
        push_time = artifact.get("push_time")
        tags = artifact.get("tags") or []

        print(f"\nArtifact: {digest[:20]}...")
        print(f"  Push time: {push_time}")
        print(f"  Tags: {[t.get('name') for t in tags] if tags else '(untagged)'}")

        # Handle untagged artifacts
        if not tags:
            delete, reason = should_delete_untagged_artifact(push_time, sha_retention_days)
            if delete:
                print(f"    ❌ UNTAGGED: DELETE - {reason}")
                artifacts_to_delete.append((digest, [], "untagged"))
            else:
                print(f"    ✅ UNTAGGED: KEEP - {reason}")
            continue

        artifact_tags_to_delete = []
        artifact_tags_to_keep = []

        for tag in tags:
            tag_name = tag.get("name")
            tag_push_time = tag.get("push_time") or push_time

            delete, reason = should_delete_tag(
                tag_name, tag_push_time, sha_retention_days, pr_retention_days
            )

            if delete:
                print(f"    ❌ {tag_name}: DELETE - {reason}")
                artifact_tags_to_delete.append(tag_name)
                tags_to_delete.append((digest, tag_name))
            else:
                print(f"    ✅ {tag_name}: KEEP - {reason}")
                artifact_tags_to_keep.append(tag_name)
                tags_to_keep.append(tag_name)

        # If all tags are to be deleted, mark the entire artifact for deletion
        if artifact_tags_to_delete and not artifact_tags_to_keep:
            print("    🗑️  Entire artifact marked for deletion (all tags expired)")
            artifacts_to_delete.append((digest, artifact_tags_to_delete, "all_tags_expired"))

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Tags to delete: {len(tags_to_delete)}")
    print(f"Tags to keep: {len(tags_to_keep)}")
    print(f"Artifacts to delete (entire image): {len(artifacts_to_delete)}")

    # Count untagged vs tagged artifacts to delete
    untagged_count = sum(1 for _, _, reason in artifacts_to_delete if reason == "untagged")
    tagged_count = len(artifacts_to_delete) - untagged_count
    print(f"  - Untagged artifacts: {untagged_count}")
    print(f"  - Artifacts with all tags expired: {tagged_count}")

    if not tags_to_delete and not artifacts_to_delete:
        print("\nNo tags or artifacts to delete. Exiting.")
        return 0

    if dry_run:
        print("\n🔍 DRY RUN MODE - No changes made")
        if artifacts_to_delete:
            print("\nArtifacts (entire images) that would be deleted:")
            for digest, tag_names, reason in artifacts_to_delete:
                if reason == "untagged":
                    print(f"  - {digest[:20]}... (untagged)")
                else:
                    print(f"  - {digest[:20]}... (tags: {', '.join(tag_names)})")

        # Show tags that would be deleted individually (where artifact has other tags to keep)
        artifact_digests_to_delete = {digest for digest, _, _ in artifacts_to_delete}
        individual_tags = [(d, t) for d, t in tags_to_delete if d not in artifact_digests_to_delete]
        if individual_tags:
            print("\nTags that would be deleted (artifact kept due to other tags):")
            for digest, tag_name in individual_tags:
                print(f"  - {tag_name} (artifact: {digest[:20]}...)")
        return 0

    # Perform deletions
    print("\n🗑️  PERFORMING DELETIONS...")
    deleted_artifacts_count = 0
    deleted_tags_count = 0
    error_count = 0

    # First, delete entire artifacts where all tags are expired or untagged
    artifact_digests_to_delete = {digest for digest, _, _ in artifacts_to_delete}

    for digest, tag_names, reason in artifacts_to_delete:
        try:
            label = (
                "untagged artifact"
                if reason == "untagged"
                else f"artifact (tags: {', '.join(tag_names)})"
            )
            print(f"  Deleting {label}: {digest[:20]}...", end=" ")
            delete_artifact(harbor_url, username, password, project_name, repository_name, digest)
            print("✓")
            deleted_artifacts_count += 1
        except requests.exceptions.RequestException as e:
            print(f"✗ Error: {e}")
            error_count += 1

    # Then, delete individual tags where the artifact has other tags to keep
    for digest, tag_name in tags_to_delete:
        if digest not in artifact_digests_to_delete:
            try:
                print(f"  Deleting tag: {tag_name}...", end=" ")
                delete_tag(
                    harbor_url, username, password, project_name, repository_name, digest, tag_name
                )
                print("✓")
                deleted_tags_count += 1
            except requests.exceptions.RequestException as e:
                print(f"✗ Error: {e}")
                error_count += 1

    print("\n" + "=" * 60)
    print(f"Deleted: {deleted_artifacts_count} artifacts (entire images)")
    print(f"Deleted: {deleted_tags_count} individual tags")
    print(f"Errors: {error_count}")
    print("=" * 60)

    return 0 if error_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
