"""The migration workflow's image-tag guard, exercised as bash — not read as YAML.

The manifest's `pipeline_image_version` carries a ⚠ comment saying a moving tag (`pr-<N>`, `latest`)
must never drive a real run. A comment cannot refuse a submit. These tests run the template's own
script body under bash, with a stub `python` on PATH, and assert the guard actually fires: the
control is verified, not assumed. The cube bucket has S3 versioning OFF, so a real run against an
image other than the one the dry run validated is not recoverable.
"""

from __future__ import annotations

import os
import subprocess  # nosec B404 -- runs bash on this repo's own manifest, fixed argv
from pathlib import Path

import pytest
import yaml

MANIFEST = Path(__file__).parents[2] / "deploy" / "migrate-s1-rtc-datamodel-workflow.yaml"


def _script_source() -> str:
    spec = yaml.safe_load(MANIFEST.read_text())["spec"]
    (template,) = [t for t in spec["templates"] if t["name"] == "migrate"]
    return template["script"]["source"]


def _run(tmp_path: Path, **env_overrides: str) -> subprocess.CompletedProcess[str]:
    """Run the template's script with a stub `python`, so only the guard decides the exit code."""
    stub_dir = tmp_path / "bin"
    stub_dir.mkdir()
    stub = stub_dir / "python"
    stub.write_text('#!/bin/sh\necho "STUB PYTHON $*"\n')
    stub.chmod(0o755)
    env = {
        "PATH": f"{stub_dir}:{os.environ['PATH']}",
        "STAC_API_URL": "https://stac",
        "CUBE_COLLECTION": "sentinel-1-grd-rtc-staging",
        "BUCKET": "bucket",
        "S3_ENDPOINT": "https://s3",
        "DRY_RUN": "true",
        "ROLLBACK": "false",
        "BACKUP_PREFIX": "",
        "SKIP_TILES": "",
        "ITEM": "",
        "ALLOW_NO_BACKUP": "false",
        "REWRITE_ROOT_GEO": "false",
        "PIPELINE_IMAGE_VERSION": "sha-abc1234",
    }
    env.update(env_overrides)
    return subprocess.run(  # noqa: S603, S607  # nosec B603 B607 -- this repo's own manifest
        ["bash", "-c", _script_source()],  # noqa: S607
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.mark.parametrize("tag", ["pr-413", "pr-9", "latest"])
def test_moving_tag_with_dry_run_false_is_refused(tmp_path: Path, tag: str) -> None:
    """The bound is the tool's, not a comment's: a real run on a moving tag exits before the driver."""
    result = _run(tmp_path, PIPELINE_IMAGE_VERSION=tag, DRY_RUN="false")
    assert result.returncode == 1, result.stdout
    assert "REFUSING" in result.stderr
    assert "STUB PYTHON" not in result.stdout, "the driver started despite the refusal"


@pytest.mark.parametrize("tag", ["pr-413", "latest"])
def test_moving_tag_is_allowed_for_a_dry_run(tmp_path: Path, tag: str) -> None:
    """Dry runs write nothing, so a moving tag is fine there — and is called out in the log."""
    result = _run(tmp_path, PIPELINE_IMAGE_VERSION=tag, DRY_RUN="true")
    assert result.returncode == 0, result.stderr
    assert "STUB PYTHON" in result.stdout
    assert "--dry-run" in result.stdout


def test_immutable_tag_runs_for_real(tmp_path: Path) -> None:
    """A `sha-<main-sha>` tag is what a real run is supposed to use; the guard stays out of its way."""
    result = _run(tmp_path, PIPELINE_IMAGE_VERSION="sha-7d662ed", DRY_RUN="false")
    assert result.returncode == 0, result.stderr
    assert "STUB PYTHON" in result.stdout
    assert "--dry-run" not in result.stdout


def test_manifest_defaults_stay_safe() -> None:
    """dry_run defaults true and the destructive opt-ins default off — checked, not assumed."""
    spec = yaml.safe_load(MANIFEST.read_text())["spec"]
    params = {p["name"]: p.get("value", "") for p in spec["arguments"]["parameters"]}
    assert params["dry_run"] == "true"
    assert params["allow_no_backup"] == "false"
    assert params["rewrite_root_geo"] == "false"
    assert params["rollback"] == "false"


def test_image_version_is_exported_to_the_script() -> None:
    """The guard reads $PIPELINE_IMAGE_VERSION — it must actually be in the container env."""
    spec = yaml.safe_load(MANIFEST.read_text())["spec"]
    (template,) = [t for t in spec["templates"] if t["name"] == "migrate"]
    env_names = {e["name"] for e in template["script"]["env"]}
    assert "PIPELINE_IMAGE_VERSION" in env_names
