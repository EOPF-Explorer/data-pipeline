"""The sentinel-2-l2a-new reconcile workflow, its script run under bash with a stub `python3`.

What it passes to the tool is the contract: the fixed collection and hosts, the bound,
--dry-run unless dry_run is exactly "false", both lists on a real run, and a real run only on
an image tag that names one image.
"""

import os
import subprocess  # nosec B404 -- runs bash on this repo's own manifest, fixed argv
from pathlib import Path

import pytest
import yaml

MANIFEST = Path(__file__).parents[2] / "deploy" / "reconcile-s2-new-versions-workflow.yaml"
OUT = "/tmp/reconcile-out"  # noqa: S108  # nosec B108 -- the pod's path, never opened here
CREATED, REPLACED = f"{OUT}/created-ids.jsonl", f"{OUT}/replaced-ids.jsonl"
TOOL = [
    "STUB", "PYTHON", "/app/scripts/reconcile_versions.py",
    "--collection", "sentinel-2-l2a-new",
    "--stac-api-url", "https://api.explorer.eopf.copernicus.eu/stac",
    "--raster-api-url", "https://api.explorer.eopf.copernicus.eu/raster",
]  # fmt: skip


def _spec() -> dict:
    spec: dict = yaml.safe_load(MANIFEST.read_text())["spec"]
    return spec


def _template() -> dict:
    (template,) = _spec()["templates"]
    return template


def _container() -> dict:
    container: dict = _template()["container"]
    return container


def _run(tmp_path: Path, **env: str) -> subprocess.CompletedProcess[str]:
    """The container's script, with the manifest defaults as env and a stub `python3`.

    The stub prints one argument per line, so a test can see where each one ends.
    """
    stub_dir = tmp_path / "bin"
    stub_dir.mkdir()
    stub = stub_dir / "python3"
    stub.write_text("#!/bin/sh\necho STUB PYTHON\nprintf '%s\\n' \"$@\"\n")
    stub.chmod(0o755)
    params = {p["name"]: p["value"] for p in _spec()["arguments"]["parameters"]}
    full_env = {
        "PATH": f"{stub_dir}:{os.environ['PATH']}",
        "PIPELINE_IMAGE_VERSION": "sha-abc1234",
        "MAX_WRITES": params["max_writes"],
        "DRY_RUN": params["dry_run"],
        "TIME_BUDGET_SECONDS": params["time_budget_seconds"],
        "RUN_ID": "reconcile-s2-new-versions-x7k2p",
        **env,
    }
    return subprocess.run(  # noqa: S603  # nosec B603 B607 -- this repo's own manifest
        ["bash", "-c", _container()["args"][0]],  # noqa: S607
        env=full_env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_defaults_are_a_dry_run_of_ten_writes_in_45_minutes(tmp_path: Path) -> None:
    result = _run(tmp_path)
    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == [
        *TOOL, "--max-writes", "10", "--time-budget", "2700", "--dry-run"
    ]  # fmt: skip


@pytest.mark.parametrize("dry_run", ["true", "False", "no", ""])
def test_only_false_is_a_real_run(tmp_path: Path, dry_run: str) -> None:
    result = _run(tmp_path, DRY_RUN=dry_run, PIPELINE_IMAGE_VERSION="pr-9")
    assert result.returncode == 0, result.stderr
    assert result.stdout.split()[-1] == "--dry-run"
    assert "--created-ids" not in result.stdout


@pytest.mark.parametrize(
    "tag", ["sha-33f03f8", "v1.23.0@sha256:" + "0" * 64, "pr-520@sha256:" + "0" * 64]
)
def test_a_real_run_records_both_lists_on_a_tag_that_names_one_image(
    tmp_path: Path, tag: str
) -> None:
    result = _run(tmp_path, DRY_RUN="false", PIPELINE_IMAGE_VERSION=tag, MAX_WRITES="500")
    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == [
        *TOOL, "--max-writes", "500", "--time-budget", "2700",
        "--created-ids", CREATED, "--replaced-ids", REPLACED,
        "--run-id", "reconcile-s2-new-versions-x7k2p",
    ]  # fmt: skip


def test_the_lists_are_the_workflows_optional_artifacts() -> None:
    artifacts = {a["name"]: a for a in _template()["outputs"]["artifacts"]}
    assert {name: a["path"] for name, a in artifacts.items()} == {
        "created-ids": CREATED,
        "replaced-ids": REPLACED,
    }
    assert all(a["optional"] for a in artifacts.values())


@pytest.mark.parametrize("tag", ["v1.23.0", "pr-520", "latest", "main", "feat-reconcile"])
def test_a_real_run_on_a_moving_tag_is_refused(tmp_path: Path, tag: str) -> None:
    result = _run(tmp_path, DRY_RUN="false", PIPELINE_IMAGE_VERSION=tag)
    assert result.returncode == 1
    assert "REFUSING" in result.stderr
    assert "STUB PYTHON" not in result.stdout, "the tool started despite the refusal"


def test_an_empty_image_tag_is_refused(tmp_path: Path) -> None:
    result = _run(tmp_path, PIPELINE_IMAGE_VERSION="")
    assert result.returncode == 2
    assert "STUB PYTHON" not in result.stdout


def test_a_parameter_cannot_inject_arguments(tmp_path: Path) -> None:
    """Parameters arrive as env and are quoted: a value cannot add a flag to the tool."""
    result = _run(tmp_path, MAX_WRITES="10 --collection sentinel-2-l2a", DRY_RUN="false")
    assert result.returncode == 0, result.stderr
    args = result.stdout.splitlines()
    assert args[args.index("--max-writes") + 1] == "10 --collection sentinel-2-l2a"
    assert args.count("--collection") == 1


def test_the_target_and_policy_are_not_parameters() -> None:
    """The key, floor and range are constants in the tool, which refuses any other collection."""
    params = {p["name"] for p in _spec()["arguments"]["parameters"]}
    assert params == {"pipeline_image_version", "max_writes", "dry_run", "time_budget_seconds"}
    script = _container()["args"][0]
    for word in ("EOPF_GEOZARR", "0.13.0", "--key", "--floor"):
        assert word not in script


def test_it_holds_the_registers_semaphore() -> None:
    """No register run creates an id while this one may be creating it (created-ids)."""
    assert _spec()["synchronization"]["semaphore"]["configMapKeyRef"] == {
        "name": "eopf-workflow-concurrency",
        "key": "eodc-register-limit",
    }


def test_the_image_is_pulled_every_time() -> None:
    """A node's cached copy of the tag must not stand in for the image the dry run used."""
    assert _container()["imagePullPolicy"] == "Always"


def test_no_deadline_kills_the_run() -> None:
    """The bounds are --max-writes and --time-budget, inside the tool; never a kill."""
    assert "activeDeadlineSeconds" not in _spec()
    assert "activeDeadlineSeconds" not in _template()


def test_catalogue_only_with_the_stac_write_credentials() -> None:
    env = {e["name"] for e in _container()["env"]}
    assert {"OIDC_TOKEN_URL", "OIDC_CLIENT_ID", "OIDC_CLIENT_SECRET"} <= env
    assert not [name for name in env if name.startswith("AWS_")]
