"""The sentinel-2-l2a-new clean workflow, its script run under bash with a stub `python3`.

What it passes to the tool is the contract: the fixed collection and floor, the bound,
--dry-run unless dry_run is exactly "false", never --clean-s3, and a real run only on an
image tag that names one image.
"""

import os
import subprocess  # nosec B404 -- runs bash on this repo's own manifest, fixed argv
from pathlib import Path

import pytest
import yaml

MANIFEST = Path(__file__).parents[2] / "deploy" / "clean-s2-new-keep-geozarr-workflow.yaml"


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
    """The container's script, with the manifest defaults as env and a stub `python3`."""
    stub_dir = tmp_path / "bin"
    stub_dir.mkdir()
    stub = stub_dir / "python3"
    stub.write_text('#!/bin/sh\necho "STUB PYTHON $*"\n')
    stub.chmod(0o755)
    params = {p["name"]: p["value"] for p in _spec()["arguments"]["parameters"]}
    full_env = {
        "PATH": f"{stub_dir}:{os.environ['PATH']}",
        "PIPELINE_IMAGE_VERSION": "sha-abc1234",
        "MAX_ITEMS": params["max_items"],
        "DRY_RUN": params["dry_run"],
        **env,
    }
    return subprocess.run(  # noqa: S603  # nosec B603 B607 -- this repo's own manifest
        ["bash", "-c", _container()["args"][0]],  # noqa: S607
        env=full_env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_defaults_are_a_bounded_dry_run_keeping_geozarr_0_13_0(tmp_path: Path) -> None:
    result = _run(tmp_path)
    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == [
        "STUB",
        "PYTHON",
        "/app/operator-tools/manage_collections.py",
        "--api-url",
        "https://api.explorer.eopf.copernicus.eu/stac",
        "clean",
        "sentinel-2-l2a-new",
        "--keep-software-min",
        "EOPF_GEOZARR=0.13.0",
        "--max-items",
        "10",
        "--dry-run",
    ]


@pytest.mark.parametrize("dry_run", ["true", "False", "no", ""])
def test_only_false_is_a_real_run(tmp_path: Path, dry_run: str) -> None:
    result = _run(tmp_path, DRY_RUN=dry_run, PIPELINE_IMAGE_VERSION="pr-9")
    assert result.returncode == 0, result.stderr
    assert "--dry-run" in result.stdout
    assert " -y" not in result.stdout


@pytest.mark.parametrize(
    "tag", ["sha-33f03f8", "v1.23.0@sha256:" + "0" * 64, "pr-515@sha256:" + "0" * 64]
)
def test_real_run_on_a_tag_that_names_one_image(tmp_path: Path, tag: str) -> None:
    result = _run(tmp_path, DRY_RUN="false", PIPELINE_IMAGE_VERSION=tag, MAX_ITEMS="2000")
    assert result.returncode == 0, result.stderr
    assert result.stdout.split()[-3:] == ["--max-items", "2000", "-y"]
    assert "--dry-run" not in result.stdout


@pytest.mark.parametrize("tag", ["v1.23.0", "pr-515", "latest", "main", "feat-clean-keep-software"])
def test_real_run_on_a_moving_tag_is_refused(tmp_path: Path, tag: str) -> None:
    result = _run(tmp_path, DRY_RUN="false", PIPELINE_IMAGE_VERSION=tag)
    assert result.returncode == 1
    assert "REFUSING" in result.stderr
    assert "STUB PYTHON" not in result.stdout, "the tool started despite the refusal"


def test_an_empty_image_tag_is_refused(tmp_path: Path) -> None:
    result = _run(tmp_path, PIPELINE_IMAGE_VERSION="")
    assert result.returncode == 2
    assert "STUB PYTHON" not in result.stdout


def test_catalogue_only_no_clean_s3_and_no_s3_credentials() -> None:
    """The stores are EODC's: nothing here may reach S3, by flag or by credential."""
    container = _container()
    assert "--clean-s3" not in container["args"][0]
    assert not [e for e in container["env"] if e["name"].startswith("AWS_")]


def test_the_collection_api_and_floor_are_fixed_not_parameters(tmp_path: Path) -> None:
    """A floor typed at submit time (0.13.1 for 0.13.0) would keep 3,943 items, pass the
    "keeps no item" guard and delete the 4,876 wanted 0.13.0 items."""
    params = {p["name"] for p in _spec()["arguments"]["parameters"]}
    assert params == {"pipeline_image_version", "max_items", "dry_run"}
    result = _run(tmp_path, KEEP_SOFTWARE_MIN="EOPF_GEOZARR=0.13.1", DRY_RUN="false")
    assert result.returncode == 0, result.stderr
    assert "--keep-software-min EOPF_GEOZARR=0.13.0 " in result.stdout


def test_the_image_is_pulled_every_time() -> None:
    """A node's cached copy of the tag must not stand in for the image the dry run used."""
    assert _container()["imagePullPolicy"] == "Always"


def test_no_deadline_kills_the_run() -> None:
    """The bound is --max-items, inside the tool; never a timeout or a kill on a prod run."""
    assert "activeDeadlineSeconds" not in _spec()
    assert "activeDeadlineSeconds" not in _template()
