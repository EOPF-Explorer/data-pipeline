"""Unit tests for scripts/run_s1tiling.py — dry-run output and failure modes."""

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

docker_available = shutil.which("docker") is not None

SCRIPT = Path(__file__).parent.parent.parent / "scripts" / "run_s1tiling.py"
ANALYSIS_DIR = Path(__file__).parent.parent.parent / "analysis"


def _dry_run(tmp_path: Path, extra_args: list[str] | None = None) -> subprocess.CompletedProcess:
    """Run run_s1tiling.py --dry-run with stub paths and return the result."""
    cfg = tmp_path / "config" / "S1GRD_RTC.cfg"
    cfg.parent.mkdir(parents=True)
    cfg.write_text("[S1Tiling]\n")

    eodag_cfg = tmp_path / "eodag.yml"
    eodag_cfg.write_text("eodag:\n")

    dem_dir = tmp_path / "DEM" / "COP_DEM_GLO30"
    dem_dir.mkdir(parents=True)

    data_dir = tmp_path / "workdir"
    data_dir.mkdir()
    (data_dir / "DEM" / "dem_db").mkdir(parents=True)
    (data_dir / "geoid").mkdir()

    cmd = [
        sys.executable,
        str(SCRIPT),
        "--tile-id",
        "31TCH",
        "--orbit-direction",
        "descending",
        "--date-start",
        "2025-02-01",
        "--date-end",
        "2025-02-14",
        "--s3-bucket",
        "esa-zarr-sentinel-explorer-tests",
        "--s3-prefix",
        "s1tiling-output",
        "--s3-endpoint",
        "https://s3.de.io.cloud.ovh.net",
        "--eodag-cfg",
        str(eodag_cfg),
        "--dem-dir",
        str(dem_dir),
        "--data-dir",
        str(data_dir),
        "--cfg",
        str(cfg),
        "--dry-run",
        *(extra_args or []),
    ]
    return subprocess.run(cmd, capture_output=True, text=True)  # noqa: S603


# ---------------------------------------------------------------------------
# Task 1: dry-run correctness
# ---------------------------------------------------------------------------


def test_dry_run_exits_zero(tmp_path):
    result = _dry_run(tmp_path)
    assert result.returncode == 0, result.stderr


def test_dry_run_docker_entrypoint_bash_before_image(tmp_path):
    """--entrypoint bash must appear before the image name in the output."""
    out = _dry_run(tmp_path).stdout
    entrypoint_pos = out.index("--entrypoint")
    image_pos = out.index("registry.orfeo-toolbox.org")
    assert entrypoint_pos < image_pos
    bash_pos = out.index("bash", entrypoint_pos)
    assert bash_pos < image_pos


def test_dry_run_docker_six_volume_mounts(tmp_path):
    """-v appears exactly 6 times in the docker command block."""
    out = _dry_run(tmp_path).stdout
    # Each -v is on its own continuation line: "  -v \"
    # Count lines whose stripped content is exactly "-v \"
    lines = out.splitlines()
    v_lines = [line for line in lines if line.strip() == "-v \\"]
    assert len(v_lines) == 6, f"Expected 6 -v mounts, got {len(v_lines)}: {v_lines}"


def test_dry_run_docker_volumes_are_absolute(tmp_path):
    """Every host path in -v mounts must be absolute (no ~ or relative ..)."""
    out = _dry_run(tmp_path).stdout
    lines = out.splitlines()
    for i, line in enumerate(lines):
        if line.strip() == "-v \\":
            # The mount spec is on the next line: "  host_path:container_path"
            mount_spec = lines[i + 1].strip().rstrip(" \\")
            host_path = mount_spec.split(":")[0]
            assert host_path.startswith("/"), f"Non-absolute host path: {host_path!r}"
            assert "~" not in host_path, f"Tilde in host path: {host_path!r}"


def test_dry_run_docker_patch_volume_points_to_analysis_dir(tmp_path):
    """-v .../analysis:/patch:ro must point to the real analysis/ directory."""
    out = _dry_run(tmp_path).stdout
    patch_line = next(
        (line.strip() for line in out.splitlines() if ":/patch:ro" in line),
        None,
    )
    assert patch_line is not None, "No mount spec with :/patch:ro found"
    host_path = patch_line.rstrip(" \\").split(":")[0]
    assert Path(host_path).resolve() == ANALYSIS_DIR.resolve()


def test_dry_run_docker_command_string(tmp_path):
    """-c must run the patch script then S1Processor."""
    out = _dry_run(tmp_path).stdout
    assert (
        "python3 /patch/s1tiling_eodag4_patch.py && S1Processor /data/config/S1GRD_RTC.cfg" in out
    )


def test_dry_run_two_s3_sync_commands(tmp_path):
    """Exactly two aws s3 sync command blocks are printed."""
    out = _dry_run(tmp_path).stdout
    # Each block starts with a line beginning with "aws"
    aws_blocks = [line for line in out.splitlines() if line.startswith("aws")]
    assert len(aws_blocks) == 2, f"Expected 2 aws blocks, got {len(aws_blocks)}"


def test_dry_run_s3_sync_targets_data_out(tmp_path):
    """First aws sync command includes data_out/<tile-id>/."""
    out = _dry_run(tmp_path).stdout
    # Each sync block prints its source path as a continuation line
    assert "data_out/31TCH/" in out


def test_dry_run_s3_sync_targets_gamma_area(tmp_path):
    """Second aws sync command includes data_gamma_area/."""
    out = _dry_run(tmp_path).stdout
    assert "data_gamma_area/" in out


def test_dry_run_both_syncs_share_same_s3_prefix(tmp_path):
    """Both syncs target the canonical s3://bucket/prefix/tile/direction/date/ key."""
    expected_prefix = (
        "s3://esa-zarr-sentinel-explorer-tests/s1tiling-output/31TCH/descending/2025-02-01/"
    )
    out = _dry_run(tmp_path).stdout
    # The S3 prefix appears as a continuation line in each sync block, plus the final print.
    # So it should appear at least twice (once per sync block).
    assert out.count(expected_prefix) >= 2, f"Expected {expected_prefix!r} at least twice in output"


def test_dry_run_last_line_is_s3_prefix(tmp_path):
    """Last non-empty line of output must be exactly the S3 prefix."""
    expected = "s3://esa-zarr-sentinel-explorer-tests/s1tiling-output/31TCH/descending/2025-02-01/"
    out = _dry_run(tmp_path).stdout
    last_line = [line for line in out.splitlines() if line.strip()][-1]
    assert last_line == expected


# ---------------------------------------------------------------------------
# Task 4: failure-mode behaviour
# ---------------------------------------------------------------------------


@pytest.mark.docker
@pytest.mark.skipif(not docker_available, reason="Docker daemon not available")
def test_s3_sync_not_attempted_after_docker_failure(tmp_path, monkeypatch):
    """If docker exits non-zero, script must exit before reaching aws s3 sync."""
    # Run without --dry-run using a nonexistent image tag so Docker fails immediately.

    cfg = tmp_path / "config" / "S1GRD_RTC.cfg"
    cfg.parent.mkdir(parents=True)
    cfg.write_text("[S1Tiling]\n")

    eodag_cfg = tmp_path / "eodag.yml"
    eodag_cfg.write_text("eodag:\n")

    dem_dir = tmp_path / "DEM" / "COP_DEM_GLO30"
    dem_dir.mkdir(parents=True)

    data_dir = tmp_path / "workdir"
    data_dir.mkdir()
    (data_dir / "DEM" / "dem_db").mkdir(parents=True)
    (data_dir / "geoid").mkdir()

    cmd = [
        sys.executable,
        str(SCRIPT),
        "--tile-id",
        "31TCH",
        "--orbit-direction",
        "descending",
        "--date-start",
        "2025-02-01",
        "--date-end",
        "2025-02-14",
        "--s3-bucket",
        "x",
        "--s3-prefix",
        "x",
        "--s3-endpoint",
        "x",
        "--eodag-cfg",
        str(eodag_cfg),
        "--dem-dir",
        str(dem_dir),
        "--data-dir",
        str(data_dir),
        "--cfg",
        str(cfg),
        # no --dry-run; Docker will fail because image does not exist locally
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)  # noqa: S603
    assert result.returncode != 0, "Expected non-zero exit when Docker fails"
    assert "aws" not in result.stdout, "S3 sync should not be attempted after Docker failure"
