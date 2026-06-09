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
    """Exactly two aws s3 sync blocks, plus one aws s3 rm prefix-purge block.

    _run() prints each command token on its own continuation line, so the verb appears as a
    stripped "sync \\" / "rm \\" line rather than a single "aws s3 sync ..." line.
    """
    lines = [line.strip() for line in _dry_run(tmp_path).stdout.splitlines()]
    assert lines.count("aws \\") == 3, "expected 3 aws command blocks (1 rm + 2 sync)"
    assert lines.count("sync \\") == 2, f"Expected 2 sync blocks, got {lines.count('sync \\')}"
    assert (
        lines.count("rm \\") == 1
    ), f"Expected 1 rm (prefix purge) block, got {lines.count('rm \\')}"


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
# cfg rendering: run-specific keys are injected (not copied verbatim)
# ---------------------------------------------------------------------------

# A base cfg carrying the static keys the renderer must overwrite.
_BASE_CFG = (
    "[DataSource]\n"
    "roi_by_tiles : 31TCH\n"
    "platform_list : S1A\n"
    "orbit_direction : DES\n"
    "first_date : 2025-02-01\n"
    "last_date : 2025-02-14\n"
    "[Processing]\n"
    "tiles : 31TCH\n"
)


def _render(tmp_path: Path, **kwargs) -> str:
    """Import run_s1tiling and render a cfg from _BASE_CFG; return the rendered text."""
    sys.path.insert(0, str(SCRIPT.parent))
    import run_s1tiling

    src = tmp_path / "base.cfg"
    src.write_text(_BASE_CFG)
    dst = tmp_path / "rendered.cfg"
    run_s1tiling._render_cfg(
        src,
        dst,
        kwargs.get("tile_id", "33UUP"),
        kwargs.get("orbit_direction", "ascending"),
        kwargs.get("date_start", "2026-06-04"),
        kwargs.get("date_end", "2026-06-06"),
        kwargs.get("platform_list", "S1A S1C"),
    )
    return dst.read_text()


def test_render_cfg_injects_date_window(tmp_path):
    """The requested window overwrites the base cfg's static dates."""
    out = _render(tmp_path)
    assert "first_date : 2026-06-04" in out
    assert "last_date : 2026-06-06" in out
    assert "2025-02-01" not in out and "2025-02-14" not in out


def test_render_cfg_injects_tile_and_orbit(tmp_path):
    """tile patches both roi_by_tiles and tiles; orbit maps to the DES/ASC short code."""
    out = _render(tmp_path, tile_id="33UUP", orbit_direction="ascending")
    assert "roi_by_tiles : 33UUP" in out
    assert "tiles : 33UUP" in out
    assert "orbit_direction : ASC" in out


def test_render_cfg_descending_maps_to_des(tmp_path):
    out = _render(tmp_path, orbit_direction="descending")
    assert "orbit_direction : DES" in out


def test_render_cfg_injects_platform(tmp_path):
    """platform_list is now run-specific (T2): default S1A S1C, overwriting the base cfg value."""
    out = _render(tmp_path)
    assert "platform_list : S1A S1C" in out


def test_render_cfg_platform_list_configurable(tmp_path):
    out = _render(tmp_path, platform_list="S1A")
    assert "platform_list : S1A" in out
    assert "platform_list : S1A S1C" not in out


# ---------------------------------------------------------------------------
# Task 1a: path-safe deletion helper + tile-id validation
# ---------------------------------------------------------------------------


def _import_script():
    sys.path.insert(0, str(SCRIPT.parent))
    import run_s1tiling

    return run_s1tiling


def test_validate_tile_id_accepts_mgrs():
    mod = _import_script()
    assert mod._validate_tile_id("31TCH") == "31TCH"


@pytest.mark.parametrize("bad", ["", "..", "/abs", "31tch", "ABCDE", "31TC", "31TCHH", "../31TCH"])
def test_validate_tile_id_rejects_malformed(bad):
    mod = _import_script()
    with pytest.raises(ValueError, match="invalid tile id"):
        mod._validate_tile_id(bad)


def test_safe_clean_removes_path_inside_data_dir(tmp_path):
    mod = _import_script()
    data_dir = tmp_path / "workdir"
    target = data_dir / "data_out" / "31TCH"
    target.mkdir(parents=True)
    (target / "stale.tif").write_text("x")

    mod._safe_clean(target, data_dir, dry_run=False)
    assert not target.exists()


def test_safe_clean_dry_run_keeps_path(tmp_path):
    mod = _import_script()
    data_dir = tmp_path / "workdir"
    target = data_dir / "data_out" / "31TCH"
    target.mkdir(parents=True)
    (target / "stale.tif").write_text("x")

    mod._safe_clean(target, data_dir, dry_run=True)
    assert (target / "stale.tif").exists()


def test_safe_clean_missing_target_is_noop(tmp_path):
    mod = _import_script()
    data_dir = tmp_path / "workdir"
    data_dir.mkdir()
    # target never created — must not raise
    mod._safe_clean(data_dir / "data_out" / "31TCH", data_dir, dry_run=False)


def test_safe_clean_refuses_equal_to_data_dir(tmp_path):
    """A target resolving to data_dir itself (e.g. empty tile collapsing the path) is refused."""
    mod = _import_script()
    data_dir = tmp_path / "workdir"
    data_dir.mkdir()
    with pytest.raises(ValueError, match="not strictly under"):
        mod._safe_clean(data_dir / "data_out" / "..", data_dir, dry_run=False)
    assert data_dir.exists()


def test_safe_clean_refuses_path_outside_data_dir(tmp_path):
    mod = _import_script()
    data_dir = tmp_path / "workdir"
    data_dir.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep.txt").write_text("x")

    with pytest.raises(ValueError, match="not strictly under"):
        mod._safe_clean(outside, data_dir, dry_run=False)
    assert (outside / "keep.txt").exists()


def test_safe_clean_refuses_absolute_escape(tmp_path):
    """An absolute path component resets the join and escapes data_dir — must be refused."""
    mod = _import_script()
    data_dir = tmp_path / "workdir"
    data_dir.mkdir()
    # Path("/a/b") / "/etc" == Path("/etc") in pathlib
    with pytest.raises(ValueError, match="not strictly under"):
        mod._safe_clean(data_dir / "data_out" / "/etc", data_dir, dry_run=False)


# ---------------------------------------------------------------------------
# Task 1b: cleanup wiring through main() (subprocess boundary stubbed)
# ---------------------------------------------------------------------------


def _invoke_main(tmp_path, monkeypatch, extra_args=None, run_stub=None):
    """Run run_s1tiling.main() with the docker/aws boundary (_run) stubbed.

    Defaults to a no-op stub; pass ``run_stub`` to simulate docker exit codes / output.
    Returns the workdir data_dir so callers can assert on-disk cleanup effects.
    """
    mod = _import_script()

    cfg = tmp_path / "config" / "S1GRD_RTC.cfg"
    cfg.parent.mkdir(parents=True, exist_ok=True)
    cfg.write_text("[DataSource]\nroi_by_tiles : 31TCH\n")
    eodag_cfg = tmp_path / "eodag.yml"
    eodag_cfg.write_text("eodag:\n")
    dem_dir = tmp_path / "DEM" / "COP_DEM_GLO30"
    dem_dir.mkdir(parents=True, exist_ok=True)
    data_dir = tmp_path / "workdir"
    data_dir.mkdir(exist_ok=True)
    (data_dir / "DEM" / "dem_db").mkdir(parents=True, exist_ok=True)
    (data_dir / "geoid").mkdir(exist_ok=True)

    argv = [
        "run_s1tiling.py",
        "--tile-id",
        "31TCH",
        "--orbit-direction",
        "descending",
        "--date-start",
        "2026-06-04",
        "--date-end",
        "2026-06-06",
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
        *(extra_args or []),
    ]
    monkeypatch.setattr(sys, "argv", argv)
    monkeypatch.setattr(mod, "_run", run_stub or (lambda *a, **k: None))  # stub docker + aws
    mod.main()
    return data_dir


def test_main_cleans_stale_output_by_default(tmp_path, monkeypatch):
    """A stale acquisition under data_out/{tile} is removed before the sync."""
    data_out_tile = tmp_path / "workdir" / "data_out" / "31TCH"
    data_out_tile.mkdir(parents=True)
    stale = data_out_tile / "s1a_31TCH_vv_DES_110_20250205t060110_GammaNaughtRTC.tif"
    stale.write_text("stale")

    _invoke_main(tmp_path, monkeypatch)
    assert not stale.exists()


def test_main_keep_output_preserves_stale(tmp_path, monkeypatch):
    data_out_tile = tmp_path / "workdir" / "data_out" / "31TCH"
    data_out_tile.mkdir(parents=True)
    stale = data_out_tile / "old.tif"
    stale.write_text("stale")

    _invoke_main(tmp_path, monkeypatch, ["--keep-output"])
    assert stale.exists()


def test_main_prune_raw_clears_data_raw(tmp_path, monkeypatch):
    data_raw = tmp_path / "workdir" / "data_raw"
    data_raw.mkdir(parents=True)
    scene = data_raw / "S1A_OLD_SCENE"
    scene.mkdir()

    _invoke_main(tmp_path, monkeypatch, ["--prune-raw"])
    assert not scene.exists()


def test_main_default_leaves_data_raw(tmp_path, monkeypatch):
    data_raw = tmp_path / "workdir" / "data_raw"
    data_raw.mkdir(parents=True)
    scene = data_raw / "S1A_OLD_SCENE"
    scene.mkdir()

    _invoke_main(tmp_path, monkeypatch)
    assert scene.exists()


def test_main_refuses_s3_purge_with_empty_bucket(tmp_path, monkeypatch):
    """The prefix purge must refuse an empty bucket rather than rm a bucket root."""
    with pytest.raises(SystemExit):
        _invoke_main(tmp_path, monkeypatch, ["--s3-bucket", ""])


def test_dry_run_prints_clean_and_purge_intents(tmp_path):
    """--dry-run prints the local-clean and S3-purge intents and runs no real deletion."""
    out = _dry_run(tmp_path).stdout
    lines = [line.strip() for line in out.splitlines()]
    assert "[dry-run] would clean" in out
    assert "data_out/31TCH" in out and "data_gamma_area" in out
    assert "rm \\" in lines, "expected an aws s3 rm prefix-purge block"
    # purge targets the run's own date-keyed prefix (dry-run helper uses date-start 2025-02-01)
    assert "s1tiling-output/31TCH/descending/2025-02-01/" in out


def test_dry_run_keep_output_skips_clean_and_purge(tmp_path):
    out = _dry_run(tmp_path, extra_args=["--keep-output"]).stdout
    lines = [line.strip() for line in out.splitlines()]
    assert "[dry-run] would clean" not in out
    assert "rm \\" not in lines, "prefix purge must be skipped under --keep-output"
    assert lines.count("sync \\") == 2, "syncs still run under --keep-output"


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


# ---------------------------------------------------------------------------
# T2: platform-render success contract — tolerate off-platform download failures
# ---------------------------------------------------------------------------


def test_requested_platform_outputs_present_true_for_requested(tmp_path):
    mod = _import_script()
    out = tmp_path / "data_out" / "31TCH"
    out.mkdir(parents=True)
    (out / "s1a_31TCH_vv_DES_037_20250210t060920_GammaNaughtRTC.tif").write_text("x")
    assert mod._requested_platform_outputs_present(out, ["S1A", "S1C"]) is True


def test_requested_platform_outputs_present_false_for_other_platform(tmp_path):
    """Only an off-platform (S1B) GeoTIFF present while S1A/S1C were requested -> not a success."""
    mod = _import_script()
    out = tmp_path / "data_out" / "31TCH"
    out.mkdir(parents=True)
    (out / "s1b_31TCH_vv_DES_037_20250210t060920_GammaNaughtRTC.tif").write_text("x")
    assert mod._requested_platform_outputs_present(out, ["S1A", "S1C"]) is False


def test_requested_platform_outputs_present_false_when_absent(tmp_path):
    mod = _import_script()
    assert mod._requested_platform_outputs_present(tmp_path / "nope", ["S1A", "S1C"]) is False


def test_main_tolerates_nonzero_docker_when_output_present(tmp_path, monkeypatch):
    """S1Processor exits non-zero (off-platform download failed) but the requested platform
    produced output -> the run continues and the aws sync is attempted (T2 success contract)."""
    data_dir = tmp_path / "workdir"
    calls = []

    def fake_run(cmd, dry_run, *, check=True):
        calls.append(cmd)
        if cmd[0] == "docker":
            out = data_dir / "data_out" / "31TCH"
            out.mkdir(parents=True, exist_ok=True)
            (out / "s1a_31TCH_vv_DES_037_20250210t060920_GammaNaughtRTC.tif").write_text("x")
            return 2  # off-platform (e.g. S1D) download failed
        return 0

    _invoke_main(tmp_path, monkeypatch, run_stub=fake_run)
    assert any(c[0] == "aws" and "sync" in c for c in calls), "sync should proceed despite rc=2"


def test_main_exits_when_docker_fails_and_no_output(tmp_path, monkeypatch):
    """Non-zero docker exit with no requested-platform output -> hard failure, no sync."""
    calls = []

    def fake_run(cmd, dry_run, *, check=True):
        calls.append(cmd)
        return 2 if cmd[0] == "docker" else 0

    with pytest.raises(SystemExit) as exc:
        _invoke_main(tmp_path, monkeypatch, run_stub=fake_run)
    assert exc.value.code == 2
    assert not any(c[0] == "aws" and "sync" in c for c in calls), "no sync after a true failure"
