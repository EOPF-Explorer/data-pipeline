"""Local Workflow 1 simulation: run S1Tiling in Docker and sync GeoTIFFs to S3.

Mirrors Argo WorkflowTemplate eopf-explorer-s1tiling.
Local-only args (--eodag-cfg, --dem-dir, --data-dir, --cfg, --keep-output, --prune-raw) have no
Argo equivalent. The cleanup flags exist because the prototype reuses $S1T_WORKDIR, whereas each
Argo workflow gets a fresh volume.
"""

import argparse
import re
import shutil
import subprocess  # nosec B404 -- runs aws-cli/s1tiling with fixed argv (no shell)
import sys
from pathlib import Path

IMAGE = "registry.orfeo-toolbox.org/s1-tiling/s1tiling:1.4.0-ubuntu-otb9.1.1"

TILE_ID_RE = re.compile(r"^[0-9]{2}[A-Z]{3}$")


def _validate_tile_id(tile_id: str) -> str:
    """Return ``tile_id`` if it is a well-formed MGRS tile (e.g. ``31TCH``); raise otherwise.

    Guards the cleanup paths: an empty/malformed id would otherwise collapse
    ``data_out/{tile_id}`` to the whole ``data_out`` directory.
    """
    if not TILE_ID_RE.match(tile_id):
        raise ValueError(f"invalid tile id (expected e.g. 31TCH): {tile_id!r}")
    return tile_id


def _safe_clean(target: Path, data_dir: Path, dry_run: bool) -> None:
    """Recursively remove ``target`` only if it resolves strictly under ``data_dir``.

    A misbuilt target (empty / ``..`` / absolute path component) cannot escape — or equal —
    the workdir: it raises instead. A missing target is a no-op; ``--dry-run`` prints the intent.
    """
    data_root = data_dir.resolve()
    resolved = target.resolve()
    if data_root not in resolved.parents:
        raise ValueError(f"refusing to delete path not strictly under {data_root}: {resolved}")
    if dry_run:
        print(f"[dry-run] would clean {resolved}")
        return
    if resolved.exists():
        shutil.rmtree(resolved)


def _render_cfg(
    src: Path,
    dst: Path,
    tile_id: str,
    orbit_direction: str,
    date_start: str,
    date_end: str,
    platform_list: str,
) -> None:
    """Render a per-run S1Tiling cfg from the committed base, patching only the run-specific keys.

    Mirrors the Argo eopf-explorer-s1tiling template: without this, S1Processor would use the
    base cfg's static first_date/last_date and ignore the requested window. ``platform_list``
    (e.g. ``S1A S1C``) is rendered too so the enabled platforms aren't hardcoded in the base cfg.
    """
    oc = "DES" if orbit_direction == "descending" else "ASC"
    subs = {
        "roi_by_tiles": tile_id,
        "tiles": tile_id,
        "orbit_direction": oc,
        "first_date": date_start,
        "last_date": date_end,
        "platform_list": platform_list,
    }
    text = src.read_text()
    for key, val in subs.items():
        text = re.sub(rf"^{key} : .*$", f"{key} : {val}", text, flags=re.MULTILINE)
    dst.write_text(text)


_GAMMA_PLATFORM_RE = re.compile(r"^(s1[abc])_", re.IGNORECASE)  # platform prefix of an output tif


def _requested_platform_outputs_present(tile_out_dir: Path, platforms: list[str]) -> bool:
    """True if ``data_out/{tile}`` holds a GammaNaughtRTC GeoTIFF for a requested platform.

    s1tiling downloads *every* platform in the window and exits non-zero if an off-platform
    download (e.g. S1D) fails, even when the requested platform produced output. The run is a
    success when the requested platform's GeoTIFFs are present, regardless of that exit code.
    """
    if not tile_out_dir.is_dir():
        return False
    wanted = {p.lower() for p in platforms}
    for tif in tile_out_dir.glob("*GammaNaughtRTC.tif"):
        m = _GAMMA_PLATFORM_RE.match(tif.name)
        if m and m.group(1).lower() in wanted:
            return True
    return False


def _run(cmd: list[str], dry_run: bool, *, check: bool = True) -> int:
    """Print and run ``cmd``; return its exit code. With ``check`` (default), a non-zero exit aborts
    the script (used for the aws steps). Pass ``check=False`` to inspect the code instead — the
    s1tiling step does this to apply its own success contract."""
    print(" \\\n  ".join(str(c) for c in cmd))
    if dry_run:
        return 0
    result = subprocess.run(cmd)  # noqa: S603  # nosec B603 -- fixed argv, no shell
    if check and result.returncode != 0:
        sys.exit(result.returncode)
    return result.returncode


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tile-id", required=True)
    ap.add_argument("--orbit-direction", required=True, choices=["descending", "ascending"])
    ap.add_argument("--date-start", required=True)
    ap.add_argument("--date-end", required=True)
    ap.add_argument("--s3-bucket", required=True)
    ap.add_argument("--s3-prefix", required=True)
    ap.add_argument("--s3-endpoint", required=True)
    ap.add_argument(
        "--aws-profile",
        default=None,
        help="aws CLI profile for the S3 sync; omit to use ambient AWS_* creds (default — matches "
        "the op-sourced eopfexplorer keys). Pass a profile name only if your setup still uses one.",
    )
    ap.add_argument("--eodag-cfg", required=True, type=Path)
    ap.add_argument("--dem-dir", required=True, type=Path)
    ap.add_argument("--data-dir", required=True, type=Path)
    ap.add_argument("--cfg", required=True, type=Path)
    ap.add_argument(
        "--platform-list",
        default="S1A",
        help="S1Tiling platform_list — run ONE platform at a time (e.g. S1A or S1C). s1tiling 1.4.0's "
        "multi-platform post-filter is broken (a 'S1A S1C' list yields 0 products), so combined "
        "lists are unsupported here; the data-driven trigger submits one product (one platform) per "
        "run anyway. S1D unsupported (#223).",
    )
    ap.add_argument(
        "--keep-output",
        action="store_true",
        help="Skip cleaning data_out/data_gamma_area and the S3 prefix (manual inspection)",
    )
    ap.add_argument(
        "--prune-raw",
        action="store_true",
        help="Also clear data_raw before the run (re-downloads scenes; bounds disk growth)",
    )
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    try:
        _validate_tile_id(args.tile_id)
    except ValueError as exc:
        sys.exit(f"Error: {exc}")

    # Fail fast on a multi-platform list: s1tiling 1.4.0's multi-platform post-filter discards
    # everything (a 'S1A S1C' list yields 0 products), so a combined list silently produces nothing.
    if len(args.platform_list.split()) != 1:
        sys.exit(
            f"Error: --platform-list must be a single platform (got {args.platform_list!r}); "
            "s1tiling 1.4.0's multi-platform filter discards all products. Run one platform per "
            "invocation (the data-driven trigger submits one product per run anyway)."
        )

    for p, name in [
        (args.cfg, "--cfg"),
        (args.eodag_cfg, "--eodag-cfg"),
        (args.dem_dir, "--dem-dir"),
    ]:
        if not p.exists():
            sys.exit(f"Error: {name} not found: {p}")

    data_dir = args.data_dir.resolve()
    abs_dem_dir = args.dem_dir.resolve()
    abs_eodag_cfg = args.eodag_cfg.resolve()
    abs_cfg = args.cfg.resolve()
    abs_dem_db = data_dir / "DEM" / "dem_db"
    abs_geoid_dir = data_dir / "geoid"
    abs_patch_dir = (Path(__file__).parent.parent / "analysis").resolve()

    # Step 1: render the per-run cfg from the --cfg source (tile/orbit/date window)
    workdir_cfg = data_dir / "config" / "S1GRD_RTC.cfg"
    workdir_cfg.parent.mkdir(parents=True, exist_ok=True)
    _render_cfg(
        abs_cfg,
        workdir_cfg,
        args.tile_id,
        args.orbit_direction,
        args.date_start,
        args.date_end,
        args.platform_list,
    )

    # Step 1b: clean prior-run outputs so only this window's products get synced. The prototype
    # reuses $S1T_WORKDIR (Argo gets a fresh volume); --keep-output opts out. data_raw is left
    # intact (date-filtered, not a contamination source) unless --prune-raw is passed.
    if not args.keep_output:
        _safe_clean(data_dir / "data_out" / args.tile_id, data_dir, args.dry_run)
        _safe_clean(data_dir / "data_gamma_area", data_dir, args.dry_run)
        if not args.dry_run:
            (data_dir / "data_out").mkdir(parents=True, exist_ok=True)
            (data_dir / "data_gamma_area").mkdir(parents=True, exist_ok=True)
    if args.prune_raw:
        _safe_clean(data_dir / "data_raw", data_dir, args.dry_run)

    # Step 2: docker run. Apply the success contract: S1Processor downloads every platform in the
    # window and exits non-zero if an off-platform (e.g. S1D) download fails, even when the requested
    # platform produced output. So we don't fail the run on a non-zero exit alone — only if the
    # requested-platform GeoTIFFs are absent from data_out/{tile}.
    rc = _run(
        [
            "docker",
            "run",
            "--rm",
            "--platform",
            "linux/amd64",
            "--entrypoint",
            "bash",
            "-v",
            f"{data_dir}:/data",
            "-v",
            f"{abs_dem_dir}:/MNT/COP_DEM_GLO30",
            "-v",
            f"{abs_dem_db}:/MNT/dem_db:ro",
            "-v",
            f"{abs_geoid_dir}:/MNT/geoid:ro",
            "-v",
            f"{abs_eodag_cfg}:/eo_config/eodag.yml:ro",
            "-v",
            f"{abs_patch_dir}:/patch:ro",
            IMAGE,
            "-c",
            "python3 /patch/s1tiling_eodag4_patch.py && S1Processor /data/config/S1GRD_RTC.cfg",
        ],
        args.dry_run,
        check=False,
    )
    if rc:
        tile_out = data_dir / "data_out" / args.tile_id
        if _requested_platform_outputs_present(tile_out, args.platform_list.split()):
            print(
                f"WARN: S1Processor exited {rc}, but requested-platform output is present in "
                f"{tile_out}; continuing (off-platform download failures tolerated)"
            )
        else:
            sys.exit(rc)

    # Step 3: purge the destination prefix, then sync GeoTIFFs + GAMMA_AREA into it. Purging first
    # makes the sync authoritative (a re-run of the same window self-heals); --delete is unusable
    # here because both syncs target one prefix. --keep-output opts out.
    s3_out = (
        f"s3://{args.s3_bucket}/{args.s3_prefix}"
        f"/{args.tile_id}/{args.orbit_direction}/{args.date_start}/"
    )
    s3_flags = ["--endpoint-url", args.s3_endpoint]
    if args.aws_profile:
        s3_flags += ["--profile", args.aws_profile]
    if not args.keep_output:
        if not all([args.s3_bucket, args.s3_prefix, args.tile_id, args.date_start]):
            sys.exit("Error: refusing S3 purge with an empty bucket/prefix/tile/date-start")
        _run(["aws", "s3", "rm", "--recursive", s3_out, *s3_flags], args.dry_run)
    _run(
        ["aws", "s3", "sync", f"{data_dir}/data_out/{args.tile_id}/", s3_out, *s3_flags],
        args.dry_run,
    )
    _run(["aws", "s3", "sync", f"{data_dir}/data_gamma_area/", s3_out, *s3_flags], args.dry_run)

    # Step 4: print output prefix for run_ingest_register.py
    print(s3_out)


if __name__ == "__main__":
    main()
