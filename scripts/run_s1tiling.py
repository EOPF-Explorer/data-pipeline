"""Local Workflow 1 simulation: run S1Tiling in Docker and sync GeoTIFFs to S3.

Mirrors Argo WorkflowTemplate eopf-explorer-s1tiling.
Local-only args (--eodag-cfg, --dem-dir, --data-dir, --cfg) have no Argo equivalent.
"""

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

IMAGE = "registry.orfeo-toolbox.org/s1-tiling/s1tiling:1.4.0-ubuntu-otb9.1.1"


def _run(cmd: list[str], dry_run: bool) -> None:
    print(" \\\n  ".join(str(c) for c in cmd))
    if not dry_run:
        result = subprocess.run(cmd)  # noqa: S603
        if result.returncode != 0:
            sys.exit(result.returncode)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tile-id", required=True)
    ap.add_argument("--orbit-direction", required=True, choices=["descending", "ascending"])
    ap.add_argument("--date-start", required=True)
    ap.add_argument("--date-end", required=True)
    ap.add_argument("--s3-bucket", required=True)
    ap.add_argument("--s3-prefix", required=True)
    ap.add_argument("--s3-endpoint", required=True)
    ap.add_argument("--eodag-cfg", required=True, type=Path)
    ap.add_argument("--dem-dir", required=True, type=Path)
    ap.add_argument("--data-dir", required=True, type=Path)
    ap.add_argument("--cfg", required=True, type=Path)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

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

    # Step 1: keep workdir cfg in sync with --cfg source
    workdir_cfg = data_dir / "config" / "S1GRD_RTC.cfg"
    workdir_cfg.parent.mkdir(parents=True, exist_ok=True)
    if not workdir_cfg.exists() or workdir_cfg.read_bytes() != abs_cfg.read_bytes():
        shutil.copy(abs_cfg, workdir_cfg)

    # Step 2: docker run
    _run(
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
    )

    # Step 3: sync GeoTIFFs and GAMMA_AREA conditions to the same S3 prefix
    s3_out = (
        f"s3://{args.s3_bucket}/{args.s3_prefix}"
        f"/{args.tile_id}/{args.orbit_direction}/{args.date_start}/"
    )
    s3_flags = ["--endpoint-url", args.s3_endpoint, "--profile", "eopfexplorer"]
    _run(
        ["aws", "s3", "sync", f"{data_dir}/data_out/{args.tile_id}/", s3_out, *s3_flags],
        args.dry_run,
    )
    _run(["aws", "s3", "sync", f"{data_dir}/data_gamma_area/", s3_out, *s3_flags], args.dry_run)

    # Step 4: print output prefix for run_ingest_register.py
    print(s3_out)


if __name__ == "__main__":
    main()
