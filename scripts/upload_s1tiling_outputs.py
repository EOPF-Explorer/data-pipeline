"""Durably upload S1Tiling GeoTIFF outputs to S3 (container-native).

The OTB s1tiling step produces GeoTIFFs in a local (emptyDir) work dir. This
script uploads them to the canonical S3 prefix via s3fs -- a *blocking* upload
that completes before the process exits -- and then VERIFIES every file landed
with the right size. A partial upload therefore fails loudly (exit 1) instead of
silently losing data, which is what the previous csi-rclone mount did (its async
VFS writeback dropped in-flight uploads when the pod terminated).

Mirrors the S3 prefix and flat layout that ingest's discover_s1tiling_* helpers
expect:  s3://{bucket}/s1tiling-output/{tile}/{orbit}/{date_start}/<name>.tif

Exit codes:
    0 -- all GeoTIFFs uploaded and verified
    1 -- nothing to upload, an upload failed after retries, a basename collision,
         or post-upload verification mismatch
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any

from tenacity import retry, stop_after_attempt, wait_exponential

log = logging.getLogger(__name__)


def s1_output_prefix(bucket: str, tile_id: str, orbit_direction: str, date_start: str) -> str:
    """Canonical S3 prefix s1tiling writes to and ingest reads from."""
    return f"s3://{bucket}/s1tiling-output/{tile_id}/{orbit_direction}/{date_start}/"


def collect_local_tifs(data_dir: str | Path, tile_id: str) -> list[Path]:
    """Acquisition GeoTIFFs (data_out/<tile>) + GAMMA_AREA conditions (data_gamma_area)."""
    data_dir = Path(data_dir)
    acq = sorted((data_dir / "data_out" / tile_id).glob("*.tif"))
    gamma = sorted((data_dir / "data_gamma_area").glob("*.tif"))
    return acq + gamma


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=2, max=60),
    reraise=True,
)
def _put_one(fs: Any, local_path: Path, remote_path: str) -> None:
    """Upload a single file, retrying transient backend errors so a blip does not
    force a re-run of the expensive OTB + CDSE-download step."""
    fs.put_file(str(local_path), remote_path)


def upload_outputs(
    fs: Any,
    data_dir: str | Path,
    tile_id: str,
    orbit_direction: str,
    date_start: str,
    bucket: str,
) -> int:
    """Upload all GeoTIFFs flat into the canonical prefix and verify they landed.

    ``fs`` is an fsspec filesystem (s3fs in production, a local filesystem in
    tests). Returns a process exit code.
    """
    files = collect_local_tifs(data_dir, tile_id)
    if not files:
        # No GeoTIFFs means S1Processor found no S1 coverage for this tile/orbit/day —
        # a legitimate empty-coverage outcome, not an error. Nothing to upload; exit 0 so
        # the empty prefix flows to ingest, which no-ops it (exit 2 -> skip register).
        # Returning non-zero here would re-fail the workflow on a no-coverage tile.
        log.info(
            "No GeoTIFFs under %s (looked in data_out/%s and data_gamma_area) — no S1 "
            "coverage for this tile/orbit/day; nothing to upload",
            data_dir,
            tile_id,
        )
        return 0

    # The destination is flat (basename only), so two source files sharing a
    # basename would silently overwrite each other AND dedup in the verify map --
    # exactly the silent-loss class this script exists to prevent. Refuse upfront.
    names = [f.name for f in files]
    dupes = sorted({n for n in names if names.count(n) > 1})
    if dupes:
        log.error("Refusing to upload: duplicate GeoTIFF basenames would collide: %s", dupes)
        return 1

    # Strip the s3:// scheme: s3fs (and local fsspec) operate on "bucket/key" paths.
    dest = s1_output_prefix(bucket, tile_id, orbit_direction, date_start)
    dest = dest[len("s3://") :].rstrip("/")
    fs.makedirs(dest, exist_ok=True)  # no-op on s3fs; creates parents on a local fs

    try:
        for f in files:
            rpath = f"{dest}/{f.name}"
            _put_one(fs, f, rpath)
            log.info("uploaded %s -> s3://%s", f.name, rpath)
    except Exception:
        log.exception("Upload failed after retries; aborting (no partial success reported)")
        return 1

    # Verify: every local file is present at dest with a matching size.
    expected = {f.name: f.stat().st_size for f in files}
    listed = fs.ls(dest, detail=True)
    got = {os.path.basename(e["name"].rstrip("/")): e["size"] for e in listed}
    missing = sorted(n for n in expected if n not in got)
    mismatch = sorted(n for n in expected if n in got and got[n] != expected[n])
    if missing or mismatch:
        log.error(
            "Upload verification FAILED: %d/%d present; missing=%s size_mismatch=%s",
            len(expected) - len(missing),
            len(expected),
            missing,
            mismatch,
        )
        return 1

    log.info("Verified %d GeoTIFFs at s3://%s", len(expected), dest)
    return 0


def _make_s3fs(endpoint: str | None) -> Any:
    import s3fs

    return s3fs.S3FileSystem(client_kwargs={"endpoint_url": endpoint} if endpoint else None)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", required=True, help="S1Tiling work dir (e.g. /data)")
    parser.add_argument("--tile-id", required=True, help="MGRS tile (e.g. 31TCH)")
    parser.add_argument("--orbit-direction", required=True, choices=["ascending", "descending"])
    parser.add_argument("--date-start", required=True, help="window start, YYYY-MM-DD")
    parser.add_argument("--s3-output-bucket", required=True)
    parser.add_argument(
        "--s3-endpoint",
        default=None,
        help="S3 endpoint; falls back to AWS_ENDPOINT_URL",
    )
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    args = _build_parser().parse_args()
    endpoint = args.s3_endpoint or os.environ.get("AWS_ENDPOINT_URL")
    fs = _make_s3fs(endpoint)
    rc = upload_outputs(
        fs,
        args.data_dir,
        args.tile_id,
        args.orbit_direction,
        args.date_start,
        args.s3_output_bucket,
    )
    sys.exit(rc)


if __name__ == "__main__":
    main()
