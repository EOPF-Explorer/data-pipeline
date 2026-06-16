"""Ingest a batch of S1Tiling GeoTIFF acquisitions into a GeoZarr V3 RTC store.

Exit codes:
    0 -- success (all acquisitions ingested)
    1 -- error during ingest (first failure aborts)
    2 -- no acquisitions found (empty prefix)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

import numpy as np
import rasterio
import zarr
from eopf_geozarr.conversion.s1_ingest import (
    _rasterio_env,  # reused: identical S3 GDAL-env handling as the acquisition reads
    consolidate_s1_store,
    discover_s1tiling_acquisitions,
    discover_s1tiling_conditions,
    ingest_s1tiling_acquisition,
    ingest_s1tiling_conditions,
)
from pyproj import CRS

log = logging.getLogger(__name__)

# Subsample factor for the has-data probe (see _band_has_data). 8x keeps the probe read cheap
# (~1/64 of a band) while reliably catching any data blob down to ~0.01% of the tile.
_DATA_PROBE_DECIMATION = 8


def _patch_cf_grid_mapping(store_path: str, orbit_direction: str) -> list[str]:
    """Inject a CF ``spatial_ref`` coordinate + ``grid_mapping`` attrs into every
    sub-group of the orbit group that holds 2D (y, x) data arrays.

    eopf_geozarr.conversion.s1_ingest writes only the GeoZarr ``proj:code`` attr,
    which rioxarray does not read. TiTiler (titiler-eopf v0.5.0) validates each
    multiscale group with ``rioxarray``: if ``ds.rio.crs`` is ``None`` the group
    is rejected, leaving the reader with zero usable groups and producing
    HTTP 500s ("not enough values to unpack" / "'tile_matrix_set'"). Adding a CF
    ``spatial_ref`` coordinate (as S2 stores have) lets rioxarray resolve the CRS.

    Returns the list of group paths patched.
    """
    root = zarr.open_group(store_path, mode="r+", zarr_format=3)
    orbit_group = root[orbit_direction]
    proj_code = dict(orbit_group.attrs).get("proj:code")
    wkt = CRS.from_user_input(proj_code).to_wkt()
    cf_attrs = {
        "crs_wkt": wkt,
        "spatial_ref": wkt,
        "grid_mapping_name": "transverse_mercator",
    }

    patched: list[str] = []

    def walk(group: zarr.Group, path: str) -> None:
        data_arrays = [
            (name, arr)
            for name, arr in group.arrays()
            if {"y", "x"}.issubset(arr.metadata.dimension_names or ())
        ]
        if data_arrays:
            if "spatial_ref" not in list(group.array_keys()):
                sref = group.create_array("spatial_ref", shape=(), dtype="int64", fill_value=0)
                sref[...] = 0
            else:
                sref = group["spatial_ref"]
            sref.attrs.update(cf_attrs)
            for _name, arr in data_arrays:
                arr.attrs.update({**dict(arr.attrs), "grid_mapping": "spatial_ref"})
            patched.append(path)
        for gname, sub in group.groups():
            walk(sub, f"{path}/{gname}")

    walk(orbit_group, orbit_direction)
    log.info("Patched CF grid_mapping into %d group(s): %s", len(patched), patched)
    return patched


def acq_time_ns(acq_stamp: str) -> int:
    """ns-since-epoch for an S1Tiling ``acq_stamp`` (``YYYYMMDDtHHMMSS``).

    Matches how the cube stores ``time``: ``eopf_geozarr`` writes
    ``np.datetime64(<ACQUISITION_DATETIME tag>)`` — the same instant the stamp encodes — so this is
    the idempotency key for "is this acquisition already a slice in the cube?".
    """
    s = acq_stamp
    iso = f"{s[0:4]}-{s[4:6]}-{s[6:8]}T{s[9:11]}:{s[11:13]}:{s[13:15]}"
    return int(np.datetime64(iso).astype("datetime64[ns]").astype("int64"))


def store_times_ns(store_path: str, orbit_direction: str) -> set[int]:
    """`time` values (ns) already present in the cube's r10m level; empty if the store, the orbit
    group, or its r10m/time are absent (a fresh tile has none)."""
    if not Path(store_path).exists():
        return set()
    root = zarr.open_group(str(store_path), mode="r", zarr_format=3)
    if orbit_direction not in root:
        return set()
    orbit: Any = root[orbit_direction]
    if "r10m" not in orbit or "time" not in orbit["r10m"]:
        return set()
    times = np.asarray(orbit["r10m"]["time"]).astype("datetime64[ns]").astype("int64")
    return {int(t) for t in times}


def new_acquisitions(acquisitions: list[dict], present_ns: set[int]) -> list[dict]:
    """Acquisitions whose ``time`` is not already a slice in the cube (idempotent re-ingest)."""
    return [a for a in acquisitions if acq_time_ns(a["acq_stamp"]) not in present_ns]


def _band_has_data(path: str | Path) -> bool:
    """True if a GeoTIFF band holds any valid (finite, non-zero) pixel.

    RTC gamma-naught backscatter is positive power; its nodata reads as 0 / NaN, so a slice with no
    real data over the tile is entirely zero/NaN. Probed on a decimated read (factor
    ``_DATA_PROBE_DECIMATION``) — a full re-read here would double the (large) per-band I/O that
    ``ingest_s1tiling_acquisition`` already does, and decimation still catches any blob down to
    ~0.01% of the tile (the floor we care about). ``_rasterio_env`` supplies the S3 GDAL env.
    """
    with _rasterio_env(path), rasterio.open(str(path)) as src:
        h = max(1, src.height // _DATA_PROBE_DECIMATION)
        w = max(1, src.width // _DATA_PROBE_DECIMATION)
        data = src.read(1, out_shape=(h, w))
    return bool(np.any(np.isfinite(data) & (data != 0)))


def _acquisition_has_data(acq: dict) -> bool:
    """True if either polarisation of an acquisition carries real data (vv probed first, then vh)."""
    return _band_has_data(acq["vv"]) or _band_has_data(acq["vh"])


def ingest_all(s3_geotiff_prefix: str, store_path: str, orbit_direction: str) -> int:
    """Run the 5-step S1 ingest pipeline, appending new acquisitions to the per-tile cube.

    Each new acquisition is appended as a ``time`` slice (``ingest_s1tiling_acquisition`` opens the
    store ``mode=r+``); acquisitions whose ``time`` is already in the cube are skipped, so a re-run
    is a no-op (T4 idempotency).

    Returns exit code: 0 = success (or nothing new to ingest), 1 = ingest error, 2 = no acquisitions.
    """
    # Step 1 -- discover acquisitions (multi-frame masked stamps are resolved upstream in
    # eopf_geozarr's discover from the ACQUISITION_DATETIME tag; data-model #184)
    acquisitions = discover_s1tiling_acquisitions(s3_geotiff_prefix)
    if not acquisitions:
        log.warning("No acquisitions found in %s", s3_geotiff_prefix)
        return 2

    # Step 1b -- drop acquisitions already present in the cube (idempotent append)
    present = store_times_ns(store_path, orbit_direction)
    acquisitions_to_ingest = new_acquisitions(acquisitions, present)
    skipped = len(acquisitions) - len(acquisitions_to_ingest)
    if skipped:
        log.info("Skipping %d acquisition(s) already present in the cube", skipped)
    if not acquisitions_to_ingest:
        log.info(
            "All %d discovered acquisition(s) already in the cube; nothing to ingest",
            len(acquisitions),
        )
        return 0

    # Step 1c -- skip new acquisitions whose produced GeoTIFFs hold no valid data (all-nodata).
    # Footprint coverage at the trigger is an unreliable proxy (swath nodata edges over-state it,
    # multi-frame mosaic under-states it); the produced pixels are the truth. This keeps any slice
    # with real data — even a tiny sliver — and drops only 0%-data slices, so they never become an
    # empty cube time step (and a dark preview). A tile whose only new scenes are empty creates no store.
    with_data = [acq for acq in acquisitions_to_ingest if _acquisition_has_data(acq)]
    empty = len(acquisitions_to_ingest) - len(with_data)
    if empty:
        log.info("Skipping %d new acquisition(s) with no valid data (all-nodata)", empty)
    acquisitions_to_ingest = with_data
    if not acquisitions_to_ingest:
        log.info("No new acquisition(s) with valid data; nothing to ingest")
        return 0

    # Step 2 -- ingest each new acquisition (abort on first error)
    for acq in acquisitions_to_ingest:
        try:
            ingest_s1tiling_acquisition(
                vv_path=acq["vv"],
                vh_path=acq["vh"],
                border_mask_path=acq["vv_mask"],
                store_path=store_path,
                orbit_direction=orbit_direction,
            )
        except Exception:
            log.exception(
                "Ingest failed for tile=%s orbit=%s stamp=%s",
                acq.get("tile"),
                acq.get("orbit_dir"),
                acq.get("acq_stamp"),
            )
            return 1

    # Step 3 -- discover conditions (non-fatal if absent)
    try:
        conditions = discover_s1tiling_conditions(s3_geotiff_prefix)
    except Exception:
        log.warning("Could not discover conditions in %s; skipping", s3_geotiff_prefix)
        conditions = []

    # Step 4 -- ingest each condition group (non-fatal per-group)
    for cond in conditions:
        try:
            ingest_s1tiling_conditions(
                store_path=store_path,
                orbit_direction=orbit_direction,
                relative_orbit=int(cond["orbit"]),
                gamma_area_path=cond.get("gamma_area"),
                lia_path=cond.get("lia"),
            )
        except Exception:
            log.warning(
                "Conditions ingest failed for tile=%s orbit=%s; continuing",
                cond.get("tile"),
                cond.get("orbit"),
            )

    # Step 5 -- consolidate
    consolidate_s1_store(store_path, orbit_direction)

    # Step 6 -- patch the CF spatial_ref coordinate omitted by eopf_geozarr.s1_ingest
    # so rioxarray can resolve the CRS (titiler-eopf v0.5.0 rejects groups where
    # ds.rio.crs is None). eopf_geozarr writes only the GeoZarr proj:code attr.
    _patch_cf_grid_mapping(store_path, orbit_direction)
    consolidate_s1_store(store_path, orbit_direction)

    return 0


def _put_tree(fs: Any, local_store: str, dest: str) -> None:
    """Upload every file under ``local_store`` to ``dest/<relpath>``.

    Maps each file explicitly rather than calling ``fs.put(..., recursive=True)``,
    whose directory-nesting behaviour depends on trailing slashes and the fsspec
    version (it can land the tree at ``dest/<basename>/...`` instead of ``dest/...``).
    Mapping per file makes the store land at exactly ``dest`` on any version.
    """
    made: set[str] = set()
    for root, _dirs, files in os.walk(local_store):
        for name in files:
            lpath = os.path.join(root, name)
            rel = os.path.relpath(lpath, local_store).replace(os.sep, "/")
            rpath = f"{dest}/{rel}"
            parent = rpath.rsplit("/", 1)[0]
            if parent not in made:
                # No-op on s3fs (bucket exists); creates parents on a local fs.
                fs.makedirs(parent, exist_ok=True)
                made.add(parent)
            fs.put_file(lpath, rpath)


def _get_tree(fs: Any, src: str, local_store: str) -> None:
    """Download every object under ``src`` to ``local_store/<relpath>`` (mirror of ``_put_tree``)."""
    for key in fs.find(src):
        rel = key[len(src) :].lstrip("/")
        lpath = os.path.join(local_store, rel.replace("/", os.sep))
        os.makedirs(os.path.dirname(lpath), exist_ok=True)
        fs.get_file(key, lpath)


def _fetch_store_from_s3(s3_uri: str, local_store: str) -> None:
    """Download an existing cube from S3 into ``local_store`` so new scenes **append** to it.

    No-op if the destination cube doesn't exist yet (the first acquisition for a tile). This is what
    turns the pipeline from "fresh store per run" into a per-tile datacube that accumulates over runs
    (T4); concurrent same-tile writes are serialised by the Argo per-tile mutex.
    """
    import s3fs

    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    fs = s3fs.S3FileSystem(client_kwargs={"endpoint_url": endpoint} if endpoint else None)
    src = s3_uri[len("s3://") :].rstrip("/")
    if not fs.exists(src):
        return
    log.info("Fetching existing cube %s -> %s (append mode)", s3_uri, local_store)
    _get_tree(fs, src, local_store)


def _drop_consolidated_metadata(local_store: str) -> None:
    """Strip zarr-v3 consolidated metadata from a fetched cube so the append can grow `time`.

    Reopening a *consolidated* store ``mode="r+"`` serves the consolidated (stale, length-1) array
    shapes, so ``eopf_geozarr``'s resize-then-write raises ``BoundsCheckError`` ("index out of bounds
    for dimension with length 1"). ``eopf_geozarr`` consolidates at the **orbit-group** level (not the
    root), so we strip ``consolidated_metadata`` from *every* group node, not just the root; zarr then
    reads per-array metadata and honours ``resize``. ``ingest_all`` re-consolidates at the end.
    """
    dropped = 0
    for zj in Path(local_store).rglob("zarr.json"):
        meta = json.loads(zj.read_text())
        if meta.get("node_type") == "group" and meta.pop("consolidated_metadata", None) is not None:
            zj.write_text(json.dumps(meta))
            dropped += 1
    if dropped:
        log.info(
            "Dropped consolidated metadata from %d group(s) in %s (so append can resize)",
            dropped,
            local_store,
        )


def _upload_store_to_s3(local_store: str, s3_uri: str) -> None:
    """Upload a local Zarr store directory to an ``s3://`` URI via s3fs.

    The endpoint is taken from ``AWS_ENDPOINT_URL`` (OVH S3 is not AWS-default);
    credentials come from the ambient ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY``.
    The destination is removed first, then re-uploaded; for an appended cube the local store is the
    full superset (existing slices fetched by ``_fetch_store_from_s3`` + the new one), so this writes
    the accumulated cube, not a fresh single-acquisition store.
    """
    import s3fs

    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    fs = s3fs.S3FileSystem(client_kwargs={"endpoint_url": endpoint} if endpoint else None)
    dest = s3_uri[len("s3://") :].rstrip("/")
    if fs.exists(dest):
        fs.rm(dest, recursive=True)
    _put_tree(fs, local_store, dest)


def run_ingest(s3_geotiff_prefix: str, store: str, orbit_direction: str) -> int:
    """Ingest into ``store``, handling ``s3://`` destinations.

    eopf_geozarr writes the store via ``pathlib.Path``, which collapses ``s3://``
    to a local path. So for an ``s3://`` destination we fetch any existing cube into a local temp
    store, append the new acquisition(s), and upload the result with s3fs. Local destinations pass
    straight through.
    """
    if not store.startswith("s3://"):
        return ingest_all(s3_geotiff_prefix, store, orbit_direction)

    tmp_dir = tempfile.mkdtemp(prefix="s1-ingest-")
    local_store = os.path.join(tmp_dir, os.path.basename(store.rstrip("/")))
    try:
        _fetch_store_from_s3(store, local_store)  # append to the existing per-tile cube (T4)
        _drop_consolidated_metadata(local_store)  # so eopf_geozarr can resize `time` on append
        rc = ingest_all(s3_geotiff_prefix, local_store, orbit_direction)
        if rc != 0:
            return rc
        log.info("Uploading store %s -> %s", local_store, store)
        _upload_store_to_s3(local_store, store)
        return 0
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--s3-geotiff-prefix",
        required=True,
        help="Local path or S3 prefix containing S1Tiling GeoTIFF files",
    )
    parser.add_argument(
        "--s3-zarr-store",
        required=True,
        help="Path to the output GeoZarr V3 store (created if absent)",
    )
    parser.add_argument(
        "--tile-id",
        required=True,
        help="MGRS tile identifier (e.g. 31TCH)",
    )
    parser.add_argument(
        "--orbit-direction",
        required=True,
        choices=["ascending", "descending"],
        help="Orbit direction",
    )
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    args = _build_parser().parse_args()
    sys.exit(run_ingest(args.s3_geotiff_prefix, args.s3_zarr_store, args.orbit_direction))


if __name__ == "__main__":
    main()
