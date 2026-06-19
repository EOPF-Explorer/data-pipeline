"""Ingest a batch of S1Tiling GeoTIFF acquisitions into a GeoZarr V3 RTC store.

Exit codes:
    0 -- success (all acquisitions ingested)
    1 -- error during ingest (first failure aborts)
    2 -- nothing to register: empty prefix, OR a fresh tile whose only new scenes are all-nodata
         (no store built) — the workflow skips register on 2 (so an uncovered edge tile is a clean skip)
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

# Max concurrent S3 transfers (PUT/GET). s3fs is async-backed, so fsspec runs the batched
# list form (fs.put/fs.get) concurrently up to this many in flight. 32 is the cross-region
# GET-benchmark optimum (plan T0); overridable without a rebuild while the in-cluster PUT
# ceiling is tuned. The per-tile Argo mutex makes this process the only writer, and each
# transfer targets independent object keys, so there is no write-write hazard.
_S3_CONCURRENCY = int(os.environ.get("S1_INGEST_S3_CONCURRENCY", "32"))


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
        # A fresh tile (no existing cube) whose only new scenes are all-nodata builds no store, so there
        # is nothing to register: return 2 (same as the empty-prefix case) so the workflow skips register
        # instead of failing it ("No acquisitions found" — the 30TWQ edge-tile failure). If the cube
        # already has slices, return 0 so register re-registers the existing cube (idempotent).
        if present:
            log.info(
                "No new acquisition(s) with valid data; cube already has %d slice(s)", len(present)
            )
            return 0
        log.info("No new acquisition(s) with valid data and no existing cube; nothing to register")
        return 2

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


def _put_files(fs: Any, pairs: list[tuple[str, str]]) -> None:
    """Upload ``(lpath, rpath)`` pairs concurrently, creating each remote parent first.

    Uses fsspec's explicit list form ``fs.put([lpaths], [rpaths], batch_size=N)``: it maps
    each pair one-to-one — preserving the no-nesting guarantee the per-file mapping gives —
    and runs the transfers concurrently on async backends (s3fs) up to ``_S3_CONCURRENCY``.
    The list form does NOT create parent dirs, so we ``makedirs`` each parent first (a no-op
    on s3fs where the bucket exists; a real mkdir on a local fs).
    """
    if not pairs:
        return
    lpaths = [lp for lp, _ in pairs]
    rpaths = [rp for _, rp in pairs]
    for parent in {rp.rsplit("/", 1)[0] for rp in rpaths}:
        fs.makedirs(parent, exist_ok=True)
    fs.put(lpaths, rpaths, batch_size=_S3_CONCURRENCY)


def _get_keys(fs: Any, src: str, keys: list[str], local_store: str) -> None:
    """Download the given remote ``keys`` (all under ``src``) to ``local_store/<relpath>`` in one
    batched ``fs.get`` (concurrent up to ``_S3_CONCURRENCY``), pre-creating the local parent dirs the
    batched list form does not. The download mirror of the ``_put_files`` upload primitive.
    """
    if not keys:
        return
    lpaths = [
        os.path.join(local_store, key[len(src) :].lstrip("/").replace("/", os.sep)) for key in keys
    ]
    for parent in {os.path.dirname(p) for p in lpaths}:
        os.makedirs(parent, exist_ok=True)
    fs.get(keys, lpaths, batch_size=_S3_CONCURRENCY)


def _coordinate_array_dirs(local_store: str) -> set[str]:
    """Store-relative dirs (posix) of every Zarr array with ``ndim <= 1`` -- the coordinate/aux
    arrays (``time``/``x``/``y``/``absolute_orbit``/``relative_orbit``/``platform``; ``spatial_ref``
    is 0-D). Read from the local ``zarr.json`` metadata, so it costs no network round-trips.

    The bulk ``ndim >= 2`` data arrays (``vv``/``vh``/``border_mask``/``gamma_area_*``/``lia_*``) are
    excluded. Used to (a) fetch only coordinate chunks on append and (b) scope the upload deletion so
    a deliberately-skipped bulk chunk is never removed. Classifying by **dimensionality** (not an
    array-name denylist) is self-maintaining and robust to sharding -- the on-disk key layout differs
    between chunked and sharded arrays, but the logical ``shape`` does not.
    """
    base = Path(local_store)
    dirs: set[str] = set()
    for zj in base.rglob("zarr.json"):
        meta = json.loads(zj.read_text())
        if meta.get("node_type") == "array" and len(meta.get("shape", [])) <= 1:
            dirs.add(zj.parent.relative_to(base).as_posix())
    return dirs


def _is_coordinate_key(rel: str, coord_dirs: set[str]) -> bool:
    """True if a store-relative object path lives under a coordinate/aux (``ndim <= 1``) array dir."""
    return any(rel == d or rel.startswith(f"{d}/") for d in coord_dirs)


def _fetch_for_append(fs: Any, src: str, local_store: str) -> None:
    """Fetch only what an append reads/writes: every ``zarr.json`` + the small coordinate arrays.

    The append reads zarr metadata + ``r10m["vv"].shape`` and the 1-D coordinate arrays, then writes
    a NEW time slice; it never reads the existing bulk ``vv``/``vh``/``border_mask``/``gamma_area_*``/
    ``lia_*`` chunks (the overwhelming majority of the objects). Skipping them turns the per-append
    fetch from O(cube) into O(metadata + coords) -- the fix for the 42-179 min outliers on large
    cubes (the fetch is the only append phase that grows with accumulated cube size; #287 already
    made the upload incremental).

    SAFE because the bulk arrays shard with **time-extent 1** (data-model ``s1_ingest.py`` builds them
    ``shards=(1, level_h, level_w)``): a new time index writes entirely new shard objects, never
    read-modify-writing a skipped one; the condition arrays are single-shard and overwritten whole. If
    that upstream sharding ever spans multiple time indices, this minimal fetch must be reverted (it
    would corrupt via RMW of an absent shard). Two batched ``fs.get`` calls -- no per-object
    round-trips; the single ``fs.find`` listing stays O(object count) but is a cheap LIST.
    """
    keys = fs.find(src)
    if not keys:
        return
    meta_keys = [k for k in keys if k.rsplit("/", 1)[-1] == "zarr.json"]
    _get_keys(fs, src, meta_keys, local_store)  # phase 1: all metadata
    coord_dirs = _coordinate_array_dirs(
        local_store
    )  # classify from the local metadata (no network)
    meta_set = set(meta_keys)
    coord_chunks = [
        k
        for k in keys
        if k not in meta_set and _is_coordinate_key(k[len(src) :].lstrip("/"), coord_dirs)
    ]
    _get_keys(fs, src, coord_chunks, local_store)  # phase 2: coordinate chunks only


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
    log.info(
        "Fetching existing cube %s -> %s (append mode: metadata + coords only)", s3_uri, local_store
    )
    _fetch_for_append(fs, src, local_store)


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


def _level_time_axis_len(level: Any) -> int | None:
    """Length of a multiscale level's time axis, read from its first ``time``-dimensioned data array
    (``vv``/``vh``/``border_mask``); ``None`` if the group has no such array (e.g. the ``conditions``
    group, whose arrays are ``(y, x)`` only)."""
    for _name, arr in level.arrays():
        dims = arr.metadata.dimension_names or ()
        if dims and dims[0] == "time":
            return int(arr.shape[0])
    return None


def _ensure_level_time_coords(local_store: str, orbit_direction: str) -> None:
    """Backfill a missing per-level ``time`` coordinate before append (self-heal -> convergence).

    eopf_geozarr's append resizes ``level["time"]`` on **every** multiscale level, assuming a fresh
    build created ``time`` at each level (data-model #192). A cube built before #192 -- or left
    half-built by an interrupted append -- can carry ``r10m/time`` yet lack it at ``r20m``/``r60m``;
    the resize then raises ``KeyError: 'time'``. Because the dedup only reads ``r10m/time``
    (``store_times_ns``), the crash recurs on every re-run (non-convergent). For each multiscale level
    missing ``time``, recreate it from ``r10m/time`` -- copying its values, dtype, ``dimension_names``
    and attrs (already the CF time attrs) -- so the backfilled coordinate is byte-identical to a fresh
    build without hardcoding the attr dict. No-op when every level already has ``time``.

    Refuses to mask a deeper corruption rather than write a wrong-length coordinate: raises if
    ``r10m`` holds slices but no ``time`` (no backfill source) or a level's time-axis length disagrees
    with ``r10m/time`` (a half-built cube a wipe must repair, plan T4).
    """
    if not Path(local_store).exists():
        return
    root = zarr.open_group(local_store, mode="r+", zarr_format=3)
    if orbit_direction not in root:
        return
    orbit: Any = root[orbit_direction]
    levels = dict(orbit.groups())
    r10m = levels.get("r10m")
    if r10m is None:
        return
    if "time" not in list(r10m.array_keys()):
        r10m_len = _level_time_axis_len(r10m)
        if r10m_len:
            raise ValueError(
                f"Cannot self-heal {orbit_direction}: r10m has {r10m_len} slice(s) but no `time` "
                "coordinate (no backfill source -- wipe + reingest)"
            )
        return  # nothing ingested yet -> the writer creates r10m/time on the first append
    src = r10m["time"]
    values = np.asarray(src[...])
    healed: list[str] = []
    for name, level in levels.items():
        if name == "r10m" or "time" in list(level.array_keys()):
            continue
        data_len = _level_time_axis_len(level)
        if data_len is None:
            continue  # not a multiscale level (e.g. the `conditions` group) -- no `time` belongs here
        if data_len != int(values.shape[0]):
            raise ValueError(
                f"Cannot self-heal {orbit_direction}/{name}/time: data length {data_len} != "
                f"r10m/time length {int(values.shape[0])} (half-built cube -- wipe + reingest)"
            )
        arr = level.create_array(
            "time",
            shape=values.shape,
            dtype=src.dtype,
            chunks=src.chunks,
            fill_value=0,
            dimension_names=list(src.metadata.dimension_names or ["time"]),
        )
        arr[...] = values
        arr.attrs.update(dict(src.attrs))
        healed.append(f"{orbit_direction}/{name}")
    if healed:
        log.info("Backfilled missing per-level `time` from r10m/time: %s", healed)


def _sync_tree(fs: Any, local_store: str, dest: str) -> None:
    """Upload only new/changed objects under ``local_store`` to ``dest``; delete vanished keys.

    Zarr append is additive: a new acquisition writes **new** shard keys at the new time index
    and rewrites only the tiny metadata in place. Exploit that instead of an ``rm`` + full
    re-upload of the whole accumulated cube every append (which re-PUT ~3600 static objects):

    - ``zarr.json`` metadata: always re-upload — it is the one thing rewritten *in place*
      (shape/attr edits) and is tiny.
    - chunk/shard objects: upload only if **absent** from the S3 listing (new) or its local
      **size differs** (changed). A content-changed blosc-compressed shard almost always
      changes compressed size. NOT ETag/MD5 — s3fs uploads shards multipart, whose ETag is
      ``<md5>-<nparts>``, not the object MD5, so an MD5 compare is both wrong and pointless.
    - deletions: drop only vanished **coordinate/metadata** keys. The append fetch
      (``_fetch_for_append``) deliberately skips the bulk >=2-D data chunks, so they are absent
      locally but MUST NOT be deleted from S3 -- a whole-cube ``set(remote) - local`` would wipe
      them. Scope the deletion to the coordinate/aux arrays + ``zarr.json`` (``_coordinate_array_dirs``);
      after a correct append every such key is present locally, so this is a no-op in the normal case.

    Both the presence and size checks come from the single ``fs.find`` listing — no extra
    round-trips. Keeps per-append upload flat as the cube grows.
    """
    remote_size = (
        {k: v.get("size") for k, v in fs.find(dest, detail=True).items()} if fs.exists(dest) else {}
    )
    pairs: list[tuple[str, str]] = []
    local_keys: set[str] = set()
    for root, _dirs, files in os.walk(local_store):
        for name in files:
            lpath = os.path.join(root, name)
            rel = os.path.relpath(lpath, local_store).replace(os.sep, "/")
            rpath = f"{dest}/{rel}"
            local_keys.add(rpath)
            if (
                name == "zarr.json"
                or rpath not in remote_size
                or os.path.getsize(lpath) != remote_size[rpath]
            ):
                pairs.append((lpath, rpath))
    _put_files(fs, pairs)
    # Scope deletion to coordinate/metadata keys (C1): the append fetch skips the bulk >=2-D chunks,
    # so they are absent locally but must not be removed from S3.
    coord_dirs = _coordinate_array_dirs(local_store)
    stale = sorted(
        k
        for k in set(remote_size) - local_keys
        if k.rsplit("/", 1)[-1] == "zarr.json"
        or _is_coordinate_key(k[len(dest) :].lstrip("/"), coord_dirs)
    )
    if stale:
        fs.rm(stale)
    # A mid-append failure can leave the cube between states; the per-tile Argo mutex serialises
    # writers and a re-run re-appends idempotently, so the next success converges. Surface counts.
    log.info(
        "Incremental upload to %s: %d/%d object(s) sent, %d deleted",
        dest,
        len(pairs),
        len(local_keys),
        len(stale),
    )


def _upload_store_to_s3(local_store: str, s3_uri: str) -> None:
    """Upload a local Zarr store directory to an ``s3://`` URI via s3fs (incrementally).

    The endpoint is taken from ``AWS_ENDPOINT_URL`` (OVH S3 is not AWS-default);
    credentials come from the ambient ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY``.
    For an appended cube the local store is the full superset (existing slices fetched by
    ``_fetch_store_from_s3`` + the new one); ``_sync_tree`` uploads only the objects that are
    new or changed vs the current S3 listing, so append cost stays flat as the cube grows.
    """
    import s3fs

    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    fs = s3fs.S3FileSystem(client_kwargs={"endpoint_url": endpoint} if endpoint else None)
    dest = s3_uri[len("s3://") :].rstrip("/")
    _sync_tree(fs, local_store, dest)


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
        _ensure_level_time_coords(local_store, orbit_direction)  # heal a level missing `time` (T2)
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
