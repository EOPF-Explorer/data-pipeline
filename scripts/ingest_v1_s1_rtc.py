"""Ingest a batch of S1Tiling GeoTIFF acquisitions into a GeoZarr V3 RTC store.

Exit codes:
    0 -- success (all acquisitions ingested)
    1 -- error during ingest (first failure aborts)
    2 -- no acquisitions found (empty prefix)
"""

from __future__ import annotations

import argparse
import logging
import sys

import zarr
from eopf_geozarr.conversion.s1_ingest import (
    consolidate_s1_store,
    discover_s1tiling_acquisitions,
    discover_s1tiling_conditions,
    ingest_s1tiling_acquisition,
    ingest_s1tiling_conditions,
)
from pyproj import CRS

log = logging.getLogger(__name__)


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


def ingest_all(s3_geotiff_prefix: str, store_path: str, orbit_direction: str) -> int:
    """Run the 5-step S1 ingest pipeline.

    Returns exit code: 0 = success, 1 = ingest error, 2 = no acquisitions.
    """
    # Step 1 -- discover acquisitions
    acquisitions = discover_s1tiling_acquisitions(s3_geotiff_prefix)
    if not acquisitions:
        log.warning("No acquisitions found in %s", s3_geotiff_prefix)
        return 2

    # Step 2 -- ingest each acquisition (abort on first error)
    for acq in acquisitions:
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
    sys.exit(ingest_all(args.s3_geotiff_prefix, args.s3_zarr_store, args.orbit_direction))


if __name__ == "__main__":
    main()
