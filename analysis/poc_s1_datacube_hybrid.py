"""PoC: validate the Phase-6 hybrid data model against the staging deployment.

Proves two implementation details before committing to the full plan (#226):

  V1 — datacube append: ingest two real S1 GRD RTC acquisitions (separate geotiff
       prefixes) into ONE per-tile Zarr by calling the *existing*
       ``ingest_s1tiling_acquisition`` twice → a multi-time datacube, written to the
       **existing** ``sentinel-1-grd-rtc-staging`` collection's store. (Plan T4;
       eopf-geozarr already supports the append, mode="r+".)

  V2 — per-acquisition catalogue: emit one STAC item per ``time`` slice into a **new**
       ``sentinel-1-grd-rtc-acquisitions`` collection, each pointing at the shared
       cube with ``sel=time=nearest::{datetime}`` viz links, and confirm each item
       renders ITS slice via the live titiler-eopf (HTTP 200). (Plan T5.)

Two collections, mirroring the decided model:
  * sentinel-1-grd-rtc-staging      — the per-tile **datacube** (already exists; cube grows here)
  * sentinel-1-grd-rtc-acquisitions — **new**; per-acquisition slice items (time series)

Usage (after both scenes' geotiffs exist on S3):
    AWS_PROFILE=eopfexplorer AWS_ENDPOINT_URL=https://s3.de.io.cloud.ovh.net \
    uv run python analysis/poc_s1_datacube_hybrid.py \
      --scene-prefix s3://esa-zarr-sentinel-explorer-tests/s1tiling-output/31TCH/descending/2026-06-04/ \
      --scene-prefix s3://esa-zarr-sentinel-explorer-tests/s1tiling-output/31TCH/descending/2026-06-06/ \
      --tile-id 31TCH --orbit-direction descending
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import tempfile
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

# The cube lives in the existing per-tile datacube collection; slices go to a new one.
CUBE_COLLECTION = "sentinel-1-grd-rtc-staging"
ACQ_COLLECTION = "sentinel-1-grd-rtc-acquisitions"
BUCKET = "esa-zarr-sentinel-explorer-tests"
HTTPS_GW = "https://s3.explorer.eopf.copernicus.eu"
STAC_API = "https://api.explorer.eopf.copernicus.eu/stac"
RASTER_API = "https://api.explorer.eopf.copernicus.eu/raster"


def _https_store(s3_uri: str) -> str:
    return s3_uri.replace(f"s3://{BUCKET}", f"{HTTPS_GW}/{BUCKET}")


def _slice_tilejson(item_id: str, when: dt.datetime, orbit: str) -> str:
    """titiler tilejson URL that renders one acquisition (sel=time) of the cube."""
    sel = urllib.parse.quote(f"time=nearest::{when.isoformat()}", safe="")
    var = urllib.parse.quote(f"/{orbit}:vh", safe="")
    return (
        f"{RASTER_API}/collections/{ACQ_COLLECTION}/items/{item_id}"
        f"/WebMercatorQuad/tilejson.json?variables={var}&bidx=1&rescale=0%2C219&assets=vh&sel={sel}"
    )


# --- V1: datacube append -----------------------------------------------------


def build_cube(scene_prefixes: list[str], local_store: str, orbit: str) -> list[int]:
    """Ingest each acquisition from each prefix into one store (append). Return time values (ns)."""
    import numpy as np
    import zarr
    from eopf_geozarr.conversion.s1_ingest import (
        consolidate_s1_store,
        discover_s1tiling_acquisitions,
        ingest_s1tiling_acquisition,
    )
    from ingest_v1_s1_rtc import _patch_cf_grid_mapping

    n = 0
    for prefix in scene_prefixes:
        acqs = discover_s1tiling_acquisitions(prefix)
        print(f"  {prefix} -> {len(acqs)} acquisition(s)")
        for acq in acqs:
            idx = ingest_s1tiling_acquisition(
                vv_path=acq["vv"],
                vh_path=acq["vh"],
                border_mask_path=acq["vv_mask"],
                store_path=local_store,
                orbit_direction=orbit,
            )
            print(f"    ingested {acq.get('acq_stamp')} -> time index {idx}")
            n += 1

    consolidate_s1_store(local_store, orbit)
    _patch_cf_grid_mapping(local_store, orbit)  # CF spatial_ref so titiler/rioxarray read the CRS
    consolidate_s1_store(local_store, orbit)

    root = zarr.open_consolidated(local_store, zarr_format=3)
    times = sorted(np.array(root[orbit]["r10m"]["time"]).tolist())
    print(f"  cube has {len(times)} time slice(s) from {n} acquisition(s)")
    return times


def upload_cube(local_store: str, s3_uri: str) -> None:
    from ingest_v1_s1_rtc import _upload_store_to_s3

    _upload_store_to_s3(local_store, s3_uri)
    print(f"  uploaded datacube -> {s3_uri}")


# --- V2: per-acquisition catalogue ------------------------------------------


def ensure_acq_collection(stac_url: str) -> None:
    """Create the new per-acquisition collection (cloning the staging collection def)."""
    from pystac_client import Client

    client = Client.open(stac_url)
    base = str(client.self_href).rstrip("/")
    src = client.get_collection(CUBE_COLLECTION).to_dict()
    src["id"] = ACQ_COLLECTION
    src["title"] = "Sentinel-1 GRD RTC — per-acquisition slices (datacube time series)"
    src["description"] = (
        "Per-acquisition STAC items for S1 GRD RTC. Each item is one time slice of the "
        f"per-tile datacube in '{CUBE_COLLECTION}', rendered via titiler sel=time."
    )
    src.pop("links", None)  # drop the source collection's self/items links; pgstac regenerates them
    client._stac_io.session.delete(f"{base}/collections/{ACQ_COLLECTION}", timeout=30)
    r = client._stac_io.session.post(f"{base}/collections", json=src, timeout=30)
    r.raise_for_status()
    print(f"  collection ready: {ACQ_COLLECTION} (HTTP {r.status_code})")


def register_per_acquisition(
    cube_store: str, times_ns: list[int], tile_id: str, orbit: str, stac_url: str
) -> list[str]:
    """Emit one item per time slice into ACQ_COLLECTION; assets→cube, viz links carry sel=time."""
    from eopf_geozarr.stac.s1_rtc import build_s1_rtc_stac_item
    from pystac_client import Client

    # open via the public HTTPS gateway (no s3 creds needed; same path titiler reads) — the builder
    # sets the item's asset hrefs to this store, so the per-acquisition items point at the cube.
    base_item = build_s1_rtc_stac_item(_https_store(cube_store), ACQ_COLLECTION)

    client = Client.open(stac_url)
    base = str(client.self_href).rstrip("/")
    item_ids = []
    for t_ns in times_ns:
        when = dt.datetime.fromtimestamp(t_ns / 1e9, tz=dt.UTC)
        item_id = f"s1-rtc-{tile_id}-{when.strftime('%Y%m%dt%H%M%S')}"
        d = base_item.to_dict()
        d["id"] = item_id
        d["collection"] = ACQ_COLLECTION
        d["properties"] = {
            **{
                k: v
                for k, v in d["properties"].items()
                if k not in ("start_datetime", "end_datetime")
            },
            "datetime": when.isoformat(),
        }
        d["links"] = [
            {
                "rel": "tilejson",
                "type": "application/json",
                "href": _slice_tilejson(item_id, when, orbit),
                "title": "tilejson (sel=time)",
            }
        ]
        client._stac_io.session.delete(
            f"{base}/collections/{ACQ_COLLECTION}/items/{item_id}", timeout=30
        )
        r = client._stac_io.session.post(
            f"{base}/collections/{ACQ_COLLECTION}/items", json=d, timeout=30
        )
        r.raise_for_status()
        print(f"  registered {item_id} (datetime {when.isoformat()}, HTTP {r.status_code})")
        item_ids.append(item_id)
    return item_ids


# --- verification ------------------------------------------------------------


def verify_rendering(item_ids: list[str], times_ns: list[int], orbit: str) -> bool:
    """Curl each item's sel=time tilejson → expect HTTP 200."""
    ok = True
    for item_id, t_ns in zip(item_ids, times_ns, strict=True):
        when = dt.datetime.fromtimestamp(t_ns / 1e9, tz=dt.UTC)
        url = _slice_tilejson(item_id, when, orbit)
        try:
            with urllib.request.urlopen(url, timeout=60) as resp:  # noqa: S310
                code: int | str = resp.status
        except Exception as e:  # noqa: BLE001
            code = f"ERR {e}"
        ok = ok and code == 200
        print(f"  [{'OK' if code == 200 else 'FAIL'}] {item_id} sel=time render -> {code}")
    return ok


def list_collection_items(collection: str, stac_url: str) -> int:
    """Print every item in a collection (validation view)."""
    from pystac_client import Client

    items = list(Client.open(stac_url).search(collections=[collection]).items())
    print(f"  '{collection}' has {len(items)} item(s):")
    for it in sorted(items, key=lambda x: x.id):
        when = it.properties.get("datetime") or it.properties.get("start_datetime")
        print(f"    - {it.id}  datetime={when}  assets=[{','.join(it.assets)}]")
    return len(items)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--scene-prefix", action="append", required=True, help="repeat per scene")
    ap.add_argument("--tile-id", required=True)
    ap.add_argument("--orbit-direction", required=True, choices=["descending", "ascending"])
    args = ap.parse_args()

    cube_store = f"s3://{BUCKET}/{CUBE_COLLECTION}/s1-grd-rtc-{args.tile_id}.zarr"

    print("V1 — build multi-time datacube (append) into the staging collection:")
    local = os.path.join(tempfile.gettempdir(), f"poc-s1-grd-rtc-{args.tile_id}.zarr")
    times = build_cube(args.scene_prefix, local, args.orbit_direction)
    if len(times) < 2:
        sys.exit(f"PoC needs >=2 time slices to prove the cube; got {len(times)}")
    upload_cube(local, cube_store)

    print("\nV2 — per-acquisition items in a new collection (sel=time):")
    ensure_acq_collection(STAC_API)
    ids = register_per_acquisition(cube_store, times, args.tile_id, args.orbit_direction, STAC_API)

    print("\nVerify rendering (titiler sel=time per slice):")
    render_ok = verify_rendering(ids, times, args.orbit_direction)

    print("\nVerify catalogue (STAC items):")
    n_acq = list_collection_items(ACQ_COLLECTION, STAC_API)

    passed = render_ok and n_acq == len(times) and len(times) >= 2
    print("\n" + ("✅ PoC PASSED" if passed else "❌ PoC FAILED"))
    print(f"   datacube: {len(times)} time slices @ {cube_store}")
    print(f"   '{ACQ_COLLECTION}': {n_acq} per-acquisition item(s) -> {ids}")
    print(f"   browse: {STAC_API}/collections/{ACQ_COLLECTION}")
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
