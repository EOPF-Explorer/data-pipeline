"""Register one STAC item per acquisition from a per-tile S1 GRD RTC datacube (plan T5).

The cube (`s1-rtc-{tile}.zarr`, collection `sentinel-1-grd-rtc-staging`) holds all acquisitions on a
`time` axis. This emits **one queryable STAC item per `time` slice** (`s1-rtc-{tile}-{datetime}`,
single `datetime`) into the env-split per-acquisition collection (`--collection`, default `…-tests`,
the cron passes `…-staging`). **No data duplication**: the item's render links point at the **cube's**
TiTiler endpoint (`--cube-collection`/items/`s1-rtc-{tile}`) with `sel=time={physical index}` — TiTiler
reconstructs the shared cube store and `isel`s this acquisition's slice. (The deployed titiler-eopf
supports `sel` by integer index, not the `nearest::{datetime}` syntax; switch to the datetime form when
it does — more reorder-proof.) The index is the slice's **physical** position in the cube's `time`
axis; it's re-derived every run, so it stays correct as the cube appends.

Usage:
    uv run python scripts/register_per_acquisition.py --store <cube-uri> --tile-id 31TCH \
      --orbit-direction descending --collection <acq-coll> --cube-collection <cube-coll> \
      --stac-api-url <url> --raster-api-url <url>
"""

from __future__ import annotations

import argparse
import copy
import datetime as dt
from typing import Any

from register_v1 import _render_to_query

# Per-acquisition collections are env-split like the cube collections (…-tests/-staging/-prod). The
# code default targets the test env (local/CP-A runs); the Argo cron passes --collection …-staging.
DEFAULT_ACQ_COLLECTION = "sentinel-1-grd-rtc-acquisitions-tests"


def acquisition_id(tile_id: str, when: dt.datetime) -> str:
    """Per-acquisition item id, e.g. ``s1-rtc-31TCH-20260607t055248``."""
    return f"s1-rtc-{tile_id}-{when.strftime('%Y%m%dt%H%M%S')}"


def _cube_item_base(raster_api: str, cube_collection: str, tile_id: str) -> str:
    """TiTiler endpoint of the shared **cube** item (``s1-rtc-{tile}`` in the cube collection).

    The per-acquisition item's render links point here — not at the acquisition item's own endpoint —
    because TiTiler reconstructs the store path from ``{collection}/{item_id}`` and only the cube path
    exists. ``sel=time={index}`` then selects this acquisition's slice.
    """
    return f"{raster_api}/collections/{cube_collection}/items/s1-rtc-{tile_id}"


def render_tilejson(
    raster_api: str, cube_collection: str, tile_id: str, render: dict, index: int
) -> str:
    """tilejson URL for the cube's slice ``index`` (composite render from ``renders.rgb`` + ``sel``)."""
    base = _cube_item_base(raster_api, cube_collection, tile_id)
    return f"{base}/WebMercatorQuad/tilejson.json?{_render_to_query(render, include_tilesize=True)}&sel=time={index}"


def render_xyz(
    raster_api: str, cube_collection: str, tile_id: str, render: dict, index: int
) -> str:
    """XYZ tile template (``{z}/{x}/{y}.png``) for the cube's slice ``index`` — the map preview."""
    base = _cube_item_base(raster_api, cube_collection, tile_id)
    q = _render_to_query(render, include_tilesize=True)
    return f"{base}/tiles/WebMercatorQuad/{{z}}/{{x}}/{{y}}.png?{q}&sel=time={index}"


def render_thumbnail(
    raster_api: str, cube_collection: str, tile_id: str, render: dict, index: int
) -> str:
    """``preview`` PNG for the cube's slice ``index`` — the static thumbnail."""
    base = _cube_item_base(raster_api, cube_collection, tile_id)
    return f"{base}/preview?format=png&{_render_to_query(render, include_tilesize=False)}&sel=time={index}"


def _reorient_item_to_orbit(item: dict, orbit: str) -> None:
    """Fix the base item's orbit-dependent **STAC metadata** to this acquisition's run ``orbit``.

    ``eopf_geozarr.build_s1_rtc_stac_item`` derives ``sat:orbit_state``, the ``renders.rgb`` orbit, and
    the ``vv``/``vh`` asset groups from the cube's *preferred* orbit (it prefers ascending), so a
    both-orbits cube mislabels a descending acquisition as ascending. Correct them so the queryable
    metadata matches the run orbit. The ``vv``/``vh`` hrefs stay on the **cube** store — only the orbit
    group changes. (Render-param tuning beyond the orbit is deferred to titiler-eopf#108.)
    """
    props = item["properties"]
    props["sat:orbit_state"] = orbit
    rgb = props.get("renders", {}).get("rgb")
    if rgb is not None:
        vv, vh = f"/{orbit}:vv", f"/{orbit}:vh"
        rgb["expression"] = f"{vv};{vh};({vv})/({vh})"
    cube_href = item.get("assets", {}).get("zarr-store", {}).get("href")
    if cube_href:
        for pol in ("vv", "vh"):
            if pol in item["assets"]:
                item["assets"][pol]["href"] = f"{cube_href}/{orbit}"


def per_acquisition_items(
    base_item: dict,
    times_ns: list[int],
    *,
    tile_id: str,
    orbit: str,
    collection: str,
    cube_collection: str,
    raster_api: str,
) -> list[dict]:
    """Clone the per-tile ``base_item`` into one item per `time` slice.

    ``times_ns`` must be in the cube's **physical** order — each item's ``sel=time={index}`` uses its
    position here. Each clone keeps the base geometry/assets/SAR+proj properties, sets a single
    ``datetime`` (drops the start/end range), targets the acquisition ``collection``, reorients the
    orbit-dependent metadata to the run ``orbit``, and gets tilejson/xyz/thumbnail links pointing at the
    **cube** endpoint (``cube_collection``/``s1-rtc-{tile}``) with the composite render + ``sel=time``.
    ``base_item`` is not mutated.
    """
    items: list[dict] = []
    for index, t_ns in enumerate(times_ns):
        when = dt.datetime.fromtimestamp(t_ns / 1e9, tz=dt.UTC)
        item_id = acquisition_id(tile_id, when)
        item = copy.deepcopy(base_item)
        item["id"] = item_id
        item["collection"] = collection
        props = {
            k: v
            for k, v in item.get("properties", {}).items()
            if k not in ("start_datetime", "end_datetime")
        }
        props["datetime"] = when.isoformat()
        item["properties"] = props
        _reorient_item_to_orbit(item, orbit)
        render = item["properties"]["renders"][
            "rgb"
        ]  # reoriented composite (build always emits it)
        item["links"] = [
            {
                "rel": "tilejson",
                "type": "application/json",
                "href": render_tilejson(raster_api, cube_collection, tile_id, render, index),
                "title": "tilejson",
            },
            {
                "rel": "xyz",
                "type": "image/png",
                "href": render_xyz(raster_api, cube_collection, tile_id, render, index),
                "title": "Sentinel-1 GRD RGB composite",
            },
        ]
        item.setdefault("assets", {})["thumbnail"] = {
            "href": render_thumbnail(raster_api, cube_collection, tile_id, render, index),
            "type": "image/png",
            "roles": ["thumbnail"],
            "title": "Sentinel-1 GRD RGB composite preview",
        }
        items.append(item)
    return items


# --- store I/O + registration (integration; exercised in main) --------------


def _open_root(store: str) -> Any:
    import zarr

    if "://" in store:
        from zarr.storage import FsspecStore

        return zarr.open_group(FsspecStore.from_url(store), mode="r")
    return zarr.open_group(store, mode="r", zarr_format=3)


def read_times_ns(store: str, orbit: str) -> list[int]:
    """`time` values (ns since epoch) from the cube's native (r10m) level, in **physical** (stored)
    order — NOT sorted. The list position is the slice's index, which the render links bake as
    ``sel=time={index}``; it must match TiTiler's `isel` over the same stored array."""
    import numpy as np

    root = _open_root(store)
    times = np.asarray(root[orbit]["r10m"]["time"]).astype("datetime64[ns]").astype("int64")
    return [int(t) for t in times]


def _upsert_items(stac_api_url: str, collection: str, items: list[dict]) -> None:
    """DELETE-then-POST each item (pgstac has no item PUT), mirroring register_v1.upsert_item."""
    from pystac_client import Client

    client = Client.open(stac_api_url)
    io = client._stac_io
    assert io is not None  # noqa: S101 -- pystac-client always sets this after open()
    base = str(client.self_href).rstrip("/")
    for item in items:
        item_id = item["id"]
        io.session.delete(f"{base}/collections/{collection}/items/{item_id}", timeout=30)
        resp = io.session.post(f"{base}/collections/{collection}/items", json=item, timeout=30)
        resp.raise_for_status()
        print(f"registered {item_id} (datetime {item['properties']['datetime']})")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--store", required=True, help="per-tile cube URI (https/s3)")
    ap.add_argument("--tile-id", required=True)
    ap.add_argument("--orbit-direction", required=True, choices=["descending", "ascending"])
    ap.add_argument(
        "--collection", default=DEFAULT_ACQ_COLLECTION, help="per-acquisition collection"
    )
    ap.add_argument(
        "--cube-collection",
        required=True,
        help="cube collection (its TiTiler endpoint serves the shared store the render links point at)",
    )
    ap.add_argument("--stac-api-url", required=True)
    ap.add_argument("--raster-api-url", required=True)
    args = ap.parse_args()

    from eopf_geozarr.stac.s1_rtc import build_s1_rtc_stac_item
    from register_v1 import s3_to_https

    base = build_s1_rtc_stac_item(args.store, args.collection).to_dict()
    # build_s1_rtc_stac_item emits s3:// hrefs for an s3 store; rewrite to the https gateway so the
    # per-acquisition items match the cube item (register_v1 does the same). Reading times below
    # still uses the s3 store, which is authoritative and avoids the gateway's read-cache lag.
    for asset in base.get("assets", {}).values():
        if isinstance(asset.get("href"), str) and asset["href"].startswith("s3://"):
            asset["href"] = s3_to_https(asset["href"])
    times = read_times_ns(args.store, args.orbit_direction)
    items = per_acquisition_items(
        base,
        times,
        tile_id=args.tile_id,
        orbit=args.orbit_direction,
        collection=args.collection,
        cube_collection=args.cube_collection,
        raster_api=args.raster_api_url,
    )
    _upsert_items(args.stac_api_url, args.collection, items)
    print(f"registered {len(items)} per-acquisition item(s) in {args.collection}")


if __name__ == "__main__":
    main()
