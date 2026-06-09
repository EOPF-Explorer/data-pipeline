"""Register one STAC item per acquisition from a per-tile S1 GRD RTC datacube (plan T5).

The cube (`s1-rtc-{tile}.zarr`, collection `sentinel-1-grd-rtc-staging`) holds all acquisitions on a
`time` axis. This emits **one queryable STAC item per `time` slice** (`s1-rtc-{tile}-{datetime}`,
single `datetime`) into the `sentinel-1-grd-rtc-acquisitions` collection, each pointing at the cube
via asset href with a `sel=time` tilejson link baked in. Explorer rendering is deferred (see #228);
the links render once titiler resolves the store from the item href — no later data change.

Usage:
    uv run python scripts/register_per_acquisition.py --store <cube-uri> --tile-id 31TCH \
      --orbit-direction descending --stac-api-url <url> --raster-api-url <url>
"""

from __future__ import annotations

import argparse
import copy
import datetime as dt
import urllib.parse
from typing import Any

DEFAULT_ACQ_COLLECTION = "sentinel-1-grd-rtc-acquisitions"


def acquisition_id(tile_id: str, when: dt.datetime) -> str:
    """Per-acquisition item id, e.g. ``s1-rtc-31TCH-20260607t055248``."""
    return f"s1-rtc-{tile_id}-{when.strftime('%Y%m%dt%H%M%S')}"


def sel_time_tilejson(
    raster_api: str,
    collection: str,
    item_id: str,
    when: dt.datetime,
    orbit: str,
    *,
    var: str = "vh",
) -> str:
    """TiTiler tilejson URL that renders this acquisition's slice of the cube (``sel=time``)."""
    sel = urllib.parse.quote(f"time=nearest::{when.isoformat()}", safe="")
    variables = urllib.parse.quote(f"/{orbit}:{var}", safe="")
    return (
        f"{raster_api}/collections/{collection}/items/{item_id}"
        f"/WebMercatorQuad/tilejson.json?variables={variables}&bidx=1&rescale=0%2C219"
        f"&assets={var}&sel={sel}"
    )


def per_acquisition_items(
    base_item: dict,
    times_ns: list[int],
    *,
    tile_id: str,
    orbit: str,
    collection: str,
    raster_api: str,
) -> list[dict]:
    """Clone the per-tile ``base_item`` into one item per `time` slice.

    Each clone keeps the base geometry/assets/SAR+proj properties, sets a single ``datetime`` (drops
    the start/end range), targets ``collection``, and carries a ``sel=time`` tilejson link. The input
    ``base_item`` is not mutated.
    """
    items: list[dict] = []
    for t_ns in times_ns:
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
        item["links"] = [
            {
                "rel": "tilejson",
                "type": "application/json",
                "href": sel_time_tilejson(raster_api, collection, item_id, when, orbit),
                "title": "tilejson (sel=time)",
            }
        ]
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
    """Sorted `time` values (ns since epoch) from the cube's native (r10m) level."""
    import numpy as np

    root = _open_root(store)
    times = np.asarray(root[orbit]["r10m"]["time"]).astype("datetime64[ns]").astype("int64")
    return sorted(int(t) for t in times)


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
    ap.add_argument("--collection", default=DEFAULT_ACQ_COLLECTION)
    ap.add_argument("--stac-api-url", required=True)
    ap.add_argument("--raster-api-url", required=True)
    args = ap.parse_args()

    from eopf_geozarr.stac.s1_rtc import build_s1_rtc_stac_item

    base = build_s1_rtc_stac_item(args.store, args.collection).to_dict()
    times = read_times_ns(args.store, args.orbit_direction)
    items = per_acquisition_items(
        base,
        times,
        tile_id=args.tile_id,
        orbit=args.orbit_direction,
        collection=args.collection,
        raster_api=args.raster_api_url,
    )
    _upsert_items(args.stac_api_url, args.collection, items)
    print(f"registered {len(items)} per-acquisition item(s) in {args.collection}")


if __name__ == "__main__":
    main()
