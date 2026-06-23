"""Register one STAC item per acquisition from a per-tile S1 GRD RTC datacube (plan T5).

The cube (`s1-rtc-{tile}.zarr`, collection `sentinel-1-grd-rtc-staging`) holds all acquisitions on a
`time` axis. This emits **one queryable STAC item per `time` slice** (`s1-rtc-{tile}-{datetime}`,
single `datetime`) into the env-split per-acquisition collection (`--collection`, default `…-tests`,
the cron passes `…-staging`). **No data duplication**: the item's render links point at the **cube's**
TiTiler endpoint (`--cube-collection`/items/`s1-rtc-{tile}`) with `sel=time={datetime}` — TiTiler
reconstructs the shared cube store and selects this acquisition's slice by its **datetime** (exact
label `.sel`, since the cube's `time` is CF-encoded; see data-model #192). Selecting by datetime instead
of a positional index makes each item self-contained: it renders the correct slice regardless of the
cube's physical slice order (which can go non-monotonic on out-of-order appends), so no slice ever needs
re-registering when the cube grows. Registration is therefore **incremental** — only acquisitions not
already in the collection are written (use `--reregister-all` for the one-time migration of items that
still carry the old index-based `sel`).

Usage:
    uv run python scripts/register_per_acquisition.py --store <cube-uri> --tile-id 31TCH \
      --orbit-direction descending --collection <acq-coll> --cube-collection <cube-coll> \
      --stac-api-url <url> --raster-api-url <url>
"""

from __future__ import annotations

import argparse
import urllib.parse

from eopf_geozarr.stac.s1_rtc import acquisition_id as acquisition_id  # re-export for trigger_cdse
from eopf_geozarr.stac.s1_rtc import build_s1_rtc_per_acquisition_items
from pystac import Item
from register_v1 import EXPLORER_BASE, _render_to_query

# Per-acquisition collections are env-split like the cube collections (…-tests/-staging/-prod). The
# code default targets the test env (local/CP-A runs); the Argo cron passes --collection …-staging.
DEFAULT_ACQ_COLLECTION = "sentinel-1-grd-rtc-acquisitions-tests"

# Item *construction* — one item per cube `time` slice, oriented to its orbit, carrying the orbit's γ⁰
# asset + a `renders.rgb` whose rescale (0.0,0.2) the builder now emits — lives in
# eopf_geozarr.stac.s1_rtc.build_s1_rtc_per_acquisition_items. This script adds only the deployment
# decoration (render/`via` links + thumbnail at the cube endpoint, `store` link, S3 alternates) and
# upserts. The old in-pipeline apply_s1_rtc_rescale override is gone (the builder emits 0.0,0.2).


def _cube_item_base(raster_api: str, cube_collection: str, tile_id: str) -> str:
    """TiTiler endpoint of the shared **cube** item (``s1-rtc-{tile}`` in the cube collection).

    The per-acquisition item's render links point here — not at the acquisition item's own endpoint —
    because TiTiler reconstructs the store path from ``{collection}/{item_id}`` and only the cube path
    exists. ``sel=time={datetime}`` then selects this acquisition's slice.
    """
    return f"{raster_api}/collections/{cube_collection}/items/s1-rtc-{tile_id}"


def _sel_time(sel_time: str) -> str:
    """The ``sel`` query fragment selecting a slice by datetime (exact label ``.sel`` in TiTiler).

    The ``:`` in the timestamp is percent-encoded so the href is unambiguous; TiTiler decodes it before
    parsing ``time=<value>``.
    """
    return f"sel=time={urllib.parse.quote(sel_time, safe='')}"


def render_tilejson(
    raster_api: str, cube_collection: str, tile_id: str, render: dict, sel_time: str
) -> str:
    """tilejson URL for the cube slice at ``sel_time`` (composite render from ``renders.rgb`` + ``sel``)."""
    base = _cube_item_base(raster_api, cube_collection, tile_id)
    return f"{base}/WebMercatorQuad/tilejson.json?{_render_to_query(render, include_tilesize=True)}&{_sel_time(sel_time)}"


def render_viewer(
    raster_api: str, cube_collection: str, tile_id: str, render: dict, sel_time: str
) -> str:
    """Interactive ``map.html`` viewer for the cube slice at ``sel_time`` — the human-clickable map.

    A raw ``{z}/{x}/{y}`` xyz tile template 422s when clicked in a STAC browser (the placeholders are
    sent to TiTiler literally); ``map.html`` fills the tile coords in itself. ``tilejson`` (above)
    serves the tile template to machine map clients.
    """
    base = _cube_item_base(raster_api, cube_collection, tile_id)
    q = _render_to_query(render, include_tilesize=True)
    return f"{base}/WebMercatorQuad/map.html?{q}&{_sel_time(sel_time)}"


def render_thumbnail(
    raster_api: str, cube_collection: str, tile_id: str, render: dict, sel_time: str
) -> str:
    """``preview`` PNG for the cube slice at ``sel_time`` — the static thumbnail."""
    base = _cube_item_base(raster_api, cube_collection, tile_id)
    return f"{base}/preview?format=png&{_render_to_query(render, include_tilesize=False)}&{_sel_time(sel_time)}"


def decorate_acquisition_item(
    item: Item, *, tile_id: str, cube_collection: str, raster_api: str
) -> dict:
    """Add the render/``via`` links + thumbnail to a per-acquisition item and return its dict.

    Construction (single ``datetime``, run-orbit metadata, the orbit's γ⁰ asset, ``renders.rgb``) is
    already done by ``build_s1_rtc_per_acquisition_items``; this adds only the deployment links. They
    point at the shared **cube** TiTiler endpoint (``cube_collection``/``s1-rtc-{tile}``) with
    ``sel=time={datetime}`` — no data duplication; the acquisition item is a reference into the cube,
    and the render stays slice-correct regardless of the cube's physical slice order. The ``viewer``
    link is a ``map.html`` deep-link into this acquisition's slice (``sel=time`` makes that possible).
    Any ``store`` link / S3 ``alternate`` blocks the caller already added are preserved.
    """
    when = item.datetime
    if when is None:  # per-acquisition items always carry a single datetime
        raise ValueError(f"per-acquisition item {item.id!r} has no datetime")
    # Exact value TiTiler matches against the CF-decoded datetime64 `time` index (second precision).
    sel_time = when.strftime("%Y-%m-%dT%H:%M:%S")
    d = item.to_dict(include_self_link=False)
    item_id = d["id"]
    collection = d.get("collection", "")
    render = d["properties"]["renders"]["rgb"]
    store_links = [lk for lk in d.get("links", []) if lk.get("rel") == "store"]
    d["links"] = [
        *store_links,
        {
            "rel": "tilejson",
            "type": "application/json",
            "href": render_tilejson(raster_api, cube_collection, tile_id, render, sel_time),
            "title": "tilejson",
        },
        {
            "rel": "viewer",
            "type": "text/html",
            "href": render_viewer(raster_api, cube_collection, tile_id, render, sel_time),
            "title": "Sentinel-1 GRD RGB composite",
        },
        {
            "rel": "via",
            "type": "text/html",
            "href": f"{EXPLORER_BASE}/collections/{collection.lower().replace('_', '-')}/items/{item_id}",
            "title": "EOPF Explorer",
        },
    ]
    d.setdefault("assets", {})["thumbnail"] = {
        "href": render_thumbnail(raster_api, cube_collection, tile_id, render, sel_time),
        "type": "image/png",
        "roles": ["thumbnail"],
        "title": "Sentinel-1 GRD RGB composite preview",
    }
    return d


def existing_item_ids(stac_api_url: str, collection: str, candidate_ids: list[str]) -> set[str]:
    """Of ``candidate_ids``, those already present in ``collection`` (one STAC search, not N lookups)."""
    from pystac_client import Client

    if not candidate_ids:
        return set()
    client = Client.open(stac_api_url)
    search = client.search(collections=[collection], ids=candidate_ids, limit=len(candidate_ids))
    return {it["id"] for it in search.items_as_dicts()}


def _upsert_items(stac_api_url: str, collection: str, items: list[dict]) -> None:
    """DELETE-then-POST each item (pgstac has no item PUT), mirroring register_v1.upsert_item."""
    from pystac_client import Client

    client = Client.open(stac_api_url)
    io = client._stac_io
    assert io is not None  # noqa: S101  # nosec B101 -- pystac-client always sets this after open()
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
    ap.add_argument(
        "--s3-endpoint",
        default="https://s3.de.io.cloud.ovh.net",
        help="S3 endpoint for the alternate-assets/storage metadata (region + tier). Default: OVH de.",
    )
    ap.add_argument(
        "--reregister-all",
        action="store_true",
        help="re-upsert every slice's item (one-time migration of items still on the old index sel); "
        "default registers only acquisitions not already in the collection",
    )
    args = ap.parse_args()

    from register_v1 import add_alternate_s3_assets, add_store_link, s3_to_https

    # Construction (one item per slice, oriented to its orbit, with the orbit's γ⁰ asset + renders.rgb)
    # is done by the library; this script adds the deployment decoration per item: s3://→https for the
    # TiTiler gateway, the cube `store` link + S3 alternate-assets/storage blocks, then the render/`via`
    # links + thumbnail (cube endpoint, sel=time). Reading from args.store (authoritative s3 in the cron).
    items = build_s1_rtc_per_acquisition_items(
        args.store, orbit=args.orbit_direction, collection_id=args.collection
    )
    records: list[dict] = []
    for item in items:
        for asset in item.assets.values():
            if asset.href and asset.href.startswith("s3://"):
                asset.href = s3_to_https(asset.href)
        add_store_link(item, args.store)
        add_alternate_s3_assets(item, args.s3_endpoint)
        records.append(
            decorate_acquisition_item(
                item,
                tile_id=args.tile_id,
                cube_collection=args.cube_collection,
                raster_api=args.raster_api_url,
            )
        )

    # Incremental by default: each item's datetime `sel` is correct for the life of the cube, so only
    # acquisitions not yet in the collection need writing (scales as the cube grows). --reregister-all
    # forces all (e.g. the migration to the new asset model).
    if not args.reregister_all:
        existing = existing_item_ids(args.stac_api_url, args.collection, [r["id"] for r in records])
        skipped = len(records)
        records = [r for r in records if r["id"] not in existing]
        print(f"{skipped - len(records)} already registered, {len(records)} new")
    _upsert_items(args.stac_api_url, args.collection, records)
    print(f"registered {len(records)} per-acquisition item(s) in {args.collection}")


if __name__ == "__main__":
    main()
