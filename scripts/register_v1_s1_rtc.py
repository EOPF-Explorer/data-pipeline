"""Register one S1 GRD RTC Zarr store as a STAC item.

Builds a STAC item from the Zarr store metadata, augments it with
visualization links and alternate S3 assets, then upserts it to the
staging STAC API.

Exit codes:
    0 -- success
    1 -- failure (item build error or API error)
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from urllib.parse import urlencode, urlparse

sys.path.insert(0, str(Path(__file__).parent))

from eopf_geozarr.stac.s1_rtc import build_s1_rtc_stac_item, pick_slice, slice_coverages
from pystac import Item, Link
from pystac_client import Client
from register_v1 import (
    add_alternate_s3_assets,
    add_store_link,
    add_thumbnail_asset,
    add_visualization_links,
    s3_to_https,
    upsert_item,
    warm_thumbnail_cache,
)
from run_ingest_register import check_env_consistency

log = logging.getLogger(__name__)

SAT_EXT = "https://stac-extensions.github.io/sat/v1.0.0/schema.json"

# Coverage-based preview-slice selection (Slice / pick_slice / slice_coverages) now lives in the
# eopf_geozarr.stac.s1_rtc library — imported above and used by _pin_preview_to_best_recent below.


def _reorient_item_to_orbit(item: dict, orbit: str) -> None:
    """Point the cube item's orbit-dependent *metadata* at the preview slice's orbit.

    The new asset model exposes both orbit groups as first-class assets, so only the orbit-scoped
    metadata needs adjusting for the default preview: ``sat:orbit_state`` and the ``renders.rgb``
    expression. The builder omits ``sat:orbit_state`` on a dual-orbit cube, so set it (and declare the
    SAT extension) for the chosen preview slice. No asset-href rewrite is needed any more.
    """
    props = item["properties"]
    props["sat:orbit_state"] = orbit
    rgb = props.get("renders", {}).get("rgb")
    if rgb is not None:
        vv, vh = f"/{orbit}:vv", f"/{orbit}:vh"
        rgb["expression"] = f"{vv};{vh};({vv})/({vh})"
    exts = item.setdefault("stac_extensions", [])
    if SAT_EXT not in exts:
        exts.append(SAT_EXT)


def _pin_preview_to_best_recent(item: Item, store: str) -> tuple[Item, str | None]:
    """Reorient the cube item to its best-recent slice's orbit and return that slice's ``sel=time`` value.

    Picks the slice the preview should default to (``pick_slice`` over ``slice_coverages``), then reuses
    ``_reorient_item_to_orbit`` so the render expression / orbit metadata target the chosen slice's orbit.
    Best-effort: any coverage-read failure (or an empty cube) leaves the item unchanged and returns
    ``None`` → the preview falls back to the default slice rather than failing registration.
    """
    try:
        chosen = pick_slice(slice_coverages(store))
    except Exception:
        log.warning(
            "Could not read slice coverage from %s; preview uses default slice",
            store,
            exc_info=True,
        )
        return item, None
    if chosen is None:
        return item, None
    item_dict = item.to_dict()
    _reorient_item_to_orbit(item_dict, chosen.orbit)
    return Item.from_dict(item_dict), chosen.dt.strftime("%Y-%m-%dT%H:%M:%S")


def acquisitions_collection_of(cube_collection: str) -> str:
    """The per-acquisition collection paired with a cube collection (its env-split sibling).

    ``sentinel-1-grd-rtc-{env}`` → ``sentinel-1-grd-rtc-acquisitions-{env}``. Overridable via
    ``--acquisitions-collection`` so platform-deploy isn't coupled to this string surgery.
    """
    return cube_collection.replace("sentinel-1-grd-rtc", "sentinel-1-grd-rtc-acquisitions", 1)


def acquisitions_search_href(stac_api_url: str, acq_collection: str, tile_id: str) -> str:
    """STAC item-search URL listing this tile's per-acquisition items (self-maintaining — no enumeration).

    Filters by id prefix ``s1-rtc-{tile}-`` (works today, no new property). Switch to
    ``grid:code='MGRS-{tile}'`` once the grid extension is pinned (data-model L1). ``tile_id`` is a
    controlled MGRS token from the cube item id, not user input.
    """
    cql2 = f"id LIKE 's1-rtc-{tile_id}-%'"
    query = urlencode({"collections": acq_collection, "filter-lang": "cql2-text", "filter": cql2})
    return f"{stac_api_url.rstrip('/')}/search?{query}"


def register(
    store: str,
    collection: str,
    stac_api_url: str,
    raster_api_url: str,
    s3_endpoint: str,
    acquisitions_collection: str | None = None,
) -> int:
    """Build and register one S1 RTC STAC item.

    Returns exit code: 0 = success, 1 = failure.
    """
    # Fail fast on a per-env bucket/collection mismatch (the 32TLR footgun): the standalone
    # register path takes a hand-typed --store + --collection, so it needs the same guard as
    # run_ingest_register. Only s3:// stores carry an identifiable bucket in the netloc.
    parsed = urlparse(store)
    if parsed.scheme == "s3":
        check_env_consistency(collection, parsed.netloc)

    try:
        item = build_s1_rtc_stac_item(store, collection)
    except Exception:
        log.exception("Failed to build STAC item from %s", store)
        return 1

    # Default the cube preview to the best-recent acquisition (most recent >80% coverage, else max
    # coverage) and reorient the item to that slice's orbit so the render targets the right group.
    item, sel_time = _pin_preview_to_best_recent(item, store)

    # build_s1_rtc_stac_item returns s3:// hrefs; TiTiler needs https:// via the gateway
    for asset in item.assets.values():
        if asset.href and asset.href.startswith("s3://"):
            asset.href = s3_to_https(asset.href)

    add_store_link(item, store)
    add_alternate_s3_assets(item, s3_endpoint)
    add_visualization_links(item, raster_api_url, collection, sel_time=sel_time)
    add_thumbnail_asset(item, raster_api_url, collection, sel_time=sel_time)
    warm_thumbnail_cache(item)

    # Cross-link to this tile's per-acquisition items (one self-maintaining search link, not N
    # enumerated links — the set grows each ingest). tile is the cube id's suffix (`s1-rtc-{tile}`).
    tile_id = item.id.removeprefix("s1-rtc-")
    acq_collection = acquisitions_collection or acquisitions_collection_of(collection)
    item.add_link(
        Link(
            "related",
            acquisitions_search_href(stac_api_url, acq_collection, tile_id),
            "application/json",
            "Per-acquisition items (this tile)",
        )
    )

    try:
        client = Client.open(stac_api_url)
        upsert_item(client, collection, item)
    except Exception:
        log.exception("Failed to upsert item %s to %s", item.id, stac_api_url)
        return 1

    # TEMPORARY (#246): no render-copy needed — run_ingest_register writes the cube directly at
    # titiler's reconstructed path (tests-output/{collection}/s1-rtc-{tile}.zarr). Revert when
    # titiler-eopf#108 lands.
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--store", required=True, help="S3 URI of the GeoZarr V3 store")
    parser.add_argument("--collection", required=True, help="Target STAC collection ID")
    parser.add_argument("--stac-api-url", required=True, help="STAC API base URL")
    parser.add_argument("--raster-api-url", required=True, help="TiTiler raster API base URL")
    parser.add_argument("--s3-endpoint", required=True, help="S3 endpoint URL")
    parser.add_argument(
        "--acquisitions-collection",
        default=None,
        help="per-acquisition collection for the cube→acquisitions cross-link "
        "(default: derived as the …-acquisitions-{env} sibling of --collection)",
    )
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    args = _build_parser().parse_args()
    sys.exit(
        register(
            store=args.store,
            collection=args.collection,
            stac_api_url=args.stac_api_url,
            raster_api_url=args.raster_api_url,
            s3_endpoint=args.s3_endpoint,
            acquisitions_collection=args.acquisitions_collection,
        )
    )


if __name__ == "__main__":
    main()
