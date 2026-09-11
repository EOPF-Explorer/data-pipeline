#!/usr/bin/env python3
"""Register proxy STAC items for the Samples Service Sentinel-2 GeoZarr stores.

coordination#287. The Samples Service publishes cpm_v300 Zarr v3 stores on
``data.eodc.eu`` with a STAC catalogue (``stac.core.eopf.eodc.eu``) whose items do
not carry the Explorer's asset layout or visualization links. This script clones
those source items into a *proxy* collection on the Explorer STAC API so the
Explorer's TiTiler, STAC browser and eodash can be pointed at Samples Service data
without copying it.

It deliberately does NOT convert, upload or expire anything: ``build_proxy_item`` is
a pure dict-in/Item-out transform, and the only write is the STAC upsert.

Render host is ``/rstaging`` (titiler-eopf **0.12.0**), not ``/raster`` (0.11.0): only
0.12.0 serves the ``assets=<key>|bands=…`` / ``|variables=…`` notation these items use,
and only 0.12.0 is being migrated to. See ``rgb_query``/``add_proxy_visualization``.

Asset mapping (see ``evidence-proxy-T1.md``, reader gate run 2026-09-11):

==========  ================================  ===========================================
key         href                              rendered with
==========  ================================  ===========================================
reflectance ``<store>/measurements/reflect…``  ``variables=/measurements/reflectance:b04``
AOT_10m     ``<store>/`` (store root)          ``variables=/quality/atmosphere/r10m:aot``
WVP_10m     ``<store>/`` (store root)          ``variables=/quality/atmosphere/r10m:wvp``
SCL_20m     ``<store>/`` (store root)          ``…/l2a_classification/r20m:scl``
==========  ================================  ===========================================

The three non-reflectance assets point at the **store root** because EODC only
consolidates metadata at the root and on ``measurements/reflectance`` — the
``quality/atmosphere`` and ``conditions/mask`` group hrefs cannot be opened over
HTTPS (same defect data-pipeline#412 fixes for our own stores).

Trailing slashes on that root are where two consumers disagree, so each field gets
the form its reader needs (measured 2026-09-11, see ``slash_bare_zarr_alternates``):

* the HTTPS ``href`` is **bare** (``…/X.zarr``) — titiler's ``GeoZarrReader``
  concatenates and 404s on ``…/X.zarr//zarr.json`` if it ends in a slash;
* a Track B ``alternate.s3.href`` **keeps the slash** — ``check_urls_confined``
  refuses a bare ``.zarr`` key as ``bare_zarr_store``, a delete that removes nothing
  and silently orphans the store.
"""

import argparse
import json
import logging
import os
import sys
import urllib.parse
from pathlib import Path
from urllib.parse import urlparse

import httpx
import stac_auth
from pystac import Asset, Item, Link
from register_v1 import (
    EXPLORER_BASE,
    add_alternate_s3_assets,
    add_derived_from_link,
    add_store_link,
    consolidate_reflectance_assets,
    fix_zarr_asset_media_types,
    remove_xarray_integration,
    upsert_item,
)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_SOURCE_STAC_API = "https://stac.core.eopf.eodc.eu"
DEFAULT_SOURCE_COLLECTION = "sentinel-2-l2a-zarr3"

# Only collections whose id carries this marker may be written to. The proxy holds
# third-party data under an Explorer-looking id; a typo that aimed it at
# ``sentinel-2-l2a`` would overwrite real Explorer items.
COLLECTION_ID_MARKER = "samples-zarr3"

# The probe strings used by the T1 reader gate, T3's rehearsal and T4, verbatim, so
# the evidence files and the registered items cannot drift apart. An unqualified
# ``/assets/AOT_10m/info`` on a root href answers 200 with all 49 variables and
# proves nothing — always pass ``variables=``.
# True-colour query for the visualization links, in the 0.12 notation: one `assets`
# parameter whose value carries the per-asset band selection. The two deployments are
# mirror images and neither accepts the other's form (measured 2026-09-11 against a prod
# S2 item on /collections/sentinel-2-l2a/items/…/preview.png):
#
#   notation                  /raster 0.11.0   /rstaging 0.12.0
#   assets=reflectance|bands=      500              200
#   repeated variables=            200              422
#
# The proxy targets /rstaging, so it emits the `assets=…|bands=…` form and must NOT
# reuse register_v1.add_visualization_links (which emits the `variables=` form).
RGB_ASSETS = "reflectance|bands=b04,b03,b02"
S2_COLOR_FORMULA = "gamma rgb 1.3, sigmoidal rgb 6 0.1, saturation 1.2"
RGB_QUERY = (
    f"assets={urllib.parse.quote(RGB_ASSETS, safe='')}"
    f"&rescale={urllib.parse.quote('0,1', safe='')}"
    f"&color_formula={urllib.parse.quote(S2_COLOR_FORMULA, safe='')}"
)

AOT_PROBE = "assets=AOT_10m|variables=/quality/atmosphere/r10m:aot"
WVP_PROBE = "assets=WVP_10m|variables=/quality/atmosphere/r10m:wvp"
SCL_PROBE = "assets=SCL_20m|variables=/conditions/mask/l2a_classification/r20m:scl"

# Proxy asset key -> (source asset key, band name, title).
# AOT and WVP are split out of the source's single ``ATM_10m`` group asset so the
# proxy uses the Explorer's asset keys and titles; the numeric fields (raster:scale,
# nodata, data_type, proj:shape) stay as EODC published them, because they describe
# the EODC store.
ROOT_HREF_ASSETS = {
    "AOT_10m": ("ATM_10m", "aot", "Aerosol optical thickness (AOT)"),
    "WVP_10m": ("ATM_10m", "wvp", "Water vapour (WVP)"),
    "SCL_20m": ("SCL_20m", "scl", "Scene classification map (SCL)"),
}

# Source assets with no proxy equivalent: the 20m/60m atmosphere groups and the 60m
# SCL duplicate the arrays already reachable from the root href.
DROPPED_ASSETS = ("ATM_10m", "ATM_20m", "ATM_60m", "SCL_60m")

# Links that only make sense in the source catalogue. ``collection`` is re-added
# pointing at the proxy collection: the STAC item schema refuses a ``collection``
# field without a matching link, so dropping it outright makes the item invalid.
DROPPED_LINK_RELS = frozenset({"root", "self", "parent", "collection", "alternate"})

EO_EXTENSION = "https://stac-extensions.github.io/eo/v2.0.0/schema.json"
RASTER_EXTENSION = "https://stac-extensions.github.io/raster/v2.0.0/schema.json"
DATACUBE_EXTENSION = "https://stac-extensions.github.io/datacube/v2.3.0/schema.json"
REQUIRED_EXTENSIONS = (EO_EXTENSION, RASTER_EXTENSION, DATACUBE_EXTENSION)

# Dropped unless a later step re-adds them: only Track B items (``--s3-endpoint``)
# carry an S3 alternate, and ``add_alternate_s3_assets`` re-declares both.
DROPPED_EXTENSION_PREFIXES = (
    "https://stac-extensions.github.io/alternate-assets/",
    "https://stac-extensions.github.io/storage/",
)


def store_root(item: Item) -> str:
    """Return the Zarr store root (no trailing slash) from the item's asset hrefs."""
    for asset in item.assets.values():
        if ".zarr" in (asset.href or ""):
            return asset.href.split(".zarr")[0] + ".zarr"
    raise ValueError(f"{item.id}: no .zarr asset href to derive the store root from")


def rebase_store_root(item: Item, new_base: str) -> str:
    """Repoint every asset href at ``<new_base>/<store>.zarr`` and return that root.

    Track B copies the stores flat under one prefix (T0.9:
    ``samples-zarr3-proxy/<id>.zarr/``), dropping the source's ``YYYY/MM/DD``
    directories — so the rewrite is "same store name, new base", not a prefix
    substitution, which could only ever match one item's source directory.
    """
    old_root = store_root(item)
    new_root = f"{new_base.rstrip('/')}/{old_root.rsplit('/', 1)[-1]}"
    rewritten = 0
    for asset in item.assets.values():
        if asset.href and asset.href.startswith(old_root):
            asset.href = new_root + asset.href[len(old_root) :]
            rewritten += 1
    logger.info(f"   🔗 Rewrote {rewritten} asset href(s) to {new_root}")
    return new_root


def build_root_href_assets(item: Item, root: str) -> None:
    """Replace the atmosphere/mask group assets with root-href AOT/WVP/SCL assets."""
    root_href = root.rstrip("/")
    built = {}
    for key, (source_key, band_name, title) in ROOT_HREF_ASSETS.items():
        source = item.assets.get(source_key)
        if source is None:
            logger.warning(f"   ⚠️  {item.id}: no {source_key} asset, skipping {key}")
            continue
        fields = dict(source.extra_fields)
        bands = [b for b in fields.get("bands", []) if b.get("name") == band_name]
        if bands:
            fields["bands"] = bands
            fields["description"] = bands[0].get("description", fields.get("description", ""))
        built[key] = Asset(
            href=root_href,
            media_type=source.media_type,
            title=title,
            roles=["data"],
            extra_fields=fields,
        )

    for key in (*DROPPED_ASSETS, *ROOT_HREF_ASSETS):
        item.assets.pop(key, None)
    item.assets.update(built)


def fill_cube_extent(item: Item) -> None:
    """Fill the reflectance ``cube:dimensions`` x/y extents from ``proj:bbox``.

    ``consolidate_reflectance_assets`` derives them from ``proj:transform``, which the
    EODC items do not carry — they publish ``proj:bbox`` (already in projected
    coordinates, same CRS as ``proj:code``) instead.
    """
    reflectance = item.assets.get("reflectance")
    bbox = item.properties.get("proj:bbox")
    if reflectance is None or not bbox or len(bbox) < 4:
        return
    dimensions = reflectance.extra_fields.get("cube:dimensions")
    if not dimensions:
        return
    x_min, y_min, x_max, y_max = bbox[:4]
    dimensions["x"]["extent"] = [x_min, x_max]
    dimensions["y"]["extent"] = [y_min, y_max]


def reconcile_extensions(item: Item) -> None:
    """Declare the extensions the proxy assets use; drop the ones they do not."""
    extensions = [
        ext for ext in item.stac_extensions if not ext.startswith(DROPPED_EXTENSION_PREFIXES)
    ]
    extensions += [ext for ext in REQUIRED_EXTENSIONS if ext not in extensions]
    item.stac_extensions = extensions


def add_proxy_visualization(item: Item, raster_api_url: str, collection: str) -> None:
    """Add the viz links in the 0.12 notation, plus the Explorer ``via`` link.

    **No ``thumbnail`` asset** (Loïc, 2026-09-11). A STAC browser renders one inline, and
    this collection is a temporary proxy that must not look like a user-facing product.
    Note the ordering question this came from has no answer at our end: pgstac normalises
    asset key order (length, then bytewise), so a ``thumbnail`` key could never be sorted
    last from the payload — only removed.

    Deliberately not ``register_v1.add_visualization_links`` /
    ``add_thumbnail_asset``: for a ``sentinel-2*`` collection those emit repeated
    full-path ``variables=`` and a **bare** ``/viewer``, and titiler-eopf 0.12.0 rejects
    both — 422 for the query, and the bare ``/viewer`` route does not exist on 0.12.0 at
    all (it is why the 2026-09-10 probe saw a 404). ``map.html`` replaces it.
    """
    base = f"{raster_api_url.rstrip('/')}/collections/{collection}/items/{item.id}"
    item.add_link(
        Link(
            "viewer",
            f"{base}/WebMercatorQuad/map.html?{RGB_QUERY}",
            "text/html",
            f"Viewer for {item.id}",
        )
    )
    item.add_link(
        Link(
            "xyz",
            f"{base}/tiles/WebMercatorQuad/{{z}}/{{x}}/{{y}}.png?{RGB_QUERY}",
            "image/png",
            "Sentinel-2 L2A True Color",
        )
    )
    item.add_link(
        Link(
            "tilejson",
            f"{base}/WebMercatorQuad/tilejson.json?{RGB_QUERY}",
            "application/json",
            f"TileJSON for {item.id}",
        )
    )
    item.add_link(
        Link(
            "via",
            f"{EXPLORER_BASE}/collections/{collection}/items/{item.id}",
            title="EOPF Explorer",
        )
    )


def slash_bare_zarr_alternates(item: Item) -> None:
    """Give every ``alternate.s3`` store-root href a trailing slash.

    ``add_alternate_s3_assets`` derives the S3 URI from the HTTPS href, which for the
    three root-href assets is a bare ``…/X.zarr``. ``s3_item_cleanup`` reads
    ``alternate.s3.href`` and its ``check_urls_confined`` refuses a bare ``.zarr`` key
    as ``bare_zarr_store``: ``_partition_by_bucket`` would treat it as a single object,
    delete ~nothing, count 0 remaining and drop the STAC item while the store lives on.
    The slash cannot go on the HTTPS href instead — titiler's reader 404s on it.
    """
    for asset in item.assets.values():
        alternate = asset.extra_fields.get("alternate")
        if not isinstance(alternate, dict):
            continue
        s3 = alternate.get("s3")
        if isinstance(s3, dict) and str(s3.get("href", "")).endswith(".zarr"):
            s3["href"] += "/"


def build_proxy_item(
    source_item: dict,
    collection: str,
    raster_api_url: str,
    stac_api_url: str,
    store_root_base: str | None = None,
    s3_endpoint: str | None = None,
) -> Item:
    """Transform a source Samples Service STAC item dict into a proxy item.

    Pure: no network access, no catalogue resolution. ``s3_endpoint`` is the one
    exception — Track B passes it and ``add_alternate_s3_assets`` then queries the
    object's storage class.
    """
    self_href = next(
        (link["href"] for link in source_item.get("links", []) if link.get("rel") == "self"),
        None,
    )
    if not self_href:
        raise ValueError(f"{source_item.get('id')}: source item has no self link")

    # from_dict deep-copies, so the caller's dict is never mutated. Stripping the
    # source catalogue's links (self included) is also what keeps to_dict() offline:
    # pystac would otherwise resolve root/parent over the network.
    item = Item.from_dict(source_item)
    item.links = [link for link in item.links if link.rel not in DROPPED_LINK_RELS]
    item.collection_id = collection
    item.add_link(
        Link(
            "collection",
            f"{stac_api_url.rstrip('/')}/collections/{collection}",
            "application/json",
            collection,
        )
    )

    root = rebase_store_root(item, store_root_base) if store_root_base else store_root(item)

    fix_zarr_asset_media_types(item)
    add_store_link(item, root)
    consolidate_reflectance_assets(item, root)
    fill_cube_extent(item)
    build_root_href_assets(item, root)
    remove_xarray_integration(item)
    reconcile_extensions(item)

    add_proxy_visualization(item, raster_api_url, collection)
    add_derived_from_link(item, self_href)

    if s3_endpoint:
        add_alternate_s3_assets(item, s3_endpoint)
        slash_bare_zarr_alternates(item)

    return item


def read_item_ids(path: Path) -> list[str]:
    """Read item ids one per line, ignoring blanks and ``#`` comments."""
    return [
        ident for raw in path.read_text().splitlines() if (ident := raw.split("#", 1)[0].strip())
    ]


def fetch_source_item(source_stac_api: str, source_collection: str, item_id: str) -> dict:
    """GET one source item. A direct GET avoids depending on the source's conformance."""
    url = f"{source_stac_api.rstrip('/')}/collections/{source_collection}/items/{item_id}"
    with httpx.Client(timeout=30.0, follow_redirects=True) as http:
        resp = http.get(url)
        resp.raise_for_status()
        return dict(resp.json())


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source-stac-api", default=DEFAULT_SOURCE_STAC_API)
    parser.add_argument("--source-collection", default=DEFAULT_SOURCE_COLLECTION)
    parser.add_argument("--collection", required=True, help="Target proxy collection id")
    parser.add_argument("--stac-api-url", required=True, help="Target STAC API")
    parser.add_argument("--raster-api-url", required=True, help="TiTiler base URL for links")
    parser.add_argument("--item-id", action="append", default=[], help="Repeatable")
    parser.add_argument("--item-ids-file", type=Path, help="One id per line, # comments ignored")
    parser.add_argument(
        "--max-items",
        type=int,
        required=True,
        help="Refuse to run if more ids than this are given (no silent truncation)",
    )
    parser.add_argument("--dry-run", type=Path, metavar="DIR", help="Write <id>.json here instead")
    parser.add_argument(
        "--store-root-base",
        metavar="URL",
        help="Track B: repoint each asset at <URL>/<store>.zarr (the OVH copies)",
    )
    parser.add_argument("--s3-endpoint", help="Track B only: add alternate.s3 to each asset")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    if COLLECTION_ID_MARKER not in args.collection:
        logger.error(
            "Refusing --collection %r: a proxy collection id must contain %r",
            args.collection,
            COLLECTION_ID_MARKER,
        )
        return 1

    for url, name in [
        (args.source_stac_api, "--source-stac-api"),
        (args.raster_api_url, "--raster-api-url"),
        (args.stac_api_url, "--stac-api-url"),
    ]:
        if urlparse(url).scheme != "https":
            logger.error("Error: %s must be an HTTPS URL, got: %r", name, url)
            return 1

    item_ids = list(args.item_id)
    if args.item_ids_file:
        item_ids += read_item_ids(args.item_ids_file)
    if not item_ids:
        logger.error("No item ids: pass --item-id and/or --item-ids-file")
        return 1

    # Bound before any network call, and refuse rather than truncate: a silently
    # trimmed list would make the run look bounded while the operator believed a
    # different set had been registered.
    if len(item_ids) > args.max_items:
        logger.error(
            "Refusing to run: %d item ids exceed --max-items %d", len(item_ids), args.max_items
        )
        return 1
    logger.info("Registering %d item(s) into %s", len(item_ids), args.collection)

    client = None
    if args.dry_run:
        args.dry_run.mkdir(parents=True, exist_ok=True)
    else:
        client = stac_auth.open_client(args.stac_api_url)
        logger.info("Target STAC API: %s", args.stac_api_url)

    for item_id in item_ids:
        source = fetch_source_item(args.source_stac_api, args.source_collection, item_id)
        item = build_proxy_item(
            source,
            args.collection,
            args.raster_api_url,
            args.stac_api_url,
            store_root_base=args.store_root_base,
            s3_endpoint=args.s3_endpoint,
        )
        if client is None:
            out = args.dry_run / f"{item.id}.json"
            out.write_text(json.dumps(item.to_dict(), indent=2))
            logger.info(f"   📄 {out}")
        else:
            upsert_item(client, args.collection, item)

    return 0


if __name__ == "__main__":
    sys.exit(main())
