#!/usr/bin/env python3
"""Register proxy STAC items for the Samples Service Sentinel-2 GeoZarr stores.

coordination#287. The Samples Service publishes cpm_v300 Zarr v3 stores on
``data.eodc.eu`` with a STAC catalogue (``stac.core.eopf.eodc.eu``) whose items do
not carry the Explorer's asset layout or visualization links. This script clones
those source items into a *proxy* collection on the Explorer STAC API so the
Explorer's TiTiler, STAC browser and eodash can be pointed at Samples Service data
without copying it.

``--mirror-explorer`` (coordination#304) runs the same loop on the Explorer's own items:
it copies ``sentinel-2-l2a`` items into a ``*mirror-rstaging*`` collection with the same
``/rstaging`` links, so our GeoZarr renders next to the proxies. See ``build_mirror_item``.

It deliberately does NOT convert or upload anything: ``build_proxy_item`` is a pure
dict-in/Item-out transform, and the only write is the STAC upsert. It does stamp a
fixed ``expires`` (see ``PROXY_EXPIRES``) — without one the items would be structurally
undeletable, and a Track B copy in our own bucket could never be reclaimed.

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
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

import httpx
import stac_auth
from pystac import Asset, Item, Link
from register_v1 import (
    DEFAULT_S3_GATEWAY,
    TIMESTAMPS_EXTENSION,
    add_alternate_s3_assets,
    add_derived_from_link,
    add_store_link,
    consolidate_reflectance_assets,
    fix_zarr_asset_media_types,
    remove_xarray_integration,
    upsert_item,
)
from s3_item_cleanup import format_expires
from storage_tier_utils import extract_region_from_endpoint
from update_stac_storage_tier import _build_storage_schemes, _tier_to_scheme_ref

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_SOURCE_STAC_API = "https://stac.core.eopf.eodc.eu"
DEFAULT_SOURCE_COLLECTION = "sentinel-2-l2a-zarr3"

# Only collections whose id carries the mode's marker may be written to. The proxy holds
# third-party data under an Explorer-looking id, and a mirror item reuses a prod item's
# id; a typo that aimed either at ``sentinel-2-l2a`` would overwrite real Explorer items
# (``upsert_item`` PUTs over an existing id).
COLLECTION_ID_MARKER = "samples-zarr3"
MIRROR_COLLECTION_ID_MARKER = "mirror-rstaging"

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

# Retention. Loïc, 2026-09-14: the proxy expires on **1 November 2026**.
#
# Without an `expires` these items are structurally undeletable — `cleanup_expired_items.
# evaluate_guards` returns `no_expires` first, before every other check — so a Track B copy
# in our own bucket could never be reclaimed by anything automated. A fixed date, not
# `now + N days`: the proxy answers coordination#287 once and the whole collection goes
# away with it, so re-registering an item must not push the date out.
#
# Track A items carry no S3 alternate (the stores are EODC's), so even a cron pointed at
# them would skip each one as `no_s3_urls` — data assets, no s3:// URL — and never delete
# it: remove them by deleting the collection, never by item id (the ids are also prod
# `sentinel-2-l2a` ids). For Track B this makes the cleanup *possible*; the cron is
# `--collection` scoped and does not target these collections today.
PROXY_EXPIRES = datetime(2026, 11, 1, tzinfo=UTC)

# What a finished proxy item must advertise. Checked before the item is written, because
# every other guard here is a warning and the render links name `reflectance` outright.
EXPECTED_ASSET_KEYS = frozenset({"reflectance", *ROOT_HREF_ASSETS})

# Links that only make sense in the source catalogue. ``collection`` is re-added
# pointing at the proxy collection: the STAC item schema refuses a ``collection``
# field without a matching link, so dropping it outright makes the item invalid.
DROPPED_LINK_RELS = frozenset({"root", "self", "parent", "collection", "alternate"})

# What a mirror item keeps of its prod source's links. A keep-list, not a drop-list: the
# rest are the source catalogue's, the /raster render links the /rstaging ones replace,
# or the Explorer `via` that 404s (see add_proxy_visualization) — and a rel prod gains
# later is dropped rather than copied blind. `derived_from` is the EODC lineage.
MIRROR_KEPT_LINK_RELS = frozenset({"store", "cite-as", "license", "derived_from"})

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
    """Replace the atmosphere/mask group assets with root-href AOT/WVP/SCL assets.

    Raises rather than skipping: the asset set IS coordination#287's criterion-1
    acceptance condition, so an item registered with two of its four assets would be
    indistinguishable from a passing one in both the exit code and the evidence file.
    """
    root_href = root.rstrip("/")
    built = {}
    for key, (source_key, band_name, title) in ROOT_HREF_ASSETS.items():
        source = item.assets.get(source_key)
        if source is None:
            raise ValueError(f"{item.id}: no {source_key} asset to build {key} from")
        fields = deepcopy(source.extra_fields)
        source_bands = fields.get("bands", [])
        bands = [b for b in source_bands if b.get("name") == band_name]
        if source_bands and not bands:
            # Failure-open here is worse than no asset: AOT and WVP are cut from the same
            # source group, so an unmatched filter leaves each one advertising BOTH bands.
            raise ValueError(
                f"{item.id}: {source_key} has no {band_name!r} band "
                f"(has {[b.get('name') for b in source_bands]}) — cannot build {key}"
            )
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

    # Allowlist, not a denylist. Any source asset this transform does not know about
    # would otherwise be proxied verbatim on a per-group href that cannot be opened over
    # HTTPS — and a source `quicklook` would reintroduce the thumbnail we deliberately do
    # not publish. EODC republished 220 items on 2026-09-10; the source schema is not
    # frozen, so this has to be failure-closed. `reflectance` is built upstream by
    # `consolidate_reflectance_assets`; everything else here is rebuilt above.
    dropped = sorted(set(item.assets) - {"reflectance"})
    item.assets = {key: item.assets[key] for key in ("reflectance",) if key in item.assets}
    item.assets.update(built)
    if dropped:
        logger.info(f"   🗑️  Dropped {len(dropped)} unproxied source asset(s): {', '.join(dropped)}")


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
    # A spec-legal `proj:bbox` is 4 OR 6 numbers; the 6-element form interleaves height,
    # so slicing the first four would read [west, south, min-height, east].
    x_min, y_min, x_max, y_max = (
        (bbox[0], bbox[1], bbox[3], bbox[4]) if len(bbox) == 6 else bbox[:4]
    )
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
    """Add the viz links in the 0.12 notation.

    **No ``via`` link.** ``register_v1`` points it at
    ``{EXPLORER_BASE}/collections/<c>/items/<id>``, which 404s: the Explorer is a static
    site with no collection or item pages (measured 2026-09-23).

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


def stamp_proxy_expires(item: Item) -> None:
    """Stamp the fixed proxy expiry so the retention cron can select these items."""
    item.properties["expires"] = format_expires(PROXY_EXPIRES)
    if TIMESTAMPS_EXTENSION not in item.stac_extensions:
        item.stac_extensions.append(TIMESTAMPS_EXTENSION)


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


def storage_to_v2(item: Item, s3_endpoint: str) -> None:
    """Move the S3 alternates' storage metadata to the storage extension v2 layout.

    ``add_alternate_s3_assets`` writes a legacy inline ``storage:scheme`` per alternate
    but declares storage v2, whose schema requires item-level ``storage:schemes`` and
    per-alternate ``storage:refs``. Prod items get converted later by
    ``update_stac_storage_tier.py``; proxy items never pass through it, so without this
    they fail STAC validation (measured on the live -ovh items, 2026-09-23).
    """
    alternates = [
        asset.extra_fields["alternate"]["s3"]
        for asset in item.assets.values()
        if isinstance(asset.extra_fields.get("alternate", {}).get("s3"), dict)
    ]
    # One store root, so one bucket; the unpack refuses anything else.
    (bucket,) = {urlparse(s3["href"]).netloc for s3 in alternates}
    # The builder names prod's bucket; the Track B copies live elsewhere.
    schemes = _build_storage_schemes(extract_region_from_endpoint(s3_endpoint))
    for scheme in schemes.values():
        scheme["bucket"] = bucket
    item.properties["storage:schemes"] = schemes
    for s3 in alternates:
        legacy = s3.pop("storage:scheme", {})
        s3["storage:refs"] = [_tier_to_scheme_ref(legacy.get("tier"), None)]


def source_self_href(source_item: dict) -> str:
    """The source item's ``self`` href — the provenance link, read before it is dropped."""
    self_href = next(
        (link["href"] for link in source_item.get("links", []) if link.get("rel") == "self"),
        None,
    )
    if not self_href:
        raise ValueError(f"{source_item.get('id')}: source item has no self link")
    return str(self_href)


def collection_link(stac_api_url: str, collection: str) -> Link:
    """The ``collection`` link — the item schema refuses a ``collection`` field without it."""
    return Link(
        "collection",
        f"{stac_api_url.rstrip('/')}/collections/{collection}",
        "application/json",
        collection,
    )


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
    self_href = source_self_href(source_item)

    # from_dict deep-copies, so the caller's dict is never mutated. Stripping the
    # source catalogue's links (self included) is also what keeps to_dict() offline:
    # pystac would otherwise resolve root/parent over the network.
    item = Item.from_dict(source_item)
    item.links = [link for link in item.links if link.rel not in DROPPED_LINK_RELS]
    item.collection_id = collection
    item.add_link(collection_link(stac_api_url, collection))

    root = rebase_store_root(item, store_root_base) if store_root_base else store_root(item)

    fix_zarr_asset_media_types(item)
    add_store_link(item, root)
    consolidate_reflectance_assets(item, root)
    fill_cube_extent(item)
    build_root_href_assets(item, root)
    remove_xarray_integration(item)
    reconcile_extensions(item)

    # Nothing above fails loudly if the source drifts, and the render links below name
    # `reflectance` unconditionally, so check the advertised set before it is written:
    # `consolidate_reflectance_assets` only recognises SR_*/B??_<res> source keys and
    # otherwise just logs. EODC republished 220 items on 2026-09-10; the schema is not
    # frozen.
    missing = EXPECTED_ASSET_KEYS - set(item.assets)
    if missing:
        raise ValueError(f"{item.id}: proxy item is missing asset(s) {sorted(missing)}")

    stamp_proxy_expires(item)
    add_proxy_visualization(item, raster_api_url, collection)
    add_derived_from_link(item, self_href)

    if s3_endpoint:
        # `https_to_s3` returns None for every host it is not told about, silently, so
        # pass the gateway the Track B hrefs actually use: zero alternates would make
        # `s3_item_cleanup` record `no_s3_urls` (not a FAILURE_STATUS) and skip the store
        # forever. Path-style (`<host>/<bucket>/<key>`) is what both our gateways serve; a
        # virtual-hosted root is the one shape `https_to_s3` parses unaided, and naming its
        # host as the gateway would make it read the bucket out of the path instead.
        parsed = urlparse(root)
        gateway = (
            DEFAULT_S3_GATEWAY if ".s3." in parsed.netloc else f"{parsed.scheme}://{parsed.netloc}"
        )
        added = add_alternate_s3_assets(item, s3_endpoint, gateway)
        if not added:
            raise ValueError(
                f"{item.id}: --s3-endpoint produced no alternate.s3 href for any asset "
                f"(store root {root!r}). Registering it would create items no deleter "
                f"can ever select."
            )
        slash_bare_zarr_alternates(item)
        storage_to_v2(item, s3_endpoint)

    return item


def build_mirror_item(
    source_item: dict, collection: str, raster_api_url: str, stac_api_url: str
) -> Item:
    """Copy an Explorer item into a mirror item whose render links target ``/rstaging``.

    The assets stay as published — our own stores — and none of the EODC-shaped steps of
    ``build_proxy_item`` run: ``build_root_href_assets`` would repoint SCL at the store
    root, which 500s on our stores (data-model#262). What goes is everything a deleter
    could use to find those stores, which prod still owns: with no ``alternate.s3`` and
    HTTPS hrefs, ``extract_s3_urls_from_item`` finds nothing, so deleting a mirror item
    removes the STAC record only. ``update_stac_storage_tier.py --add-missing`` would
    re-derive the alternates from the hrefs — never run it on a mirror collection.

    Pure, like ``build_proxy_item``: no network access.
    """
    self_href = source_self_href(source_item)

    item = Item.from_dict(source_item)
    item.links = [link for link in item.links if link.rel in MIRROR_KEPT_LINK_RELS]
    item.collection_id = collection
    item.add_link(collection_link(stac_api_url, collection))
    # STAC best practices: a copy of another STAC item points back at it with `canonical`.
    item.add_link(Link("canonical", self_href, "application/geo+json"))

    item.assets.pop("thumbnail", None)  # none on the proxy either (Loïc, 2026-09-11)
    for asset in item.assets.values():
        asset.extra_fields.pop("alternate", None)
    item.properties.pop("storage:schemes", None)
    # Not `reconcile_extensions`: it also declares datacube, which prod does not, and our
    # reflectance asset then fails the datacube v2.3.0 schema (validated 2026-09-23).
    item.stac_extensions = [
        ext for ext in item.stac_extensions if not ext.startswith(DROPPED_EXTENSION_PREFIXES)
    ]

    missing = EXPECTED_ASSET_KEYS - set(item.assets)
    if missing:
        raise ValueError(f"{item.id}: mirror item is missing asset(s) {sorted(missing)}")

    stamp_proxy_expires(item)
    add_proxy_visualization(item, raster_api_url, collection)
    return item


def read_item_ids(path: Path) -> list[str]:
    """Read item ids one per line, ignoring blanks and ``#`` comments."""
    return [
        ident for raw in path.read_text().splitlines() if (ident := raw.split("#", 1)[0].strip())
    ]


def fetch_source_item(source_stac_api: str, source_collection: str, item_id: str) -> dict:
    """GET one source item. A direct GET avoids depending on the source's conformance.

    Both path components are percent-encoded: ``--item-ids-file`` is operator-edited and
    httpx normalises RFC 3986 dot-segments, so an unquoted id of ``../../<other>/items/X``
    would silently retarget the GET at another collection — making ``--source-collection``
    no bound at all. ``quote(safe="")`` also stops ``#`` from truncating the URL.

    Redirects are NOT followed: the HTTPS check in ``main`` validates the URL the operator
    typed, and a 3xx could downgrade it to http or move it to another host, after which
    whatever came back would be registered as if it had been asked for.
    """
    url = (
        f"{source_stac_api.rstrip('/')}"
        f"/collections/{urllib.parse.quote(source_collection, safe='')}"
        f"/items/{urllib.parse.quote(item_id, safe='')}"
    )
    with httpx.Client(timeout=30.0, follow_redirects=False) as http:
        resp = http.get(url)
        resp.raise_for_status()
        source = dict(resp.json())
    # The id decides the filename written, the id registered and what --max-items counts.
    # Taking it from the response would let the source choose all three.
    returned = source.get("id")
    if returned != item_id:
        raise ValueError(f"{item_id}: source returned a different item id ({returned!r})")
    return source


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source-stac-api", default=DEFAULT_SOURCE_STAC_API)
    parser.add_argument("--source-collection", default=DEFAULT_SOURCE_COLLECTION)
    parser.add_argument("--collection", required=True, help="Target collection id")
    parser.add_argument(
        "--mirror-explorer",
        action="store_true",
        help="Mirror Explorer items into a *mirror-rstaging* collection (build_mirror_item)",
    )
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
    parser.add_argument(
        "--dry-run",
        type=Path,
        metavar="DIR",
        help=(
            "Write <id>.json here instead of registering. Not fully offline: with "
            "--s3-endpoint it still issues read-only S3 head_object calls, because the "
            "storage tier is part of the item being previewed."
        ),
    )
    parser.add_argument(
        "--store-root-base",
        metavar="URL",
        help="Track B: repoint each asset at <URL>/<store>.zarr (the OVH copies)",
    )
    parser.add_argument("--s3-endpoint", help="Track B only: add alternate.s3 to each asset")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    marker = MIRROR_COLLECTION_ID_MARKER if args.mirror_explorer else COLLECTION_ID_MARKER
    if marker not in args.collection or args.collection == args.source_collection:
        logger.error(
            "Refusing --collection %r: it must contain %r and differ from --source-collection",
            args.collection,
            marker,
        )
        return 1

    # Every URL that decides where data is read from or written to, not just the three
    # the operator types most often: --store-root-base rewrites every asset href.
    for url, name in [
        (args.source_stac_api, "--source-stac-api"),
        (args.raster_api_url, "--raster-api-url"),
        (args.stac_api_url, "--stac-api-url"),
        (args.store_root_base, "--store-root-base"),
        (args.s3_endpoint, "--s3-endpoint"),
    ]:
        if url is not None and urlparse(url).scheme != "https":
            logger.error("Error: %s must be an HTTPS URL, got: %r", name, url)
            return 1

    # Track B needs both. --store-root-base alone registers OVH stores with no alternate,
    # which no deleter can ever reclaim; --s3-endpoint alone derives alternates from EODC's
    # host, and `https_to_s3` then reads its first path segment (`collections`) as a bucket.
    if bool(args.store_root_base) != bool(args.s3_endpoint):
        logger.error("--store-root-base and --s3-endpoint go together (Track B) or not at all")
        return 1
    if args.mirror_explorer and args.store_root_base:
        logger.error("--mirror-explorer keeps the source's asset hrefs: no Track B flags")
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

    failed: list[str] = []
    for item_id in item_ids:
        try:
            source = fetch_source_item(args.source_stac_api, args.source_collection, item_id)
            if args.mirror_explorer:
                item = build_mirror_item(
                    source, args.collection, args.raster_api_url, args.stac_api_url
                )
            else:
                item = build_proxy_item(
                    source,
                    args.collection,
                    args.raster_api_url,
                    args.stac_api_url,
                    store_root_base=args.store_root_base,
                    s3_endpoint=args.s3_endpoint,
                )
            if client is None:
                # `item_id`, never `item.id`: the id names a file under --dry-run, and a
                # path-bearing id would write outside that directory.
                out = args.dry_run / f"{item_id}.json"
                out.write_text(json.dumps(item.to_dict(), indent=2))
                logger.info(f"   📄 {out}")
            else:
                upsert_item(client, args.collection, item)
        except Exception as exc:  # noqa: BLE001 - one bad item must not hide the rest
            failed.append(item_id)
            logger.error("   ❌ %s: %s", item_id, exc)
            continue

    # Without this, a mid-run failure leaves a partially populated collection and no
    # record of which ids landed — the operator cannot tell a clean run from a torn one.
    registered = len(item_ids) - len(failed)
    logger.info("Registered %d/%d item(s) into %s", registered, len(item_ids), args.collection)
    if failed:
        logger.error("Failed (%d): %s", len(failed), ", ".join(failed))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
