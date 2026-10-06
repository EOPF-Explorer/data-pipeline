#!/usr/bin/env python3
"""Register proxy STAC items for the Samples Service Sentinel-2 GeoZarr stores.

coordination#287. The Samples Service publishes cpm_v300 Zarr v3 stores on
``data.eodc.eu`` with a STAC catalogue (``stac.core.eopf.eodc.eu``) whose items do
not carry the Explorer's asset layout or visualization links. This script clones
those source items into a *proxy* collection on the Explorer STAC API so the
Explorer's TiTiler, STAC browser and eodash can be pointed at Samples Service data
without copying it.

``--items-json`` is the **pipeline mode** that publishes new Sentinel-2 scenes into
``sentinel-2-l2a-new`` once our conversion stops (plan rev 3, 2026-09-30): it registers the ids
``query_stac.py discover`` wrote, **create-only** (a 409 is ``exists``, never a PUT), appends each
201 to ``--created-ids`` (the rollback input), and refuses old-generation sources. What a
run may do is decided by the target collection, never by a flag (see ``TARGET_RUNS``):
into ``sentinel-2-l2a-new`` only this mode may write, and without an ``expires``; our
converted archive, ``sentinel-2-l2a``, takes no run at all.

It deliberately does NOT convert or upload anything: ``build_proxy_item`` is a pure
dict-in/Item-out transform, and the only write is the STAC upsert (a create in pipeline
mode). Outside ``sentinel-2-l2a-new`` it stamps a fixed ``expires`` (see ``PROXY_EXPIRES``) —
without one the items would be structurally undeletable.

Render host is ``/rstaging`` (titiler-eopf **0.12.0**), not ``/raster`` (0.11.0): only
0.12.0 serves the ``assets=<key>|bands=…`` / ``|variables=…`` notation these items use,
and only 0.12.0 is being migrated to. See ``rgb_query``/``add_proxy_visualization``.

Asset mapping (see ``evidence-proxy-T1.md``, reader gate run 2026-09-11 on ``/raster``
0.11). The last column is the reader-gate probe, not the render contract: the items'
own links use the ``assets=<key>|bands=…`` form (``RGB_QUERY``), because ``/rstaging``
0.12 answers the repeated ``variables=`` form with a 422.

==========  ================================  ===========================================
key         href                              reader-gate probe (/raster 0.11)
==========  ================================  ===========================================
reflectance ``<store>/measurements/reflect…``  ``variables=/measurements/reflectance:b04``
AOT_10m     ``<store>/`` (store root)          ``variables=/quality/atmosphere/r10m:aot``
WVP_10m     ``<store>/`` (store root)          ``variables=/quality/atmosphere/r10m:wvp``
SCL_20m     ``<store>/`` (store root)          ``…/l2a_classification/r20m:scl``
==========  ================================  ===========================================

The three non-reflectance assets point at the **store root** because EODC only
consolidates metadata at the root and on ``measurements/reflectance`` — the
``quality/atmosphere`` and ``conditions/mask`` group hrefs cannot be opened over
HTTPS (same defect data-pipeline#412 fixes for our own stores). That root ``href`` is
**bare** (``…/X.zarr``): titiler's ``GeoZarrReader`` concatenates and 404s on
``…/X.zarr//zarr.json`` if it ends in a slash (measured 2026-09-11).
"""

import argparse
import json
import logging
import os
import sys
import urllib.parse
from collections import Counter
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic
from urllib.parse import urlparse

import httpx
import requests
import stac_auth
from pystac import Asset, Item, Link
from pystac_client import Client
from register_v1 import (
    TIMESTAMPS_EXTENSION,
    add_derived_from_link,
    add_store_link,
    consolidate_reflectance_assets,
    fix_zarr_asset_media_types,
    remove_xarray_integration,
    upsert_item,
)
from s3_item_cleanup import extract_s3_urls_from_item, format_expires
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_SOURCE_STAC_API = "https://stac.core.eopf.eodc.eu"
DEFAULT_SOURCE_COLLECTION = "sentinel-2-l2a-zarr3"

# The user-facing, temporary collection the pipeline publishes EODC-hosted scenes into
# (plan rev 3). Its items carry no ``expires`` (see ``PROXY_EXPIRES``).
TRANSITION_COLLECTION = "sentinel-2-l2a-new"

# Target collection -> the kinds of run allowed to write it (``run_kind``). Exact ids, not
# a marker substring: every item written here reuses a prod ``sentinel-2-l2a`` id, and a
# near miss (``sentinel-2-l2a-staging``, ``…-samples-zarr3x``) would pass a substring check.
# ``sentinel-2-l2a`` itself, our converted archive, is absent: no run may write it (plan
# rev 3, R1). Into ``TRANSITION_COLLECTION`` only the pipeline may write, because it is the
# one that never PUTs (``upsert_item`` replaces an existing id). ``-rollback`` is the
# scratch collection the pipeline's rollback is exercised in (plan rev 2, T10).
TARGET_RUNS = {
    TRANSITION_COLLECTION: {"pipeline"},
    "sentinel-2-l2a-samples-zarr3": {"track-a", "pipeline"},
    "sentinel-2-l2a-samples-zarr3-rollback": {"track-a", "pipeline"},
}

# A run stops attempting ids after this many failures in a row: a source or target outage
# would otherwise spend ~1.5 min per id (three timed-out GETs) across a 1,000-id list. The
# ids it did not attempt are counted as failed, so the run exits 1 and a later window
# (the daily catch-up) registers them.
MAX_CONSECUTIVE_FAILURES = 10

# --time-budget ceiling: the register step's 14400 s pod backstop (platform-deploy
# templates/eopf-eodc-register-job.yaml) minus 30 min for pod start and the id in flight.
MAX_TIME_BUDGET_SECONDS = 12_600

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
# evaluate_guards` returns `no_expires` first, before every other check. A fixed date, not
# `now + N days`: the proxy answers coordination#287 once and the whole collection goes
# away with it, so re-registering an item must not push the date out.
#
# Proxy items carry no S3 alternate (the stores are EODC's), so even a cron pointed at
# them would skip each one as `no_s3_urls` — data assets, no s3:// URL — and never delete
# it: remove them by deleting the collection, never by item id (the ids are also prod
# `sentinel-2-l2a` ids). `main` refuses to stamp this date once it has passed: the item
# would be born expired.
#
# Items registered into `sentinel-2-l2a-new` get no `expires` at all (plan rev 2 D2, rev 3
# R3): they are the Explorer's user-facing scenes, which no retention job may select, and
# this date would stop the pipeline on 1 Nov, when `main` starts refusing it.
PROXY_EXPIRES = datetime(2026, 11, 1, tzinfo=UTC)

# What a finished proxy item must advertise. Checked before the item is written, because
# every other guard here is a warning and the render links name `reflectance` outright.
EXPECTED_ASSET_KEYS = frozenset({"reflectance", *ROOT_HREF_ASSETS})

# What a proxy item keeps of its source's links. A keep-list, like the asset prune: the
# rest are the source catalogue's (root/self/parent/alternate), re-added here
# (``collection``, which the item schema requires, re-pointed at the proxy collection), or a
# rel EODC adds later, e.g. its own render links, which would then sit ahead of ours. The
# source schema is not frozen.
SOURCE_KEPT_LINK_RELS = frozenset({"cite-as", "license"})

EO_EXTENSION = "https://stac-extensions.github.io/eo/v2.0.0/schema.json"
RASTER_EXTENSION = "https://stac-extensions.github.io/raster/v2.0.0/schema.json"
DATACUBE_EXTENSION = "https://stac-extensions.github.io/datacube/v2.3.0/schema.json"
REQUIRED_EXTENSIONS = (EO_EXTENSION, RASTER_EXTENSION, DATACUBE_EXTENSION)

# Dropped: a proxy item carries no S3 alternate (see ``assert_no_s3_urls``).
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
    # HTTPS — and a source `quicklook` would sit next to the thumbnail we render ourselves
    # (`add_proxy_visualization`). EODC republished 220 items on 2026-09-10; the source
    # schema is not frozen, so this has to be failure-closed. `reflectance` is built
    # upstream by `consolidate_reflectance_assets`; everything else here is rebuilt above.
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
    """Add the viz links and the ``thumbnail`` asset in the 0.12 notation.

    **No ``via`` link.** ``register_v1`` points it at
    ``{EXPLORER_BASE}/collections/<c>/items/<id>``, which 404s: the Explorer is a static
    site with no collection or item pages (measured 2026-09-23).

    The ``thumbnail`` is what a STAC browser shows as the item's preview, in the item list
    and the collection's "Thumbnails" view; without it the proxy items showed footprints
    only, next to prod's previews. Same true colour as prod's (``/preview``), rendered by
    ``raster_api_url`` like the links, but capped at 512 px as WebP: ~80 KB and ~1 s uncached
    on ``/rstaging``, against ~1.8 MB and ~3 s at the default size as PNG (one item, measured
    2026-09-30). A browser page of items still costs one render per item until the render
    cache holds them. Not pre-rendered like ``register_v1``'s
    ``warm_thumbnail_cache``: that cache's TTL is 1 h on ``/rstaging`` (platform-deploy
    ``hr-titiler-eopf-test.yaml``) and EODC publishes in one 00:00-07:30Z burst, so a
    preview warmed at registration would mostly expire before anyone browses.

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
    item.add_asset(
        "thumbnail",
        Asset(
            href=f"{base}/preview?format=webp&max_size=512&{RGB_QUERY}",
            media_type="image/webp",
            roles=["thumbnail"],
            title="Sentinel-2 L2A True Color Preview",
        ),
    )


def set_expires(item: Item, expires: datetime | None) -> None:
    """Stamp ``expires`` so the retention cron can select the item, or, for ``None``, remove it.

    Removed rather than left alone: a source ``expires`` is the source's retention, not ours,
    and an item in ``TRANSITION_COLLECTION`` must carry none (see ``PROXY_EXPIRES``).
    """
    if expires is None:
        item.properties.pop("expires", None)
        return
    item.properties["expires"] = format_expires(expires)
    if TIMESTAMPS_EXTENSION not in item.stac_extensions:
        item.stac_extensions.append(TIMESTAMPS_EXTENSION)


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
    *,
    expires: datetime | None,
) -> Item:
    """Transform a source Samples Service STAC item dict into a proxy item.

    Pure: no network access, no catalogue resolution. ``expires`` has no default:
    ``TRANSITION_COLLECTION`` gets ``None``, and a forgotten argument must not stamp the
    proxy's date onto one of its items.
    """
    self_href = source_self_href(source_item)

    # from_dict deep-copies, so the caller's dict is never mutated. Stripping the
    # source catalogue's links (self included) is also what keeps to_dict() offline:
    # pystac would otherwise resolve root/parent over the network.
    # Fail closed on projection drift, like the missing-asset check below: without an
    # item-level proj:code, consolidate_reflectance_assets silently falls back to
    # EPSG:32632, a wrong CRS on an item that still validates (the T28RBS precedent).
    if "proj:code" not in source_item.get("properties", {}):
        raise ValueError(f"{source_item.get('id')}: source item has no item-level proj:code")

    item = Item.from_dict(source_item)
    item.links = [link for link in item.links if link.rel in SOURCE_KEPT_LINK_RELS]
    item.collection_id = collection
    item.add_link(collection_link(stac_api_url, collection))

    root = store_root(item)

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
    # `fill_cube_extent` returns silently without a proj:bbox, and the datacube extension
    # `reconcile_extensions` declares requires the x/y extents.
    dimensions = item.assets["reflectance"].extra_fields.get("cube:dimensions") or {}
    if any("extent" not in dimensions.get(axis, {}) for axis in ("x", "y")):
        raise ValueError(f"{item.id}: reflectance cube:dimensions x/y have no extent")

    set_expires(item, expires)
    add_proxy_visualization(item, raster_api_url, collection)
    add_derived_from_link(item, self_href)
    return item


def assert_no_s3_urls(item: Item) -> None:
    """Refuse an item that advertises an S3 location a deleter could act on wrongly.

    Its stores are EODC's, and the cleanup cron's default bucket is prod's.
    """
    urls = extract_s3_urls_from_item(item.to_dict(transform_hrefs=False))
    if urls:
        raise ValueError(f"{item.id}: advertises S3 location(s) {sorted(urls)[:2]}")


def is_old_generation(source_item: dict) -> bool:
    """True if a source ``SR_*`` asset, or one of its bands, carries ``raster:scale``/``offset``.

    The marker of the pre-N0513 EODC generation (a 9 Sep item has it; 600/600 N0513 items
    sampled 2026-09-28 have none, their scaling lives in the stores' CF attributes). A mixed
    user-facing collection means double scaling and black nodata, so the pipeline refuses these.
    """
    return any(
        field in fields
        for key, asset in source_item.get("assets", {}).items()
        if key.startswith("SR_")
        for fields in (asset, *asset.get("bands", []))
        for field in ("raster:scale", "raster:offset")
    )


def create_item(client: Client, collection_id: str, item: Item) -> str:
    """POST one item: ``created``, or ``exists`` on a 409. Never a PUT, unlike ``upsert_item``.

    A PUT on 409 would replace whatever the target already holds under that id: an item an
    earlier run registered, or the test collection's comparison items. An existing item is
    left exactly as it is. ``transform_hrefs=False`` for the reason ``upsert_item``
    gives.
    """
    io = client._stac_io
    assert io is not None  # noqa: S101  # nosec B101 -- pystac-client always sets this after open()
    resp = io.session.post(
        f"{str(client.self_href).rstrip('/')}/collections/{collection_id}/items",
        json=item.to_dict(transform_hrefs=False),
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    if resp.status_code == 409:
        logger.info(f"   ⏭️  {item.id} exists (HTTP 409), left unchanged")
        return "exists"
    resp.raise_for_status()
    logger.info(f"✅ Created {item.id} (HTTP {resp.status_code})")
    return "created"


def read_item_ids(path: Path) -> list[str]:
    """Read item ids one per line, ignoring blanks and ``#`` comments."""
    return [
        ident for raw in path.read_text().splitlines() if (ident := raw.split("#", 1)[0].strip())
    ]


def source_item_url(source_stac_api: str, source_collection: str, item_id: str) -> str:
    """Where one source item is fetched, and so where discover must have found it.

    Both path components are percent-encoded: ``--item-ids-file`` is operator-edited and
    httpx normalises RFC 3986 dot-segments, so an unquoted id of ``../../<other>/items/X``
    would silently retarget the GET at another collection — making ``--source-collection``
    no bound at all. ``quote(safe="")`` also stops ``#`` from truncating the URL.
    """
    return (
        f"{source_stac_api.rstrip('/')}"
        f"/collections/{urllib.parse.quote(source_collection, safe='')}"
        f"/items/{urllib.parse.quote(item_id, safe='')}"
    )


def read_items_json(
    path: Path, collection: str, source_stac_api: str, source_collection: str
) -> list[str]:
    """Read the ids from ``query_stac.py discover``'s ``items.json``.

    Each row names the collection discover deduplicated against, and the ``source_url`` it
    found the item at. A row for another collection means the list was made for a different
    target, and its dedup proves nothing here. A row found anywhere but ``source_item_url``
    was discovered on another source than the one each id is fetched from. Exact equality
    with EODC's self links: if they ever change form, every run is refused, loudly.
    """
    rows = json.loads(path.read_text())
    if not isinstance(rows, list) or not all(
        isinstance(row, dict) and isinstance(row.get("item_id"), str) for row in rows
    ):
        raise ValueError(f"{path} is not a list of rows with a string item_id")
    wrong = sorted({str(row.get("collection")) for row in rows} - {collection})
    if wrong:
        raise ValueError(f"{path} has rows for {wrong}, not --collection {collection!r}")
    elsewhere = [
        (row["item_id"], row.get("source_url"))
        for row in rows
        if row.get("source_url")
        != source_item_url(source_stac_api, source_collection, row["item_id"])
    ]
    if elsewhere:
        raise ValueError(
            f"{path}: {len(elsewhere)} row(s) not found at "
            f"{source_item_url(source_stac_api, source_collection, '<id>')}, e.g. {elsewhere[0]}"
        )
    return [row["item_id"] for row in rows]


class CreatedIdsError(Exception):
    """The created-ids list cannot be written: ``main`` stops the run, it does not skip the id."""


def record_created(path: Path, collection: str, item_id: str, **extra: bool) -> None:
    """Append one line to the created-ids list, at once: it is what a rollback deletes.

    A failed append ends the run (``CreatedIdsError``): every later create would go
    unrecorded too, and the error message is then the only record of an item that may
    exist. It can leave a torn last line, which a rollback reader must expect.
    """
    line = {"id": item_id, "collection": collection, "ts": datetime.now(UTC).isoformat(), **extra}
    try:
        with path.open("a") as out:
            out.write(json.dumps(line) + "\n")
    except OSError as exc:
        raise CreatedIdsError(
            f"cannot record {item_id} in {path} ({exc}); it may exist in {collection} "
            "with no created-ids line"
        ) from exc


def _is_transient(exc: BaseException) -> bool:
    """A timeout, a dropped connection, a 429 or a 5xx is worth another GET; the rest are answers.

    The waits (2 s, 4 s) outlast short bursts only. A sustained 429 fails the ids, 10 in a row
    stop the run, and the daily catch-up retries them (hourly windows do not overlap).
    """
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        return status >= 500 or status == 429
    return isinstance(exc, httpx.TransportError)


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, max=30),
    reraise=True,
)
def fetch_source_item(source_stac_api: str, source_collection: str, item_id: str) -> dict:
    """GET one source item. A direct GET avoids depending on the source's conformance.

    The URL is ``source_item_url``'s, percent-encoded. Redirects are NOT followed: the HTTPS
    check in ``main`` validates the URL the operator typed, and a 3xx could downgrade it to
    http or move it to another host, after which whatever came back would be registered as
    if it had been asked for.

    Up to three attempts on a transient error (``_is_transient``): EODC does return 502s
    (see ``upsert_item``), and one must not turn an hourly run red.
    """
    url = source_item_url(source_stac_api, source_collection, item_id)
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


def _seconds(value: str) -> float:
    """argparse type for --time-budget: a stop that can actually fire, and fire first.

    `nan` would compare False forever, and a budget within 30 min of the register step's
    14400 s pod backstop (templates/eopf-eodc-register-job.yaml in platform-deploy) would let
    the kill, which can land between a create and its created-ids line, stop the run instead.
    The range check refuses both, `nan` and `inf` included (every comparison with `nan` is
    False). Unlike cleanup_expired_items._budget_seconds, an empty value is refused, not
    read as "no budget": the template always passes one, and a run with no stop is exactly
    what this flag exists to prevent.
    """
    seconds = float(value)
    if not 0 < seconds <= MAX_TIME_BUDGET_SECONDS:
        raise argparse.ArgumentTypeError(
            f"must be seconds in (0, {MAX_TIME_BUDGET_SECONDS}], got {value!r}"
        )
    return seconds


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source-stac-api", default=DEFAULT_SOURCE_STAC_API)
    parser.add_argument("--source-collection", default=DEFAULT_SOURCE_COLLECTION)
    parser.add_argument("--collection", required=True, help="Target collection id")
    parser.add_argument("--stac-api-url", required=True, help="Target STAC API")
    parser.add_argument("--raster-api-url", required=True, help="TiTiler base URL for links")
    parser.add_argument("--item-id", action="append", default=[], help="Repeatable")
    parser.add_argument("--item-ids-file", type=Path, help="One id per line, # comments ignored")
    parser.add_argument(
        "--items-json",
        type=Path,
        metavar="PATH",
        help="Pipeline mode: register the ids in query_stac.py discover's items.json, create-only",
    )
    parser.add_argument(
        "--created-ids",
        type=Path,
        metavar="PATH",
        help="Pipeline mode (required): append one JSON line per created item, the rollback input",
    )
    parser.add_argument(
        "--time-budget",
        type=_seconds,
        metavar="SECONDS",
        help=(
            "Start no new id after this many seconds; the rest count as failed. It stops "
            "between ids, where a pod deadline could kill a create before its created-ids line"
        ),
    )
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
        help="Write <id>.json here instead of registering",
    )
    return parser.parse_args(argv)


def run_kind(args: argparse.Namespace) -> str:
    """The kind of run the flags ask for, as ``TARGET_RUNS`` names it."""
    return "pipeline" if args.items_json else "track-a"


def register_one(
    args: argparse.Namespace,
    client: Client | None,
    expires: datetime | None,
    item_id: str,
) -> str:
    """Fetch, build and write one id. Returns its outcome, a key of ``main``'s counts."""
    source = fetch_source_item(args.source_stac_api, args.source_collection, item_id)
    if args.items_json and is_old_generation(source):
        # Content-based, so a retry cannot succeed: counted, not failed (T18 alerts on it).
        logger.warning("   ⚠️  %s: old-generation source (raster:scale on SR_*), refused", item_id)
        return "refused_generation"
    item = build_proxy_item(
        source, args.collection, args.raster_api_url, args.stac_api_url, expires=expires
    )
    assert_no_s3_urls(item)
    if client is None:
        # A plain name (refused otherwise in main), and fetch_source_item has already made
        # sure the source did not substitute another id.
        out = args.dry_run / f"{item_id}.json"
        out.write_text(json.dumps(item.to_dict(), indent=2))
        logger.info(f"   📄 {out}")
        return "written"
    if not args.items_json:
        upsert_item(client, args.collection, item)
        return "registered"
    # The created-ids list, not the item's own `created` (EODC's timestamp, copied from the
    # source), is what tells a rollback which items this run created.
    try:
        outcome = create_item(client, args.collection, item)
    except requests.RequestException as exc:
        # A timeout, a reset or a 5xx can arrive after the server committed the item, and the
        # next run's 409 would then hide it from every list. Record it as uncertain. A line
        # can be false (an error before anything was sent), and its id may name an item this
        # run did not create, so a rollback must re-check each item's CONTENT before deleting
        # it (EODC data hrefs, no alternate.s3), never just that it exists.
        if exc.response is None or exc.response.status_code >= 500:
            record_created(args.created_ids, args.collection, item.id, uncertain=True)
        raise
    if outcome == "created":
        record_created(args.created_ids, args.collection, item.id)
    return outcome


def log_summary(collection: str, counts: Counter[str]) -> None:
    """The pipeline's one-line result: fixed keys, for the logs and the alerts to read."""
    keys = ("created", "exists", "refused_generation", "failed")
    summary = " ".join(f"{key}={counts[key]}" for key in keys)
    logger.info("Summary for %s: %s", collection, summary)


def main(argv: list[str] | None = None) -> int:
    # The time budget counts from here, not from the first id, so it stays close to the pod's
    # own clock: opening the target client has no timeout.
    started = monotonic()
    args = parse_args(argv)

    # Every URL that decides where data is read from or written to.
    for url, name in [
        (args.source_stac_api, "--source-stac-api"),
        (args.raster_api_url, "--raster-api-url"),
        (args.stac_api_url, "--stac-api-url"),
    ]:
        if urlparse(url).scheme != "https":
            logger.error("Error: %s must be an HTTPS URL, got: %r", name, url)
            return 1

    if args.items_json and (args.item_id or args.item_ids_file):
        logger.error(
            "--items-json builds Track A items from that file's ids alone: no --item-id or "
            "--item-ids-file"
        )
        return 1
    # Without the list a pipeline run could not be rolled back: the items' `created` is
    # EODC's timestamp. Outside the pipeline an upsert cannot tell a create from a replace.
    if bool(args.items_json) != bool(args.created_ids):
        logger.error("--items-json and --created-ids go together")
        return 1
    # Each target takes only the runs TARGET_RUNS lists, all with the same item ids: e.g.
    # a Track A run aimed at TRANSITION_COLLECTION would PUT over the pipeline's items.
    kind = run_kind(args)
    runs = TARGET_RUNS.get(args.collection, set())
    if kind not in runs or args.collection == args.source_collection:
        logger.error(
            "Refusing a %s run into --collection %r: it takes %s, and must differ from "
            "--source-collection",
            kind,
            args.collection,
            sorted(runs) or "none",
        )
        return 1

    # By target, not by flag: TRANSITION_COLLECTION items carry none (see PROXY_EXPIRES). So
    # the pipeline keeps running there after 1 Nov, when this refusal stops every other run.
    expires = None if args.collection == TRANSITION_COLLECTION else PROXY_EXPIRES
    if expires is not None and datetime.now(UTC) >= expires:
        logger.error(
            "Refusing to run: the fixed proxy expiry %s has passed, so every item would be "
            "registered already expired",
            format_expires(expires),
        )
        return 1

    if args.items_json:
        try:
            item_ids = read_items_json(
                args.items_json, args.collection, args.source_stac_api, args.source_collection
            )
        except (OSError, ValueError) as exc:
            logger.error("--items-json: %s", exc)
            return 1
    else:
        item_ids = list(args.item_id)
        if args.item_ids_file:
            item_ids += read_item_ids(args.item_ids_file)
    # A duplicate would be fetched and written twice, and counted twice against the bound.
    item_ids = list(dict.fromkeys(item_ids))
    if not item_ids and args.items_json:
        # Most hourly windows: EODC publishes in one daily burst, 00:00-07:30Z.
        log_summary(args.collection, Counter())
        return 0
    if not item_ids:
        logger.error("No item ids: pass --item-id and/or --item-ids-file")
        return 1
    # An id names a file under --dry-run: one carrying a path could write elsewhere.
    not_plain = [ident for ident in item_ids if "/" in ident or ident.startswith(".")]
    if not_plain:
        logger.error("Refusing item ids that are not plain names: %s", not_plain[:3])
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
        if args.created_ids:
            # Fail here, not after the first create: an unwritable list would leave every
            # item of the run created but unrecorded.
            args.created_ids.parent.mkdir(parents=True, exist_ok=True)
            args.created_ids.touch()
        client = stac_auth.open_client(args.stac_api_url)
        logger.info("Target STAC API: %s", args.stac_api_url)

    counts: Counter[str] = Counter()
    failed: list[str] = []
    streak = 0
    for n, item_id in enumerate(item_ids):
        # Both stops fall between ids, never inside one: a create and its created-ids line
        # always complete together.
        out_of_time = args.time_budget is not None and monotonic() - started >= args.time_budget
        if streak == MAX_CONSECUTIVE_FAILURES or out_of_time:
            logger.error(
                "Stopping after %s: %d id(s) not attempted, counted as failed",
                f"the {args.time_budget:g} s time budget"
                if out_of_time
                else f"{streak} failures in a row",
                len(item_ids) - n,
            )
            failed += item_ids[n:]
            break
        try:
            counts[register_one(args, client, expires, item_id)] += 1
            streak = 0
        except CreatedIdsError as exc:
            # Every later create would go unrecorded too: stop, and say which id may exist.
            logger.error("Stopping: %s. %d id(s) not attempted", exc, len(item_ids) - n - 1)
            failed += item_ids[n:]
            break
        except Exception as exc:  # noqa: BLE001 - one bad item must not hide the rest
            failed.append(item_id)
            streak += 1
            logger.error("   ❌ %s: %s", item_id, exc)
    counts["failed"] = len(failed)

    # Without this, a mid-run failure leaves a partially populated collection and no
    # record of which ids landed — the operator cannot tell a clean run from a torn one.
    if args.items_json and not args.dry_run:
        log_summary(args.collection, counts)
    else:
        logger.info(
            "%s %d/%d item(s) for %s",
            "Wrote" if args.dry_run else "Registered",
            counts["written"] + counts["registered"],
            len(item_ids),
            args.collection,
        )
    if failed:
        logger.error("Failed (%d): %s", len(failed), ", ".join(failed))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
