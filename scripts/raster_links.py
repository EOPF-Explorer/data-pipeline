"""Visualization links in titiler-eopf 0.12's form: S2 L2A, S1 RTC (cubes, acquisitions), OLCI.

The one builder for them: ``register_v1`` and ``register_proxy`` use it to register S2 L2A items,
and ``operator-tools/migrate_item_api_links.py`` uses it to rewrite the items already in the
catalogue, so a migrated item and a newly registered one carry identical links. The S1 and OLCI
register code does not use it yet (its crons stay suspended until it does).

Plain dicts (STAC JSON), so the migration can compare them with stored items as is; register
code wraps them with ``Link.from_dict`` / ``Asset.from_dict``.
"""

import urllib.parse
from typing import Any

# True-colour query in the 0.12 notation: one `assets` parameter whose value carries the
# per-asset band selection, and no `bidx` (the item route ignores it). The two deployments
# are mirror images and neither accepts the other's form (measured 2026-09-11 against a prod
# S2 item on /collections/sentinel-2-l2a/items/…/preview.png):
#
#   notation                  /raster 0.11.0   /rstaging 0.12.0
#   assets=reflectance|bands=      500              200
#   repeated variables=            200              422
RGB_BANDS = ("b04", "b03", "b02")
RGB_ASSETS = f"reflectance|bands={','.join(RGB_BANDS)}"
S2_COLOR_FORMULA = "gamma rgb 1.3, sigmoidal rgb 6 0.1, saturation 1.2"
RGB_QUERY = (
    f"assets={urllib.parse.quote(RGB_ASSETS, safe='')}"
    f"&rescale={urllib.parse.quote('0,1', safe='')}"
    f"&color_formula={urllib.parse.quote(S2_COLOR_FORMULA, safe='')}"
)


# The titiler-eopf 0.11 form register_v1 wrote for sentinel-2 items up to v1.21.x, and still
# writes for L1C. Kept here once: the catalogue link migration recognises exactly this.
S2_LEGACY_QUERY = (
    "rescale=0%2C1&color_formula=gamma%20rgb%201.3%2C%20sigmoidal%20rgb%206%200.1%2C%20"
    "saturation%201.2&variables=%2Fmeasurements%2Freflectance%3Ab04"
    "&variables=%2Fmeasurements%2Freflectance%3Ab03"
    "&variables=%2Fmeasurements%2Freflectance%3Ab02&bidx=1"
)


def missing_rgb_bands(reflectance: dict[str, Any] | None) -> list[str]:
    """The bands these links select that a ``reflectance`` asset (its JSON fields) lacks; band
    names match case-insensitively. Empty means the links can render."""
    bands = (reflectance or {}).get("bands") or []
    names = {str(band.get("name", "")).lower() for band in bands if isinstance(band, dict)}
    return [band for band in RGB_BANDS if band not in names]


def _item_url(raster_api_url: str, collection: str, item_id: str) -> str:
    return f"{raster_api_url.rstrip('/')}/collections/{collection}/items/{item_id}"


def s2_true_colour_links(
    raster_api_url: str, collection: str, item_id: str
) -> list[dict[str, str]]:
    """The ``viewer``, ``xyz`` and ``tilejson`` links, in that order.

    The viewer is ``map.html``: 0.12 has no item ``/viewer`` route (404).
    """
    base = _item_url(raster_api_url, collection, item_id)
    return [
        {
            "rel": "viewer",
            "href": f"{base}/WebMercatorQuad/map.html?{RGB_QUERY}",
            "type": "text/html",
            "title": f"Viewer for {item_id}",
        },
        {
            "rel": "xyz",
            "href": f"{base}/tiles/WebMercatorQuad/{{z}}/{{x}}/{{y}}.png?{RGB_QUERY}",
            "type": "image/png",
            "title": "Sentinel-2 L2A True Color",
        },
        {
            "rel": "tilejson",
            "href": f"{base}/WebMercatorQuad/tilejson.json?{RGB_QUERY}",
            "type": "application/json",
            "title": f"TileJSON for {item_id}",
        },
    ]


def s2_true_colour_thumbnail(raster_api_url: str, collection: str, item_id: str) -> dict[str, Any]:
    """The ``thumbnail`` asset: a 512 px WebP preview (~80 KB and ~1 s uncached on /rstaging,
    against ~1.8 MB and ~3 s for the full-size PNG; one item, measured 2026-09-30)."""
    return {
        "href": f"{_item_url(raster_api_url, collection, item_id)}/preview?format=webp&max_size=512&{RGB_QUERY}",
        "type": "image/webp",
        "title": "Sentinel-2 L2A True Color Preview",
        "roles": ["thumbnail"],
    }


# --- S1 RTC: sentinel-1-grd-rtc-staging cubes and their per-acquisition items ---
# 0.12 reads `sel` only as an asset option (`assets=<asset>|…|sel=time=…`): the item route ignores
# a top-level sel, and a cube read without one returns a 200 with the wrong slice, not an error.
# So every S1 link carries its slice, and the builders refuse to build one without it. The cube
# and acquisition queries differ in detail because each was proven as written on /rstaging 0.12.4
# (link-forms_2026-11-04): do not unify them without a new proof.
S1_ORBIT_ASSETS = {
    "ascending": "gamma0-rtc-backscatter-asc",
    "descending": "gamma0-rtc-backscatter-desc",
}
S1_TITLE = "VV, VH, VV/VH composite"
S1_ACQUISITION_THUMBNAIL_TITLE = "Sentinel-1 GRD RGB composite preview"
# (band key in `assets`, positional expression over vv=b1 and vh=b2, the three rescales)
_S1_CUBE = ("variables", "b1;b2;(b1)/(b2)", ("0.0,0.4", "0.0,0.1", "1.0,15.0"))
_S1_ACQUISITION = ("bands", "b1;b2;b1/b2", ("0,0.4", "0,0.1", "1,15"))


def _s1_query(form: tuple[str, str, tuple[str, ...]], orbit: str, sel_time: str) -> str:
    if orbit not in S1_ORBIT_ASSETS or not sel_time:
        raise ValueError(f"an S1 link needs an orbit and a slice, got {orbit!r} and {sel_time!r}")
    key, expression, rescales = form
    value = f"{S1_ORBIT_ASSETS[orbit]}|{key}=vv,vh|sel=time={sel_time}"
    q = urllib.parse.quote
    return "&".join(
        [f"assets={q(value, safe='')}", f"expression={q(expression, safe='')}"]
        + [f"rescale={q(r, safe='')}" for r in rescales]
    )


def _s1_links(base: str, query: str, item_id: str) -> list[dict[str, str]]:
    """viewer, tilejson and xyz: the order the S1 items carry today."""
    return [
        {
            "rel": "viewer",
            "href": f"{base}/WebMercatorQuad/map.html?{query}",
            "type": "text/html",
            "title": S1_TITLE,
        },
        {
            "rel": "tilejson",
            "href": f"{base}/WebMercatorQuad/tilejson.json?{query}",
            "type": "application/json",
            "title": f"TileJSON for {item_id}",
        },
        {
            "rel": "xyz",
            "href": f"{base}/tiles/WebMercatorQuad/{{z}}/{{x}}/{{y}}.png?{query}",
            "type": "image/png",
            "title": S1_TITLE,
        },
    ]


def s1_rtc_cube_links(
    raster_api_url: str, collection: str, item_id: str, orbit: str, sel_time: str
) -> list[dict[str, str]]:
    """A cube's links on its own item route, pinned to ``sel_time`` (its best-recent slice)."""
    query = f"{_s1_query(_S1_CUBE, orbit, sel_time)}&tilesize=256"
    return _s1_links(_item_url(raster_api_url, collection, item_id), query, item_id)


def s1_rtc_cube_thumbnail(
    raster_api_url: str, collection: str, item_id: str, orbit: str, sel_time: str
) -> dict[str, Any]:
    query = _s1_query(_S1_CUBE, orbit, sel_time)
    return {
        "href": f"{_item_url(raster_api_url, collection, item_id)}/preview?format=webp&max_size=512&{query}",
        "type": "image/webp",
        "title": S1_TITLE,
        "roles": ["thumbnail"],
    }


def s1_rtc_acquisition_links(
    raster_api_url: str, collection: str, item_id: str, orbit: str, sel_time: str
) -> list[dict[str, str]]:
    """An acquisition's links on its OWN item route, at its datetime. Not the cube's route: on 0.12
    that 500s for the acquisitions of the six single-orbit cubes, whose items list the other
    orbit's assets without an href."""
    query = _s1_query(_S1_ACQUISITION, orbit, sel_time)
    return _s1_links(_item_url(raster_api_url, collection, item_id), query, item_id)


def s1_rtc_acquisition_thumbnail(
    raster_api_url: str, collection: str, item_id: str, orbit: str, sel_time: str
) -> dict[str, Any]:
    query = _s1_query(_S1_ACQUISITION, orbit, sel_time)
    return {
        "href": f"{_item_url(raster_api_url, collection, item_id)}/preview?format=webp&max_size=512&{query}",
        "type": "image/webp",
        "title": S1_ACQUISITION_THUMBNAIL_TITLE,
        "roles": ["thumbnail"],
    }


# --- OLCI: sentinel-3-olci-l1-efr-staging ---
# The asset route: the item route can serve neither tilejson nor the viewer for OLCI on 0.12.
# `radianceData` points at the measurements group, so the variables are bare names.
OLCI_ASSET = "radianceData"
OLCI_RGB = (("oa08_radiance", "10,300"), ("oa06_radiance", "22,345"), ("oa04_radiance", "36,390"))
OLCI_QUERY = "&".join(
    [f"variables={band}" for band, _ in OLCI_RGB]
    + [f"rescale={urllib.parse.quote(r, safe='')}" for _, r in OLCI_RGB]
    + [f"color_formula={urllib.parse.quote(S2_COLOR_FORMULA, safe='')}", "bidx=1"]
)


def olci_links(raster_api_url: str, collection: str, item_id: str) -> list[dict[str, str]]:
    """viewer, xyz and tilejson (today's order). The viewer adds minzoom=0: without it, a
    high-latitude item opens blank on a short screen (fitBounds lands below the tilejson minzoom)."""
    base = f"{_item_url(raster_api_url, collection, item_id)}/assets/{OLCI_ASSET}"
    return [
        {
            "rel": "viewer",
            "href": f"{base}/WebMercatorQuad/map.html?{OLCI_QUERY}&minzoom=0",
            "type": "text/html",
            "title": f"Viewer for {item_id}",
        },
        {
            "rel": "xyz",
            "href": f"{base}/tiles/WebMercatorQuad/{{z}}/{{x}}/{{y}}.png?{OLCI_QUERY}",
            "type": "image/png",
            "title": "Sentinel-3 OLCI False Color",
        },
        {
            "rel": "tilejson",
            "href": f"{base}/WebMercatorQuad/tilejson.json?{OLCI_QUERY}",
            "type": "application/json",
            "title": f"TileJSON for {item_id}",
        },
    ]


def olci_thumbnail(raster_api_url: str, collection: str, item_id: str) -> dict[str, Any]:
    base = f"{_item_url(raster_api_url, collection, item_id)}/assets/{OLCI_ASSET}"
    return {
        "href": f"{base}/preview?format=webp&max_size=512&{OLCI_QUERY}",
        "type": "image/webp",
        "title": "Sentinel-3 OLCI False Color Preview",
        "roles": ["thumbnail"],
    }
