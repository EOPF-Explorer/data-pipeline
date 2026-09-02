"""Generate aligned S1 RTC collection templates from the live collections + the data-model asset model.

Patches the *stale* fields of each live collection (``item_assets``, ``summaries`` platform /
processing:level, ``stac_extensions``, ``extent``, ``renders`` — dropped on the dual-orbit cube) so the collection metadata matches the
migrated new-model items, while preserving the good fields (title/description/keywords/providers/license/
links). ``item_assets`` is derived from ``eopf_geozarr.stac.s1_rtc`` so it cannot drift from the builder;
the ``extent`` is derived from the live items so re-running after new ingests keeps it aligned.

Read-only on the API (the base collection + the extent scan); writes the version-controlled templates
``stac/{id}.json`` (basing on the committed template if present, else the live collection with the
API-managed links stripped). Apply with:

    uv run operator-tools/manage_collections.py create --update stac/{id}.json
"""

from __future__ import annotations

import argparse
import copy
import json
import urllib.request
from pathlib import Path
from typing import Any

from eopf_geozarr.stac.s1_rtc import (
    BORDER_MASK_DTYPE,
    GAMMA0_DTYPE,
    GAMMA0_NODATA,
    GAMMA0_UNIT,
    GSD,
    RENDER_EXT,
    SAR_EXT,
    SAT_EXT,
    ZARR_MEDIA_TYPE,
    _rgb_render,
)

DEFAULT_STAC = "https://api.explorer.eopf.copernicus.eu/stac"
CUBE_COLLECTION = "sentinel-1-grd-rtc-staging"
ACQ_COLLECTION = "sentinel-1-grd-rtc-acquisitions-staging"
_ORBITS = (("asc", "ascending"), ("desc", "descending"))


def _gamma0_bands() -> list[dict[str, Any]]:
    return [
        {
            "name": pol,
            "description": f"γ⁰ RTC backscatter, {pol.upper()} polarization",
            "data_type": GAMMA0_DTYPE,
            "nodata": GAMMA0_NODATA,
            "unit": GAMMA0_UNIT,
        }
        for pol in ("vv", "vh")
    ]


def item_assets() -> dict[str, Any]:
    """``item_assets`` for the new model — the superset across single- and dual-orbit items."""
    assets: dict[str, Any] = {
        "zarr-store": {
            "type": ZARR_MEDIA_TYPE,
            "roles": ["data"],
            "title": "Sentinel-1 GRD RTC Zarr store",
        }
    }
    for short, orbit in _ORBITS:
        assets[f"gamma0-rtc-backscatter-{short}"] = {
            "type": ZARR_MEDIA_TYPE,
            "roles": ["data"],
            "title": f"γ⁰ RTC backscatter ({orbit})",
            "bands": _gamma0_bands(),
            "data_type": GAMMA0_DTYPE,
            "nodata": GAMMA0_NODATA,
            "unit": GAMMA0_UNIT,
            "gsd": GSD,
        }
        assets[f"border-mask-{short}"] = {
            "type": ZARR_MEDIA_TYPE,
            "roles": ["data"],
            "title": f"Valid-data mask ({orbit})",
            "bands": [
                {
                    "name": "border_mask",
                    "description": "Valid-data mask (0 = border/no-data, non-zero = valid)",
                    "data_type": BORDER_MASK_DTYPE,
                    "nodata": 0,
                }
            ],
            "gsd": GSD,
        }
    assets["thumbnail"] = {
        "type": "image/png",
        "roles": ["thumbnail"],
        "title": "Sentinel-1 GRD RGB composite preview",
    }
    return assets


# The orbit the collection-level fallback names. Items are single-orbit and carry their own
# correct render, so this only ever applies to a client that has no item in hand.
_COLLECTION_RENDER_ORBIT = "ascending"


def _collection_render() -> dict[str, Any]:
    """Fallback render for the *per-acquisition* collection, taken from the builder.

    Not emitted for the cube collection: a cube item exposes both the ``/ascending`` and the
    ``/descending`` group, so no single collection-level ``expression`` is true for it — see
    ``align_collection``.

    Derived from ``eopf_geozarr.stac.s1_rtc._rgb_render`` rather than restated, because a
    hand-copied duplicate is exactly how this block came to carry ``rescale: [[0.0, 0.2]]`` for
    two months after upstream had already diagnosed that single shared pair as a rendering bug
    (it saturates the VV/VH ratio band to a flat purple) and moved every item to one stretch per
    band. Importing the private helper is deliberate: if upstream renames it this fails loudly at
    import, which is the failure mode we want over silently serving a stale recipe.

    ⚠️ eodash resolves ``config renders > collection STAC renders > item renders``
    (``createLayers.js``), so this block OUTRANKS each item's own. It names one orbit and is
    therefore wrong for the 683 of 1420 live items that are descending — kept deliberately as a
    fallback for clients holding no item. Pinned against drift by
    ``tests/unit/test_build_s1_rtc_collections.py::test_collection_render_tracks_the_builder``.
    """
    render = dict(_rgb_render(_COLLECTION_RENDER_ORBIT))
    # `assets` is required by the render extension at collection level — the γ⁰ backscatter assets
    # this render draws from (VV/VH are bands within them). Items omit it; they have real assets.
    render["assets"] = [f"gamma0-rtc-backscatter-{short}" for short, _ in _ORBITS]
    return {"rgb": render}


def align_collection(
    coll: dict[str, Any], *, is_cube: bool, extent: dict[str, Any]
) -> dict[str, Any]:
    """Return a copy of ``coll`` with the stale fields patched to the new model (pure, no I/O)."""
    c = copy.deepcopy(coll)
    c["item_assets"] = item_assets()
    # Extent is derived from the live items. When the collection has no items yet, compute_extent
    # yields no spatial bbox (None) — keep the base collection's spatial extent rather than write a
    # degenerate min>max bbox (which pystac does not reject). Temporal is always taken live.
    c["extent"] = {
        "spatial": extent.get("spatial") or c.get("extent", {}).get("spatial"),
        "temporal": extent["temporal"],
    }
    # A cube item carries BOTH orbit groups (`/ascending` and `/descending`) and its
    # `sat:orbit_state` is only the orbit of the preview slice that `_pin_preview_to_best_recent`
    # chose — 113 of the 170 live cube items say "ascending", 57 "descending". A collection-level
    # `expression` has to name one group, so any value here is false for the other half of the
    # collection. Every item already carries its own correct per-orbit `renders`, and nothing reads
    # the collection-level block (all consumers read `item.properties.renders`), so the cube simply
    # does not declare one. Same reasoning as the deliberately absent `eodash:rasterform`, pinned by
    # tests/test_stac_collections.py::test_rasterform_absent_everywhere_else.
    if is_cube:
        c.pop("renders", None)
    else:
        c["renders"] = _collection_render()

    summaries = dict(c.get("summaries", {}))
    summaries.pop("processing:level", None)  # items carry no processing:level (deferred)
    if is_cube:
        summaries.pop("platform", None)  # cube items omit platform (a cube mixes S1A/S1C)
    else:
        summaries["platform"] = ["sentinel-1a", "sentinel-1c"]  # normalized; S1B is decommissioned
    c["summaries"] = summaries

    # Extensions the collection object itself uses: sar/sat summaries, plus render only where a
    # `renders` field is actually emitted. (item_assets + bands are STAC 1.1 core;
    # gsd/constellation/instruments are common metadata.)
    c["stac_extensions"] = [SAR_EXT, SAT_EXT] if is_cube else [SAR_EXT, SAT_EXT, RENDER_EXT]
    return c


def _get_json(url: str) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=60) as resp:  # noqa: S310  # nosec B310 -- https STAC API
        data: dict[str, Any] = json.load(resp)
    return data


def fetch_collection(stac_url: str, collection_id: str) -> dict[str, Any]:
    return _get_json(f"{stac_url.rstrip('/')}/collections/{collection_id}")


# Navigation/queryables links are managed by the STAC API on registration — a template must not carry them.
_API_LINK_RELS = frozenset({"self", "root", "parent", "child", "items", "data", "queryables"})


def _strip_api_links(coll: dict[str, Any]) -> dict[str, Any]:
    coll["links"] = [
        lk
        for lk in coll.get("links", [])
        if lk.get("rel") not in _API_LINK_RELS and "queryables" not in str(lk.get("rel", ""))
    ]
    return coll


def load_base(stac_url: str, collection_id: str, template_dir: Path) -> dict[str, Any]:
    """Base collection to patch: the committed template if present (canonical, clean), else the live
    collection with the API-managed navigation links stripped."""
    template = template_dir / f"{collection_id}.json"
    if template.exists():
        return dict(json.loads(template.read_text()))
    return _strip_api_links(fetch_collection(stac_url, collection_id))


def compute_extent(stac_url: str, collection_id: str) -> dict[str, Any]:
    """Spatial bbox union + temporal ``[earliest, null]`` derived from the live items.

    ``spatial`` is ``None`` when no item carried a bbox (e.g. an empty collection) so the caller can
    fall back to a known frame rather than emit a degenerate ``min>max`` bbox.
    """
    url: str | None = f"{stac_url.rstrip('/')}/collections/{collection_id}/items?limit=100"
    w = s = 1e9
    e = n = -1e9
    tmin: str | None = None
    while url:
        page = _get_json(url)
        for feat in page["features"]:
            bbox = feat.get("bbox")
            if bbox:
                w, s = min(w, bbox[0]), min(s, bbox[1])
                e, n = max(e, bbox[2]), max(n, bbox[3])
            props = feat["properties"]
            t = props.get("datetime") or props.get("start_datetime")
            if t and (tmin is None or t < tmin):
                tmin = t
        url = next((lk["href"] for lk in page.get("links", []) if lk.get("rel") == "next"), None)
    # w stays at its 1e9 init iff no item bbox was seen (then w > e) → no live spatial bbox.
    spatial = {"bbox": [[round(w, 4), round(s, 4), round(e, 4), round(n, 4)]]} if w <= e else None
    return {"spatial": spatial, "temporal": {"interval": [[tmin, None]]}}


def build(
    stac_url: str, collection_id: str, *, is_cube: bool, template_dir: Path
) -> dict[str, Any]:
    coll = load_base(stac_url, collection_id, template_dir)
    extent = compute_extent(stac_url, collection_id)
    return align_collection(coll, is_cube=is_cube, extent=extent)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--stac-api-url", default=DEFAULT_STAC)
    ap.add_argument(
        "--out-dir", default="stac", help="version-controlled template dir to write {id}.json"
    )
    args = ap.parse_args()

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    import pystac

    for coll_id, is_cube in ((CUBE_COLLECTION, True), (ACQ_COLLECTION, False)):
        aligned = build(args.stac_api_url, coll_id, is_cube=is_cube, template_dir=out)
        pystac.Collection.from_dict(
            aligned
        )  # structural validation (raises on a malformed collection)
        path = out / f"{coll_id}.json"
        path.write_text(json.dumps(aligned, indent=2, ensure_ascii=False))
        print(
            f"wrote {path}  (item_assets={sorted(aligned['item_assets'])}, extent bbox={aligned['extent']['spatial']['bbox']})"
        )


if __name__ == "__main__":
    main()
