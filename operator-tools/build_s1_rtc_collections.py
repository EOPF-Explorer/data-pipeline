"""Generate aligned S1 RTC collection templates from the live collections + the data-model asset model.

Patches the *stale* fields of each live collection (``item_assets``, ``summaries`` platform /
processing:level, ``stac_extensions``, ``extent``, ``renders``) so the collection metadata matches the
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


def _collection_render() -> dict[str, Any]:
    """Orbit-generic, informational render (titiler renders from the per-item ``renders``)."""
    vv, vh = "/ascending:vv", "/ascending:vh"
    return {
        "rgb": {
            "title": "VV, VH, VV/VH composite",
            # `assets` is required by the render extension — the γ⁰ backscatter assets this render draws
            # from (VV/VH are bands within them).
            "assets": ["gamma0-rtc-backscatter-asc", "gamma0-rtc-backscatter-desc"],
            "expression": f"{vv};{vh};({vv})/({vh})",
            "rescale": [[0.0, 0.2]],
            "bidx": [1],
            "tilesize": 256,
        }
    }


def align_collection(
    coll: dict[str, Any], *, is_cube: bool, extent: dict[str, Any]
) -> dict[str, Any]:
    """Return a copy of ``coll`` with the stale fields patched to the new model (pure, no I/O)."""
    c = copy.deepcopy(coll)
    c["item_assets"] = item_assets()
    c["extent"] = extent
    c["renders"] = _collection_render()

    summaries = dict(c.get("summaries", {}))
    summaries.pop("processing:level", None)  # items carry no processing:level (deferred)
    if is_cube:
        summaries.pop("platform", None)  # cube items omit platform (a cube mixes S1A/S1C)
    else:
        summaries["platform"] = ["sentinel-1a", "sentinel-1c"]  # normalized; S1B is decommissioned
    c["summaries"] = summaries

    # Extensions the collection object itself uses: sar/sat summaries + the renders field.
    # (item_assets + bands are STAC 1.1 core; gsd/constellation/instruments are common metadata.)
    c["stac_extensions"] = [SAR_EXT, SAT_EXT, RENDER_EXT]
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
    """Spatial bbox union + temporal ``[earliest, null]`` derived from the live items."""
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
    return {
        "spatial": {"bbox": [[round(w, 4), round(s, 4), round(e, 4), round(n, 4)]]},
        "temporal": {"interval": [[tmin, None]]},
    }


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
