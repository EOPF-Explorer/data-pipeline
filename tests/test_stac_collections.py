"""Tests for STAC collection definition files in stac/.

Covers the Sentinel-1 GRD RTC collections' asset model (PR #279) and the
Sentinel-2 L2A eodash layer-exclusivity + GeoZarr style-link metadata (issue #206).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import pystac
import pytest
from pystac import Collection

STAC_DIR = Path(__file__).parent.parent / "stac"


def _load(filename: str) -> dict[str, Any]:
    return cast("dict[str, Any]", json.loads((STAC_DIR / filename).read_text()))


# --- Sentinel-1 GRD RTC collection asset model (PR #279) ---------------------


@pytest.mark.parametrize(
    ("collection_id", "required_data_assets"),
    [
        # staging carries the new asset model (PR #279); the tests collection is still legacy vv/vh
        (
            "sentinel-1-grd-rtc-staging",
            {"gamma0-rtc-backscatter-asc", "gamma0-rtc-backscatter-desc"},
        ),
        ("sentinel-1-grd-rtc-tests", {"vv", "vh"}),
    ],
)
def test_s1_rtc_collection_valid(collection_id: str, required_data_assets: set[str]) -> None:
    """Each S1 GRD RTC collection definition must load as a valid pystac Collection."""
    col_path = STAC_DIR / f"{collection_id}.json"
    assert col_path.exists(), f"Collection file not found: {col_path}"

    col = pystac.Collection.from_file(str(col_path))

    assert col.id == collection_id
    assert col.extent.temporal.intervals[0][0] is not None

    asset_keys = set(col.extra_fields.get("item_assets", {}).keys())
    assert "zarr-store" in asset_keys
    assert required_data_assets <= asset_keys

    sar_ext = "https://stac-extensions.github.io/sar/v1.0.0/schema.json"
    assert sar_ext in col.stac_extensions


# --- S1 RTC cube collection vs. what its items actually carry ----------------

# `manage_collections.py create --update` PUTs this template wholesale over the live
# collection, so every field here is an assertion about the 170 live cube items. The values
# below were read off those items (GET /collections/sentinel-1-grd-rtc-staging/items), not
# copied from the file, and are pinned exactly so a hand edit or a regeneration cannot
# reintroduce the legacy `vv`/`vh` model or a value the items contradict.
S1_CUBE = "sentinel-1-grd-rtc-staging.json"

GAMMA0_BANDS = [
    {
        "name": "vv",
        "description": "γ⁰ RTC backscatter, VV polarization",
        "data_type": "float32",
        "nodata": "nan",
        "unit": "gamma0 (linear power)",
    },
    {
        "name": "vh",
        "description": "γ⁰ RTC backscatter, VH polarization",
        "data_type": "float32",
        "nodata": "nan",
        "unit": "gamma0 (linear power)",
    },
]
BORDER_MASK_BANDS = [
    {
        "name": "border_mask",
        "description": "Valid-data mask (0 = border/no-data, non-zero = valid)",
        "data_type": "uint8",
        "nodata": 0,
    }
]
ZARR_TYPE = "application/vnd.zarr; version=3"


def _gamma0_asset(orbit: str) -> dict[str, Any]:
    return {
        "type": ZARR_TYPE,
        "roles": ["data"],
        "title": f"γ⁰ RTC backscatter ({orbit})",
        "bands": GAMMA0_BANDS,
        "data_type": "float32",
        "nodata": "nan",
        "unit": "gamma0 (linear power)",
        "gsd": 10,
    }


def _border_mask_asset(orbit: str) -> dict[str, Any]:
    return {
        "type": ZARR_TYPE,
        "roles": ["data"],
        "title": f"Valid-data mask ({orbit})",
        "bands": BORDER_MASK_BANDS,
        "gsd": 10,
    }


EXPECTED_CUBE_ITEM_ASSETS = {
    "zarr-store": {
        "type": ZARR_TYPE,
        "roles": ["data"],
        "title": "Sentinel-1 GRD RTC Zarr store",
    },
    "gamma0-rtc-backscatter-asc": _gamma0_asset("ascending"),
    "border-mask-asc": _border_mask_asset("ascending"),
    "gamma0-rtc-backscatter-desc": _gamma0_asset("descending"),
    "border-mask-desc": _border_mask_asset("descending"),
    "thumbnail": {
        "type": "image/png",
        "roles": ["thumbnail"],
        "title": "Sentinel-1 GRD RGB composite preview",
    },
}

# Every value below appears on all 170 live items. `platform` and `processing:level` are
# absent on purpose: the items carry neither (a cube mixes platforms and no processing
# metadata is emitted yet), and build_s1_rtc_collections.align_collection pops both for the
# cube — adding either here would be reverted on the next regeneration *and* be false.
EXPECTED_CUBE_SUMMARIES = {
    "gsd": [10],
    "instruments": ["c-sar"],
    "constellation": ["sentinel-1"],
    "sar:instrument_mode": ["IW"],
    "sar:frequency_band": ["C"],
    "sar:center_frequency": [5.405],
    "sar:polarizations": [["VV", "VH"]],
    "sar:product_type": ["GRD"],
    "sat:orbit_state": ["ascending", "descending"],
}


def test_s1_cube_item_assets_match_the_items() -> None:
    """The asc/desc asset model, band-for-band — not just the key names.

    Cube items carry the superset {zarr-store, thumbnail, gamma0-rtc-backscatter-{asc,desc},
    border-mask-{asc,desc}}; 164 of 170 carry all six, the remaining 6 are single-orbit and
    carry one orbit's pair. The legacy `vv`/`vh` keys the live collection still advertises
    exist on no item.
    """
    item_assets = _load(S1_CUBE)["item_assets"]
    assert item_assets == EXPECTED_CUBE_ITEM_ASSETS
    assert "vv" not in item_assets and "vh" not in item_assets


def test_s1_cube_summaries_match_the_items() -> None:
    assert _load(S1_CUBE)["summaries"] == EXPECTED_CUBE_SUMMARIES


def test_s1_cube_declares_no_collection_render() -> None:
    """A cube item exposes BOTH orbit groups, so no single expression describes the collection.

    `sat:orbit_state` on a cube item is only the orbit of the slice `_pin_preview_to_best_recent`
    pinned (113 of 170 ascending, 57 descending); the other orbit's assets are still there. The
    block this replaces claimed `/ascending:vv;/ascending:vh;(/ascending:vv)/(/ascending:vh)` with
    a single `rescale` pair for a three-band expression — wrong orbit for a third of the items and
    the wrong number of stretches for all of them (items use [[0.0,0.4],[0.0,0.1],[1.0,15.0]]).
    Every item carries its own correct per-orbit `renders`, and every consumer reads *that*, so the
    collection declares none — and therefore must not declare the render extension either. Same
    reasoning as test_rasterform_absent_everywhere_else below.
    """
    data = _load(S1_CUBE)
    assert "renders" not in data
    assert data["stac_extensions"] == [
        "https://stac-extensions.github.io/sar/v1.0.0/schema.json",
        "https://stac-extensions.github.io/sat/v1.0.0/schema.json",
    ]


def test_s1_cube_description_does_not_claim_one_orbit_per_tile() -> None:
    """Pre-#279 the cube was one store per (tile, orbit); it is now one store per tile, both."""
    description = _load(S1_CUBE)["description"]
    assert "orbit direction" not in description
    assert "ascending and descending" in description


# --- S1 RTC template/live link reconciliation (issue #348) -------------------

# `manage_collections.py create --update` PUTs the template wholesale, so a link that
# exists only on the live collection is destroyed on the next apply. These templates
# drifted exactly that way (the cube carried only `license`; the acquisitions template
# carried nothing at all) while live had EGM2008 + the cross-links. Pin the rel multiset
# so a regeneration or a hand edit cannot silently re-arm that.
S1_TEMPLATE_RELS = {
    "sentinel-1-grd-rtc-staging.json": ["license", "related", "related"] + ["xyz"] * 4,
    # The acquisitions collection additionally ships its own pre-aggregation links, so a
    # template apply cannot wipe what aggregate_items wrote (issue #348). They must stay LAST:
    # aggregate_items strips-then-appends, and any other position makes the two writers
    # permanently reorder each other. Pinned by
    # test_aggregate_items.py::TestTemplateSurvivesAggregation.
    "sentinel-1-grd-rtc-acquisitions-staging.json": ["derived_from", "license", "related"]
    + ["xyz"] * 4
    + ["pre-aggregation"] * 2,
}


@pytest.mark.parametrize(("filename", "expected_rels"), sorted(S1_TEMPLATE_RELS.items()))
def test_s1_templates_carry_live_links(filename: str, expected_rels: list[str]) -> None:
    """The template must be a superset of the live collection's non-API links."""
    rels = sorted(link["rel"] for link in _load(filename)["links"])
    assert rels == sorted(expected_rels)


def test_s1_templates_carry_no_api_managed_links() -> None:
    """Navigation/queryables links are owned by the API — a template must not carry them."""
    api_rels = {"self", "root", "parent", "child", "items", "data"}
    for filename in S1_TEMPLATE_RELS:
        for link in _load(filename)["links"]:
            rel = link["rel"]
            assert rel not in api_rels, f"{filename} carries API-managed rel={rel}"
            assert "queryables" not in rel, f"{filename} carries API-managed rel={rel}"


# --- eodash baseLayers (issues #270 / #348) ----------------------------------

# The basemap set eodash offers in its layer switcher. The same four links are
# duplicated across every eodash collection's JSON: a shared source constant would
# only be reachable from build_s1_rtc_collections.py, leaving S2 (hand-written static
# JSON no generator touches) on a second mechanism — and drift between two mechanisms
# is invisible. So the contract is pinned once here instead, and the data is repeated.
# Attribution strings are issue #270's, verbatim; issue #348 restates them identically
# for the S1 collections, so the two families cannot drift.
#
# `href` and `attribution` are pinned, not just id/type/roles. Without them a link can
# claim id "terrain-light" while serving OSM tiles, or carry a one-character attribution,
# and still pass — both were demonstrated against the id/type/roles-only version. EOX and
# OpenStreetMap require these credits on these tiles, so the exact string is the contract.
ATTRIBUTION_OSM = (
    '{ OSM: Data &copy; <a href="http://www.openstreetmap.org/copyright" '
    'target="_blank">OpenStreetMap</a> contributors and '
    '<a href="https://maps.eox.at/#data" target="_blank">others</a>, '
    'Rendering &copy; <a href="http://eox.at" target="_blank">EOX</a> }'
)
ATTRIBUTION_OVERLAY = (
    '{ Overlay: Data &copy; <a href="http://www.openstreetmap.org/copyright" '
    'target="_blank">OpenStreetMap</a> contributors, Made with Natural Earth, '
    'Rendering &copy; <a href="https://eox.at" target="_blank">EOX</a> }'
)
ATTRIBUTION_CLOUDLESS = (
    '{ EOxCloudless 2024: <a xmlns:dct="http://purl.org/dc/terms/" '
    'href="https://s2maps.eu" target="_blank" property="dct:title">'
    "Sentinel-2 cloudless - s2maps.eu</a> by "
    '<a xmlns:cc="http://creativecommons.org/ns#" href="https://eox.at" '
    'target="_blank" property="cc:attributionName" rel="cc:attributionURL">'
    "EOX IT Services GmbH</a> (Contains modified Copernicus Sentinel data 2024) }"
)

_TILES = "https://s2maps-tiles.eu/wmts/1.0.0"

# (id, title, type, roles, href, attribution)
EXPECTED_BASELAYERS = [
    (
        "OSM",
        "OSM Background",
        "image/jpeg",
        ["baselayer", "invisible"],
        f"{_TILES}/osm_3857/default/g/{{z}}/{{y}}/{{x}}.jpeg",
        ATTRIBUTION_OSM,
    ),
    (
        "terrain-light",
        "Terrain Light",
        "image/jpeg",
        ["baselayer", "visible"],
        f"{_TILES}/terrain-light_3857/default/g/{{z}}/{{y}}/{{x}}.jpeg",
        ATTRIBUTION_OSM,
    ),
    (
        "overlay_bright",
        "Overlay labels",
        "image/png",
        ["overlay", "visible"],
        f"{_TILES}/overlay_base_bright_3857/default/g/{{z}}/{{y}}/{{x}}.png",
        ATTRIBUTION_OVERLAY,
    ),
    (
        "cloudless-2024",
        "EOxCloudless 2024",
        "image/jpeg",
        ["baselayer", "invisible"],
        f"{_TILES}/s2cloudless-2024_3857/default/g/{{z}}/{{y}}/{{x}}.jpeg",
        ATTRIBUTION_CLOUDLESS,
    ),
]

BASELAYER_COLLECTIONS = [
    "sentinel-1-grd-rtc-staging.json",
    "sentinel-1-grd-rtc-acquisitions-staging.json",
    "sentinel-2-l2a.json",
    "sentinel-2-l2a-staging.json",
]


def _xyz_links(filename: str) -> list[dict[str, Any]]:
    return [link for link in _load(filename)["links"] if link.get("rel") == "xyz"]


@pytest.mark.parametrize("filename", BASELAYER_COLLECTIONS)
def test_baselayers_present_in_order(filename: str) -> None:
    """Every eodash collection offers the same four basemaps, in the same order."""
    actual = [
        (
            lk.get("id"),
            lk.get("title"),
            lk.get("type"),
            lk.get("roles"),
            lk.get("href"),
            lk.get("attribution"),
        )
        for lk in _xyz_links(filename)
    ]
    assert actual == EXPECTED_BASELAYERS


@pytest.mark.parametrize("filename", BASELAYER_COLLECTIONS)
def test_exactly_one_visible_baselayer(filename: str) -> None:
    """eodash shows one basemap at a time; `overlay` is a different class and is exempt."""
    visible = [
        lk for lk in _xyz_links(filename) if {"baselayer", "visible"} <= set(lk.get("roles", []))
    ]
    assert len(visible) == 1, f"{filename}: expected exactly one visible baselayer"


@pytest.mark.parametrize("filename", BASELAYER_COLLECTIONS)
def test_attribution_present_and_nonempty(filename: str) -> None:
    """The EOx/s2maps tiles must be attributed wherever they are shown (#270 / #348)."""
    for link in _xyz_links(filename):
        attribution = link.get("attribution", "")
        assert attribution.strip(), f"{filename}: {link.get('id')} lacks attribution"


# --- Sentinel-2 L2A eodash collection metadata (issue #206) ------------------

# Collections that must carry the eodash GeoZarr layer metadata.
EODASH_COLLECTIONS = ["sentinel-2-l2a.json", "sentinel-2-l2a-staging.json"]

STYLE_HREF = (
    "https://raw.githubusercontent.com/EOPF-Explorer/eodash-assets/"
    "refs/heads/main/styles/geozarr.json"
)


@pytest.mark.parametrize("filename", EODASH_COLLECTIONS)
def test_collection_is_valid(filename: str) -> None:
    """Template still loads as a valid pystac Collection."""
    Collection.from_file(str(STAC_DIR / filename))


@pytest.mark.parametrize("filename", EODASH_COLLECTIONS)
def test_layer_exclusive_set(filename: str) -> None:
    assert _load(filename).get("eodash:layerExclusive") is True


@pytest.mark.parametrize("filename", EODASH_COLLECTIONS)
def test_single_style_link_bound_to_reflectance(filename: str) -> None:
    """Exactly one rel=style link, pointing at geozarr.json for the reflectance asset."""
    styles = [link for link in _load(filename)["links"] if link.get("rel") == "style"]
    assert len(styles) == 1, "expected exactly one style link (idempotent)"
    style = styles[0]
    assert style["href"] == STYLE_HREF
    assert style["type"] == "application/json"
    assert style["asset:keys"] == ["reflectance"]


@pytest.mark.parametrize("filename", EODASH_COLLECTIONS)
def test_style_targets_existing_asset(filename: str) -> None:
    """asset:keys must reference a real item_asset key."""
    data = _load(filename)
    style = next(link for link in data["links"] if link.get("rel") == "style")
    for key in style["asset:keys"]:
        assert key in data.get("item_assets", {}), f"{key} not in item_assets"


def test_no_leak_into_other_collections() -> None:
    """eodash:layerExclusive / style links must not appear on non-S2-L2A collections."""
    for path in STAC_DIR.glob("*.json"):
        if path.name in EODASH_COLLECTIONS:
            continue
        data = json.loads(path.read_text())
        assert "eodash:layerExclusive" not in data, f"unexpected field in {path.name}"
        assert not [
            link for link in data.get("links", []) if link.get("rel") == "style"
        ], f"unexpected style link in {path.name}"


# --- pre-aggregation links (issues #270 / #348) -------------------------------

# Collections whose templates carry pre-aggregation links. The S1 cube collection is
# deliberately absent: its items are datacubes with no `datetime`, so it is never
# aggregated and has no links to protect.
AGGREGATED_COLLECTIONS = [
    "sentinel-2-l2a.json",
    "sentinel-2-l2a-staging.json",
    "sentinel-1-grd-rtc-acquisitions-staging.json",
]


@pytest.mark.parametrize("filename", AGGREGATED_COLLECTIONS)
def test_pre_aggregation_links_are_last(filename: str) -> None:
    """The templates carry the pre-aggregation links so a `create --update` cannot wipe them.

    aggregate_items.py strips-then-appends, so they must sit at the END of the array or the
    two writers permanently reorder each other's output. Pinned end-to-end by
    tests/unit/test_aggregate_items.py::TestTemplateSurvivesAggregation.
    """
    rels = [link["rel"] for link in _load(filename)["links"]]
    assert rels[-2:] == ["pre-aggregation", "pre-aggregation"]


# --- eodash:rasterform (issue #348) -------------------------------------------

# The bands form eodash builds its TiTiler parameter picker from. S1 uses a single
# orbit-agnostic form: its `${properties.sat:orbit_state}` placeholders are resolved
# against each item at render time (eodash/eodash#424), so one collection-level
# declaration serves both orbits and no per-item field is needed.
RASTERFORM_BASE = (
    "https://raw.githubusercontent.com/EOPF-Explorer/eodash-assets/refs/heads/main/forms/"
)

EXPECTED_RASTERFORMS = {
    "sentinel-2-l2a.json": RASTERFORM_BASE + "bandsform.json",
    "sentinel-2-l2a-staging.json": RASTERFORM_BASE + "bandsform.json",
    "sentinel-1-grd-rtc-acquisitions-staging.json": RASTERFORM_BASE + "s1-bandsform.json",
}


@pytest.mark.parametrize("filename", sorted(EXPECTED_RASTERFORMS))
def test_rasterform_points_at_expected_form(filename: str) -> None:
    assert _load(filename).get("eodash:rasterform") == EXPECTED_RASTERFORMS[filename]


def test_rasterform_absent_everywhere_else() -> None:
    """No other template declares a form — the S1 cube collection least of all.

    Cube items are dual-orbit; their `sat:orbit_state` is only the orbit of the slice
    `_pin_preview_to_best_recent` pinned, and it does not follow the time slider. A
    collection-level form there would substitute an orbit the displayed slice may not
    have. Only the per-acquisition items are single-orbit by construction.
    """
    for path in STAC_DIR.glob("*.json"):
        if path.name in EXPECTED_RASTERFORMS:
            continue
        data = json.loads(path.read_text())
        assert "eodash:rasterform" not in data, f"unexpected rasterform in {path.name}"
