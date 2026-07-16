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


# --- eodash baseLayers (issue #348) -----------------------------------------

# The basemap set eodash offers in its layer switcher. The same four links are
# duplicated across every eodash collection's JSON: a shared source constant would
# only be reachable from build_s1_rtc_collections.py, leaving S2 (hand-written static
# JSON no generator touches) on a second mechanism — and drift between two mechanisms
# is invisible. So the contract is pinned once here instead, and the data is repeated.
EXPECTED_BASELAYERS = [
    ("OSM", "image/jpeg", ["baselayer", "invisible"]),
    ("terrain-light", "image/jpeg", ["baselayer", "visible"]),
    ("overlay_bright", "image/png", ["overlay", "visible"]),
    ("cloudless-2024", "image/jpeg", ["baselayer", "invisible"]),
]

# (filename, require_attribution). #348 asked for attribution on the S1 links; S2's
# predate the request and are backfilled separately (they are applied from `main`,
# so carrying the change here would leave main's copy to overwrite it).
BASELAYER_COLLECTIONS = [
    ("sentinel-1-grd-rtc-staging.json", True),
    ("sentinel-1-grd-rtc-acquisitions-staging.json", True),
    ("sentinel-2-l2a.json", False),
    ("sentinel-2-l2a-staging.json", False),
]


def _xyz_links(filename: str) -> list[dict[str, Any]]:
    return [link for link in _load(filename)["links"] if link.get("rel") == "xyz"]


@pytest.mark.parametrize("filename", [f for f, _ in BASELAYER_COLLECTIONS])
def test_baselayers_present_in_order(filename: str) -> None:
    """Every eodash collection offers the same four basemaps, in the same order."""
    actual = [(lk.get("id"), lk.get("type"), lk.get("roles")) for lk in _xyz_links(filename)]
    assert actual == EXPECTED_BASELAYERS


@pytest.mark.parametrize("filename", [f for f, _ in BASELAYER_COLLECTIONS])
def test_exactly_one_visible_baselayer(filename: str) -> None:
    """eodash shows one basemap at a time; `overlay` is a different class and is exempt."""
    visible = [
        lk for lk in _xyz_links(filename) if {"baselayer", "visible"} <= set(lk.get("roles", []))
    ]
    assert len(visible) == 1, f"{filename}: expected exactly one visible baselayer"


@pytest.mark.parametrize("filename", [f for f, require in BASELAYER_COLLECTIONS if require])
def test_attribution_present_and_nonempty(filename: str) -> None:
    """The EOx/s2maps tiles must carry their attribution (issue #348)."""
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
