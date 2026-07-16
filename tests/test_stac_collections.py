#!/usr/bin/env python3
"""Tests for eodash STAC collection metadata (issue #206).

Verifies the two live Sentinel-2 L2A collection templates expose the eodash
layer-exclusivity flag and the GeoZarr style link, and that the additions did
not leak into items or other collections.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import pytest
from pystac import Collection

STAC_DIR = Path(__file__).parent.parent / "stac"

# Collections that must carry the eodash GeoZarr layer metadata.
EODASH_COLLECTIONS = ["sentinel-2-l2a.json", "sentinel-2-l2a-staging.json"]

STYLE_HREF = (
    "https://raw.githubusercontent.com/EOPF-Explorer/eodash-assets/"
    "refs/heads/main/styles/geozarr.json"
)


def _load(filename: str) -> dict[str, Any]:
    return cast("dict[str, Any]", json.loads((STAC_DIR / filename).read_text()))


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


# --- baselayer attribution (issue #270) --------------------------------------

# The basemaps eodash offers in its layer switcher, and the attribution each must carry.
# Attribution strings are issue #270's, verbatim; the same values are used for the S1
# collections (issue #348 restates them identically), so the two families cannot drift.
EXPECTED_BASELAYERS = [
    ("OSM", "image/jpeg", ["baselayer", "invisible"]),
    ("terrain-light", "image/jpeg", ["baselayer", "visible"]),
    ("overlay_bright", "image/png", ["overlay", "visible"]),
    ("cloudless-2024", "image/jpeg", ["baselayer", "invisible"]),
]


def _xyz_links(filename: str) -> list[dict[str, Any]]:
    return [link for link in _load(filename)["links"] if link.get("rel") == "xyz"]


@pytest.mark.parametrize("filename", EODASH_COLLECTIONS)
def test_baselayers_present_in_order(filename: str) -> None:
    actual = [(lk.get("id"), lk.get("type"), lk.get("roles")) for lk in _xyz_links(filename)]
    assert actual == EXPECTED_BASELAYERS


@pytest.mark.parametrize("filename", EODASH_COLLECTIONS)
def test_every_baselayer_has_attribution(filename: str) -> None:
    """The EOx/s2maps tiles must be attributed wherever they are shown (issue #270)."""
    for link in _xyz_links(filename):
        assert link.get("attribution", "").strip(), f"{filename}: {link.get('id')} lacks attribution"


@pytest.mark.parametrize("filename", EODASH_COLLECTIONS)
def test_exactly_one_visible_baselayer(filename: str) -> None:
    """eodash shows one basemap at a time; `overlay` is a different class and is exempt."""
    visible = [
        lk for lk in _xyz_links(filename) if {"baselayer", "visible"} <= set(lk.get("roles", []))
    ]
    assert len(visible) == 1, f"{filename}: expected exactly one visible baselayer"


@pytest.mark.parametrize("filename", EODASH_COLLECTIONS)
def test_pre_aggregation_links_are_last(filename: str) -> None:
    """The templates carry the pre-aggregation links so a `create --update` cannot wipe them.

    aggregate_items.py strips-then-appends, so they must sit at the END of the array or the
    two writers permanently reorder each other's output. Pinned end-to-end by
    tests/unit/test_aggregate_items.py::TestTemplateSurvivesAggregation.
    """
    rels = [link["rel"] for link in _load(filename)["links"]]
    assert rels[-2:] == ["pre-aggregation", "pre-aggregation"]
