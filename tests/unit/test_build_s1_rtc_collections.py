"""Unit tests for operator-tools/build_s1_rtc_collections.py — collection alignment (pure, no I/O)."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

ot = Path(__file__).parent.parent.parent / "operator-tools"
sys.path.insert(0, str(ot))

import build_s1_rtc_collections as b  # noqa: E402

EXTENT = {
    "spatial": {"bbox": [[-3.0, 41.4, 4.37, 44.25]]},
    "temporal": {"interval": [["2025-02-05T06:01:10Z", None]]},
}


def _live(is_cube: bool) -> dict[str, Any]:
    return {
        "type": "Collection",
        "id": "x",
        "stac_version": "1.1.0",
        "title": "keep me",
        "description": "keep me",
        "license": "proprietary",
        "providers": [{"name": "ESA"}],
        "links": [],
        "stac_extensions": ["...sar...", "...sat...", "...proj..."],
        "summaries": {
            "platform": ["Sentinel-1A", "Sentinel-1B"],
            "processing:level": ["L2"],
            "sar:product_type": ["GRD"],
            "sat:orbit_state": ["ascending", "descending"],
            "gsd": [10],
        },
        "item_assets": {"vv": {}, "vh": {}, "zarr-store": {}},
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [["2014-04-03", None]]},
        },
    }


def test_item_assets_match_new_model() -> None:
    a = b.item_assets()
    assert set(a) == {
        "zarr-store",
        "gamma0-rtc-backscatter-asc",
        "gamma0-rtc-backscatter-desc",
        "border-mask-asc",
        "border-mask-desc",
        "thumbnail",
    }
    g = a["gamma0-rtc-backscatter-asc"]
    assert [band["name"] for band in g["bands"]] == ["vv", "vh"]
    assert g["data_type"] == "float32"
    assert g["gsd"] == 10
    assert a["border-mask-desc"]["bands"][0]["name"] == "border_mask"


def test_align_cube_drops_platform_and_processing_level() -> None:
    c = b.align_collection(_live(True), is_cube=True, extent=EXTENT)
    assert "platform" not in c["summaries"]  # cube items omit platform
    assert "processing:level" not in c["summaries"]
    assert "vv" not in c["item_assets"]
    assert "gamma0-rtc-backscatter-asc" in c["item_assets"]
    # extent (spatial bbox + temporal) is derived from the live items, not a fixed frame
    assert c["extent"]["spatial"]["bbox"] == EXTENT["spatial"]["bbox"]
    assert c["extent"]["temporal"] == EXTENT["temporal"]
    assert "rgb" in c["renders"]
    assert c["title"] == "keep me"  # good fields preserved
    assert c["providers"] == [{"name": "ESA"}]
    # only the extensions the collection object uses
    assert any("sar" in e for e in c["stac_extensions"])
    assert any("render" in e for e in c["stac_extensions"])


def test_align_acq_sets_normalized_platform() -> None:
    c = b.align_collection(_live(False), is_cube=False, extent=EXTENT)
    assert c["summaries"]["platform"] == ["sentinel-1a", "sentinel-1c"]
    assert "processing:level" not in c["summaries"]


def test_align_uses_live_spatial_extent() -> None:
    # The collection extent (spatial bbox + temporal) is the live-derived one from compute_extent —
    # the STAC Browser frame tracks the actual ingested footprint, no fixed AOI to hand-maintain.
    c = b.align_collection(_live(False), is_cube=False, extent=EXTENT)
    assert c["extent"] == EXTENT


def test_align_does_not_mutate_input() -> None:
    live = _live(True)
    b.align_collection(live, is_cube=True, extent=EXTENT)
    assert live["item_assets"] == {"vv": {}, "vh": {}, "zarr-store": {}}  # input untouched


def test_compute_extent_returns_none_spatial_for_empty_collection(monkeypatch) -> None:
    # A collection with no items carries no live bbox → spatial=None, so align_collection can fall
    # back to the base frame instead of writing a degenerate (min>max) bbox.
    monkeypatch.setattr(b, "_get_json", lambda url: {"features": [], "links": []})
    ext = b.compute_extent("https://x/stac", "empty-collection")
    assert ext["spatial"] is None
    assert ext["temporal"] == {"interval": [[None, None]]}


def test_align_falls_back_to_base_spatial_when_no_live_items() -> None:
    # When the live extent has no spatial bbox (empty collection), the base collection's spatial
    # extent is preserved — never a degenerate min>max bbox; the live temporal is still applied.
    live = _live(True)
    empty_extent = {"spatial": None, "temporal": {"interval": [["2025-01-01T00:00:00Z", None]]}}
    c = b.align_collection(live, is_cube=True, extent=empty_extent)
    assert c["extent"]["spatial"] == {"bbox": [[-180, -90, 180, 90]]}  # base frame kept
    assert c["extent"]["temporal"] == empty_extent["temporal"]  # live temporal still used


def test_align_preserves_eodash_and_reconciled_links() -> None:
    """align_collection must not touch links (issue #348).

    The whole template-as-superset design rests on this: the templates carry the eodash
    baselayers, the pre-aggregation links, and the links that previously existed only on the
    live collection. align_collection patches the *stale* fields (item_assets/extent/renders/
    summaries/stac_extensions) — if it ever rewrote links, regenerating would silently drop all
    of that and re-arm the clobber that `create --update` performs.
    """
    live = _live(is_cube=True)
    live["links"] = [
        {"rel": "license", "href": "https://legal", "type": "application/pdf"},
        {"rel": "related", "href": "https://earth-info.nga.mil/", "type": "text/html"},
        {
            "rel": "xyz",
            "href": "https://s2maps-tiles.eu/wmts/1.0.0/osm_3857/default/g/{z}/{y}/{x}.jpeg",
            "type": "image/jpeg",
            "id": "OSM",
            "roles": ["baselayer", "invisible"],
            "attribution": "{ OSM: ... }",
        },
        {
            "rel": "pre-aggregation",
            "href": "https://s3/x/daily.json",
            "type": "application/json",
            "aggregation:interval": "daily",
        },
    ]
    out = b.align_collection(live, is_cube=True, extent=EXTENT)
    assert out["links"] == live["links"]
