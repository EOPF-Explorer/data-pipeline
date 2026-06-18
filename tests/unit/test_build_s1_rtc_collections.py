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
    assert c["extent"] == EXTENT
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


def test_align_does_not_mutate_input() -> None:
    live = _live(True)
    b.align_collection(live, is_cube=True, extent=EXTENT)
    assert live["item_assets"] == {"vv": {}, "vh": {}, "zarr-store": {}}  # input untouched
