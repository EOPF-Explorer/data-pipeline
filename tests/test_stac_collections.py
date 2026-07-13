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


# --- Sentinel-2 L2A eodash collection metadata (issue #206) ------------------

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
