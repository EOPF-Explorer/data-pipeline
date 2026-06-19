"""Tests for STAC collection definition files in stac/."""

from __future__ import annotations

from pathlib import Path

import pystac
import pytest

STAC_DIR = Path(__file__).parent.parent / "stac"


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
