from __future__ import annotations

from datetime import UTC, datetime

from pystac import Asset, Item

from scripts.augment_stac_item import _get_s2_quicklook_var_path as augment_quicklook_path
from scripts.register import _get_s2_quicklook_var_path as register_quicklook_path
from scripts.register import rewrite_asset_hrefs


def _build_item(href: str) -> Item:
    item = Item(
        id="test-item",
        geometry={"type": "Point", "coordinates": [0.0, 0.0]},
        bbox=[0.0, 0.0, 0.0, 0.0],
        datetime=datetime.now(UTC),
        properties={},
    )
    item.add_asset(
        "TCI_10m",
        Asset(href=href, media_type="application/vnd+zarr", roles=["visual"], title="TCI"),
    )
    return item


def test_rewrite_asset_hrefs_inserts_quicklook_level() -> None:
    item = _build_item(
        "s3://source-bucket/sentinel-2-l2a/test-item.zarr/quality/l2a_quicklook/r10m/tci"
    )

    rewrite_asset_hrefs(
        item,
        "s3://source-bucket/sentinel-2-l2a/test-item.zarr",
        "s3://dest-bucket/output/test-item.zarr",
        "https://s3.example.com",
    )

    href = item.assets["TCI_10m"].href
    assert href.startswith("https://dest-bucket.s3.example.com")
    assert "/quality/l2a_quicklook/r10m/0/tci" in href


def test_quicklook_variable_path_helpers_include_level() -> None:
    item = _build_item(
        "https://dest-bucket.s3.example.com/output/test-item.zarr/quality/l2a_quicklook/r10m/0/tci"
    )

    expected = "/quality/l2a_quicklook/r10m/0:tci"
    assert register_quicklook_path(item) == expected
    assert augment_quicklook_path(item) == expected
