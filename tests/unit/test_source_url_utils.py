"""Unit tests for source_url_utils.py — the shared source_url -> (item_id, zarr href) rules.

These rules are shared by convert_v1_s2 and prestage_source precisely so the staged
key, the convert output path and the registered geozarr URL cannot drift apart.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

import source_url_utils  # noqa: E402
from source_url_utils import derive_item_id, is_stac_item_url  # noqa: E402

ITEM_ID = "S2B_MSIL2A_20260713T104619_N0512_R051_T31UDP_20260713T131840"


@pytest.mark.parametrize(
    ("source_url", "expected"),
    [
        # STAC item URL — the normal pipeline path.
        (f"https://stac.example.com/collections/sentinel-2-l2a/items/{ITEM_ID}", ITEM_ID),
        (f"https://stac.example.com/collections/sentinel-2-l2a/items/{ITEM_ID}/", ITEM_ID),
        # Direct .json item.
        (f"https://example.com/{ITEM_ID}.json", ITEM_ID),
        # Direct zarr URL (demo reconversion from cpm-manual/): the .zarr suffix must go,
        # otherwise the output path becomes <item>.zarr.zarr.
        (f"https://objects.eodc.eu:443/bucket/13/products/cpm_v270/{ITEM_ID}.zarr", ITEM_ID),
        (f"https://objects.eodc.eu:443/bucket/13/products/cpm_v270/{ITEM_ID}.zarr/", ITEM_ID),
        # Staged source: prestage names the segment item_id, so convert round-trips it.
        (f"s3://esa-zarr-sentinel-explorer-fra/source-cache/{ITEM_ID}", ITEM_ID),
    ],
)
def test_derive_item_id(source_url, expected):
    assert derive_item_id(source_url) == expected


def test_derive_item_id_round_trips_through_the_staged_url():
    """The invariant prestage/convert/register all rely on."""
    stac_url = f"https://stac.example.com/collections/sentinel-2-l2a/items/{ITEM_ID}"
    staged = f"s3://bucket/source-cache/{derive_item_id(stac_url)}"

    assert derive_item_id(staged) == derive_item_id(stac_url)


@pytest.mark.parametrize(
    ("source_url", "expected"),
    [
        (f"https://stac.example.com/collections/x/items/{ITEM_ID}", True),
        (f"https://example.com/{ITEM_ID}.json", True),
        (f"https://objects.eodc.eu/bucket/{ITEM_ID}.zarr", False),
        (f"s3://bucket/source-cache/{ITEM_ID}", False),
    ],
)
def test_is_stac_item_url(source_url, expected):
    assert is_stac_item_url(source_url) is expected


def test_resolve_zarr_url_returns_direct_zarr_untouched(monkeypatch):
    """A non-STAC source must not trigger an item fetch."""
    monkeypatch.setattr(
        source_url_utils,
        "get_zarr_url",
        lambda url: (_ for _ in ()).throw(AssertionError("must not fetch")),
    )
    direct = f"s3://bucket/source-cache/{ITEM_ID}"

    assert source_url_utils.resolve_zarr_url(direct) == direct


def test_resolve_zarr_url_fetches_stac_item(monkeypatch):
    monkeypatch.setattr(source_url_utils, "get_zarr_url", lambda url: "https://eodc/x.zarr")
    stac_url = f"https://stac.example.com/collections/x/items/{ITEM_ID}"

    assert source_url_utils.resolve_zarr_url(stac_url) == "https://eodc/x.zarr"
