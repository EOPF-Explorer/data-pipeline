"""Unit tests for source_url_utils.py — the shared source_url -> (item_id, zarr href) rules.

These rules are shared by convert_v1_s2 and prestage_source precisely so the staged
key, the convert output path and the registered geozarr URL cannot drift apart.

The Sentinel-3 cases are not hypothetical: the ids and hrefs below are copied from live
``sentinel-3-olci-l1-efr`` / ``-slstr-l1-rbt`` items on stac.core.eopf.eodc.eu. They are
here so that "these rules are mission-agnostic" is enforced rather than asserted — if
someone adds a Sentinel-2 assumption (an id pattern, a tile-code split), they break.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

import source_url_utils  # noqa: E402

ITEM_ID = "S2B_MSIL2A_20260713T104619_N0512_R051_T31UDP_20260713T131840"

# Real Sentinel-3 ids (live EOPF sample service). Note the long "____" run and the
# trailing "_004" baseline — nothing about them resembles an S2 id, which is the point.
S3_OLCI_ITEM_ID = (
    "S3A_OL_1_EFR____20260714T222153_20260714T222243_20260715T003629_0050_141_329_1080_PS1_O_NR_004"
)
S3_SLSTR_ITEM_ID = (
    "S3A_SL_1_RBT____20260715T000042_20260715T000342_20260715T023031_0179_141_330_1080_PS1_O_NR_004"
)


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
        # --- Sentinel-3: same rules, no mission branching (live OLCI/SLSTR URLs) ---
        (
            "https://stac.core.eopf.eodc.eu/collections/sentinel-3-olci-l1-efr"
            f"/items/{S3_OLCI_ITEM_ID}",
            S3_OLCI_ITEM_ID,
        ),
        (
            "https://objects.eodc.eu:443/e05ab01a9d56408d82ac32d69a5aae2a:202607-s03olcefr-eu"
            f"/14/products/cpm_v270/{S3_OLCI_ITEM_ID}.zarr",
            S3_OLCI_ITEM_ID,
        ),
        (
            "https://stac.core.eopf.eodc.eu/collections/sentinel-3-slstr-l1-rbt"
            f"/items/{S3_SLSTR_ITEM_ID}",
            S3_SLSTR_ITEM_ID,
        ),
        (f"s3://esa-zarr-sentinel-explorer-fra/source-cache/{S3_OLCI_ITEM_ID}", S3_OLCI_ITEM_ID),
    ],
)
def test_derive_item_id(source_url, expected):
    assert source_url_utils.derive_item_id(source_url) == expected


@pytest.mark.parametrize(
    ("collection", "item_id"),
    [
        ("sentinel-2-l2a", ITEM_ID),
        ("sentinel-3-olci-l1-efr", S3_OLCI_ITEM_ID),
        ("sentinel-3-slstr-l1-rbt", S3_SLSTR_ITEM_ID),
    ],
)
def test_derive_item_id_round_trips_through_the_staged_url(collection, item_id):
    """The invariant prestage/convert/register all rely on — for any mission."""
    stac_url = f"https://stac.example.com/collections/{collection}/items/{item_id}"
    staged = f"s3://bucket/source-cache/{source_url_utils.derive_item_id(stac_url)}"

    assert source_url_utils.derive_item_id(staged) == item_id


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
    assert source_url_utils.is_stac_item_url(source_url) is expected


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
