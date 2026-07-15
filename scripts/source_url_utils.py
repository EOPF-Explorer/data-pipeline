#!/usr/bin/env python3
"""Resolve a pipeline ``source_url`` to an item ID and a Zarr href.

Extracted from ``convert_v1_s2.py`` so ``prestage_source.py`` can apply the *same*
rules without importing the conversion stack (xarray/dask/eopf_geozarr) into a
1-CPU copy pod.

These rules are mission-agnostic: they read the URL, never the product. Sentinel-2 is
the first caller, but Sentinel-3 items resolve identically (checked against live
``sentinel-3-olci-l1-efr`` / ``-slstr-l1-rbt`` / ``-slstr-l2-frp`` items — same
``product`` asset, same ``<host>/<tenant:container>/<date>/products/<cpm>/<id>.zarr``
layout), so a Sentinel-3 pipeline reuses this module as-is. Do not add mission
branching here; if a mission ever needs different rules, that is a new resolver, not
an ``if``.

Load-bearing invariant (data-pipeline#182 / #339): the staged key prestage writes,
the output path convert builds, and the geozarr URL register publishes must all
agree on one ``item_id``. They agree because all three derive it here, from the
STAC item URL basename — which the pipeline relies on being equal to the item's
``id`` property. Keep this module free of heavy imports so both callers can use it.
"""

from __future__ import annotations

from urllib.parse import urlparse

import httpx


def get_zarr_url(stac_item_url: str) -> str:
    """Get Zarr asset URL from STAC item (priority: product, zarr, any .zarr)."""
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        assets = client.get(stac_item_url).raise_for_status().json().get("assets", {})

    # Try priority assets first
    for key in ["product", "zarr"]:
        if key in assets and (href := assets[key].get("href")):
            return str(href)

    # Fallback: any asset with .zarr in href
    for asset in assets.values():
        if ".zarr" in asset.get("href", ""):
            return str(asset["href"])

    raise RuntimeError("No Zarr asset found in STAC item")


def is_stac_item_url(source_url: str) -> bool:
    """True if ``source_url`` addresses a STAC item (rather than a Zarr store)."""
    return "/items/" in source_url or source_url.endswith(".json")


def resolve_zarr_url(source_url: str) -> str:
    """Return the Zarr store URL for ``source_url``, fetching the STAC item if needed."""
    return get_zarr_url(source_url) if is_stac_item_url(source_url) else source_url


def derive_item_id(source_url: str) -> str:
    """Derive the item ID from any accepted ``source_url`` form.

    Handles the STAC item URL (``…/items/<id>``), a direct item JSON, a direct Zarr
    store (``…/<id>.zarr``) and a pre-staged copy (``s3://…/source-cache/<id>``).
    Stripping ``.zarr`` is what keeps a direct-Zarr source from producing an
    ``<id>.zarr.zarr`` output path.
    """
    basename = urlparse(source_url).path.rstrip("/").split("/")[-1]
    return basename.removesuffix(".json").removesuffix(".zarr")
