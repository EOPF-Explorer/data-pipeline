#!/usr/bin/env python3
"""
Patch S1Tiling 1.4.0 for EODAG 4.0.0 compatibility.

EODAG 4.0.0 introduced several breaking changes for S1Tiling:
1. `productType` kwarg to dag.search() was renamed to `collection`.
   Having both causes cop_dataspace to fail silently → falls back to peps.
2. Product properties use STAC names (sat:orbit_state, platform, etc.)
   instead of legacy EODAG names (orbitDirection, platformSerialIdentifier, etc.)
3. cop_dataspace OData v4 rejects `polarizationChannels` and `sensorMode`.
4. cop_dataspace requires UPPERCASE orbit direction ("DESCENDING" not "descending").
5. `relativeOrbitNumber` search param silently returns 0 results on cop_dataspace.

It also raises eodag's hardcoded 60 s stream read timeout to 300 s so a throttled
CDSE download survives a transient stall instead of failing the product pass
(exit 68) — T7 Task 0.

This script patches:
- S1FileManager.py: fixes the search() call (issues 1, 3, 4, 5)
- s1/product.py: adds legacy→STAC property name fallback (issue 2)
- eodag/utils/__init__.py: raises the stream read timeout 60 s → 300 s

Usage (inside the Docker container):
    python3 /patch/s1tiling_eodag4_patch.py
"""

import pathlib
import re

S1T_PKG = pathlib.Path("/opt/S1TilingEnv/lib/python3.10/site-packages/s1tiling/libs")
EODAG_PKG = pathlib.Path("/opt/S1TilingEnv/lib/python3.10/site-packages/eodag")

# eodag streams downloads with a hardcoded per-read socket timeout
# (DEFAULT_STREAM_REQUESTS_TIMEOUT, defined in eodag/utils/__init__.py and reused
# at every stream call-site in http.py / api/product/_product.py). Under CDSE
# throttle the server stalls past 60 s, requests raises "Read timed out", eodag
# does not catch/retry it, and s1tiling fails the whole product pass (exit 68).
# The value is a constant (not config), so it can only be raised on disk. We
# rewrite the single definition so all call-sites pick up the new value at once.
# Verified in-image against s1tiling:1.4.0 / eodag 4.0.0 (T7 Task 0).
_STREAM_TIMEOUT_OLD = "DEFAULT_STREAM_REQUESTS_TIMEOUT = 60"
_STREAM_TIMEOUT_NEW = "DEFAULT_STREAM_REQUESTS_TIMEOUT = 300"

# S1FileManager filters the requested platform out of the search results in a
# post-step. The stock filter (byte-identical in s1tiling 1.4.0 and 1.4.1) ran
# only for len(platform_list) > 1 and matched the legacy `platformSerialIdentifier`
# property. Both are wrong under eodag 4 + cop_dataspace:
#   - eodag 4 dropped `platformSerialIdentifier`; products carry the STAC
#     `platform` property ("sentinel-1a"), so filter_property matched nothing
#     (the documented "S1A S1C -> 0 products" multi-platform bug).
#   - the search param `platformSerialIdentifier` is also ignored by cop_dataspace
#     OData (same class as relativeOrbitNumber), so off-platform products (notably
#     S1D) are returned and, with the post-filter a no-op for len==1, downloaded
#     then discarded.
# The rewrite matches the STAC `platform` (with an S1x->sentinel-1x value map) and
# runs for any non-empty list, dropping off-platform products before download.
_PLATFORM_FILTER_OLD = (
    "        # Filter platform -- if it could not be done earlier in the search() request.\n"
    "        if len(platform_list) > 1:\n"
    "            filtered_products = SearchResult([])\n"
    "            for platform in platform_list:\n"
    "                filtered_products.extend(products.filter_property(platformSerialIdentifier=platform))\n"
    "            products = filtered_products\n"
)
_PLATFORM_FILTER_NEW = (
    "        # Filter platform -- eodag 4 dropped `platformSerialIdentifier`; products\n"
    '        # carry the STAC `platform` property ("sentinel-1a"). Map the requested\n'
    "        # S1x codes and run for any non-empty list so a single-platform run (e.g.\n"
    "        # S1A) also drops the off-platform products (e.g. S1D) cop_dataspace OData\n"
    "        # returns despite the search param.\n"
    "        if len(platform_list) >= 1:\n"
    "            filtered_products = SearchResult([])\n"
    "            for platform in platform_list:\n"
    '                stac_platform = f"sentinel-1{platform[-1].lower()}"\n'
    "                filtered_products.extend(products.filter_property(platform=stac_platform))\n"
    "            products = filtered_products\n"
)
# Unique to the rewritten block -> used as the idempotency sentinel.
_PLATFORM_FILTER_MARK = "filter_property(platform=stac_platform)"


def _rewrite_platform_postfilter(src: str) -> str:
    """Rewrite S1FileManager's platform post-filter for eodag 4 + single-platform.

    Pure transform (no file IO) so it can be unit-tested on a fixture. Three
    states, checked in order (mirrors `_rewrite_stream_timeout`):
      1. already patched -> return unchanged (idempotent no-op; re-running the
         patch in a fresh container must not error).
      2. anchor present  -> replace and return.
      3. neither present -> raise (s1tiling version/layout drift). A silent no-op
         here would leak off-platform downloads (S1D) in-cluster while the patch
         reported success -- the exact failure mode this guard exists to prevent;
         it also forces the cluster re-check the plan requires if the seam moves.
    """
    if _PLATFORM_FILTER_MARK in src:
        return src
    if _PLATFORM_FILTER_OLD not in src:
        raise RuntimeError(
            "S1FileManager platform post-filter anchor not found; s1tiling layout "
            "may have changed -- refusing to silently skip."
        )
    return src.replace(_PLATFORM_FILTER_OLD, _PLATFORM_FILTER_NEW, 1)


def patch_s1filemanager() -> None:
    """Fix search() call: add collection param, remove unsupported kwargs."""
    fpath = S1T_PKG / "S1FileManager.py"
    src = fpath.read_text()

    # Replace productType with collection (EODAG 4.0.0 rename).
    # Keeping productType alongside collection causes cop_dataspace to fail
    # silently, making EODAG fall back to peps.
    src = src.replace(
        "productType=product_type,",
        "collection=product_type,",
        1,  # only first occurrence
    )

    # Remove polarizationChannels (unsupported by cop_dataspace OData)
    src = re.sub(r"\n\s*# If we have eodag.*\n", "\n", src)
    src = re.sub(r"\n\s*polarizationChannels=dag_polarization_param,", "", src)

    # Remove sensorMode="IW" (unsupported by cop_dataspace OData)
    src = re.sub(r'\n\s*sensorMode="IW",', "", src)

    # Remove relativeOrbitNumber (not supported by cop_dataspace OData —
    # returns 0 results silently). S1Tiling has post-search filtering for
    # relative orbits when len(relative_orbit_list) > 1, and when list
    # has exactly 1 element, orbitNumber=None was passed anyway for most configs.
    src = re.sub(
        r"\n\s*relativeOrbitNumber=dag_orbit_list_param,.*",
        "",
        src,
    )

    # cop_dataspace OData requires UPPERCASE orbit direction values
    # ("DESCENDING" not "descending"). S1Tiling's k_dir_assoc produces lowercase.
    src = src.replace(
        "{ 'ASC': 'ascending', 'DES': 'descending' }",
        "{ 'ASC': 'ASCENDING', 'DES': 'DESCENDING' }",
    )

    # Restrict search to cop_dataspace only — prevents EODAG from querying PEPS
    # (which is a built-in provider that cannot be removed via user config).
    # Without this, a PEPS outage causes S1Tiling to report a tile download failure
    # even when all products are available on CDSE.
    src = src.replace(
        "collection=product_type,",
        'collection=product_type,\n                provider="cop_dataspace",',
        1,
    )

    # Rewrite the platform post-filter for eodag 4 (STAC `platform`) and run it
    # for any non-empty platform_list so a single-platform run drops off-platform
    # (S1D) products before download. See _rewrite_platform_postfilter.
    src = _rewrite_platform_postfilter(src)

    fpath.write_text(src)
    print(f"  Patched {fpath.name}")


def patch_product_property() -> None:
    """Add STAC property name fallback to product_property()."""
    fpath = S1T_PKG / "s1" / "product.py"
    src = fpath.read_text()

    old = (
        "def product_property(prod: EOProduct, key: str, default=None):\n"
        '    """\n'
        "    Returns the required (EODAG) product property, "
        "or default in the property isn't found.\n"
        '    """\n'
        "    res = prod.properties.get(key, default)\n"
        "    return res"
    )

    new = (
        "def product_property(prod: EOProduct, key: str, default=None):\n"
        '    """\n'
        "    Returns the required (EODAG) product property, "
        "or default in the property isn't found.\n"
        "    EODAG 4.0.0 uses STAC property names; "
        "fall back to them for legacy keys.\n"
        '    """\n'
        "    _FALLBACK = {\n"
        '        "orbitDirection": "sat:orbit_state",\n'
        '        "platformSerialIdentifier": "platform",\n'
        '        "relativeOrbitNumber": "sat:relative_orbit",\n'
        '        "orbitNumber": "sat:absolute_orbit",\n'
        '        "polarizationChannels": "sar:polarizations",\n'
        '        "startTimeFromAscendingNode": "start_datetime",\n'
        '        "completionTimeFromAscendingNode": "end_datetime",\n'
        "    }\n"
        "    res = prod.properties.get(key, None)\n"
        "    if res is None and key in _FALLBACK:\n"
        "        res = prod.properties.get(_FALLBACK[key], default)\n"
        '        if key == "polarizationChannels" and isinstance(res, list):\n'
        '            res = "+".join(res)\n'
        "    return res if res is not None else default"
    )

    if old not in src:
        print(f"  WARNING: product_property() not found in {fpath.name}, skipping")
        return

    src = src.replace(old, new)
    fpath.write_text(src)
    print(f"  Patched {fpath.name}")


def _rewrite_stream_timeout(src: str) -> str:
    """Raise eodag's hardcoded stream read timeout 60 s -> 300 s.

    Pure transform (no file IO) so it can be unit-tested on a fixture. Three
    states, checked in order:
      1. already patched -> return unchanged (idempotent no-op; re-running the
         patch in a fresh container must not error).
      2. anchor present  -> replace and return.
      3. neither present -> raise (eodag version/layout drift). This deliberately
         diverges from the silent-skip style of patch_product_property() above:
         a silent no-op here would ship a stale fix in-cluster, which is the
         exact failure mode this guard exists to prevent.
    """
    if _STREAM_TIMEOUT_NEW in src:
        return src
    if _STREAM_TIMEOUT_OLD not in src:
        raise RuntimeError(
            f"eodag stream-timeout anchor {_STREAM_TIMEOUT_OLD!r} not found; "
            "eodag layout may have changed -- refusing to silently skip."
        )
    return src.replace(_STREAM_TIMEOUT_OLD, _STREAM_TIMEOUT_NEW, 1)


def patch_eodag_stream_timeout() -> None:
    """Raise eodag's stream read timeout so a throttled CDSE download rides out a
    transient stall instead of failing the product pass (exit 68) — T7 Task 0."""
    fpath = EODAG_PKG / "utils" / "__init__.py"
    fpath.write_text(_rewrite_stream_timeout(fpath.read_text()))
    print(f"  Patched eodag stream timeout -> 300s in {fpath.name}")


if __name__ == "__main__":
    print("Applying S1Tiling EODAG 4.0.0 compatibility patches...")
    patch_s1filemanager()
    patch_product_property()
    patch_eodag_stream_timeout()
    print("Done.")
