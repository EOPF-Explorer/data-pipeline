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
6. The platform post-filter `filter_property(platformSerialIdentifier=...)` matches
   nothing on eodag-4 (products carry STAC `platform`, not `platformSerialIdentifier`)
   and only ran for `len(platform_list) > 1`, so off-platform products (e.g. S1D) on
   a single-platform request were downloaded then discarded (wasted CDSE egress). We
   post-filter by product-id PREFIX for any non-empty platform_list (EOPF caching P0 /
   T1). The id prefix (S1A/S1C/S1D…) is format-stable, unlike the `platform` value.

It also raises eodag's hardcoded 60 s stream read timeout to 300 s so a throttled
CDSE download survives a transient stall instead of failing the product pass
(exit 68) — T7 Task 0.

This script patches:
- S1FileManager.py: fixes the search() call (issues 1, 3, 4, 5)
- S1FileManager.py: platform post-filter by product-id prefix (issue 6)
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


def patch_s1filemanager() -> None:
    """Fix search() call: add collection param, remove unsupported kwargs."""
    fpath = S1T_PKG / "S1FileManager.py"
    src = fpath.read_text()

    # Idempotency guard: the `collection=`/`provider=` rewrite is NOT self-reversing
    # — a second run would re-match `collection=product_type,` and inject a duplicate
    # `provider="cop_dataspace",` (SyntaxError: keyword argument repeated). The patch
    # normally runs once per fresh container, but guard it so a re-run is a no-op,
    # matching the idempotent contract of the other patches in this module.
    if 'provider="cop_dataspace",' in src:
        print(f"  {fpath.name} search() already patched; skipping")
        return

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


# S1Tiling's platform post-filter (S1FileManager._search_products) drops
# off-platform products AFTER the search but BEFORE download. On eodag-4 /
# cop_dataspace it is doubly broken: (a) it only runs for len(platform_list) > 1,
# so a single-platform request (the prod path) never filters; and (b) it matches
# `filter_property(platformSerialIdentifier=...)`, but eodag-4 products carry the
# STAC `platform` key, not `platformSerialIdentifier`, so it matches nothing.
# Net effect: off-platform products (e.g. S1D on an S1A request) are downloaded
# then discarded — wasted CDSE egress (EOPF caching P0 / T1).
#
# We replace it with a product-id PREFIX match that runs for any non-empty
# platform_list. Every product id starts with its platform code (S1A/S1C/S1D…),
# which is format-stable — unlike the `platform` property value, whose exact
# spelling (`sentinel-1a`?) is provider-dependent and burned a prior attempt.
# Verified in eodag's cop_dataspace mapping: `platformSerialIdentifier: '$.null'`
# (always None — why the original filter matched nothing) and
# `title: '{$.Name#remove_extension}'` (the S1A_IW_GRDH_… SAFE name) — so the id
# prefix is read off `properties['title']` (and `['id']` as a fallback).
# Conservative by design: we only drop a product POSITIVELY identified as
# off-platform; a product whose code we cannot parse is KEPT, so this can never
# cause coverage loss (worst case degrades to the pre-patch redundant download).
# Verified in-image against s1tiling 1.4.0 and 1.4.1 (byte-identical anchor).
_PLATFORM_POSTFILTER_MARKER = "# [EOPF T1 platform-prefix filter]"

_PLATFORM_POSTFILTER_OLD = (
    "        # Filter platform -- if it could not be done earlier in the search() request.\n"
    "        if len(platform_list) > 1:\n"
    "            filtered_products = SearchResult([])\n"
    "            for platform in platform_list:\n"
    "                filtered_products.extend(products.filter_property(platformSerialIdentifier=platform))\n"
    "            products = filtered_products"
)

_PLATFORM_POSTFILTER_NEW = (
    "        # [EOPF T1 platform-prefix filter] cop_dataspace/eodag-4 ignores the\n"
    "        # platformSerialIdentifier search param AND drops it from product\n"
    "        # properties, so the upstream filter_property matched nothing and only ran\n"
    "        # for len>1. Classify each product by the platform code (S1 + a mission\n"
    "        # letter) at the head of its id/title, and keep only requested platforms,\n"
    "        # for any non-empty platform_list. The SAME _eopf_code() normalises both\n"
    "        # the request and the product, so the compare is symmetric (a cfg value\n"
    "        # like 'S1A' or even 'S1A_EXTRA' both reduce to code 'S1A'). Conservative:\n"
    "        # we filter only when the request maps to a recognised code set, and drop\n"
    "        # only a product POSITIVELY identified as off-platform — an unparseable id\n"
    "        # or an uninterpretable request keeps everything, so coverage loss is\n"
    "        # impossible (worst case degrades to the pre-patch redundant download).\n"
    "        if platform_list:\n"
    "            def _eopf_code(_s):\n"
    "                _u = _s.upper()\n"
    '                return _u[:3] if len(_u) >= 3 and _u[:2] == "S1" and _u[2].isalpha() else None\n'
    "\n"
    "            _eopf_wanted = {_c for _c in (_eopf_code(_p) for _p in platform_list if _p) if _c}\n"
    "\n"
    "            def _eopf_prod_code(_prod):\n"
    '                for _k in ("id", "title"):\n'
    "                    _v = _prod.properties.get(_k)\n"
    "                    if isinstance(_v, str) and (_c := _eopf_code(_v)):\n"
    "                        return _c\n"
    "                return None\n"
    "\n"
    "            if _eopf_wanted:\n"
    "                products = SearchResult(\n"
    "                    [_prod for _prod in products\n"
    "                     if (_pc := _eopf_prod_code(_prod)) is None or _pc in _eopf_wanted]\n"
    "                )"
)


def _rewrite_platform_postfilter(src: str) -> str:
    """Replace S1Tiling's platform post-filter with a product-id prefix match.

    Pure string transform (no file IO) so it is unit-testable on a fixture and on
    the real vendored source. Three states, checked in order (mirrors
    `_rewrite_stream_timeout`):
      1. already patched (marker present) -> return unchanged (idempotent).
      2. anchor present -> replace and return.
      3. neither present -> raise (S1Tiling version/layout drift). Fail loud rather
         than silently shipping a stale fix that re-introduces the S1D over-pull.
    """
    if _PLATFORM_POSTFILTER_MARKER in src:
        return src
    if _PLATFORM_POSTFILTER_OLD not in src:
        raise RuntimeError(
            "S1Tiling platform post-filter anchor not found; S1FileManager layout "
            "may have changed -- refusing to silently skip the S1D-over-pull fix."
        )
    return src.replace(_PLATFORM_POSTFILTER_OLD, _PLATFORM_POSTFILTER_NEW, 1)


def patch_platform_postfilter() -> None:
    """Post-filter off-platform products by id prefix, for any non-empty
    platform_list — kills the redundant S1D download (EOPF caching P0 / T1)."""
    fpath = S1T_PKG / "S1FileManager.py"
    fpath.write_text(_rewrite_platform_postfilter(fpath.read_text()))
    print(f"  Patched platform post-filter (id-prefix) in {fpath.name}")


if __name__ == "__main__":
    print("Applying S1Tiling EODAG 4.0.0 compatibility patches...")
    patch_s1filemanager()
    patch_platform_postfilter()
    patch_product_property()
    patch_eodag_stream_timeout()
    print("Done.")
