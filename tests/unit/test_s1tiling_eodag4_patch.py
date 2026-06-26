"""Unit tests for the S1Tiling/EODAG-4 on-disk patch.

The patch (`analysis/s1tiling_eodag4_patch.py`) rewrites installed site-packages
source at container start. It is a standalone script (not an importable package),
so it is loaded here via importlib from the analysis/ directory (same dir the
container mounts at /patch — see test_run_s1tiling.py).

These tests cover the stream-timeout patch (T7 Task 0): eodag's hardcoded 60 s
stream read timeout is raised to 300 s so a throttled CDSE download rides out a
transient stall instead of failing the whole product pass (exit 68).
"""

import importlib.util
from pathlib import Path

import pytest

ANALYSIS_DIR = Path(__file__).parent.parent.parent / "analysis"
_MODULE_PATH = ANALYSIS_DIR / "s1tiling_eodag4_patch.py"


def _load_patch_module():
    spec = importlib.util.spec_from_file_location("s1tiling_eodag4_patch", _MODULE_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


patch_mod = _load_patch_module()


class TestRewriteStreamTimeout:
    """The pure string transform `_rewrite_stream_timeout` (no file IO)."""

    # Mirrors the real eodag/utils/__init__.py neighbourhood (verified in-image
    # against s1tiling:1.4.0 / eodag 4.0.0): the search timeouts sit right above
    # the stream timeout and must NOT be touched.
    FIXTURE = (
        "HTTP_REQ_TIMEOUT = 5\n"
        "DEFAULT_SEARCH_TIMEOUT = 20\n"
        "#: default timeout for stream requests (in seconds)\n"
        "DEFAULT_STREAM_REQUESTS_TIMEOUT = 60\n"
    )

    def test_rewrites_stream_timeout_60_to_300(self):
        out = patch_mod._rewrite_stream_timeout(self.FIXTURE)
        assert "DEFAULT_STREAM_REQUESTS_TIMEOUT = 300" in out
        assert "DEFAULT_STREAM_REQUESTS_TIMEOUT = 60" not in out

    def test_leaves_search_timeouts_untouched(self):
        out = patch_mod._rewrite_stream_timeout(self.FIXTURE)
        assert "HTTP_REQ_TIMEOUT = 5" in out
        assert "DEFAULT_SEARCH_TIMEOUT = 20" in out

    def test_idempotent_on_already_patched(self):
        already = self.FIXTURE.replace(
            "DEFAULT_STREAM_REQUESTS_TIMEOUT = 60",
            "DEFAULT_STREAM_REQUESTS_TIMEOUT = 300",
        )
        # Re-running over already-patched source is a no-op, not an error.
        assert patch_mod._rewrite_stream_timeout(already) == already

    def test_raises_when_anchor_absent(self):
        # eodag version/layout drift: anchor gone and never patched -> fail loud
        # rather than silently shipping a stale fix in-cluster.
        with pytest.raises(RuntimeError):
            patch_mod._rewrite_stream_timeout("HTTP_REQ_TIMEOUT = 5\n")


class TestRewritePlatformPostfilter:
    """The pure string transform `_rewrite_platform_postfilter` (no file IO).

    S1FileManager filters the requested platform out of the search results in a
    post-step. eodag 4 dropped the `platformSerialIdentifier` property (products
    carry the STAC `platform`, e.g. "sentinel-1a") and cop_dataspace OData ignores
    the `platformSerialIdentifier` *search* param too, so the stock post-filter —
    which ran only for len>1 and matched `platformSerialIdentifier` — did nothing
    for a single-platform run (the off-platform S1D leaks through) and matched
    nothing for a multi-platform one (the "S1A S1C -> 0 products" bug). The
    rewrite matches the STAC `platform` and runs for any non-empty list.
    """

    # Mirrors the real S1FileManager.py neighbourhood (verified byte-identical in
    # s1tiling 1.4.0 and 1.4.1): the relative-orbit post-filter sits right above
    # the platform one and must NOT be touched.
    FIXTURE = (
        "        # Filter relative_orbits -- if it could not be done earlier in the search() request.\n"
        "        if len(relative_orbit_list) > 1:\n"
        "            filtered_products = SearchResult([])\n"
        "            for rel_orbit in relative_orbit_list:\n"
        "                filtered_products.extend(products.filter_property(relativeOrbitNumber=rel_orbit))\n"
        "            products = filtered_products\n"
        "\n"
        "        # Filter platform -- if it could not be done earlier in the search() request.\n"
        "        if len(platform_list) > 1:\n"
        "            filtered_products = SearchResult([])\n"
        "            for platform in platform_list:\n"
        "                filtered_products.extend(products.filter_property(platformSerialIdentifier=platform))\n"
        "            products = filtered_products\n"
        "\n"
        "        # Final log\n"
    )

    def test_runs_for_single_platform_and_matches_stac_platform(self):
        out = patch_mod._rewrite_platform_postfilter(self.FIXTURE)
        # (b) runs for any non-empty list, not only len>1.
        assert "if len(platform_list) >= 1:" in out
        # (a) matches the eodag-4 STAC `platform` property, not the dropped one.
        assert "filter_property(platform=stac_platform)" in out
        assert "filter_property(platformSerialIdentifier=platform)" not in out

    def test_maps_s1x_codes_to_stac_platform_value(self):
        # S1A -> sentinel-1a value map applied per requested platform.
        out = patch_mod._rewrite_platform_postfilter(self.FIXTURE)
        assert 'stac_platform = f"sentinel-1{platform[-1].lower()}"' in out

    def test_leaves_relative_orbit_filter_untouched(self):
        # Surgical: the relative-orbit post-filter above must be byte-identical.
        out = patch_mod._rewrite_platform_postfilter(self.FIXTURE)
        assert "if len(relative_orbit_list) > 1:" in out
        assert "filter_property(relativeOrbitNumber=rel_orbit)" in out

    def test_patched_block_is_valid_python(self):
        # Adversarial: the rewritten block must still parse (wrap the 8-space
        # indented body in a def so it forms a compilable unit).
        out = patch_mod._rewrite_platform_postfilter(self.FIXTURE)
        wrapper = "def _f(platform_list, relative_orbit_list, products, SearchResult):\n" + out
        compile(wrapper, "<patched>", "exec")

    def test_idempotent_on_already_patched(self):
        once = patch_mod._rewrite_platform_postfilter(self.FIXTURE)
        # Re-running over already-patched source is a no-op, not an error.
        assert patch_mod._rewrite_platform_postfilter(once) == once

    def test_raises_when_anchor_absent(self):
        # s1tiling layout drift: anchor gone and never patched -> fail loud rather
        # than silently shipping a stale no-op (the property/value map needs the
        # cluster check the plan requires; a blind no-op would hide that).
        with pytest.raises(RuntimeError):
            patch_mod._rewrite_platform_postfilter("        # Final log\n")
