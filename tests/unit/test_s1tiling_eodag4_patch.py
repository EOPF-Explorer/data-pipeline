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

    Built from the real anchor (`_PLATFORM_POSTFILTER_OLD`) so the fixture stays in
    lockstep with the vendored S1FileManager.py (verified in-image against s1tiling
    1.4.0 and 1.4.1 — byte-identical).
    """

    # A minimal but syntactically valid neighbourhood: the anchor block as a class
    # method body (class -> def -> 8-space body, matching the real indentation).
    FIXTURE = (
        "class _FM:\n"
        "    def _search_products(self, platform_list, products):\n"
        "        SearchResult = list\n"
        + patch_mod._PLATFORM_POSTFILTER_OLD
        + "\n"
        "        return products\n"
    )

    def test_rewrites_postfilter_to_prefix_match(self):
        out = patch_mod._rewrite_platform_postfilter(self.FIXTURE)
        assert patch_mod._PLATFORM_POSTFILTER_MARKER in out
        # the broken len>1 / platformSerialIdentifier block is gone
        assert "filter_property(platformSerialIdentifier=platform)" not in out
        assert "if len(platform_list) > 1:" not in out

    def test_rewritten_source_compiles(self):
        out = patch_mod._rewrite_platform_postfilter(self.FIXTURE)
        # compile (not just ast.parse) so method-scope issues with the injected
        # walrus + nested defs are caught at the real altitude (a class-method body).
        compile(out, "<rewritten>", "exec")

    def test_idempotent_on_already_patched(self):
        once = patch_mod._rewrite_platform_postfilter(self.FIXTURE)
        assert patch_mod._rewrite_platform_postfilter(once) == once

    def test_raises_when_anchor_absent(self):
        with pytest.raises(RuntimeError):
            patch_mod._rewrite_platform_postfilter("def f(): pass\n")


class TestPlatformPostfilterRuntime:
    """Exercise the *injected* filter logic itself (not just that it compiles).

    The new block is dedented and exec'd with a fake `SearchResult`/products so the
    real runtime predicate is what gets tested — the bug that bit the prior attempt
    was behavioural (dropped the wanted product), not syntactic.
    """

    @staticmethod
    def _run(platform_list, ids):
        import textwrap

        class _FakeProd:
            def __init__(self, **props):
                self.properties = props

        # Build products; an id of None simulates an unparseable identifier.
        products = [
            _FakeProd() if _id is None else _FakeProd(id=_id) for _id in ids
        ]
        ns = {
            "SearchResult": list,
            "platform_list": platform_list,
            "products": products,
        }
        block = textwrap.dedent(patch_mod._PLATFORM_POSTFILTER_NEW)
        exec(compile(block, "<postfilter>", "exec"), ns)  # noqa: S102 - trusted patch text
        return [p.properties.get("id") for p in ns["products"]]

    def test_single_platform_drops_offplatform_s1d(self):
        # The prod path: request S1A, an S1D also came back -> S1D dropped pre-download.
        kept = self._run(["S1A"], ["S1A_IW_GRDH_1", "S1D_IW_GRDH_2", "S1A_IW_GRDH_3"])
        assert kept == ["S1A_IW_GRDH_1", "S1A_IW_GRDH_3"]

    def test_multi_platform_keeps_all_requested(self):
        # The "S1A S1C -> 0 products" bug: both requested platforms must survive.
        kept = self._run(["S1A", "S1C"], ["S1A_IW_1", "S1C_IW_2", "S1D_IW_3"])
        assert kept == ["S1A_IW_1", "S1C_IW_2"]

    def test_unparseable_id_is_kept_no_coverage_loss(self):
        # Conservative contract: a product we cannot classify is NEVER dropped
        # (this is the exact failure mode — coverage loss — of the reverted attempt).
        kept = self._run(["S1A"], ["S1A_IW_1", None, "weird-id-without-prefix"])
        assert "S1A_IW_1" in kept
        assert None in kept  # unparseable kept
        assert "weird-id-without-prefix" in kept

    def test_id_read_from_title_when_id_absent(self):
        import textwrap

        class _FakeProd:
            def __init__(self, **props):
                self.properties = props

        products = [
            _FakeProd(title="S1A_IW_GRDH_x"),
            _FakeProd(title="S1D_IW_GRDH_y"),
        ]
        ns = {"SearchResult": list, "platform_list": ["S1A"], "products": products}
        exec(  # noqa: S102
            compile(textwrap.dedent(patch_mod._PLATFORM_POSTFILTER_NEW), "<pf>", "exec"),
            ns,
        )
        assert [p.properties.get("title") for p in ns["products"]] == ["S1A_IW_GRDH_x"]

    def test_empty_platform_list_is_noop(self):
        kept = self._run([], ["S1A_IW_1", "S1D_IW_2"])
        assert kept == ["S1A_IW_1", "S1D_IW_2"]  # no filtering when unspecified

    def test_longer_platform_value_still_filters(self):
        # A cfg value longer than the 3-char code (S1Tiling's validation accepts any
        # 'S1*' token) must reduce to the code and filter correctly — NOT drop
        # everything (the asymmetric-match bug that the symmetric _eopf_code prevents).
        kept = self._run(["S1A_EXTRA"], ["S1A_IW_1", "S1D_IW_2"])
        assert kept == ["S1A_IW_1"]

    def test_uninterpretable_request_keeps_all_no_coverage_loss(self):
        # A request mapping to no recognised code (e.g. "S1") must drop NOTHING — it
        # degrades to no filtering rather than dropping the wanted product (#311 class).
        kept = self._run(["S1"], ["S1A_IW_1", "S1D_IW_2"])
        assert kept == ["S1A_IW_1", "S1D_IW_2"]

    def test_real_cop_dataspace_shape_uuid_id_safe_name_in_title(self):
        # eodag cop_dataspace: properties['id'] is a UUID, the S1A_… SAFE name is in
        # properties['title'] (mapping title: '{$.Name#remove_extension}'). The UUID
        # id must be ignored (not a valid code prefix) and the platform read off title.
        import textwrap

        class _FakeProd:
            def __init__(self, **props):
                self.properties = props

        products = [
            _FakeProd(id="b1c2d3e4-0000-1111-2222-333344445555", title="S1A_IW_GRDH_1SDV_x"),
            _FakeProd(id="ffffffff-0000-1111-2222-333344445555", title="S1D_IW_GRDH_1SDV_y"),
        ]
        ns = {"SearchResult": list, "platform_list": ["S1A"], "products": products}
        exec(  # noqa: S102
            compile(textwrap.dedent(patch_mod._PLATFORM_POSTFILTER_NEW), "<pf>", "exec"),
            ns,
        )
        assert [p.properties["title"] for p in ns["products"]] == ["S1A_IW_GRDH_1SDV_x"]
