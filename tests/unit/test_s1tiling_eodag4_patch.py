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
