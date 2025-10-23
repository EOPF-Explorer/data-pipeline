"""Tests for get_conversion_params.py - Collection parameter lookup."""

import json

from scripts.get_conversion_params import get_conversion_params, main


class TestGetConversionParams:
    """Test parameter lookup logic."""

    def test_sentinel_2_exact(self):
        """Sentinel-2 collection returns S2 config."""
        params = get_conversion_params("sentinel-2-l2a")
        assert params["groups"] == "/quality/l2a_quicklook/r10m"
        assert "--crs-groups" in params["extra_flags"]
        assert params["spatial_chunk"] == 4096
        assert params["tile_width"] == 512

    def test_sentinel_2_with_suffix(self):
        """Sentinel-2 with suffix matches."""
        params = get_conversion_params("sentinel-2-l2a-dp-test")
        assert params["groups"] == "/quality/l2a_quicklook/r10m"

    def test_sentinel_1_exact(self):
        """Sentinel-1 collection returns S1 config."""
        params = get_conversion_params("sentinel-1-l1-grd")
        assert params["groups"] == "/measurements"
        assert "--gcp-group" in params["extra_flags"]

    def test_sentinel_1_with_suffix(self):
        """Sentinel-1 with suffix matches."""
        params = get_conversion_params("sentinel-1-l1-grd-dp-production")
        assert params["groups"] == "/measurements"

    def test_unknown_defaults_to_s2(self):
        """Unknown collection defaults to Sentinel-2."""
        params = get_conversion_params("sentinel-3-olci")
        assert params["groups"] == "/quality/l2a_quicklook/r10m"

    def test_case_insensitive(self):
        """Collection matching is case-insensitive."""
        lower = get_conversion_params("sentinel-2-l2a")
        upper = get_conversion_params("SENTINEL-2-L2A")
        assert lower == upper


class TestCLI:
    """Test command-line interface."""

    def test_json_output(self, capsys):
        """JSON format outputs valid JSON."""
        main(["--collection", "sentinel-2-l2a", "--format", "json"])
        output = capsys.readouterr().out
        params = json.loads(output)
        assert params["groups"] == "/quality/l2a_quicklook/r10m"

    def test_shell_output(self, capsys):
        """Shell format outputs environment variables."""
        main(["--collection", "sentinel-1-l1-grd"])
        output = capsys.readouterr().out
        assert "ZARR_GROUPS='/measurements'" in output
        assert "CHUNK=4096" in output

    def test_single_param(self, capsys):
        """Single parameter extraction."""
        main(["--collection", "sentinel-2-l2a", "--param", "groups"])
        output = capsys.readouterr().out.strip()
        assert output == "/quality/l2a_quicklook/r10m"
