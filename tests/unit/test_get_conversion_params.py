"""Tests for get_conversion_params.py - Collection registry logic."""

import json
import os

import pytest

from scripts.get_conversion_params import (
    _match_collection_config,
    get_conversion_params,
    main,
)


class TestMatchCollectionConfig:
    """Test pattern matching logic."""

    def test_exact_match_s2(self):
        """Exact collection ID matches S2 pattern."""
        config = _match_collection_config("sentinel-2-l2a")
        assert config is not None
        assert config["pattern"] == "sentinel-2-l2a*"

    def test_pattern_match_s2_with_suffix(self):
        """S2 collection with suffix matches pattern."""
        config = _match_collection_config("sentinel-2-l2a-dp-test")
        assert config is not None
        assert config["conversion"]["groups"] == "/quality/l2a_quicklook/r10m"

    def test_exact_match_s1(self):
        """Exact collection ID matches S1 pattern."""
        config = _match_collection_config("sentinel-1-l1-grd")
        assert config is not None
        assert config["pattern"] == "sentinel-1-l1-grd*"

    def test_pattern_match_s1_with_suffix(self):
        """S1 collection with suffix matches pattern."""
        config = _match_collection_config("sentinel-1-l1-grd-dp-production")
        assert config is not None
        assert config["conversion"]["groups"] == "/measurements"
        assert "--gcp-group" in config["conversion"]["extra_flags"]

    def test_no_match_unknown_collection(self):
        """Unknown collection returns None."""
        config = _match_collection_config("sentinel-3-olci")
        assert config is None

    def test_no_match_empty_string(self):
        """Empty collection ID returns None."""
        config = _match_collection_config("")
        assert config is None


class TestGetConversionParams:
    """Test parameter retrieval with fallback."""

    def test_s2_parameters(self):
        """S2 L2A returns correct conversion parameters."""
        params = get_conversion_params("sentinel-2-l2a")
        assert params["groups"] == "/quality/l2a_quicklook/r10m"
        assert params["extra_flags"] == "--crs-groups /quality/l2a_quicklook/r10m"
        assert params["spatial_chunk"] == 4096
        assert params["tile_width"] == 512

    def test_s1_parameters(self):
        """S1 GRD returns correct conversion parameters."""
        params = get_conversion_params("sentinel-1-l1-grd")
        assert params["groups"] == "/measurements"
        assert params["extra_flags"] == "--gcp-group /conditions/gcp"
        assert params["spatial_chunk"] == 4096
        assert params["tile_width"] == 512

    def test_s2_with_suffix_uses_same_config(self):
        """S2 variants use same config."""
        params1 = get_conversion_params("sentinel-2-l2a")
        params2 = get_conversion_params("sentinel-2-l2a-dp-test")
        assert params1 == params2

    def test_s1_with_suffix_uses_same_config(self):
        """S1 variants use same config."""
        params1 = get_conversion_params("sentinel-1-l1-grd")
        params2 = get_conversion_params("sentinel-1-l1-grd-production")
        assert params1 == params2

    def test_unknown_collection_falls_back_to_default(self):
        """Unknown collection falls back to S2 default."""
        params = get_conversion_params("sentinel-3-olci")
        # Should use sentinel-2-l2a as default
        assert params["groups"] == "/quality/l2a_quicklook/r10m"
        assert params["spatial_chunk"] == 4096


class TestMainCLI:
    """Test CLI interface."""

    def test_shell_format_default(self, capsys):
        """Default shell output format."""
        result = main(["--collection", "sentinel-2-l2a"])
        assert result == 0
        captured = capsys.readouterr()
        assert "ZARR_GROUPS='/quality/l2a_quicklook/r10m'" in captured.out
        assert "EXTRA_FLAGS='--crs-groups /quality/l2a_quicklook/r10m'" in captured.out
        assert "CHUNK=4096" in captured.out
        assert "TILE_WIDTH=512" in captured.out

    def test_shell_format_s1(self, capsys):
        """Shell output for S1."""
        result = main(["--collection", "sentinel-1-l1-grd", "--format", "shell"])
        assert result == 0
        captured = capsys.readouterr()
        assert "ZARR_GROUPS='/measurements'" in captured.out
        assert "EXTRA_FLAGS='--gcp-group /conditions/gcp'" in captured.out
        assert "CHUNK=4096" in captured.out

    def test_json_format(self, capsys):
        """JSON output format."""
        result = main(["--collection", "sentinel-2-l2a", "--format", "json"])
        assert result == 0
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert data["groups"] == "/quality/l2a_quicklook/r10m"
        assert data["spatial_chunk"] == 4096

    def test_single_param_groups(self, capsys):
        """Get single parameter: groups."""
        result = main(["--collection", "sentinel-1-l1-grd", "--param", "groups"])
        assert result == 0
        captured = capsys.readouterr()
        assert captured.out.strip() == "/measurements"

    def test_single_param_extra_flags(self, capsys):
        """Get single parameter: extra_flags."""
        result = main(["--collection", "sentinel-1-l1-grd", "--param", "extra_flags"])
        assert result == 0
        captured = capsys.readouterr()
        assert "--gcp-group /conditions/gcp" in captured.out

    def test_single_param_spatial_chunk(self, capsys):
        """Get single parameter: spatial_chunk."""
        result = main(["--collection", "sentinel-2-l2a", "--param", "spatial_chunk"])
        assert result == 0
        captured = capsys.readouterr()
        assert captured.out.strip() == "4096"

    def test_single_param_tile_width(self, capsys):
        """Get single parameter: tile_width."""
        result = main(["--collection", "sentinel-2-l2a", "--param", "tile_width"])
        assert result == 0
        captured = capsys.readouterr()
        assert captured.out.strip() == "512"

    def test_missing_collection_arg(self, capsys):
        """Missing --collection argument fails."""
        with pytest.raises(SystemExit):
            main([])

    def test_unknown_collection_uses_default(self, capsys):
        """Unknown collection uses default config."""
        result = main(["--collection", "sentinel-99-unknown"])
        assert result == 0
        captured = capsys.readouterr()
        # Should fall back to S2 default
        assert "ZARR_GROUPS='/quality/l2a_quicklook/r10m'" in captured.out


class TestEnvironmentVariableOverrides:
    """Test environment variable override functionality."""

    def test_override_groups(self, monkeypatch):
        """OVERRIDE_GROUPS overrides default groups."""
        monkeypatch.setenv("OVERRIDE_GROUPS", "/custom/groups")
        params = get_conversion_params("sentinel-2-l2a")
        assert params["groups"] == "/custom/groups"
        assert params["spatial_chunk"] == 4096  # Other params unchanged

    def test_override_extra_flags(self, monkeypatch):
        """OVERRIDE_EXTRA_FLAGS overrides default flags."""
        monkeypatch.setenv("OVERRIDE_EXTRA_FLAGS", "--custom-flag")
        params = get_conversion_params("sentinel-1-l1-grd")
        assert params["extra_flags"] == "--custom-flag"

    def test_override_spatial_chunk(self, monkeypatch):
        """OVERRIDE_SPATIAL_CHUNK overrides default chunk size."""
        monkeypatch.setenv("OVERRIDE_SPATIAL_CHUNK", "8192")
        params = get_conversion_params("sentinel-2-l2a")
        assert params["spatial_chunk"] == 8192
        assert isinstance(params["spatial_chunk"], int)

    def test_override_tile_width(self, monkeypatch):
        """OVERRIDE_TILE_WIDTH overrides default tile width."""
        monkeypatch.setenv("OVERRIDE_TILE_WIDTH", "1024")
        params = get_conversion_params("sentinel-1-l1-grd")
        assert params["tile_width"] == 1024
        assert isinstance(params["tile_width"], int)

    def test_multiple_overrides(self, monkeypatch):
        """Multiple overrides work together."""
        monkeypatch.setenv("OVERRIDE_GROUPS", "/test/path")
        monkeypatch.setenv("OVERRIDE_SPATIAL_CHUNK", "2048")
        params = get_conversion_params("sentinel-2-l2a")
        assert params["groups"] == "/test/path"
        assert params["spatial_chunk"] == 2048
        # Non-overridden values remain default
        assert params["extra_flags"] == "--crs-groups /quality/l2a_quicklook/r10m"

    def test_override_empty_string(self, monkeypatch):
        """Empty string override is allowed."""
        monkeypatch.setenv("OVERRIDE_EXTRA_FLAGS", "")
        params = get_conversion_params("sentinel-1-l1-grd")
        assert params["extra_flags"] == ""

    def test_no_override_uses_default(self):
        """Without env vars, uses configuration defaults."""
        # Ensure no env vars are set
        for var in [
            "OVERRIDE_GROUPS",
            "OVERRIDE_EXTRA_FLAGS",
            "OVERRIDE_SPATIAL_CHUNK",
            "OVERRIDE_TILE_WIDTH",
        ]:
            if var in os.environ:
                del os.environ[var]

        params = get_conversion_params("sentinel-2-l2a")
        assert params["groups"] == "/quality/l2a_quicklook/r10m"
        assert params["spatial_chunk"] == 4096
