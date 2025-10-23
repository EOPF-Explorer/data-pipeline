"""Unit tests for convert.py - GeoZarr conversion configuration."""

from scripts.convert import CONFIGS


class TestConversionConfigs:
    """Test conversion configuration dictionaries."""

    def test_s2_config_has_all_groups(self):
        """Verify S2 config includes all 4 required groups."""
        s2 = CONFIGS["sentinel-2"]
        assert len(s2["groups"]) == 4
        assert "/measurements/reflectance/r10m" in s2["groups"]
        assert "/measurements/reflectance/r20m" in s2["groups"]
        assert "/measurements/reflectance/r60m" in s2["groups"]
        assert "/quality/l2a_quicklook/r10m" in s2["groups"]

    def test_s2_config_has_crs_groups(self):
        """Verify S2 config includes CRS groups."""
        s2 = CONFIGS["sentinel-2"]
        assert s2["crs_groups"] == ["/conditions/geometry"]

    def test_s2_config_optimized_chunks(self):
        """Verify S2 config has optimized chunk and tile sizes."""
        s2 = CONFIGS["sentinel-2"]
        assert s2["spatial_chunk"] == 1024
        assert s2["tile_width"] == 256
        assert s2["enable_sharding"] is True

    def test_s1_config_structure(self):
        """Verify S1 GRD config structure."""
        s1 = CONFIGS["sentinel-1"]
        assert s1["groups"] == ["/measurements"]
        assert s1["crs_groups"] == ["/conditions/gcp"]
        assert s1["spatial_chunk"] == 4096
        assert s1["tile_width"] == 512
        assert s1["enable_sharding"] is False

    def test_both_missions_present(self):
        """Verify both mission configs exist."""
        assert "sentinel-1" in CONFIGS
        assert "sentinel-2" in CONFIGS

    def test_config_keys_consistent(self):
        """Verify all configs have consistent keys."""
        required_keys = {
            "groups",
            "crs_groups",
            "spatial_chunk",
            "tile_width",
            "enable_sharding",
        }
        for mission, config in CONFIGS.items():
            assert set(config.keys()) == required_keys, f"{mission} missing required keys"
