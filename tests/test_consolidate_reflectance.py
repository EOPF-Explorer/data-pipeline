#!/usr/bin/env python3
"""Unit tests for reflectance asset consolidation."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pystac import Item

# Import the function we're testing
import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
from register import consolidate_reflectance_assets


def get_fixture_files():
    """Get all STAC item fixture files."""
    fixtures_dir = Path(__file__).parent / "fixtures" / "stac_to_register"
    return list(fixtures_dir.glob("*.json"))


@pytest.fixture(params=get_fixture_files())
def stac_item(request):
    """Load a STAC item from fixtures."""
    with open(request.param) as f:
        return Item.from_dict(json.load(f))


def has_old_format_assets(item: Item) -> bool:
    """Check if item has old-format reflectance assets (SR_* or B*_*)."""
    return any(
        key.startswith("SR_") or (key.startswith("B") and "_" in key and any(key.endswith(f"_{res}") for res in ["10m", "20m", "60m"]))
        for key in item.assets.keys()
    )


def has_new_format_asset(item: Item) -> bool:
    """Check if item has new-format reflectance asset with cube metadata."""
    if "reflectance" not in item.assets:
        return False
    reflectance = item.assets["reflectance"]
    return hasattr(reflectance, "extra_fields") and "cube:variables" in reflectance.extra_fields


class TestConsolidateReflectanceAssets:
    """Test suite for consolidate_reflectance_assets function.
    
    Tests all items in tests/fixtures/stac_to_register/ to ensure
    consolidation works for any item dropped in that folder.
    """

    def test_after_consolidation_has_reflectance_asset(self, stac_item):
        """Test that after consolidation, item has a reflectance asset."""
        consolidate_reflectance_assets(
            stac_item,
            "s3://test-bucket/test-prefix/sentinel-2-l2a/test-item.zarr",
            "https://test-endpoint.com"
        )
        
        assert "reflectance" in stac_item.assets, "Should have reflectance asset after consolidation"

    def test_after_consolidation_no_old_format_assets(self, stac_item):
        """Test that after consolidation, old-format assets are removed."""
        consolidate_reflectance_assets(
            stac_item,
            "s3://test-bucket/test-prefix/sentinel-2-l2a/test-item.zarr",
            "https://test-endpoint.com"
        )
        
        # Verify no old-format assets remain
        for key in stac_item.assets.keys():
            assert not key.startswith("SR_"), f"SR_ asset {key} should be removed"
            assert not (key.startswith("B") and "_" in key and any(key.endswith(f"_{res}") for res in ["10m", "20m", "60m"])), \
                f"Band asset {key} should be removed"

    def test_reflectance_asset_has_bands_array(self, stac_item):
        """Test that reflectance asset has bands array."""
        consolidate_reflectance_assets(
            stac_item,
            "s3://test-bucket/test-prefix/sentinel-2-l2a/test-item.zarr",
            "https://test-endpoint.com"
        )
        
        reflectance = stac_item.assets["reflectance"]
        assert hasattr(reflectance, "extra_fields"), "Asset should have extra_fields"
        assert "bands" in reflectance.extra_fields, "Should have bands array"
        
        bands = reflectance.extra_fields["bands"]
        assert isinstance(bands, list), "Bands should be a list"
        assert len(bands) > 0, "Should have at least one band"
        
        # Verify all bands have required structure
        for band in bands:
            assert "name" in band, "Band should have name"
            assert "/" in band["name"], "Band name should include resolution prefix (e.g., r10m/b02)"
            assert "description" in band, "Band should have description"
            assert "gsd" in band, "Band should have gsd"

    def test_reflectance_asset_has_cube_variables(self, stac_item):
        """Test that reflectance asset has cube:variables."""
        consolidate_reflectance_assets(
            stac_item,
            "s3://test-bucket/test-prefix/sentinel-2-l2a/test-item.zarr",
            "https://test-endpoint.com"
        )
        
        reflectance = stac_item.assets["reflectance"]
        assert "cube:variables" in reflectance.extra_fields, "Should have cube:variables"
        
        cube_vars = reflectance.extra_fields["cube:variables"]
        assert isinstance(cube_vars, dict), "cube:variables should be a dict"
        assert len(cube_vars) > 0, "Should have at least one variable"
        
        # Verify all variables have required structure
        for var_name, var_data in cube_vars.items():
            assert "dimensions" in var_data, f"Variable {var_name} should have dimensions"
            assert "description" in var_data, f"Variable {var_name} should have description"
            assert "type" in var_data, f"Variable {var_name} should have type"
            assert var_data["dimensions"] == ["y", "x"], f"Variable {var_name} should have y,x dimensions"
            assert var_data["type"] == "data", f"Variable {var_name} should have type 'data'"

    def test_reflectance_asset_has_cube_dimensions(self, stac_item):
        """Test that reflectance asset has cube:dimensions."""
        consolidate_reflectance_assets(
            stac_item,
            "s3://test-bucket/test-prefix/sentinel-2-l2a/test-item.zarr",
            "https://test-endpoint.com"
        )
        
        reflectance = stac_item.assets["reflectance"]
        assert "cube:dimensions" in reflectance.extra_fields, "Should have cube:dimensions"
        
        cube_dims = reflectance.extra_fields["cube:dimensions"]
        assert isinstance(cube_dims, dict), "cube:dimensions should be a dict"
        assert "x" in cube_dims, "Should have x dimension"
        assert "y" in cube_dims, "Should have y dimension"
        
        # Verify dimension structure
        for dim_name in ["x", "y"]:
            dim = cube_dims[dim_name]
            assert "type" in dim, f"Dimension {dim_name} should have type"
            assert dim["type"] == "spatial", f"Dimension {dim_name} should be spatial"
            assert "axis" in dim, f"Dimension {dim_name} should have axis"
            assert dim["axis"] == dim_name, f"Dimension {dim_name} should have axis={dim_name}"
            assert "reference_system" in dim, f"Dimension {dim_name} should have reference_system"

    def test_reflectance_asset_media_type(self, stac_item):
        """Test that reflectance asset has correct media type."""
        consolidate_reflectance_assets(
            stac_item,
            "s3://test-bucket/test-prefix/sentinel-2-l2a/test-item.zarr",
            "https://test-endpoint.com"
        )
        
        reflectance = stac_item.assets["reflectance"]
        assert reflectance.media_type == "application/vnd+zarr; version=2; profile=multiscales", \
            "Should have correct zarr media type"

    def test_reflectance_asset_href_structure(self, stac_item):
        """Test that reflectance asset has correct href structure."""
        consolidate_reflectance_assets(
            stac_item,
            "s3://test-bucket/test-prefix/sentinel-2-l2a/test-item.zarr",
            "https://test-endpoint.com"
        )
        
        reflectance = stac_item.assets["reflectance"]
        assert "measurements/reflectance" in reflectance.href, \
            "HREF should point to measurements/reflectance"
        assert reflectance.href.startswith("https://"), "HREF should be HTTPS"

    def test_band_names_match_cube_variables(self, stac_item):
        """Test that band names correspond to cube:variables keys."""
        consolidate_reflectance_assets(
            stac_item,
            "s3://test-bucket/test-prefix/sentinel-2-l2a/test-item.zarr",
            "https://test-endpoint.com"
        )
        
        reflectance = stac_item.assets["reflectance"]
        bands = reflectance.extra_fields["bands"]
        cube_vars = reflectance.extra_fields["cube:variables"]
        
        # Extract band names without resolution prefix
        band_names = {band["name"].split("/")[-1] for band in bands}
        var_names = set(cube_vars.keys())
        
        assert band_names == var_names, \
            f"Band names {band_names} should match variable names {var_names}"

    def test_reflectance_asset_roles(self, stac_item):
        """Test that reflectance asset has correct roles."""
        consolidate_reflectance_assets(
            stac_item,
            "s3://test-bucket/test-prefix/sentinel-2-l2a/test-item.zarr",
            "https://test-endpoint.com"
        )
        
        reflectance = stac_item.assets["reflectance"]
        assert "data" in reflectance.roles, "Should have 'data' role"
        assert "reflectance" in reflectance.roles, "Should have 'reflectance' role"

    def test_bands_are_sorted(self, stac_item):
        """Test that bands are sorted by name for consistency."""
        consolidate_reflectance_assets(
            stac_item,
            "s3://test-bucket/test-prefix/sentinel-2-l2a/test-item.zarr",
            "https://test-endpoint.com"
        )
        
        reflectance = stac_item.assets["reflectance"]
        bands = reflectance.extra_fields["bands"]
        
        band_names = [band["name"] for band in bands]
        assert band_names == sorted(band_names), "Bands should be sorted by name"
