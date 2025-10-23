"""Tests for validate_geozarr.py - GeoZarr compliance validation."""

import json
import subprocess

import pytest

from scripts.validate_geozarr import main, validate_geozarr


class TestValidateGeozarr:
    """Test validation logic."""

    def test_successful_validation(self, mocker):
        """Validation passes when subprocess exits 0."""
        mock_run = mocker.patch("scripts.validate_geozarr.subprocess.run")
        mock_run.return_value = mocker.Mock(
            returncode=0,
            stdout="All checks passed",
            stderr="",
        )

        result = validate_geozarr("s3://bucket/dataset.zarr")

        assert result["valid"] is True
        assert result["exit_code"] == 0
        assert "All checks passed" in result["stdout"]
        mock_run.assert_called_once_with(
            ["eopf-geozarr", "validate", "s3://bucket/dataset.zarr"],
            capture_output=True,
            text=True,
            timeout=300,
        )

    def test_failed_validation(self, mocker):
        """Validation fails when subprocess exits non-zero."""
        mock_run = mocker.patch("scripts.validate_geozarr.subprocess.run")
        mock_run.return_value = mocker.Mock(
            returncode=1,
            stdout="",
            stderr="Missing required attribute: spatial_ref",
        )

        result = validate_geozarr("s3://bucket/invalid.zarr")

        assert result["valid"] is False
        assert result["exit_code"] == 1
        assert "Missing required attribute" in result["stderr"]

    def test_verbose_flag_passed(self, mocker):
        """Verbose flag is passed to subprocess."""
        mock_run = mocker.patch("scripts.validate_geozarr.subprocess.run")
        mock_run.return_value = mocker.Mock(returncode=0, stdout="", stderr="")

        validate_geozarr("s3://bucket/dataset.zarr", verbose=True)

        mock_run.assert_called_once_with(
            ["eopf-geozarr", "validate", "s3://bucket/dataset.zarr", "--verbose"],
            capture_output=True,
            text=True,
            timeout=300,
        )

    def test_timeout_handling(self, mocker):
        """Handles subprocess timeout gracefully."""
        mock_run = mocker.patch("scripts.validate_geozarr.subprocess.run")
        mock_run.side_effect = subprocess.TimeoutExpired(
            cmd=["eopf-geozarr", "validate"], timeout=300
        )

        result = validate_geozarr("s3://bucket/large.zarr")

        assert result["valid"] is False
        assert result["exit_code"] == -1
        assert "timeout" in result["error"].lower()

    def test_subprocess_exception(self, mocker):
        """Handles subprocess exceptions."""
        mock_run = mocker.patch("scripts.validate_geozarr.subprocess.run")
        mock_run.side_effect = FileNotFoundError("eopf-geozarr not found")

        result = validate_geozarr("s3://bucket/dataset.zarr")

        assert result["valid"] is False
        assert result["exit_code"] == -1
        assert "not found" in result["error"]


class TestMainCLI:
    """Test CLI interface."""

    def test_basic_validation(self, mocker):
        """Basic validation without options."""
        mock_validate = mocker.patch("scripts.validate_geozarr.validate_geozarr")
        mock_validate.return_value = {
            "valid": True,
            "exit_code": 0,
            "stdout": "OK",
            "stderr": "",
        }
        # Mock TMS and CF validation functions
        mocker.patch(
            "scripts.validate_geozarr.validate_tile_matrix_set", return_value={"valid": True}
        )
        mocker.patch(
            "scripts.validate_geozarr.validate_cf_conventions", return_value={"valid": True}
        )
        mocker.patch("sys.argv", ["validate_geozarr.py", "s3://bucket/dataset.zarr"])

        with pytest.raises(SystemExit) as exc_info:
            main()

        assert exc_info.value.code == 0
        mock_validate.assert_called_once_with("s3://bucket/dataset.zarr", False)

    def test_with_item_id(self, mocker):
        """Includes item ID in output."""
        mock_validate = mocker.patch("scripts.validate_geozarr.validate_geozarr")
        mock_validate.return_value = {"valid": True, "exit_code": 0}
        # Mock TMS and CF validation functions
        mocker.patch(
            "scripts.validate_geozarr.validate_tile_matrix_set", return_value={"valid": True}
        )
        mocker.patch(
            "scripts.validate_geozarr.validate_cf_conventions", return_value={"valid": True}
        )
        mocker.patch(
            "sys.argv",
            ["validate_geozarr.py", "s3://bucket/dataset.zarr", "--item-id", "test-item-123"],
        )

        with pytest.raises(SystemExit) as exc_info:
            main()

        assert exc_info.value.code == 0

    def test_with_output_file(self, mocker, tmp_path):
        """Writes results to output file."""
        mock_validate = mocker.patch("scripts.validate_geozarr.validate_geozarr")
        mock_validate.return_value = {"valid": True, "exit_code": 0}
        # Mock TMS and CF validation functions
        mocker.patch(
            "scripts.validate_geozarr.validate_tile_matrix_set", return_value={"valid": True}
        )
        mocker.patch(
            "scripts.validate_geozarr.validate_cf_conventions", return_value={"valid": True}
        )

        output_file = tmp_path / "results.json"
        mocker.patch(
            "sys.argv",
            ["validate_geozarr.py", "s3://bucket/dataset.zarr", "--output", str(output_file)],
        )

        with pytest.raises(SystemExit):
            main()

        assert output_file.exists()
        data = json.loads(output_file.read_text())
        with open(output_file) as f:
            data = json.load(f)

        assert data["dataset_path"] == "s3://bucket/dataset.zarr"
        assert data["validations"]["geozarr"]["valid"] is True

    def test_verbose_flag(self, mocker):
        """Verbose flag is passed through."""
        mock_validate = mocker.patch("scripts.validate_geozarr.validate_geozarr")
        mock_validate.return_value = {"valid": True, "exit_code": 0}
        mocker.patch("sys.argv", ["validate_geozarr.py", "s3://bucket/dataset.zarr", "--verbose"])

        with pytest.raises(SystemExit):
            main()

        mock_validate.assert_called_once_with("s3://bucket/dataset.zarr", True)

    def test_failed_validation_exits_1(self, mocker):
        """Failed validation exits with code 1."""
        mock_validate = mocker.patch("scripts.validate_geozarr.validate_geozarr")
        mock_validate.return_value = {"valid": False, "exit_code": 1}
        mocker.patch("sys.argv", ["validate_geozarr.py", "s3://bucket/invalid.zarr"])

        with pytest.raises(SystemExit) as exc_info:
            main()

        assert exc_info.value.code == 1

    def test_creates_output_directory(self, mocker, tmp_path):
        """Creates output directory if it doesn't exist."""
        mock_validate = mocker.patch("scripts.validate_geozarr.validate_geozarr")
        mock_validate.return_value = {"valid": True, "exit_code": 0}

        nested_output = tmp_path / "deep" / "nested" / "results.json"
        mocker.patch(
            "sys.argv",
            ["validate_geozarr.py", "s3://bucket/dataset.zarr", "--output", str(nested_output)],
        )

        with pytest.raises(SystemExit):
            main()

        assert nested_output.exists()
        assert nested_output.parent.exists()


class TestValidateStacItem:
    """Test STAC item validation."""

    def test_valid_stac_item(self, mocker, tmp_path):
        """Test valid STAC item passes."""
        from scripts.validate_geozarr import validate_stac_item

        item_file = tmp_path / "item.json"
        item_file.write_text(
            json.dumps(
                {
                    "type": "Feature",
                    "stac_version": "1.0.0",
                    "id": "test-item",
                    "geometry": {"type": "Point", "coordinates": [0, 0]},
                    "bbox": [0, 0, 0, 0],
                    "properties": {"datetime": "2025-01-01T00:00:00Z"},
                    "links": [],
                    "assets": {},
                }
            )
        )

        result = validate_stac_item(item_file)

        assert result["valid"] is True
        assert result["item_id"] == "test-item"

    def test_invalid_stac_item(self, mocker, tmp_path):
        """Test invalid STAC item fails."""
        from scripts.validate_geozarr import validate_stac_item

        item_file = tmp_path / "bad_item.json"
        item_file.write_text(json.dumps({"invalid": "data"}))

        result = validate_stac_item(item_file)

        assert result["valid"] is False
        assert "error" in result


class TestValidateTileMatrixSet:
    """Test TileMatrixSet validation."""

    def test_valid_tms(self, mocker):
        """Test valid TileMatrixSet."""
        from scripts.validate_geozarr import validate_tile_matrix_set

        mock_store = mocker.Mock()
        mock_store.attrs.asdict.return_value = {
            "tile_matrix_set": {
                "id": "WebMercatorQuad",
                "crs": "http://www.opengis.net/def/crs/EPSG/0/3857",
                "tileMatrices": [
                    {
                        "id": "0",
                        "scaleDenominator": 559082264.0287178,
                        "cellSize": 156543.03392804097,
                        "pointOfOrigin": [-20037508.342789244, 20037508.342789244],
                        "tileWidth": 256,
                        "tileHeight": 256,
                        "matrixWidth": 1,
                        "matrixHeight": 1,
                    }
                ],
            }
        }

        mock_patch = mocker.patch("zarr.open", return_value=mock_store)
        result = validate_tile_matrix_set("s3://bucket/dataset.zarr")
        mock_patch.stop()

        assert result["valid"] is True
        assert result["tms_id"] == "WebMercatorQuad"
        assert "3857" in result["crs"]

    def test_missing_tms(self, mocker):
        """Test missing TileMatrixSet attribute."""
        from scripts.validate_geozarr import validate_tile_matrix_set

        mock_store = mocker.Mock()
        mock_store.attrs.asdict.return_value = {}  # No tile_matrix_set

        mock_patch = mocker.patch("zarr.open", return_value=mock_store)
        result = validate_tile_matrix_set("s3://bucket/dataset.zarr")
        mock_patch.stop()

        assert result["valid"] is False
        assert "Missing" in result["error"]

    def test_tms_exception(self, mocker):
        """Test TMS validation exception handling."""
        from scripts.validate_geozarr import validate_tile_matrix_set

        mock_patch = mocker.patch("zarr.open", side_effect=Exception("Zarr error"))
        result = validate_tile_matrix_set("s3://bucket/dataset.zarr")
        mock_patch.stop()

        assert result["valid"] is False
        assert "error" in result


class TestValidateCFConventions:
    """Test CF-conventions validation."""

    def test_valid_cf(self, mocker):
        """Test valid CF-conventions."""
        from scripts.validate_geozarr import validate_cf_conventions

        mock_var = mocker.Mock()
        mock_var.attrs = {"standard_name": "air_temperature"}

        mock_ds = mocker.Mock()
        mock_ds.data_vars = {"temp": mock_var}
        mock_ds.__getitem__ = mocker.Mock(return_value=mock_var)  # Support ds[var_name]
        mock_ds.cf.decode.return_value = mock_ds

        mock_patch = mocker.patch("xarray.open_zarr", return_value=mock_ds)
        result = validate_cf_conventions("s3://bucket/dataset.zarr")
        mock_patch.stop()

        assert result["valid"] is True

    def test_cf_warnings(self, mocker):
        """Test CF-conventions with warnings."""
        from scripts.validate_geozarr import validate_cf_conventions

        mock_var = mocker.Mock()
        mock_var.attrs = {}  # Missing standard_name/long_name

        mock_ds = mocker.Mock()
        mock_ds.data_vars = {"temp": mock_var}
        mock_ds.__getitem__ = mocker.Mock(return_value=mock_var)  # Support ds[var_name]
        mock_ds.cf.decode.return_value = mock_ds

        mock_patch = mocker.patch("xarray.open_zarr", return_value=mock_ds)
        result = validate_cf_conventions("s3://bucket/dataset.zarr")
        mock_patch.stop()

        assert result["valid"] is True
        assert "warnings" in result
        assert len(result["warnings"]) > 0

    def test_cf_exception(self, mocker):
        """Test CF validation exception handling."""
        from scripts.validate_geozarr import validate_cf_conventions

        mock_patch = mocker.patch("xarray.open_zarr", side_effect=Exception("xarray error"))
        result = validate_cf_conventions("s3://bucket/dataset.zarr")
        mock_patch.stop()

        assert result["valid"] is False
        assert "error" in result


class TestMainWithStacItem:
    """Test main() with STAC item validation."""

    def test_with_stac_item(self, mocker, tmp_path):
        """Test validation with STAC item."""
        mock_validate_geozarr = mocker.patch("scripts.validate_geozarr.validate_geozarr")
        mock_validate_geozarr.return_value = {"valid": True, "exit_code": 0}

        # Mock TMS and CF to return valid so overall validation passes
        mocker.patch(
            "scripts.validate_geozarr.validate_tile_matrix_set", return_value={"valid": True}
        )
        mocker.patch(
            "scripts.validate_geozarr.validate_cf_conventions", return_value={"valid": True}
        )

        item_file = tmp_path / "item.json"
        item_file.write_text(
            json.dumps(
                {
                    "type": "Feature",
                    "stac_version": "1.0.0",
                    "id": "test-item",
                    "geometry": {"type": "Point", "coordinates": [0, 0]},
                    "bbox": [0, 0, 0, 0],
                    "properties": {"datetime": "2025-01-01T00:00:00Z"},
                    "links": [],
                    "assets": {},
                }
            )
        )

        mocker.patch(
            "sys.argv",
            ["validate_geozarr.py", "s3://bucket/dataset.zarr", "--stac-item", str(item_file)],
        )

        with pytest.raises(SystemExit) as exc_info:
            main()

        assert exc_info.value.code == 0

    def test_skip_tms(self, mocker):
        """Test --skip-tms flag."""
        mock_validate = mocker.patch("scripts.validate_geozarr.validate_geozarr")
        mock_validate.return_value = {"valid": True, "exit_code": 0}
        mock_tms = mocker.patch("scripts.validate_geozarr.validate_tile_matrix_set")
        mock_cf = mocker.patch("scripts.validate_geozarr.validate_cf_conventions")
        mock_cf.return_value = {"valid": True}

        mocker.patch("sys.argv", ["validate_geozarr.py", "s3://bucket/dataset.zarr", "--skip-tms"])

        with pytest.raises(SystemExit):
            main()

        mock_tms.assert_not_called()

    def test_skip_cf(self, mocker):
        """Test --skip-cf flag."""
        mock_validate = mocker.patch("scripts.validate_geozarr.validate_geozarr")
        mock_validate.return_value = {"valid": True, "exit_code": 0}
        mock_tms = mocker.patch("scripts.validate_geozarr.validate_tile_matrix_set")
        mock_tms.return_value = {"valid": True}
        mock_cf = mocker.patch("scripts.validate_geozarr.validate_cf_conventions")

        mocker.patch("sys.argv", ["validate_geozarr.py", "s3://bucket/dataset.zarr", "--skip-cf"])

        with pytest.raises(SystemExit):
            main()

        mock_cf.assert_not_called()
