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
        mocker.patch("sys.argv", ["validate_geozarr.py", "s3://bucket/dataset.zarr"])

        with pytest.raises(SystemExit) as exc_info:
            main()

        assert exc_info.value.code == 0
        mock_validate.assert_called_once_with("s3://bucket/dataset.zarr", False)

    def test_with_item_id(self, mocker):
        """Includes item ID in output."""
        mock_validate = mocker.patch("scripts.validate_geozarr.validate_geozarr")
        mock_validate.return_value = {"valid": True, "exit_code": 0}
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

        output_file = tmp_path / "results.json"
        mocker.patch(
            "sys.argv",
            ["validate_geozarr.py", "s3://bucket/dataset.zarr", "--output", str(output_file)],
        )

        with pytest.raises(SystemExit):
            main()

        assert output_file.exists()
        data = json.loads(output_file.read_text())
        assert data["validation"]["valid"] is True

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
