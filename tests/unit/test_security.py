"""Security-focused tests for input validation at script entry points."""

import sys
from unittest.mock import patch

import pytest

# ── query_stac.py validators ──────────────────────────────────────────────────
from query_stac import _require_https, _validate_bbox


class TestRequireHttps:
    def test_accepts_https_url(self):
        _require_https("https://stac.example.com/api", "TEST_URL")

    def test_rejects_http_url(self):
        with pytest.raises(SystemExit, match="must be an HTTPS URL"):
            _require_https("http://stac.example.com/api", "TEST_URL")

    def test_rejects_file_scheme(self):
        with pytest.raises(SystemExit, match="must be an HTTPS URL"):
            _require_https("file:///etc/passwd", "TEST_URL")

    def test_rejects_ftp_scheme(self):
        with pytest.raises(SystemExit, match="must be an HTTPS URL"):
            _require_https("ftp://example.com/data", "TEST_URL")

    def test_rejects_empty_string(self):
        with pytest.raises(SystemExit, match="must be an HTTPS URL"):
            _require_https("", "TEST_URL")

    def test_error_message_includes_name(self):
        with pytest.raises(SystemExit) as exc_info:
            _require_https("http://bad.com", "SOURCE_STAC_API_URL")
        assert "SOURCE_STAC_API_URL" in str(exc_info.value)


class TestValidateBbox:
    def test_accepts_valid_bbox(self):
        _validate_bbox([-5.14, 41.33, 9.56, 51.09])

    def test_accepts_integer_coordinates(self):
        _validate_bbox([-5, 41, 9, 51])

    def test_rejects_wrong_length_short(self):
        with pytest.raises(SystemExit, match="4 numbers"):
            _validate_bbox([1.0, 2.0, 3.0])

    def test_rejects_wrong_length_long(self):
        with pytest.raises(SystemExit, match="4 numbers"):
            _validate_bbox([1.0, 2.0, 3.0, 4.0, 5.0])

    def test_rejects_non_list(self):
        with pytest.raises(SystemExit, match="4 numbers"):
            _validate_bbox("not a list")

    def test_rejects_string_element(self):
        with pytest.raises(SystemExit, match="must be a number"):
            _validate_bbox([1.0, 2.0, "three", 4.0])

    def test_rejects_none_element(self):
        with pytest.raises(SystemExit, match="must be a number"):
            _validate_bbox([1.0, 2.0, None, 4.0])


# ── query_stac.py main() integration ─────────────────────────────────────────


def test_query_stac_main_rejects_http_source(monkeypatch):
    """main() exits on non-HTTPS source URL."""
    argv = [
        "script",
        "http://stac.example.com",  # source — not HTTPS
        "sentinel-2-l2a",
        "https://target.example.com",
        "sentinel-2-l2a-staging",
        "2024-01-01T00:00:00Z",
        "24",
        "[-5, 41, 9, 51]",
    ]
    with patch.object(sys, "argv", argv):
        from query_stac import main

        with pytest.raises(SystemExit):
            main()


def test_query_stac_main_rejects_http_target(monkeypatch):
    """main() exits on non-HTTPS target URL."""
    argv = [
        "script",
        "https://stac.example.com",
        "sentinel-2-l2a",
        "http://target.example.com",  # target — not HTTPS
        "sentinel-2-l2a-staging",
        "2024-01-01T00:00:00Z",
        "24",
        "[-5, 41, 9, 51]",
    ]
    with patch.object(sys, "argv", argv):
        from query_stac import main

        with pytest.raises(SystemExit):
            main()


def test_query_stac_main_rejects_invalid_bbox(monkeypatch):
    """main() exits when bbox has wrong number of elements."""
    argv = [
        "script",
        "https://stac.example.com",
        "sentinel-2-l2a",
        "https://target.example.com",
        "sentinel-2-l2a-staging",
        "2024-01-01T00:00:00Z",
        "24",
        "[1, 2, 3]",  # only 3 elements
    ]
    with patch.object(sys, "argv", argv):
        from query_stac import main

        with pytest.raises(SystemExit):
            main()


# ── register_v1.py main() ────────────────────────────────────────────────────


def test_register_v1_rejects_http_source_url():
    """main() returns 1 and does not call run_registration on non-HTTPS source URL."""
    from register_v1 import main

    result = main(
        [
            "--source-url",
            "http://stac.example.com/item.json",
            "--collection",
            "test",
            "--stac-api-url",
            "https://api.example.com/stac",
            "--raster-api-url",
            "https://raster.example.com",
            "--s3-endpoint",
            "https://s3.example.com",
            "--s3-output-bucket",
            "mybucket",
            "--s3-output-prefix",
            "myprefix",
        ]
    )
    assert result == 1


def test_register_v1_rejects_http_stac_api_url():
    """main() returns 1 when --stac-api-url is not HTTPS."""
    from register_v1 import main

    result = main(
        [
            "--source-url",
            "https://stac.example.com/item.json",
            "--collection",
            "test",
            "--stac-api-url",
            "http://api.example.com/stac",
            "--raster-api-url",
            "https://raster.example.com",
            "--s3-endpoint",
            "https://s3.example.com",
            "--s3-output-bucket",
            "mybucket",
            "--s3-output-prefix",
            "myprefix",
        ]
    )
    assert result == 1


def test_register_v1_rejects_http_explorer_base_url(monkeypatch):
    """main() returns 1 when EXPLORER_BASE_URL env var is not HTTPS."""
    monkeypatch.setenv("EXPLORER_BASE_URL", "http://explorer.example.com")
    import importlib

    import register_v1

    importlib.reload(register_v1)

    result = register_v1.main(
        [
            "--source-url",
            "https://stac.example.com/item.json",
            "--collection",
            "test",
            "--stac-api-url",
            "https://api.example.com/stac",
            "--raster-api-url",
            "https://raster.example.com",
            "--s3-endpoint",
            "https://s3.example.com",
            "--s3-output-bucket",
            "mybucket",
            "--s3-output-prefix",
            "myprefix",
        ]
    )
    assert result == 1


# ── convert_v1_s2.py main() ──────────────────────────────────────────────────


def test_convert_v1_s2_rejects_http_source_url():
    """main() returns 1 when --source-url is not HTTPS."""
    from convert_v1_s2 import main

    result = main.__wrapped__() if hasattr(main, "__wrapped__") else None

    # Call via argparse by patching sys.argv
    with patch.object(
        sys,
        "argv",
        [
            "convert_v1_s2",
            "--source-url",
            "http://stac.example.com/item.json",
            "--collection",
            "test",
            "--s3-output-bucket",
            "mybucket",
            "--s3-output-prefix",
            "myprefix",
        ],
    ):
        from convert_v1_s2 import main as convert_main

        result = convert_main()

    assert result == 1


def test_convert_v1_s2_accepts_https_source_url_proceeds_to_conversion(mocker):
    """main() proceeds past validation with a valid HTTPS URL."""
    mocker.patch("convert_v1_s2.run_conversion", return_value="s3://bucket/output.zarr")

    with patch.object(
        sys,
        "argv",
        [
            "convert_v1_s2",
            "--source-url",
            "https://stac.example.com/item.json",
            "--collection",
            "test",
            "--s3-output-bucket",
            "mybucket",
            "--s3-output-prefix",
            "myprefix",
        ],
    ):
        from convert_v1_s2 import main as convert_main

        result = convert_main()

    assert result == 0
