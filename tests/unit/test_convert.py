"""Unit tests for convert.py helpers and CLI."""

from types import SimpleNamespace

import pytest

import scripts.convert as convert_module
from scripts.convert import CONFIGS, get_config


class TestConversionConfigs:
    """Validate mission configuration dictionaries."""

    def test_sentinel2_config_groups(self) -> None:
        """Sentinel-2 config exposes all expected reflectance groups."""
        s2 = CONFIGS["sentinel-2"]
        assert "/measurements/reflectance/r10m" in s2["groups"]
        assert "/measurements/reflectance/r20m" in s2["groups"]
        assert "/measurements/reflectance/r60m" in s2["groups"]
        assert "/quality/l2a_quicklook/r10m" in s2["groups"]
        assert s2["crs_groups"] == ["/conditions/geometry"]
        assert s2["spatial_chunk"] == 1024
        assert s2["tile_width"] == 256
        assert s2["enable_sharding"] is True

    def test_sentinel1_config_groups(self) -> None:
        """Sentinel-1 config exposes VH configuration."""
        s1 = CONFIGS["sentinel-1"]
        assert s1["groups"] == ["/measurements"]
        assert s1["crs_groups"] == ["/conditions/gcp"]
        assert s1["spatial_chunk"] == 4096
        assert s1["tile_width"] == 512
        assert s1["enable_sharding"] is False

    def test_config_key_consistency(self) -> None:
        """All configs share the same key set."""
        expected = {
            "groups",
            "crs_groups",
            "spatial_chunk",
            "tile_width",
            "enable_sharding",
        }
        for name, config in CONFIGS.items():
            assert set(config) == expected, f"{name} missing expected keys"

    def test_get_config_defaults_to_s2(self) -> None:
        """Unknown collection IDs fall back to Sentinel-2."""
        result = get_config("unknown-collection")
        assert result == CONFIGS["sentinel-2"]
        assert result is not CONFIGS["sentinel-2"]  # defensive copy

    def test_get_config_matches_prefix(self) -> None:
        """Prefix detection pulls Sentinel-1 config from collection id."""
        result = get_config("sentinel-1-grd")
        assert result == CONFIGS["sentinel-1"]
        assert result is not CONFIGS["sentinel-1"]


class FakeHttpResponse:
    """Minimal httpx.Response replacement."""

    def __init__(self, payload: dict):
        self._payload = payload

    def raise_for_status(self) -> "FakeHttpResponse":
        return self

    def json(self) -> dict:
        return self._payload


class FakeHttpClient:
    """Minimal httpx.Client stand-in for deterministic responses."""

    def __init__(self, payload: dict):
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get(self, url: str) -> FakeHttpResponse:
        return FakeHttpResponse(self._payload)


def test_get_zarr_url_prefers_product(monkeypatch: pytest.MonkeyPatch) -> None:
    """Product asset takes precedence when present."""
    payload = {"assets": {"product": {"href": "s3://bucket/product.zarr"}}}
    monkeypatch.setattr(convert_module.httpx, "Client", lambda *a, **k: FakeHttpClient(payload))

    result = convert_module.get_zarr_url("https://example/items/test")

    assert result == "s3://bucket/product.zarr"


def test_get_zarr_url_falls_back_to_any_zarr(monkeypatch: pytest.MonkeyPatch) -> None:
    """Falls back to the first asset containing .zarr in href."""
    payload = {"assets": {"other": {"href": "https://foo/bar/data.zarr"}}}
    monkeypatch.setattr(convert_module.httpx, "Client", lambda *a, **k: FakeHttpClient(payload))

    result = convert_module.get_zarr_url("https://example/items/test")

    assert result == "https://foo/bar/data.zarr"


def test_get_zarr_url_raises_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raises a RuntimeError when no Zarr asset is found."""
    payload = {"assets": {"foo": {"href": "https://foo/bar.tif"}}}
    monkeypatch.setattr(convert_module.httpx, "Client", lambda *a, **k: FakeHttpClient(payload))

    with pytest.raises(RuntimeError):
        convert_module.get_zarr_url("https://example/items/test")


def test_run_conversion_with_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    """run_conversion applies overrides and orchestrates conversion helpers."""

    calls = {}
    monkeypatch.setattr(convert_module, "get_zarr_url", lambda url: "s3://source/input.zarr")

    class FakeFS:
        def __init__(self) -> None:
            self.rm_calls = []

        def rm(self, path: str, recursive: bool = False) -> None:
            self.rm_calls.append((path, recursive))
            raise FileNotFoundError()

    fake_fs = FakeFS()
    monkeypatch.setattr(convert_module.fsspec, "filesystem", lambda *a, **k: fake_fs)
    monkeypatch.setattr(convert_module, "get_storage_options", lambda url: {"token": "abc"})

    fake_dt = SimpleNamespace(children={"a": object(), "b": object()})
    monkeypatch.setattr(
        convert_module.xr,
        "open_datatree",
        lambda *a, **k: fake_dt,
    )

    def record_create(**kwargs):
        calls.update(kwargs)

    monkeypatch.setattr(convert_module, "create_geozarr_dataset", record_create)

    result = convert_module.run_conversion(
        source_url="https://example/stac/collections/foo/items/S2_TEST",
        collection="sentinel-2-l2a",
        s3_output_bucket="out-bucket",
        s3_output_prefix="out-prefix",
        groups="/foo,/bar",
        spatial_chunk=2048,
        tile_width=128,
        enable_sharding=False,
    )

    expected_output = "s3://out-bucket/out-prefix/sentinel-2-l2a/S2_TEST.zarr"
    assert result == expected_output
    assert fake_fs.rm_calls == [(expected_output, True)]
    assert calls["dt_input"] is fake_dt
    assert calls["groups"] == ["/foo", "/bar"]
    assert calls["spatial_chunk"] == 2048
    assert calls["tile_width"] == 128
    assert calls["enable_sharding"] is False
    assert calls["output_path"] == expected_output


def test_run_conversion_skips_stac_lookup_for_direct_zarr(monkeypatch: pytest.MonkeyPatch) -> None:
    """Direct Zarr URLs bypass STAC lookup and reuse default config."""

    called = False

    def fail_call(url: str) -> str:
        nonlocal called
        called = True
        return "should-not-be-used"

    monkeypatch.setattr(convert_module, "get_zarr_url", fail_call)

    class CleanFS:
        def __init__(self) -> None:
            self.rm_calls = []

        def rm(self, path: str, recursive: bool = False) -> None:
            self.rm_calls.append((path, recursive))

    clean_fs = CleanFS()
    monkeypatch.setattr(convert_module.fsspec, "filesystem", lambda *a, **k: clean_fs)
    monkeypatch.setattr(convert_module, "get_storage_options", lambda url: {})

    fake_dt = SimpleNamespace(children={})
    monkeypatch.setattr(convert_module.xr, "open_datatree", lambda *a, **k: fake_dt)

    create_kwargs = {}

    def capture_create(**kwargs):
        create_kwargs.update(kwargs)

    monkeypatch.setattr(convert_module, "create_geozarr_dataset", capture_create)

    result = convert_module.run_conversion(
        source_url="s3://input/data.zarr",
        collection="sentinel-1-grd",
        s3_output_bucket="bucket",
        s3_output_prefix="prefix",
    )

    expected_output = "s3://bucket/prefix/sentinel-1-grd/data.zarr.zarr"
    assert result == expected_output
    assert not called
    assert clean_fs.rm_calls == [(expected_output, True)]
    assert create_kwargs["groups"] == CONFIGS["sentinel-1"]["groups"]
    assert create_kwargs["crs_groups"] == CONFIGS["sentinel-1"]["crs_groups"]


def test_run_conversion_warns_on_cleanup_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-FileNotFound cleanup errors do not abort conversion."""

    monkeypatch.setattr(convert_module, "get_zarr_url", lambda url: "s3://source/input.zarr")

    class ErrorFS:
        def rm(self, path: str, recursive: bool = False) -> None:
            raise RuntimeError("boom")

    monkeypatch.setattr(convert_module.fsspec, "filesystem", lambda *a, **k: ErrorFS())
    monkeypatch.setattr(convert_module, "get_storage_options", lambda url: {})

    fake_dt = SimpleNamespace(children={})
    monkeypatch.setattr(convert_module.xr, "open_datatree", lambda *a, **k: fake_dt)
    monkeypatch.setattr(convert_module, "create_geozarr_dataset", lambda **kwargs: None)

    result = convert_module.run_conversion(
        source_url="https://example/stac/items/S2_TEST",
        collection="sentinel-2-l2a",
        s3_output_bucket="bucket",
        s3_output_prefix="prefix",
    )

    assert result.endswith("/S2_TEST.zarr")


def test_convert_main_invokes_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """CLI main function forwards arguments into run_conversion."""

    parsed = SimpleNamespace(
        source_url="src",
        collection="sentinel-2",
        s3_output_bucket="bucket",
        s3_output_prefix="prefix",
        groups=None,
        spatial_chunk=None,
        tile_width=None,
        enable_sharding=False,
    )

    class FakeParser:
        def add_argument(self, *args, **kwargs):
            return None

        def parse_args(self):
            return parsed

    monkeypatch.setattr(convert_module.argparse, "ArgumentParser", lambda *a, **k: FakeParser())

    received = {}

    def capture_run(**kwargs):
        received.update(kwargs)

    monkeypatch.setattr(convert_module, "run_conversion", capture_run)

    convert_module.main()

    assert received == {
        "source_url": "src",
        "collection": "sentinel-2",
        "s3_output_bucket": "bucket",
        "s3_output_prefix": "prefix",
        "groups": None,
        "spatial_chunk": None,
        "tile_width": None,
        "enable_sharding": False,
    }
