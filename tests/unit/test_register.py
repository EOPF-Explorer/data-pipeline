"""Concise unit tests for scripts.register.

Fixture-driven and parametrized to be compact while preserving coverage for
projection extraction, visualization links, thumbnail generation, xarray
cleanup, upsert behavior and CLI wiring.
"""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from pystac import Asset, Item
from pystac.extensions.projection import ProjectionExtension

import scripts.register as register_module


def make_item(collection: str | None = None, item_id: str = "id") -> Item:
    return Item(
        id=item_id,
        geometry=None,
        bbox=None,
        datetime=datetime.now(UTC),
        properties={},
        collection=collection,
    )


def make_asset(
    href: str, media_type: str = "application/vnd+zarr", roles=None, extra=None
) -> Asset:
    a = Asset(href=href, media_type=media_type, roles=roles or [])
    if extra is not None:
        a.extra_fields = extra
    return a


def test_s3_helpers_and_rewrite():
    assert (
        register_module.s3_to_https("s3://b/p.zarr", "https://s3.example")
        == "https://b.s3.example/p.zarr"
    )

    it = make_item(item_id="t")
    it.add_asset("data", make_asset("s3://old/base.zarr/path", extra=None))
    register_module.rewrite_asset_hrefs(
        it, "s3://old/base.zarr", "s3://new/out.zarr", "https://s3.e"
    )
    assert it.assets["data"].href.startswith("https://new.s3.e/out.zarr")


@patch("scripts.register.zarr.open")
def test_add_projection_from_zarr(mock_open):
    store = MagicMock()
    store.attrs = {"spatial_ref": {"spatial_ref": "32633", "crs_wkt": "WKT"}}
    mock_open.return_value = store
    it = make_item()
    it.add_asset("a", make_asset("https://x/z.zarr"))
    register_module.add_projection_from_zarr(it)
    proj = ProjectionExtension.ext(it)
    assert proj.epsg == 32633 and proj.wkt2 == "WKT"


@patch("scripts.register.zarr.open")
def test_add_projection_from_zarr_ignores_errors(mock_open):
    mock_open.side_effect = RuntimeError("boom")
    it = make_item()
    it.add_asset("a", make_asset("https://x/z.zarr"))
    register_module.add_projection_from_zarr(it)
    assert "proj:epsg" not in it.properties


@pytest.mark.parametrize(
    "collection,assets,expect_vh",
    [
        ("sentinel-2-l2a", {"TCI": ".../reflectance/r60m/TCI"}, False),
        ("sentinel-1-grd", {"vh": ".../base.zarr/measurements/vh"}, True),
    ],
)
def test_add_visualization_links_and_tilejson(collection, assets, expect_vh):
    it = make_item(collection=collection, item_id=f"{collection}-1")
    for k, v in assets.items():
        it.add_asset(k, make_asset(f"https://ex/{v}"))
    register_module.add_visualization_links(it, "https://titiler", collection)
    assert any(link.rel == "viewer" for link in it.links)
    xyz = [link for link in it.links if link.rel == "xyz"]
    assert xyz and ("vh" in xyz[0].href.lower()) == expect_vh


def test_remove_xarray_integration_removes_fields():
    a = make_asset(
        "https://ex/z.zarr",
        extra={
            "xarray:open_dataset_kwargs": {"c": {}},
            "xarray:open_datatree_kwargs": {"e": True},
            "alternate": {"xarray": {}},
            "keep": 1,
        },
    )
    it = make_item()
    it.add_asset("d", a)
    register_module.remove_xarray_integration(it)
    ef = a.extra_fields
    assert "xarray:open_dataset_kwargs" not in ef and "alternate" not in ef and ef.get("keep") == 1


@pytest.mark.parametrize(
    "has_vh,existing_thumb,expected_thumb",
    [
        (True, False, True),
        (False, False, True),
        (False, True, True),
    ],
)
def test_add_thumbnail_variants(has_vh, existing_thumb, expected_thumb):
    coll = "sentinel-1-grd" if has_vh else "sentinel-2-l2a"
    it = make_item(collection=coll)
    if has_vh:
        it.add_asset("vh", make_asset("https://ex/base.zarr/measurements/VH"))
    if existing_thumb:
        it.add_asset("thumbnail", make_asset("https://ex/t.png", media_type="image/png"))
    register_module.add_thumbnail_asset(it, "https://titiler", coll)
    assert ("thumbnail" in it.assets) == expected_thumb


def test_run_registration_pipeline(monkeypatch):
    payload = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "S2_TEST",
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "bbox": [0, 0, 0, 0],
        "properties": {"datetime": "2025-01-01T00:00:00Z"},
        "collection": "sentinel-2-l2a",
        "assets": {
            "data": {
                "href": "s3://source-bucket/product.zarr/measurements/reflectance/r10m/B04",
                "type": "application/vnd+zarr",
            }
        },
    }

    class C:
        def __init__(self):
            self.request_url = None

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, url):
            self.request_url = url
            return SimpleNamespace(raise_for_status=lambda: None, json=lambda: payload)

    monkeypatch.setattr(register_module.httpx, "Client", lambda *a, **k: C())

    seen = []
    monkeypatch.setattr(
        register_module, "rewrite_asset_hrefs", lambda item, o, n, e: seen.append((o, n, e))
    )
    monkeypatch.setattr(
        register_module, "add_projection_from_zarr", lambda item: seen.append("proj")
    )
    monkeypatch.setattr(
        register_module, "remove_xarray_integration", lambda item: seen.append("xarray")
    )
    monkeypatch.setattr(
        register_module, "add_visualization_links", lambda item, b, c: seen.append("viz")
    )
    monkeypatch.setattr(
        register_module, "add_thumbnail_asset", lambda item, b, c: seen.append("thumb")
    )
    monkeypatch.setattr(
        register_module, "add_derived_from_link", lambda item, s: seen.append("derived")
    )

    upsert = {}

    def record_upsert(client, coll, item):
        upsert["coll"] = coll
        upsert["item"] = item

    monkeypatch.setattr(register_module, "upsert_item", record_upsert)
    monkeypatch.setattr(register_module, "Client", SimpleNamespace(open=lambda url: MagicMock()))

    register_module.run_registration(
        source_url="https://stac.example.com/collections/sentinel-2-l2a/items/S2_TEST",
        collection="sentinel-2-l2a",
        stac_api_url="https://stac.example.com",
        raster_api_url="https://titiler.example.com",
        s3_endpoint="https://s3.example.com",
        s3_output_bucket="out-bucket",
        s3_output_prefix="converted",
    )

    assert (
        any(x == "viz" for x in seen)
        and any(x == "thumb" for x in seen)
        and any(x == "derived" for x in seen)
    )
    assert (
        upsert.get("coll") == "sentinel-2-l2a"
        and upsert.get("item").collection_id == "sentinel-2-l2a"
    )


def test_upsert_item_variants():
    def make_client(exists):
        coll = MagicMock()
        if exists:
            coll.get_item.return_value = object()
        else:
            coll.get_item.side_effect = RuntimeError("no")
        session = MagicMock()
        session.post.return_value = SimpleNamespace(status_code=201, raise_for_status=lambda: None)
        return (
            SimpleNamespace(
                get_collection=lambda cid: coll,
                self_href="https://stac.example.com",
                _stac_io=SimpleNamespace(session=session),
            ),
            session,
        )

    client, sess = make_client(False)
    it = make_item(item_id="T")
    register_module.upsert_item(client, "sentinel-2-l2a", it)
    assert sess.delete.call_count == 0 and sess.post.call_count == 1

    client2, sess2 = make_client(True)
    it2 = make_item(item_id="T2")
    register_module.upsert_item(client2, "sentinel-2-l2a", it2)
    assert sess2.delete.call_count == 1 and sess2.post.call_count == 1


def test_register_main_exit_codes(monkeypatch):
    monkeypatch.setattr(register_module, "run_registration", lambda *a, **k: None)
    assert (
        register_module.main(
            [
                "--source-url",
                "s",
                "--collection",
                "c",
                "--stac-api-url",
                "https://s",
                "--raster-api-url",
                "https://r",
                "--s3-endpoint",
                "https://e",
                "--s3-output-bucket",
                "b",
                "--s3-output-prefix",
                "p",
            ]
        )
        == 0
    )
    monkeypatch.setattr(
        register_module,
        "run_registration",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    assert (
        register_module.main(
            [
                "--source-url",
                "s",
                "--collection",
                "c",
                "--stac-api-url",
                "https://s",
                "--raster-api-url",
                "https://r",
                "--s3-endpoint",
                "https://e",
                "--s3-output-bucket",
                "b",
                "--s3-output-prefix",
                "p",
            ]
        )
        == 1
    )


def test_register_main_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """register.main returns 1 when run_registration raises."""

    def boom(*args):
        raise RuntimeError("boom")

    monkeypatch.setattr(register_module, "run_registration", boom)

    exit_code = register_module.main(
        [
            "--source-url",
            "src",
            "--collection",
            "coll",
            "--stac-api-url",
            "https://stac",
            "--raster-api-url",
            "https://titiler",
            "--s3-endpoint",
            "https://s3",
            "--s3-output-bucket",
            "bucket",
            "--s3-output-prefix",
            "prefix",
        ]
    )

    assert exit_code == 1
