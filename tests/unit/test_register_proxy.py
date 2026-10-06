"""Unit tests for the Samples Service proxy item transform (coordination#287).

Everything here runs offline, and the ``no_network`` autouse fixture enforces it: a pystac
link resolution, a STAC client, or any real HTTP transport raises, so a guard regression
fails the test instead of reaching a live catalogue. The target hosts are ``.invalid``
(RFC 2606) for the same reason; only fixture data carries real URLs.
"""

import json
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pystac.stac_io
import pytest
import register_proxy
import requests
from register_proxy import (
    PROXY_EXPIRES,
    assert_no_s3_urls,
    build_proxy_item,
    fetch_source_item,
    fill_cube_extent,
    is_old_generation,
    main,
    read_item_ids,
)
from s3_item_cleanup import extract_s3_urls_from_item

EODC_FIXTURES = Path(__file__).parent.parent / "fixtures/eodc"
# The generation EODC publishes since ~22 Sep 2026 (all 9,945 items of 22-28 Sep): no
# raster:scale/offset anywhere, the scaling lives in the stores' CF attributes.
FIXTURE = EODC_FIXTURES / "S2C_MSIL2A_20260927T133931_N0513_R067_T28WDE_20260927T170511.json"
# The generation before it: raster:scale on the SR_* assets. The pipeline refuses these.
OLD_GENERATION_FIXTURE = (
    EODC_FIXTURES / "S2B_MSIL2A_20260907T130029_N0512_R138_T26TLL_20260907T145009.json"
)
COLLECTION = "sentinel-2-l2a-samples-zarr3-rollback"
NEW = "sentinel-2-l2a-new"  # the pipeline's user-facing target (plan rev 3)
RASTER = "https://rstaging.invalid/rstaging"
STAC_API = "https://stac.invalid/stac"


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    """Any read or write over the wire is a bug in a test, or in a guard it exercises."""

    def _explode(*args, **kwargs):
        raise AssertionError("tests must not reach the network")

    monkeypatch.setattr(pystac.stac_io.DefaultStacIO, "read_text", _explode)
    monkeypatch.setattr("stac_auth.open_client", _explode)
    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", _explode)


class _Before1Nov(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 9, 28, tzinfo=tz)


@pytest.fixture(autouse=True)
def frozen_clock(monkeypatch):
    """main refuses once PROXY_EXPIRES has passed; the suite must not start failing then."""
    monkeypatch.setattr(register_proxy, "datetime", _Before1Nov)


@pytest.fixture
def source():
    return json.loads(FIXTURE.read_text())


@pytest.fixture
def old_source():
    return json.loads(OLD_GENERATION_FIXTURE.read_text())


@pytest.fixture
def proxy(source):
    return build_proxy_item(source, COLLECTION, RASTER, STAC_API, expires=PROXY_EXPIRES).to_dict()


def store_link(item):
    return next(link["href"] for link in item["links"] if link["rel"] == "store")


def link_href(item, rel):
    return next((link["href"] for link in item["links"] if link["rel"] == rel), None)


# --- assets ---


def test_asset_keys_are_exactly_the_proxy_set(proxy):
    assert set(proxy["assets"]) == {"reflectance", "AOT_10m", "WVP_10m", "SCL_20m", "thumbnail"}


def test_the_thumbnail_is_a_small_webp_preview_on_the_render_host(proxy):
    """What a STAC browser shows for the item; without it the list had footprints only."""
    thumbnail = proxy["assets"]["thumbnail"]
    base = f"{RASTER}/collections/{COLLECTION}/items/{proxy['id']}"
    # The tiles' whole true-colour query: without rescale/color_formula it renders near-black.
    xyz_query = link_href(proxy, "xyz").split("?", 1)[1]
    assert thumbnail["href"] == f"{base}/preview?format=webp&max_size=512&{xyz_query}"
    assert thumbnail["roles"] == ["thumbnail"]
    assert thumbnail["type"] == "image/webp"


def test_reflectance_is_the_consolidated_multiscales_group(proxy):
    reflectance = proxy["assets"]["reflectance"]
    assert reflectance["href"].endswith("/measurements/reflectance")
    assert len(reflectance["bands"]) == 12
    assert "raster:scale" not in reflectance


def test_root_href_assets_all_point_at_the_store_root(proxy):
    hrefs = {key: proxy["assets"][key]["href"] for key in ("AOT_10m", "WVP_10m", "SCL_20m")}
    assert len(set(hrefs.values())) == 1
    root = next(iter(hrefs.values()))
    # NO trailing slash: titiler's GeoZarrReader concatenates and 404s on
    # `<root>//zarr.json`. Measured against an EODC store 2026-09-11.
    assert root.endswith(".zarr")
    assert root == store_link(proxy)


@pytest.mark.parametrize("key", ["AOT_10m", "WVP_10m", "SCL_20m"])
def test_root_href_assets_never_point_at_a_group_or_array(proxy, key):
    href = proxy["assets"][key]["href"]
    for tail in ("/r10m", "/r20m", "/aot", "/wvp", "/scl", "/quality/atmosphere", "/mask"):
        assert not href.rstrip("/").endswith(tail)


@pytest.mark.parametrize(
    ("key", "band", "gsd"),
    [("AOT_10m", "aot", 10), ("WVP_10m", "wvp", 10), ("SCL_20m", "scl", 20)],
)
def test_root_href_assets_carry_their_single_band_and_source_metadata(
    source, proxy, key, band, gsd
):
    asset = proxy["assets"][key]
    assert [b["name"] for b in asset["bands"]] == [band]
    assert asset["gsd"] == gsd
    assert asset["roles"] == ["data"]
    assert asset["type"] == "application/vnd.zarr; version=3"
    # Numeric fields stay as EODC published them — they describe the EODC store. N0513
    # publishes no raster:scale (it is in the stores' CF attributes), so none is invented.
    source_asset = source["assets"][register_proxy.ROOT_HREF_ASSETS[key][0]]
    for field in ("nodata", "data_type", "proj:shape", "raster:scale", "raster:offset"):
        assert asset.get(field) == source_asset.get(field), field


# --- properties, links, extensions ---


def test_expires_is_the_fixed_proxy_date(proxy):
    """Loïc, 2026-09-14: the proxy expires on 1 November 2026.

    A fixed date, not `now + N days`: re-registering an item must not push it out. An
    item with no `expires` at all is what `cleanup_expired_items.evaluate_guards` refuses
    first (`no_expires`).
    """
    assert proxy["properties"]["expires"] == "2026-11-01T00:00:00Z"
    assert any("timestamps" in ext for ext in proxy["stac_extensions"])


def test_projection_and_cube_extent(proxy):
    assert proxy["properties"]["proj:code"] == "EPSG:32628"
    dimensions = proxy["assets"]["reflectance"]["cube:dimensions"]
    assert dimensions["x"]["extent"] == [399960.0, 509760.0]
    assert dimensions["y"]["extent"] == [7890240.0, 8000040.0]


def test_source_catalogue_links_are_stripped(proxy):
    rels = {link["rel"] for link in proxy["links"]}
    assert rels.isdisjoint({"root", "self", "parent", "alternate"})
    # `collection` survives, but re-pointed at the proxy collection, never EODC's.
    assert "stac.core.eopf.eodc.eu" not in link_href(proxy, "collection")


def test_proxy_links_are_present_and_collection_scoped(proxy):
    base = f"{RASTER}/collections/{COLLECTION}/items/{proxy['id']}"
    for rel in ("store", "derived_from", "viewer", "xyz", "tilejson"):
        assert link_href(proxy, rel), f"missing {rel} link"
    # register_v1's Explorer `via` target 404s (the Explorer has no item pages).
    assert link_href(proxy, "via") is None
    assert link_href(proxy, "xyz").startswith(f"{base}/tiles/WebMercatorQuad/")
    assert link_href(proxy, "tilejson").startswith(f"{base}/WebMercatorQuad/tilejson.json?")
    assert "/rstaging/" in link_href(proxy, "xyz")
    assert link_href(proxy, "derived_from").startswith("https://stac.core.eopf.eodc.eu/")


def test_visualization_links_use_the_0_12_notation(proxy):
    """`assets=reflectance|bands=b04,b03,b02` — the only form /rstaging 0.12.0 serves.

    Measured 2026-09-11 on a prod S2 item: this form is 200 on /rstaging and 500 on
    /raster; the repeated full-path `variables=` form is the exact mirror image
    (422 / 200). Emitting the wrong one produces links that do not render.
    """
    encoded = "assets=reflectance%7Cbands%3Db04%2Cb03%2Cb02"
    hrefs = {rel: link_href(proxy, rel) for rel in ("xyz", "tilejson", "viewer")}
    hrefs["thumbnail"] = proxy["assets"]["thumbnail"]["href"]
    for rel, query in hrefs.items():
        assert encoded in query, rel
        assert "variables=" not in query, rel
        assert "bidx=" not in query, rel


def test_viewer_is_map_html_not_the_bare_viewer_route(proxy):
    """The bare `/viewer` route does not exist on titiler-eopf 0.12.0 (checked against
    /rstaging's OpenAPI 2026-09-11) — that is what 404'd in the 2026-09-10 probe."""
    viewer = link_href(proxy, "viewer")
    assert "/WebMercatorQuad/map.html?" in viewer
    assert not viewer.split("?")[0].endswith("/viewer")


def test_extensions_are_reconciled(proxy):
    extensions = proxy["stac_extensions"]
    assert "https://stac-extensions.github.io/eo/v2.0.0/schema.json" in extensions
    assert "https://stac-extensions.github.io/raster/v2.0.0/schema.json" in extensions
    assert "https://stac-extensions.github.io/datacube/v2.3.0/schema.json" in extensions
    assert not any("alternate-assets" in ext or "/storage/" in ext for ext in extensions)


def test_source_dict_is_not_mutated(source):
    before = json.dumps(source, sort_keys=True)
    build_proxy_item(source, COLLECTION, RASTER, STAC_API, expires=PROXY_EXPIRES)
    assert json.dumps(source, sort_keys=True) == before


def test_missing_self_link_is_refused(source):
    source["links"] = [link for link in source["links"] if link["rel"] != "self"]
    with pytest.raises(ValueError, match="no self link"):
        build_proxy_item(source, COLLECTION, RASTER, STAC_API, expires=PROXY_EXPIRES)


# --- CLI bounds ---


def test_read_item_ids_skips_blanks_and_comments(tmp_path):
    path = tmp_path / "ids.txt"
    path.write_text("# header\n\nA  # trailing\n  B\n#C\n")
    assert read_item_ids(path) == ["A", "B"]


def cli(*args):
    return main(
        [
            "--collection",
            COLLECTION,
            "--raster-api-url",
            RASTER,
            "--stac-api-url",
            STAC_API,
            *args,
        ]
    )


@patch("register_proxy.fetch_source_item")
@patch("register_proxy.upsert_item")
@patch("register_proxy.stac_auth.open_client")
def test_more_ids_than_max_items_exits_before_any_network_call(client, upsert, fetch):
    assert cli("--item-id", "a", "--item-id", "b", "--max-items", "1") == 1
    fetch.assert_not_called()
    client.assert_not_called()
    upsert.assert_not_called()


@pytest.mark.parametrize(
    "extra",
    [
        ["--collection", "sentinel-2-l2a"],  # prod: not a proxy collection
        ["--item-id", "../elsewhere"],  # an id that is a path
        ["--item-id", ".hidden"],
    ],
)
@patch("register_proxy.fetch_source_item")
@patch("register_proxy.upsert_item")
def test_refused_before_any_network_call(upsert, fetch, extra):
    """argparse keeps the last --collection, so `extra` overrides cli()'s default."""
    assert cli("--item-id", "a", "--max-items", "2", *extra) == 1
    fetch.assert_not_called()
    upsert.assert_not_called()


@pytest.mark.parametrize("flag", ["--source-stac-api", "--raster-api-url", "--stac-api-url"])
@patch("register_proxy.fetch_source_item")
@patch("register_proxy.upsert_item")
def test_every_url_flag_must_be_https(upsert, fetch, flag, caplog):
    """A cleartext --stac-api-url would send the OIDC bearer in the clear."""
    assert cli("--item-id", "a", "--max-items", "1", flag, "http://plain.invalid/x") == 1
    assert "must be an HTTPS URL" in caplog.text
    fetch.assert_not_called()
    upsert.assert_not_called()


@patch("register_proxy.fetch_source_item")
def test_a_passed_proxy_expiry_is_refused(fetch, monkeypatch):
    """PROXY_EXPIRES is a fixed date: after it, every item would be born expired."""
    monkeypatch.setattr(register_proxy, "PROXY_EXPIRES", datetime(2026, 9, 1, tzinfo=UTC))
    assert cli("--item-id", "a", "--max-items", "1") == 1
    fetch.assert_not_called()


def test_duplicate_ids_are_fetched_once_and_a_dry_run_says_wrote(source, tmp_path, caplog):
    caplog.set_level("INFO", logger="register_proxy")
    with patch("register_proxy.fetch_source_item", return_value=source) as fetch:
        rc = cli(
            "--item-id", source["id"], "--item-id", source["id"], "--max-items", "1",
            "--dry-run", str(tmp_path),
        )  # fmt: skip
    assert rc == 0
    fetch.assert_called_once()
    assert f"Wrote 1/1 item(s) for {COLLECTION}" in caplog.text


@patch("register_proxy.upsert_item")
def test_dry_run_writes_json_and_never_upserts(upsert, source, tmp_path):
    with patch("register_proxy.fetch_source_item", return_value=source) as fetch:
        assert cli("--item-id", source["id"], "--max-items", "1", "--dry-run", str(tmp_path)) == 0
    fetch.assert_called_once()
    upsert.assert_not_called()
    written = json.loads((tmp_path / f"{source['id']}.json").read_text())
    assert written["collection"] == COLLECTION


@patch("register_proxy.upsert_item")
@patch("register_proxy.stac_auth.open_client", return_value=MagicMock())
def test_within_max_items_registers_each_id(client, upsert, source):
    with patch("register_proxy.fetch_source_item", return_value=source):
        assert cli("--item-id", source["id"], "--max-items", "12") == 0
    upsert.assert_called_once()
    assert upsert.call_args[0][1] == COLLECTION


def test_collection_link_points_at_the_proxy_collection(proxy):
    """The STAC item schema refuses a `collection` field with no `collection` link."""
    assert link_href(proxy, "collection") == f"{STAC_API}/collections/{COLLECTION}"


# --- Guards added after the 2026-09-14 review ---------------------------------


def test_an_item_that_must_have_no_s3_location_is_refused_if_it_has_one(source):
    """Proxy items name stores we do not own; an s3:// href on one would let a cleanup
    delete them."""
    proxy = build_proxy_item(source, COLLECTION, RASTER, STAC_API, expires=PROXY_EXPIRES)
    assert_no_s3_urls(proxy)  # control: the item as built passes
    proxy.assets["reflectance"].href = "s3://esa-zarr-sentinel-explorer-fra/X.zarr/"
    with pytest.raises(ValueError, match="advertises S3 location"):
        assert_no_s3_urls(proxy)


def test_an_unknown_source_asset_is_dropped_not_proxied(source):
    """The prune is an allowlist: EODC's schema is not frozen (220 items republished)."""
    source["assets"]["CLD_20m"] = dict(source["assets"]["SCL_20m"])
    source["assets"]["quicklook"] = {
        "href": "https://data.eodc.eu/x/quicklook.png",
        "roles": ["thumbnail", "overview"],
    }
    proxy = build_proxy_item(source, COLLECTION, RASTER, STAC_API, expires=PROXY_EXPIRES).to_dict()
    assert set(proxy["assets"]) == {"reflectance", "AOT_10m", "WVP_10m", "SCL_20m", "thumbnail"}
    assert proxy["assets"]["thumbnail"]["href"].startswith(RASTER)  # ours, not the quicklook


def test_a_missing_source_asset_is_refused(source):
    """The four-asset set IS criterion 1 of #287 — a partial item must not look like a pass."""
    del source["assets"]["ATM_10m"]
    with pytest.raises(ValueError, match="no ATM_10m asset"):
        build_proxy_item(source, COLLECTION, RASTER, STAC_API, expires=PROXY_EXPIRES)


def test_a_band_filter_that_matches_nothing_is_refused(source):
    """Failure-open here makes AOT and WVP each advertise BOTH bands, silently."""
    for band in source["assets"]["ATM_10m"]["bands"]:
        band["name"] = band["name"].upper()
    with pytest.raises(ValueError, match="no 'aot' band"):
        build_proxy_item(source, COLLECTION, RASTER, STAC_API, expires=PROXY_EXPIRES)


def test_aot_and_wvp_do_not_share_mutable_fields(source):
    """`dict(extra_fields)` is shallow — both assets ended up on one `proj:shape` list."""
    proxy = build_proxy_item(source, COLLECTION, RASTER, STAC_API, expires=PROXY_EXPIRES)
    aot = proxy.assets["AOT_10m"].extra_fields
    wvp = proxy.assets["WVP_10m"].extra_fields
    assert aot.get("proj:shape") is not wvp.get("proj:shape")


def test_a_source_without_item_level_proj_code_is_refused(source):
    """register_v1 would silently fall back to EPSG:32632: a wrong CRS on a valid item."""
    del source["properties"]["proj:code"]
    with pytest.raises(ValueError, match="no item-level proj:code"):
        build_proxy_item(source, COLLECTION, RASTER, STAC_API, expires=PROXY_EXPIRES)


def test_a_source_without_proj_bbox_is_refused(source):
    """No bbox means no x/y extent, which the declared datacube extension requires."""
    del source["properties"]["proj:bbox"]
    with pytest.raises(ValueError, match="x/y have no extent"):
        build_proxy_item(source, COLLECTION, RASTER, STAC_API, expires=PROXY_EXPIRES)


def test_a_six_element_proj_bbox_is_read_without_its_heights(proxy):
    """[west, south, min-z, east, north, max-z]: slicing four would read a height as east."""
    item = MagicMock()
    dims = {"x": {}, "y": {}}
    item.assets = {"reflectance": MagicMock(extra_fields={"cube:dimensions": dims})}
    item.properties = {"proj:bbox": [1.0, 2.0, -5.0, 3.0, 4.0, 9.0]}
    fill_cube_extent(item)
    assert dims == {"x": {"extent": [1.0, 3.0]}, "y": {"extent": [2.0, 4.0]}}


def test_a_source_render_link_is_not_carried_over(source):
    """The source links are a keep-list: an EODC xyz would sit ahead of ours."""
    source["links"].append({"rel": "xyz", "href": "https://eodc.invalid/tiles/{z}/{x}/{y}"})
    source["links"].append({"rel": "preview", "href": "https://eodc.invalid/p.png"})
    proxy = build_proxy_item(source, COLLECTION, RASTER, STAC_API, expires=PROXY_EXPIRES).to_dict()
    xyz = [link["href"] for link in proxy["links"] if link["rel"] == "xyz"]
    assert len(xyz) == 1 and xyz[0].startswith(RASTER)
    assert link_href(proxy, "preview") is None


def test_a_missing_reflectance_asset_is_refused(source):
    """The render links name `assets=reflectance` unconditionally."""
    for key in ("SR_10m", "SR_20m", "SR_60m"):
        source["assets"].pop(key, None)
    with pytest.raises(ValueError, match="missing asset"):
        build_proxy_item(source, COLLECTION, RASTER, STAC_API, expires=PROXY_EXPIRES)


def test_the_source_cannot_choose_which_id_gets_registered(source):
    """id decides the dry-run filename, the registered id, and what --max-items counts."""
    source["id"] = "../escaped"
    with patch("httpx.Client") as client:
        client.return_value.__enter__.return_value.get.return_value = httpx.Response(
            200, json=source, request=httpx.Request("GET", "https://x/y")
        )
        with pytest.raises(ValueError, match="different item id"):
            fetch_source_item("https://stac.example", "c", "REQUESTED")


def test_a_redirect_from_the_source_is_refused_not_followed():
    """A 3xx could downgrade to http or change host; what came back would be registered
    as if it had been asked for."""
    real_client = httpx.Client

    def redirect(request):
        return httpx.Response(302, headers={"Location": "http://elsewhere.invalid/x"})

    def client(**kwargs):
        return real_client(transport=httpx.MockTransport(redirect), **kwargs)

    with patch("register_proxy.httpx.Client", client), pytest.raises(httpx.HTTPStatusError):
        fetch_source_item("https://stac.invalid", "c", "ID")


def test_dot_segments_in_an_item_id_cannot_retarget_the_collection():
    """httpx normalises RFC 3986 dot-segments, so the id must be percent-encoded."""
    seen = {}

    def capture(url):
        seen["url"] = url
        return httpx.Response(404, request=httpx.Request("GET", url))

    with patch("httpx.Client") as client:
        client.return_value.__enter__.return_value.get.side_effect = capture
        with pytest.raises(httpx.HTTPStatusError):
            fetch_source_item("https://stac.example", "samples", "../../sentinel-2-l2a/items/EVIL")
    assert "/collections/samples/items/" in seen["url"]
    assert "sentinel-2-l2a/items" not in seen["url"]


@patch("register_proxy.upsert_item")
@patch("register_proxy.stac_auth.open_client", return_value=MagicMock())
def test_one_failing_item_does_not_abandon_the_rest(client, upsert, source):
    """A partially populated prod collection with no record of what landed is the worst case."""
    calls = []

    def flaky(_api, _collection, item_id):
        calls.append(item_id)
        if item_id == "BAD":
            raise ValueError("boom")
        return source

    with patch("register_proxy.fetch_source_item", side_effect=flaky):
        rc = cli("--item-id", "BAD", "--item-id", source["id"], "--max-items", "2")
    assert rc == 1, "a torn run must not report success"
    assert calls == ["BAD", source["id"]], "the second id must still be attempted"
    upsert.assert_called_once()


# --- pipeline mode: --items-json, create-only (plan rev 2, T1/T2) ---


class FakeTarget:
    """The target collection behind a mocked pystac Client: POST creates an id once, then 409s.

    Stands in at the session, below ``create_item``, so "never a PUT" is proven by counting
    ``session.put`` rather than by patching the writer that would issue it.
    """

    def __init__(self, existing=(), fail=None):
        self.ids = set(existing)
        self.fail = fail or {}  # id -> the HTTP status to answer, or an exception to raise
        self.client = MagicMock(self_href=STAC_API)
        self.session = self.client._stac_io.session
        self.session.post.side_effect = self.post

    def post(self, url, json, **kwargs):
        assert url == f"{STAC_API}/collections/{json['collection']}/items"
        answer = self.fail.get(json["id"])
        if isinstance(answer, Exception):
            raise answer
        resp = requests.Response()
        if answer:
            resp.status_code = answer
        else:
            resp.status_code = 409 if json["id"] in self.ids else 201
            self.ids.add(json["id"])
        return resp


# Where discover finds each id, and where register_proxy's defaults fetch it from.
SOURCE_ITEMS = "https://stac.core.eopf.eodc.eu/collections/sentinel-2-l2a-zarr3/items/"


def pipeline_cli(tmp_path, ids, *extra, collection=COLLECTION, rows_for=None):
    """main() in pipeline mode, on an items.json shaped like `query_stac.py discover`'s."""
    rows = [
        {"source_url": SOURCE_ITEMS + i, "collection": rows_for or collection,
         "item_id": i, "datetime": "2026-09-27T13:39:31.025000+00:00"}
        for i in ids
    ]  # fmt: skip
    items = tmp_path / "items.json"
    items.write_text(json.dumps(rows))
    return main(
        [
            "--collection", collection, "--raster-api-url", RASTER, "--stac-api-url", STAC_API,
            "--items-json", str(items), "--created-ids", str(tmp_path / "created.jsonl"),
            "--max-items", "30", *extra,
        ]
    )  # fmt: skip


def created_ids(tmp_path):
    return [json.loads(line) for line in (tmp_path / "created.jsonl").read_text().splitlines()]


@pytest.fixture
def eodc(source):
    """fetch_source_item answering every id with the N0513 fixture under that id."""
    with patch(
        "register_proxy.fetch_source_item", side_effect=lambda _a, _c, i: {**source, "id": i}
    ) as fetch:
        yield fetch


def run_pipeline(tmp_path, target, ids, extra=(), **kwargs):
    with patch("register_proxy.stac_auth.open_client", return_value=target.client):
        return pipeline_cli(tmp_path, ids, *extra, **kwargs)


def test_pipeline_201_is_created_and_recorded(eodc, tmp_path, caplog):
    caplog.set_level("INFO", logger="register_proxy")
    target = FakeTarget()
    assert run_pipeline(tmp_path, target, ["A", "B"]) == 0
    lines = created_ids(tmp_path)
    assert [(line["id"], line["collection"]) for line in lines] == [
        ("A", COLLECTION),
        ("B", COLLECTION),
    ]
    assert lines[0]["ts"].startswith("2026-09-28T")
    assert "created=2 exists=0 refused_generation=0 failed=0" in caplog.text


def test_pipeline_409_is_exists_and_never_a_put(eodc, tmp_path, caplog):
    """upsert_item's PUT on 409 would replace an item an earlier run registered."""
    caplog.set_level("INFO", logger="register_proxy")
    target = FakeTarget(existing={"A"})
    assert run_pipeline(tmp_path, target, ["A"]) == 0
    target.session.put.assert_not_called()
    target.session.patch.assert_not_called()
    target.session.delete.assert_not_called()
    assert created_ids(tmp_path) == []
    assert "created=0 exists=1 refused_generation=0 failed=0" in caplog.text


def test_pipeline_rerun_of_the_same_list_is_all_exists(eodc, tmp_path, caplog):
    caplog.set_level("INFO", logger="register_proxy")
    target = FakeTarget()
    assert run_pipeline(tmp_path, target, ["A", "B"]) == 0
    assert run_pipeline(tmp_path, target, ["A", "B"]) == 0
    assert "created=0 exists=2 refused_generation=0 failed=0" in caplog.text
    target.session.put.assert_not_called()
    assert [line["id"] for line in created_ids(tmp_path)] == ["A", "B"], "appended once each"


def test_pipeline_list_has_the_201s_and_the_creates_that_may_have_committed(eodc, tmp_path, caplog):
    """A 5xx, a timeout or a reset can come after the server committed the item; the next
    run's 409 would then hide it from every rollback list. A 4xx is a refusal: no line."""
    caplog.set_level("INFO", logger="register_proxy")
    fail = {"C": 502, "D": requests.ReadTimeout("slow"), "E": requests.ConnectionError("reset"),
            "F": 400}  # fmt: skip
    target = FakeTarget(existing={"B"}, fail=fail)
    assert run_pipeline(tmp_path, target, ["A", "B", "C", "D", "E", "F"]) == 1
    lines = [(line["id"], line.get("uncertain", False)) for line in created_ids(tmp_path)]
    assert lines == [("A", False), ("C", True), ("D", True), ("E", True)]
    assert "created=1 exists=1 refused_generation=0 failed=4" in caplog.text
    target.session.put.assert_not_called()


@patch("register_proxy.fetch_source_item")
@patch("register_proxy.stac_auth.open_client")
def test_pipeline_empty_list_exits_0_before_any_network_call(client, fetch, tmp_path, caplog):
    """Most hourly windows are empty: EODC publishes in one burst, 00:00-07:30Z."""
    caplog.set_level("INFO", logger="register_proxy")
    assert pipeline_cli(tmp_path, []) == 0
    client.assert_not_called()
    fetch.assert_not_called()
    assert "created=0 exists=0 refused_generation=0 failed=0" in caplog.text


def test_pipeline_refuses_an_old_generation_source_and_exits_0(old_source, tmp_path, caplog):
    """Content-based, so a retry cannot succeed: counted, never failed (review P11)."""
    caplog.set_level("INFO", logger="register_proxy")
    target = FakeTarget()
    with patch("register_proxy.fetch_source_item", return_value=old_source):
        assert run_pipeline(tmp_path, target, [old_source["id"]]) == 0
    target.session.post.assert_not_called()
    assert "created=0 exists=0 refused_generation=1 failed=0" in caplog.text


def test_the_generation_marker_is_raster_scale_or_offset_on_sr(source, old_source):
    assert is_old_generation(old_source)
    assert not is_old_generation(source)
    for field in ("raster:scale", "raster:offset"):
        on_band = deepcopy(source)
        on_band["assets"]["SR_20m"]["bands"][0][field] = 0.0001
        assert is_old_generation(on_band), field
    elsewhere = deepcopy(source)
    elsewhere["assets"]["ATM_10m"]["raster:scale"] = 0.001  # not an SR_* asset
    assert not is_old_generation(elsewhere)


def test_the_proxy_mode_keeps_registering_old_generation_sources(old_source, tmp_path):
    """The guard is the pipeline's: the 09-21 comparison items are N0512."""
    with patch("register_proxy.fetch_source_item", return_value=old_source):
        rc = cli("--item-id", old_source["id"], "--max-items", "1", "--dry-run", str(tmp_path))
    assert rc == 0
    assert (tmp_path / f"{old_source['id']}.json").exists()


def test_a_transition_item_has_no_expires_even_if_its_source_had_one(source, tmp_path):
    """D2/R3: the user-facing scenes must never expire."""
    source["properties"]["expires"] = "2026-10-15T00:00:00Z"
    out = tmp_path / "out"
    with patch("register_proxy.fetch_source_item", return_value=source):
        rc = pipeline_cli(tmp_path, [source["id"]], "--dry-run", str(out), collection=NEW)
    assert rc == 0
    written = json.loads((out / f"{source['id']}.json").read_text())
    assert "expires" not in written["properties"]
    assert written["collection"] == NEW
    assert extract_s3_urls_from_item(written) == set()


def test_the_transition_pipeline_keeps_running_after_the_proxy_expiry(
    source, tmp_path, monkeypatch
):
    monkeypatch.setattr(register_proxy, "PROXY_EXPIRES", datetime(2026, 9, 1, tzinfo=UTC))
    out = tmp_path / "out"
    with patch("register_proxy.fetch_source_item", return_value=source):
        new = pipeline_cli(tmp_path, [source["id"]], "--dry-run", str(out), collection=NEW)
        proxy = pipeline_cli(tmp_path, [source["id"]], "--dry-run", str(out))
    assert (new, proxy) == (0, 1)


@pytest.mark.parametrize(
    "run",
    [
        lambda tmp_path: pipeline_cli(tmp_path, ["A"], collection="sentinel-2-l2a"),
        lambda _: cli("--collection", "sentinel-2-l2a", "--item-id", "a", "--max-items", "1"),
    ],
    ids=["pipeline", "track-a"],
)
@patch("register_proxy.fetch_source_item")
@patch("register_proxy.upsert_item")
@patch("register_proxy.stac_auth.open_client")
def test_no_run_may_write_the_converted_archive(client, upsert, fetch, tmp_path, run):
    """Plan rev 3, R1: `sentinel-2-l2a` keeps our converted items and takes no run at all."""
    assert run(tmp_path) == 1
    client.assert_not_called()
    fetch.assert_not_called()
    upsert.assert_not_called()


def test_the_transition_template_describes_the_items_it_will_hold(source):
    """eodash builds its filters from the collection's summaries: they must match the items."""
    template = json.loads((Path(__file__).parents[2] / "stac" / f"{NEW}.json").read_text())
    item = build_proxy_item(source, NEW, RASTER, STAC_API, expires=None).to_dict()
    summaries, reflectance = template["summaries"], template["item_assets"]["reflectance"]
    for key in ("platform", "processing:level", "constellation", "product:type"):
        assert item["properties"][key] in summaries[key], key
    assert "eo:cloud_cover" in summaries
    built = item["assets"]["reflectance"]
    bands = {band["name"] for band in built["bands"]}
    assert {band["name"] for band in summaries["bands"]} == bands
    assert {band["name"] for band in reflectance["bands"]} == bands
    assert set(reflectance["cube:variables"]) == set(built["cube:variables"])
    assert set(template["item_assets"]) == set(item["assets"]) - {"thumbnail"}


@pytest.mark.parametrize(
    ("extra", "collection"),
    [
        (["--item-id", "other"], NEW),  # ids from anywhere but discover's list
        ([], "sentinel-2-l2a-staging"),  # near misses: a substring check would take them
        ([], "sentinel-2-l2a-samples-zarr3x"),
        ([], "sentinel-2-l2a-samples-zarr3"),  # deleted 2026-10-05
        ([], "sentinel-2-l2a-samples-zarr3-ovh"),  # deleted 2026-10-05
        ([], "sentinel-2-l2a-mirror-rstaging"),  # deleted 2026-10-05
        ([], "sentinel-2-l2a-zarr3"),  # the source collection itself
    ],
)
@patch("register_proxy.fetch_source_item")
@patch("register_proxy.stac_auth.open_client")
def test_pipeline_targets_and_flags_are_refused_before_any_network_call(
    client, fetch, tmp_path, extra, collection
):
    assert pipeline_cli(tmp_path, ["A"], *extra, collection=collection) == 1
    client.assert_not_called()
    fetch.assert_not_called()


@pytest.mark.parametrize(
    "collection",
    [
        NEW,
        "sentinel-2-l2a",
        "sentinel-2-l2a-staging",
        "sentinel-2-l2a-samples-zarr3x",
        "sentinel-2-l2a-samples-zarr3",  # deleted 2026-10-05
    ],
)
@patch("register_proxy.fetch_source_item")
@patch("register_proxy.upsert_item")
def test_a_track_a_run_is_refused_outside_its_exact_targets(upsert, fetch, collection):
    """The user-facing collection takes only the pipeline, the one run that never PUTs."""
    assert cli("--collection", collection, "--item-id", "a", "--max-items", "1") == 1
    fetch.assert_not_called()
    upsert.assert_not_called()


@patch("register_proxy.fetch_source_item")
@patch("register_proxy.stac_auth.open_client")
def test_a_row_for_another_collection_is_refused(client, fetch, tmp_path):
    """discover deduplicated that list against another target: it proves nothing here."""
    assert pipeline_cli(tmp_path, ["A"], rows_for="sentinel-2-l2a") == 1
    client.assert_not_called()
    fetch.assert_not_called()


@pytest.mark.parametrize(
    "rows",
    [
        # discovered on another source than the one each id is fetched from
        [{"item_id": "A", "collection": COLLECTION,
          "source_url": "https://stac.core.eopf.eodc.eu/collections/sentinel-2-l2a/items/A"}],
        # the right source, but another id's URL
        [{"item_id": "A", "collection": COLLECTION, "source_url": SOURCE_ITEMS + "B"}],
        [{"item_id": "A", "collection": COLLECTION}],  # no source_url at all
        {"item_id": "A"},  # not a list
        ["A"],  # not rows
        [{"item_id": None, "collection": COLLECTION, "source_url": SOURCE_ITEMS + "None"}],
    ],
)  # fmt: skip
@patch("register_proxy.fetch_source_item")
@patch("register_proxy.stac_auth.open_client")
def test_a_list_not_shaped_like_discovers_is_refused(client, fetch, tmp_path, rows, caplog):
    items = tmp_path / "items.json"
    items.write_text(json.dumps(rows))
    created = tmp_path / "created.jsonl"
    rc = cli("--items-json", str(items), "--created-ids", str(created), "--max-items", "5")
    assert rc == 1
    assert "--items-json:" in caplog.text, "a logged refusal, not a traceback"
    client.assert_not_called()
    fetch.assert_not_called()


def test_a_pipeline_dry_run_leaves_the_created_ids_list_alone(source, tmp_path):
    """A dry run pointed at the real rollback list must not create or touch it."""
    with patch("register_proxy.fetch_source_item", return_value=source):
        assert pipeline_cli(tmp_path, [source["id"]], "--dry-run", str(tmp_path / "out")) == 0
    assert not (tmp_path / "created.jsonl").exists()


@patch("register_proxy.fetch_source_item")
@patch("register_proxy.stac_auth.open_client")
def test_items_json_and_created_ids_go_together(client, fetch, tmp_path):
    items = tmp_path / "items.json"
    items.write_text("[]")
    assert cli("--items-json", str(items), "--max-items", "1") == 1
    listed = ["--created-ids", str(tmp_path / "c.jsonl"), "--item-id", "a", "--max-items", "1"]
    assert cli(*listed) == 1
    client.assert_not_called()
    fetch.assert_not_called()


@patch("register_proxy.fetch_source_item")
@patch("register_proxy.stac_auth.open_client")
def test_an_unwritable_created_ids_list_fails_before_any_network_call(client, fetch, tmp_path):
    """Otherwise every item of the run would be created and none recorded."""
    (tmp_path / "file").write_text("")
    items = tmp_path / "items.json"
    row = {"item_id": "A", "collection": COLLECTION, "source_url": SOURCE_ITEMS + "A"}
    items.write_text(json.dumps([row]))
    with pytest.raises(OSError):
        cli("--items-json", str(items), "--created-ids", str(tmp_path / "file/c.jsonl"),
            "--max-items", "1")  # fmt: skip
    client.assert_not_called()
    fetch.assert_not_called()


def test_a_run_stops_after_ten_failures_in_a_row(tmp_path, caplog):
    """An outage would otherwise cost ~1.5 min per id, across up to 4,000 ids."""
    caplog.set_level("INFO", logger="register_proxy")
    target = FakeTarget()
    ids = [f"ID{n}" for n in range(12)]
    down = httpx.ConnectError("down")
    with patch("register_proxy.fetch_source_item", side_effect=down) as fetch:
        assert run_pipeline(tmp_path, target, ids) == 1
    assert fetch.call_count == register_proxy.MAX_CONSECUTIVE_FAILURES
    assert "created=0 exists=0 refused_generation=0 failed=12" in caplog.text
    # The ids it never tried are named too: they are the ones a re-run must cover.
    assert "Failed (12): ID0, " in caplog.text and "ID10, ID11" in caplog.text


def test_a_run_stops_starting_ids_once_its_time_budget_is_spent(eodc, tmp_path, caplog):
    """The budget stops BETWEEN ids; a pod deadline would kill a create before its line."""
    caplog.set_level("INFO", logger="register_proxy")
    target = FakeTarget()
    clock = iter(range(0, 1000, 5))  # main starts at 0; the ids are checked at 5, 10, 15
    with patch("register_proxy.monotonic", side_effect=lambda: next(clock)):
        assert run_pipeline(tmp_path, target, ["A", "B", "C"], extra=["--time-budget", "10"]) == 1
    # A is started at 5; B, checked exactly at the budget, is not.
    assert [line["id"] for line in created_ids(tmp_path)] == ["A"]
    assert "created=1 exists=0 refused_generation=0 failed=2" in caplog.text
    assert "the 10 s time budget: 2 id(s) not attempted" in caplog.text


# 12601: within 30 min of the register step's 14400 s pod backstop, the kill would be the stop.
@pytest.mark.parametrize("budget", ["nan", "inf", "0", "-5", "12601"])
def test_a_time_budget_that_could_never_stop_a_run_is_refused(budget, tmp_path):
    """`nan` compares False forever: the pod deadline, a kill, would become the stop."""
    with pytest.raises(SystemExit):
        pipeline_cli(tmp_path, ["A"], "--time-budget", budget)


def test_a_create_that_cannot_be_recorded_stops_the_run(eodc, tmp_path, caplog):
    """Every later create would go unrecorded too; the message names the one that may exist."""
    target = FakeTarget()
    created = tmp_path / "created.jsonl"
    created.write_text("")
    created.chmod(0o444)  # the run's touch still works; the first append does not
    caplog.set_level("INFO", logger="register_proxy")
    assert run_pipeline(tmp_path, target, ["A", "B"]) == 1
    assert target.session.post.call_count == 1, "B must not be created"
    assert "Stopping: cannot record A" in caplog.text
    # The summary and the Failed line still come out, for the alerts and the operator.
    assert "created=0 exists=0 refused_generation=0 failed=2" in caplog.text
    assert "Failed (2): A, B" in caplog.text


def test_a_success_resets_the_failure_streak(source, tmp_path):
    target = FakeTarget()
    ids = [f"ID{n}" for n in range(21)]

    def every_tenth_answers(_api, _collection, item_id):
        if int(item_id[2:]) % 10 != 9:
            raise httpx.ConnectError("flaky")
        return {**source, "id": item_id}

    with patch("register_proxy.fetch_source_item", side_effect=every_tenth_answers) as fetch:
        assert run_pipeline(tmp_path, target, ids) == 1
    assert fetch.call_count == 21
    assert [line["id"] for line in created_ids(tmp_path)] == ["ID9", "ID19"]


@pytest.fixture
def no_sleep(monkeypatch):
    sleeps = []
    monkeypatch.setattr(fetch_source_item.retry, "sleep", sleeps.append)
    return sleeps


def fake_source(*answers):
    """Patch httpx.Client so each GET gets the next answer: a status code or an exception."""
    calls = []
    real_client = httpx.Client

    def handler(request):
        calls.append(request.url)
        answer = answers[len(calls) - 1]
        if isinstance(answer, Exception):
            raise answer
        return httpx.Response(answer, json={"id": "ID"})

    def client(**kwargs):
        return real_client(transport=httpx.MockTransport(handler), **kwargs)

    return patch("register_proxy.httpx.Client", client), calls


@pytest.mark.parametrize(
    "first", [502, 503, 429, httpx.ReadTimeout("slow"), httpx.ConnectError("x")]
)
def test_a_transient_source_error_is_retried(no_sleep, first):
    patched, calls = fake_source(first, 200)
    with patched:
        assert fetch_source_item("https://stac.invalid", "c", "ID") == {"id": "ID"}
    assert len(calls) == 2
    assert len(no_sleep) == 1


def test_a_source_that_stays_down_fails_after_three_attempts(no_sleep):
    patched, calls = fake_source(502, 502, 502)
    with patched, pytest.raises(httpx.HTTPStatusError):
        fetch_source_item("https://stac.invalid", "c", "ID")
    assert len(calls) == 3


@pytest.mark.parametrize("status", [404, 302])
def test_an_answer_from_the_source_is_not_retried(no_sleep, status):
    patched, calls = fake_source(status)
    with patched, pytest.raises(httpx.HTTPStatusError):
        fetch_source_item("https://stac.invalid", "c", "ID")
    assert len(calls) == 1
    assert no_sleep == []
