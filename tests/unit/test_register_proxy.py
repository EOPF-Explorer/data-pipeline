"""Unit tests for the Samples Service proxy item transform (coordination#287).

Everything here runs offline. The ``no_stac_network`` autouse fixture makes
``pystac``'s concrete ``DefaultStacIO.read_text`` raise, so any accidental link
resolution (``root``/``parent``/``collection``) fails the test instead of silently
reaching the source catalogue.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pystac.stac_io
import pytest
from register_proxy import (
    AOT_PROBE,
    SCL_PROBE,
    WVP_PROBE,
    build_proxy_item,
    fetch_source_item,
    main,
    read_item_ids,
)

FIXTURE = (
    Path(__file__).parent.parent
    / "fixtures/eodc/S2B_MSIL2A_20260907T130029_N0512_R138_T26TLL_20260907T145009.json"
)
COLLECTION = "sentinel-2-l2a-samples-zarr3"
RASTER = "https://api.explorer.eopf.copernicus.eu/rstaging"
STAC_API = "https://api.explorer.eopf.copernicus.eu/stac"
OVH_BASE = (
    "https://s3.explorer.eopf.copernicus.eu/esa-zarr-sentinel-explorer-tests/samples-zarr3-proxy"
)


@pytest.fixture(autouse=True)
def no_stac_network(monkeypatch):
    """Any pystac read over the wire is a bug in the transform."""

    def _explode(*args, **kwargs):
        raise AssertionError("build_proxy_item must not resolve STAC links over the network")

    monkeypatch.setattr(pystac.stac_io.DefaultStacIO, "read_text", _explode)


@pytest.fixture
def source():
    return json.loads(FIXTURE.read_text())


@pytest.fixture
def proxy(source):
    return build_proxy_item(source, COLLECTION, RASTER, STAC_API).to_dict()


def store_link(item):
    return next(link["href"] for link in item["links"] if link["rel"] == "store")


def link_href(item, rel):
    return next((link["href"] for link in item["links"] if link["rel"] == rel), None)


# --- assets ---


def test_asset_keys_are_exactly_the_proxy_set(proxy):
    assert set(proxy["assets"]) == {"reflectance", "AOT_10m", "WVP_10m", "SCL_20m"}


def test_no_thumbnail_asset(proxy):
    """Deliberate: a temporary proxy must not look like a user-facing product.

    Removing it was the only option — pgstac normalises asset key order (length, then
    bytewise), so `thumbnail` can never be sorted last from the payload.
    """
    assert "thumbnail" not in proxy["assets"]
    assert not any("thumbnail" in (a.get("roles") or []) for a in proxy["assets"].values())


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
def test_root_href_assets_carry_their_single_band_and_source_metadata(proxy, key, band, gsd):
    asset = proxy["assets"][key]
    assert [b["name"] for b in asset["bands"]] == [band]
    assert asset["gsd"] == gsd
    assert asset["roles"] == ["data"]
    assert asset["type"] == "application/vnd.zarr; version=3"
    # Numeric fields stay as EODC published them — they describe the EODC store.
    assert "raster:scale" in asset


def test_probe_strings_are_exact():
    assert AOT_PROBE == "assets=AOT_10m|variables=/quality/atmosphere/r10m:aot"
    assert WVP_PROBE == "assets=WVP_10m|variables=/quality/atmosphere/r10m:wvp"
    assert SCL_PROBE == "assets=SCL_20m|variables=/conditions/mask/l2a_classification/r20m:scl"


# --- properties, links, extensions ---


def test_expires_is_the_fixed_proxy_date(proxy):
    """Loïc, 2026-09-14: the proxy expires on 1 November 2026.

    A fixed date, not `now + N days`: re-registering an item must not push it out. An
    item with no `expires` at all is what `cleanup_expired_items.evaluate_guards` refuses
    first (`no_expires`), which would leave the Track B copies in our own bucket
    unreclaimable by anything automated.
    """
    assert proxy["properties"]["expires"] == "2026-11-01T00:00:00Z"
    assert any("timestamps" in ext for ext in proxy["stac_extensions"])


def test_projection_and_cube_extent(proxy):
    assert proxy["properties"]["proj:code"] == "EPSG:32626"
    dimensions = proxy["assets"]["reflectance"]["cube:dimensions"]
    assert dimensions["x"]["extent"] == [300000.0, 409800.0]
    assert dimensions["y"]["extent"] == [4490220.0, 4600020.0]


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
    for rel in ("xyz", "tilejson", "viewer"):
        query = link_href(proxy, rel)
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
    build_proxy_item(source, COLLECTION, RASTER, STAC_API)
    assert json.dumps(source, sort_keys=True) == before


# --- Track B: store root rebase + S3 alternates ---


def test_store_root_base_rewrites_every_asset_href_flat(source):
    proxy = build_proxy_item(
        source, f"{COLLECTION}-ovh", RASTER, STAC_API, store_root_base=OVH_BASE
    ).to_dict()
    for key in ("reflectance", "AOT_10m", "WVP_10m", "SCL_20m"):
        assert proxy["assets"][key]["href"].startswith(OVH_BASE)
    # Flat: the source's YYYY/MM/DD directories are dropped (T0.9).
    assert store_link(proxy) == f"{OVH_BASE}/{proxy['id']}.zarr"


def test_store_root_base_keeps_the_sub_paths_below_the_store(source):
    proxy = build_proxy_item(source, COLLECTION, RASTER, STAC_API, store_root_base=OVH_BASE)
    reflectance = proxy.assets["reflectance"].href
    assert reflectance == f"{OVH_BASE}/{proxy.id}.zarr/measurements/reflectance"


@patch("register_v1.get_s3_storage_class", return_value="STANDARD")
def test_s3_endpoint_adds_alternates_and_their_extensions(_tier, source):
    proxy = build_proxy_item(
        source,
        f"{COLLECTION}-ovh",
        RASTER,
        STAC_API,
        store_root_base=OVH_BASE,
        s3_endpoint="https://s3.de.io.cloud.ovh.net",
    ).to_dict()
    alternate = proxy["assets"]["AOT_10m"]["alternate"]["s3"]
    assert alternate["href"].startswith("s3://esa-zarr-sentinel-explorer-tests/")
    assert any("alternate-assets" in ext for ext in proxy["stac_extensions"])


@patch("register_v1.get_s3_storage_class", return_value="STANDARD")
def test_the_two_consumers_each_get_the_slash_form_they_need(_tier, source):
    """titiler wants a bare `.zarr` href; s3_item_cleanup wants a trailing slash.

    They read different fields, so both can be satisfied — but only deliberately.
    A bare `s3://…/X.zarr` is `bare_zarr_store`: the delete removes nothing, counts 0
    remaining, and drops the STAC item while the whole store lives on.
    """
    from s3_item_cleanup import check_urls_confined

    proxy = build_proxy_item(
        source,
        f"{COLLECTION}-ovh",
        RASTER,
        STAC_API,
        store_root_base=OVH_BASE,
        s3_endpoint="https://s3.de.io.cloud.ovh.net",
    ).to_dict()
    allowed = [("esa-zarr-sentinel-explorer-tests", "samples-zarr3-proxy/")]
    for key in ("AOT_10m", "WVP_10m", "SCL_20m"):
        asset = proxy["assets"][key]
        assert asset["href"].endswith(".zarr"), "titiler 404s on a trailing slash"
        s3_href = asset["alternate"]["s3"]["href"]
        assert s3_href.endswith(".zarr/"), "cleanup refuses a bare .zarr key"
        assert check_urls_confined({s3_href}, allowed) == []


def test_missing_self_link_is_refused(source):
    source["links"] = [link for link in source["links"] if link["rel"] != "self"]
    with pytest.raises(ValueError, match="no self link"):
        build_proxy_item(source, COLLECTION, RASTER, STAC_API)


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


@patch("register_proxy.fetch_source_item")
@patch("register_proxy.upsert_item")
def test_a_non_proxy_collection_is_refused(upsert, fetch):
    assert (
        main(
            [
                "--collection",
                "sentinel-2-l2a",
                "--raster-api-url",
                RASTER,
                "--stac-api-url",
                "https://api.explorer.eopf.copernicus.eu/stac",
                "--item-id",
                "a",
                "--max-items",
                "1",
            ]
        )
        == 1
    )
    fetch.assert_not_called()
    upsert.assert_not_called()


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


@patch("register_v1.get_s3_storage_class", return_value="STANDARD")
def test_s3_endpoint_works_off_the_explorer_gateway(_tier, source):
    """Track B may be served from OVH directly, not only through the Explorer gateway.

    `https_to_s3` recognises the gateway it is given and returns None for every other
    host, so the gateway must come from the store root we actually wrote. Getting this
    wrong is silent: zero alternates, no log line, exit 0 — and `s3_item_cleanup` then
    records `no_s3_urls`, which is not a FAILURE_STATUS, so the store is skipped by the
    retention cron on every run, forever.
    """
    ovh_direct = (
        "https://s3.de.io.cloud.ovh.net/esa-zarr-sentinel-explorer-tests/samples-zarr3-proxy"
    )
    proxy = build_proxy_item(
        source,
        f"{COLLECTION}-ovh",
        RASTER,
        STAC_API,
        store_root_base=ovh_direct,
        s3_endpoint="https://s3.de.io.cloud.ovh.net",
    ).to_dict()
    for key in ("reflectance", "AOT_10m", "WVP_10m", "SCL_20m"):
        href = proxy["assets"][key]["alternate"]["s3"]["href"]
        assert href.startswith("s3://esa-zarr-sentinel-explorer-tests/samples-zarr3-proxy/")


@patch("register_v1.get_s3_storage_class", return_value="STANDARD")
def test_an_unconvertible_store_root_is_refused_not_registered(_tier, source):
    """No alternate means no deleter can ever select the item — refuse to write it."""
    with pytest.raises(ValueError, match="no alternate.s3 href"):
        build_proxy_item(
            source,
            f"{COLLECTION}-ovh",
            RASTER,
            STAC_API,
            store_root_base="ftp://example.invalid/bucket/prefix",
            s3_endpoint="https://s3.de.io.cloud.ovh.net",
        )


def test_an_unknown_source_asset_is_dropped_not_proxied(source):
    """The prune is an allowlist: EODC's schema is not frozen (220 items republished)."""
    source["assets"]["CLD_20m"] = dict(source["assets"]["SCL_20m"])
    source["assets"]["quicklook"] = {
        "href": "https://data.eodc.eu/x/quicklook.png",
        "roles": ["thumbnail", "overview"],
    }
    proxy = build_proxy_item(source, COLLECTION, RASTER, STAC_API).to_dict()
    assert set(proxy["assets"]) == {"reflectance", "AOT_10m", "WVP_10m", "SCL_20m"}


def test_a_missing_source_asset_is_refused(source):
    """The four-asset set IS criterion 1 of #287 — a partial item must not look like a pass."""
    del source["assets"]["ATM_10m"]
    with pytest.raises(ValueError, match="no ATM_10m asset"):
        build_proxy_item(source, COLLECTION, RASTER, STAC_API)


def test_a_band_filter_that_matches_nothing_is_refused(source):
    """Failure-open here makes AOT and WVP each advertise BOTH bands, silently."""
    for band in source["assets"]["ATM_10m"]["bands"]:
        band["name"] = band["name"].upper()
    with pytest.raises(ValueError, match="no 'aot' band"):
        build_proxy_item(source, COLLECTION, RASTER, STAC_API)


def test_aot_and_wvp_do_not_share_mutable_fields(source):
    """`dict(extra_fields)` is shallow — both assets ended up on one `proj:shape` list."""
    proxy = build_proxy_item(source, COLLECTION, RASTER, STAC_API)
    aot = proxy.assets["AOT_10m"].extra_fields
    wvp = proxy.assets["WVP_10m"].extra_fields
    assert aot.get("proj:shape") is not wvp.get("proj:shape")


def test_a_missing_reflectance_asset_is_refused(source):
    """The render links name `assets=reflectance` unconditionally."""
    for key in ("SR_10m", "SR_20m", "SR_60m"):
        source["assets"].pop(key, None)
    with pytest.raises(ValueError, match="missing asset"):
        build_proxy_item(source, COLLECTION, RASTER, STAC_API)


def test_the_source_cannot_choose_which_id_gets_registered(source):
    """id decides the dry-run filename, the registered id, and what --max-items counts."""
    source["id"] = "../escaped"
    with patch("httpx.Client") as client:
        client.return_value.__enter__.return_value.get.return_value = httpx.Response(
            200, json=source, request=httpx.Request("GET", "https://x/y")
        )
        with pytest.raises(ValueError, match="different item id"):
            fetch_source_item("https://stac.example", "c", "REQUESTED")


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


def test_a_non_https_store_root_base_is_refused(source):
    """--store-root-base rewrites every asset href; it gets the same guard as the APIs."""
    with patch("register_proxy.fetch_source_item", return_value=source) as fetch:
        rc = cli(
            "--item-id",
            source["id"],
            "--max-items",
            "1",
            "--store-root-base",
            "http://s3.typo-host.example/bucket/prefix",
        )
    assert rc == 1
    fetch.assert_not_called()


@pytest.mark.parametrize(
    ("flag", "value"),
    [("--store-root-base", OVH_BASE), ("--s3-endpoint", "https://s3.de.io.cloud.ovh.net")],
)
@patch("register_proxy.fetch_source_item")
@patch("register_proxy.upsert_item")
@patch("register_proxy.stac_auth.open_client")
def test_a_track_b_flag_on_its_own_is_refused(client, upsert, fetch, flag, value):
    """Alone, one registers unreclaimable OVH stores, the other bogus `s3://collections/…`."""
    assert cli("--item-id", "a", "--max-items", "1", flag, value) == 1
    client.assert_not_called()
    fetch.assert_not_called()
    upsert.assert_not_called()
