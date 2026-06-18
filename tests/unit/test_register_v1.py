"""Unit tests for register_v1.py — upsert_item."""

import contextlib
import sys
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest
import requests

# Add scripts directory to path
scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

from register_v1 import (  # noqa: E402
    _render_to_query,
    _select_render,
    add_thumbnail_asset,
    add_visualization_links,
    upsert_item,
)


def _make_client(item_exists: bool, base_url: str = "https://stac.example.com") -> MagicMock:
    """Build a minimal pystac_client.Client mock."""
    client = MagicMock()
    client.self_href = base_url

    if item_exists:
        # get_item() returns normally → exists = True
        client.get_collection.return_value.get_item.return_value = MagicMock()
    else:
        # get_item() raises → exists = False
        client.get_collection.return_value.get_item.side_effect = Exception("not found")

    return client


def _make_response(status_code: int) -> Mock:
    resp = Mock(spec=requests.Response)
    resp.status_code = status_code
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(
            f"{status_code} Error", response=resp
        )
    else:
        resp.raise_for_status.return_value = None
    return resp


def _make_item(item_id: str = "test-item-001") -> MagicMock:
    item = MagicMock()
    item.id = item_id
    item.to_dict.return_value = {"id": item_id, "type": "Feature"}
    return item


class TestUpsertItemDeleteFailure:
    """DELETE fails → raise_for_status raises → POST must not be called."""

    @pytest.mark.parametrize("status_code", [403, 404, 500, 503])
    def test_raises_on_delete_failure(self, status_code):
        client = _make_client(item_exists=True)
        client._stac_io.session.delete.return_value = _make_response(status_code)
        item = _make_item()

        with pytest.raises(requests.HTTPError):
            upsert_item(client, "my-collection", item)

    @pytest.mark.parametrize("status_code", [403, 404, 500, 503])
    def test_post_not_called_when_delete_fails(self, status_code):
        client = _make_client(item_exists=True)
        client._stac_io.session.delete.return_value = _make_response(status_code)
        item = _make_item()

        with contextlib.suppress(requests.HTTPError):
            upsert_item(client, "my-collection", item)

        client._stac_io.session.post.assert_not_called()


class TestUpsertItemDeleteSuccess:
    """DELETE succeeds → POST is called with the item payload."""

    def test_delete_then_post_when_item_exists(self):
        client = _make_client(item_exists=True, base_url="https://stac.example.com")
        client._stac_io.session.delete.return_value = _make_response(200)
        client._stac_io.session.post.return_value = _make_response(201)
        item = _make_item("existing-item")

        upsert_item(client, "my-collection", item)

        client._stac_io.session.delete.assert_called_once_with(
            "https://stac.example.com/collections/my-collection/items/existing-item",
            timeout=30,
        )
        client._stac_io.session.post.assert_called_once()
        post_call = client._stac_io.session.post.call_args
        assert post_call.kwargs["json"] == item.to_dict()


class TestUpsertItemNewItem:
    """Item does not exist → no DELETE, POST called directly."""

    def test_no_delete_when_item_is_new(self):
        client = _make_client(item_exists=False)
        client._stac_io.session.post.return_value = _make_response(201)
        item = _make_item("new-item")

        upsert_item(client, "my-collection", item)

        client._stac_io.session.delete.assert_not_called()
        client._stac_io.session.post.assert_called_once()

    def test_no_delete_when_get_item_returns_none(self):
        """get_item() returning None (not raising) must be treated as no item → no DELETE.

        pystac-client's get_item() does not raise on 404 — it returns None. The old
        code checked only for exceptions, so None returned means exists=True (wrong).
        """
        client = _make_client(item_exists=False)
        # Override to return None instead of raising (matches pystac-client 404 behavior)
        client.get_collection.return_value.get_item.side_effect = None
        client.get_collection.return_value.get_item.return_value = None
        client._stac_io.session.delete.return_value = _make_response(200)
        client._stac_io.session.post.return_value = _make_response(201)

        upsert_item(client, "my-collection", _make_item("new-item"))

        client._stac_io.session.delete.assert_not_called()

    def test_post_url_for_new_item(self):
        client = _make_client(item_exists=False, base_url="https://stac.example.com")
        client._stac_io.session.post.return_value = _make_response(201)

        upsert_item(client, "sentinel-2", _make_item())

        post_call = client._stac_io.session.post.call_args
        assert post_call.args[0] == "https://stac.example.com/collections/sentinel-2/items"

    def test_raises_on_post_failure(self):
        client = _make_client(item_exists=False)
        client._stac_io.session.post.return_value = _make_response(500)

        with pytest.raises(requests.HTTPError):
            upsert_item(client, "my-collection", _make_item())


# =============================================================================
# Render-extension visualization
# =============================================================================

import datetime as _dt  # noqa: E402

from pystac import Asset, Item  # noqa: E402

RASTER_BASE = "https://api.example.com/raster"
S1_RGB_EXPR = "/descending:vv;/descending:vh;(/descending:vv)/(/descending:vh)"


def _real_item(renders: dict | None = None) -> Item:
    """A real pystac Item with vv/vh assets and an optional renders property."""
    props: dict = {}
    if renders is not None:
        props["renders"] = renders
    item = Item(
        id="s1-rtc-31TCH",
        geometry=None,
        bbox=None,
        datetime=_dt.datetime(2026, 6, 7, tzinfo=_dt.UTC),
        properties=props,
    )
    href = "s3://bucket/s1-grd-rtc-31TCH.zarr/descending"
    for pol in ("vv", "vh"):
        item.add_asset(pol, Asset(href=href, roles=["data"]))
    return item


def _s1_rgb_renders() -> dict:
    return {
        "rgb": {
            "title": "VV, VH, VV/VH composite",
            "expression": S1_RGB_EXPR,
            "rescale": [[0.0, 0.1]],
            "bidx": [1],
            "tilesize": 256,
        }
    }


class TestSelectRender:
    def test_returns_none_without_renders(self):
        assert _select_render(_real_item()) is None

    def test_prefers_rgb_name(self):
        renders = {"other": {"expression": "x"}, "rgb": {"expression": "y"}}
        assert _select_render(_real_item(renders))["expression"] == "y"

    def test_falls_back_to_first_render(self):
        renders = {"only": {"expression": "z"}}
        assert _select_render(_real_item(renders))["expression"] == "z"


class TestRenderToQuery:
    def test_serializes_render_fields(self):
        q = _render_to_query(_s1_rgb_renders()["rgb"], include_tilesize=True)
        assert q.count("rescale=") == 1
        assert "rescale=0.0%2C0.1" in q
        assert "bidx=1" in q
        assert "tilesize=256" in q
        assert "expression=" in q

    def test_tilesize_excluded_when_requested(self):
        q = _render_to_query(_s1_rgb_renders()["rgb"], include_tilesize=False)
        assert "tilesize" not in q

    def test_one_rescale_param_per_pair(self):
        render = {"expression": "a;b", "rescale": [[0, 1], [0, 2]]}
        q = _render_to_query(render, include_tilesize=False)
        assert q.count("rescale=") == 2


class TestVisualizationFromRenders:
    def test_xyz_and_tilejson_use_render_expression(self):
        item = _real_item(_s1_rgb_renders())
        add_visualization_links(item, RASTER_BASE, "sentinel-1-grd-rtc-staging")

        xyz = next(link for link in item.links if link.rel == "xyz")
        tilejson = next(link for link in item.links if link.rel == "tilejson")
        # render expression is used, NOT the old VH-grayscale rescale=0,219
        for link in (xyz, tilejson):
            assert "expression=" in link.href
            assert "rescale=0%2C219" not in link.href
            assert "0.1" in link.href
        assert "/tiles/WebMercatorQuad/{z}/{x}/{y}.png" in xyz.href
        assert "tilejson.json" in tilejson.href

    def test_thumbnail_uses_render_expression(self):
        item = _real_item(_s1_rgb_renders())
        add_thumbnail_asset(item, RASTER_BASE, "sentinel-1-grd-rtc-staging")

        thumb = item.assets["thumbnail"]
        assert "expression=" in thumb.href
        assert "rescale=0%2C219" not in thumb.href
        assert "tilesize" not in thumb.href  # not valid on /preview
        assert thumb.href.startswith(f"{RASTER_BASE}/collections/")

    def test_falls_back_to_mission_default_without_renders(self):
        item = _real_item()  # no renders property
        add_thumbnail_asset(item, RASTER_BASE, "sentinel-1-grd-rtc-staging")
        # legacy VH grayscale path still applies when no render config present
        assert "thumbnail" in item.assets


_SEL = "2026-06-07T05:52:48"
_SEL_Q = "sel=time=2026-06-07T05%3A52%3A48"  # colons percent-encoded


class TestSelTimePinsSlice:
    """The cube preview pins the best-recent slice via ``sel=time={datetime}`` on its links + thumbnail."""

    def test_thumbnail_asset_carries_sel_time(self):
        item = _real_item(_s1_rgb_renders())
        add_thumbnail_asset(item, RASTER_BASE, "sentinel-1-grd-rtc-staging", sel_time=_SEL)
        assert _SEL_Q in item.assets["thumbnail"].href

    def test_xyz_and_tilejson_carry_sel_time(self):
        item = _real_item(_s1_rgb_renders())
        add_visualization_links(item, RASTER_BASE, "sentinel-1-grd-rtc-staging", sel_time=_SEL)
        for rel in ("xyz", "tilejson"):
            link = next(link for link in item.links if link.rel == rel)
            assert _SEL_Q in link.href

    def test_no_sel_time_by_default_backcompat(self):
        # sel_time omitted (S2 + any other caller) => links/thumbnail unchanged, no sel.
        item = _real_item(_s1_rgb_renders())
        add_visualization_links(item, RASTER_BASE, "sentinel-1-grd-rtc-staging")
        add_thumbnail_asset(item, RASTER_BASE, "sentinel-1-grd-rtc-staging")
        hrefs = [link.href for link in item.links] + [item.assets["thumbnail"].href]
        assert all("sel=time" not in h for h in hrefs)
