"""Unit tests for register_v1.py — upsert_item + expires stamping."""

import contextlib
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest
import requests
from pystac import Asset, Item

# Add scripts directory to path
scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

from register_v1 import (  # noqa: E402
    TIMESTAMPS_EXTENSION,
    _render_to_query,
    _select_render,
    add_expires,
    add_thumbnail_asset,
    add_visualization_links,
    resolve_exclude_ids,
    resolve_retention_days,
    upsert_item,
)


def _make_client(base_url: str = "https://stac.example.com") -> MagicMock:
    """Build a minimal pystac_client.Client mock — only self_href + _stac_io.session used."""
    client = MagicMock()
    client.self_href = base_url
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


class TestUpsertItemNewItem:
    """First POST succeeds (item did not exist) → no DELETE, single POST."""

    def test_no_delete_when_post_succeeds(self):
        client = _make_client()
        client._stac_io.session.post.return_value = _make_response(201)
        item = _make_item("new-item")

        upsert_item(client, "my-collection", item)

        client._stac_io.session.delete.assert_not_called()
        client._stac_io.session.post.assert_called_once()
        assert client._stac_io.session.post.call_args.kwargs["json"] == item.to_dict()

    def test_post_url_for_new_item(self):
        client = _make_client(base_url="https://stac.example.com")
        client._stac_io.session.post.return_value = _make_response(201)

        upsert_item(client, "sentinel-2", _make_item())

        post_call = client._stac_io.session.post.call_args
        assert post_call.args[0] == "https://stac.example.com/collections/sentinel-2/items"

    def test_raises_on_post_failure_without_delete(self):
        """A non-409 POST error (e.g. 500) raises immediately and never DELETEs."""
        client = _make_client()
        client._stac_io.session.post.return_value = _make_response(500)

        with pytest.raises(requests.HTTPError):
            upsert_item(client, "my-collection", _make_item())

        client._stac_io.session.delete.assert_not_called()


class TestUpsertItemExistingItem:
    """First POST returns 409 (item exists) → DELETE then re-POST. Regression for the
    cron-wide register-stac 409 on re-registering the fixed-id cube item s1-rtc-{tile}."""

    def test_409_triggers_delete_then_repost(self):
        client = _make_client(base_url="https://stac.example.com")
        client._stac_io.session.post.side_effect = [_make_response(409), _make_response(201)]
        client._stac_io.session.delete.return_value = _make_response(200)
        item = _make_item("existing-item")

        upsert_item(client, "my-collection", item)

        client._stac_io.session.delete.assert_called_once_with(
            "https://stac.example.com/collections/my-collection/items/existing-item",
            timeout=30,
        )
        assert client._stac_io.session.post.call_count == 2
        for call in client._stac_io.session.post.call_args_list:
            assert call.kwargs["json"] == item.to_dict()


class TestUpsertItemDeleteFailure:
    """First POST 409, then DELETE fails → raises, and the re-POST is not attempted."""

    @pytest.mark.parametrize("status_code", [403, 404, 500, 503])
    def test_raises_on_delete_failure(self, status_code):
        client = _make_client()
        client._stac_io.session.post.side_effect = [_make_response(409), _make_response(201)]
        client._stac_io.session.delete.return_value = _make_response(status_code)

        with pytest.raises(requests.HTTPError):
            upsert_item(client, "my-collection", _make_item())

    @pytest.mark.parametrize("status_code", [403, 404, 500, 503])
    def test_no_repost_when_delete_fails(self, status_code):
        client = _make_client()
        client._stac_io.session.post.side_effect = [_make_response(409), _make_response(201)]
        client._stac_io.session.delete.return_value = _make_response(status_code)

        with contextlib.suppress(requests.HTTPError):
            upsert_item(client, "my-collection", _make_item())

        # only the first POST (the 409) was made; no re-POST after the delete failed
        assert client._stac_io.session.post.call_count == 1


# =============================================================================
# Render-extension visualization
# =============================================================================

import datetime as _dt  # noqa: E402

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
    def test_viewer_and_tilejson_use_render_expression(self):
        item = _real_item(_s1_rgb_renders())
        add_visualization_links(item, RASTER_BASE, "sentinel-1-grd-rtc-staging")

        viewer = next(link for link in item.links if link.rel == "viewer")
        tilejson = next(link for link in item.links if link.rel == "tilejson")
        # render expression is used, NOT the old VH-grayscale rescale=0,219
        for link in (viewer, tilejson):
            assert "expression=" in link.href
            assert "rescale=0%2C219" not in link.href
            assert "0.1" in link.href
        # the human viewer is the interactive map.html, not a raw {z}/{x}/{y} tile template
        assert "/WebMercatorQuad/map.html" in viewer.href
        # the sole raw {z}/{x}/{y} tile template is the machine-facing rel=xyz link
        xyz_links = [link for link in item.links if "{z}/{x}/{y}" in link.href]
        assert [link.rel for link in xyz_links] == ["xyz"]
        assert "tilejson.json" in tilejson.href

    def test_xyz_tile_template_from_render(self):
        item = _real_item(_s1_rgb_renders())
        add_visualization_links(item, RASTER_BASE, "sentinel-1-grd-rtc-staging")

        xyz = [link for link in item.links if link.rel == "xyz"]
        assert len(xyz) == 1
        xyz = xyz[0]
        # raw XYZ tile template for machine map clients (QGIS/Leaflet/OpenLayers)
        assert "/tiles/WebMercatorQuad/{z}/{x}/{y}.png?" in xyz.href
        assert xyz.media_type == "image/png"
        assert xyz.title == "VV, VH, VV/VH composite"
        # query is byte-identical to the tilejson link's query
        tilejson = next(link for link in item.links if link.rel == "tilejson")
        assert xyz.href.split("?", 1)[1] == tilejson.href.split("?", 1)[1]
        # ordered immediately after tilejson
        rels = [link.rel for link in item.links]
        assert rels.index("xyz") == rels.index("tilejson") + 1

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

    def test_s2_fallback_xyz_unchanged(self):
        # No-renders S2 item must keep the legacy hardcoded true-color xyz template
        # untouched (no-regression guarantee for the deliberate reversal on S1).
        item = _real_item()  # no renders property
        add_visualization_links(item, RASTER_BASE, "sentinel-2-l2a")
        xyz = next(link for link in item.links if link.rel == "xyz")
        assert "/tiles/WebMercatorQuad/{z}/{x}/{y}.png?" in xyz.href
        assert xyz.media_type == "image/png"
        assert xyz.title == "Sentinel-2 L2A True Color"
        assert "color_formula=" in xyz.href


_SEL = "2026-06-07T05:52:48"
_SEL_Q = "sel=time=2026-06-07T05%3A52%3A48"  # colons percent-encoded


class TestSelTimePinsSlice:
    """The cube preview pins the best-recent slice via ``sel=time={datetime}`` on its links + thumbnail."""

    def test_thumbnail_asset_carries_sel_time(self):
        item = _real_item(_s1_rgb_renders())
        add_thumbnail_asset(item, RASTER_BASE, "sentinel-1-grd-rtc-staging", sel_time=_SEL)
        assert _SEL_Q in item.assets["thumbnail"].href

    def test_viewer_and_tilejson_carry_sel_time(self):
        item = _real_item(_s1_rgb_renders())
        add_visualization_links(item, RASTER_BASE, "sentinel-1-grd-rtc-staging", sel_time=_SEL)
        for rel in ("viewer", "tilejson"):
            link = next(link for link in item.links if link.rel == rel)
            assert _SEL_Q in link.href

    def test_xyz_carries_sel_time(self):
        item = _real_item(_s1_rgb_renders())
        add_visualization_links(item, RASTER_BASE, "sentinel-1-grd-rtc-staging", sel_time=_SEL)
        xyz = next(link for link in item.links if link.rel == "xyz")
        assert _SEL_Q in xyz.href

    def test_no_sel_time_by_default_backcompat(self):
        # sel_time omitted (S2 + any other caller) => links/thumbnail unchanged, no sel.
        item = _real_item(_s1_rgb_renders())
        add_visualization_links(item, RASTER_BASE, "sentinel-1-grd-rtc-staging")
        add_thumbnail_asset(item, RASTER_BASE, "sentinel-1-grd-rtc-staging")
        hrefs = [link.href for link in item.links] + [item.assets["thumbnail"].href]
        assert all("sel=time" not in h for h in hrefs)


# === expires stamping (coordination#183, Task 2) ===


def _expires_item(item_id: str = "S2_test") -> Item:
    """A minimal real pystac Item for exercising add_expires."""
    return Item(
        id=item_id,
        geometry={"type": "Point", "coordinates": [0.0, 0.0]},
        bbox=[0.0, 0.0, 0.0, 0.0],
        datetime=datetime(2024, 1, 1, tzinfo=UTC),
        properties={},
    )


class TestAddExpires:
    """add_expires stamps properties.expires + the timestamps extension."""

    def test_sets_expires_roughly_retention_days_ahead(self) -> None:
        item = _expires_item()
        add_expires(item, 183)

        assert "expires" in item.properties
        expires = datetime.strptime(item.properties["expires"], "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=UTC
        )
        delta = expires - datetime.now(UTC)
        # Allow a small window for wall-clock drift during the test.
        assert timedelta(days=182, hours=23) < delta <= timedelta(days=183)

    def test_expires_is_utc_z_formatted(self) -> None:
        item = _expires_item()
        add_expires(item, 183)
        assert item.properties["expires"].endswith("Z")

    def test_appends_timestamps_extension_exactly_once(self) -> None:
        item = _expires_item()
        add_expires(item, 183)
        add_expires(item, 183)  # re-stamp must not duplicate the extension URL
        assert item.stac_extensions.count(TIMESTAMPS_EXTENSION) == 1

    def test_zero_retention_is_a_noop(self) -> None:
        item = _expires_item()
        add_expires(item, 0)
        assert "expires" not in item.properties
        assert TIMESTAMPS_EXTENSION not in item.stac_extensions

    def test_negative_retention_is_a_noop(self) -> None:
        item = _expires_item()
        add_expires(item, -5)
        assert "expires" not in item.properties
        assert TIMESTAMPS_EXTENSION not in item.stac_extensions

    def test_excluded_item_is_not_stamped(self) -> None:
        # A demo scene in the denylist stays structurally undeletable even when
        # re-registered with a positive retention (the reconversion case).
        item = _expires_item(item_id="S2_demo")
        add_expires(item, 183, exclude_ids={"S2_demo"})
        assert "expires" not in item.properties
        assert TIMESTAMPS_EXTENSION not in item.stac_extensions

    def test_non_excluded_item_is_still_stamped(self) -> None:
        item = _expires_item(item_id="S2_pipeline")
        add_expires(item, 183, exclude_ids={"S2_demo"})
        assert "expires" in item.properties


class TestResolveExcludeIds:
    """resolve_exclude_ids reads the demo denylist; when the env is unset it
    falls back to the baked file so demo protection is never accidentally off."""

    def test_unset_falls_back_to_baked_demo_file(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # A forgotten EXPIRES_EXCLUDE_FILE must NOT unprotect demo scenes — the
        # baked /app/scripts/demo_exclude_ids.txt is used by default (coordination#183).
        from s3_item_cleanup import BAKED_EXCLUDE_FILE, load_exclude_ids

        monkeypatch.delenv("EXPIRES_EXCLUDE_FILE", raising=False)
        ids = resolve_exclude_ids()
        assert ids  # the baked file ships real demo ids
        assert ids == load_exclude_ids(str(BAKED_EXCLUDE_FILE))

    def test_env_overrides_baked(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        f = tmp_path / "demo.txt"
        f.write_text("# demo\nS2_demo_a\nS2_demo_b\n")
        monkeypatch.setenv("EXPIRES_EXCLUDE_FILE", str(f))
        assert resolve_exclude_ids() == {"S2_demo_a", "S2_demo_b"}


class TestResolveRetentionDays:
    """resolve_retention_days reads EXPIRES_RETENTION_DAYS, default 183."""

    def test_defaults_to_183_when_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("EXPIRES_RETENTION_DAYS", raising=False)
        assert resolve_retention_days() == 183

    def test_reads_override_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("EXPIRES_RETENTION_DAYS", "30")
        assert resolve_retention_days() == 30

    def test_zero_disables(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("EXPIRES_RETENTION_DAYS", "0")
        assert resolve_retention_days() == 0

    def test_empty_env_falls_back_to_default_not_crash(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # An empty value in a manifest must not crash the registration hot path.
        monkeypatch.setenv("EXPIRES_RETENTION_DAYS", "")
        assert resolve_retention_days() == 183
