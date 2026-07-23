"""Unit tests for register_per_acquisition.py — per-acquisition item decoration.

Item *construction* (one item per cube `time` slice, oriented to its orbit, single `datetime`) now lives
in data-model (``eopf_geozarr.stac.s1_rtc.build_s1_rtc_per_acquisition_items``, tested there). This script
adds only the deployment decoration — render/`via` links + thumbnail pointing at the shared **cube**
TiTiler endpoint with ``sel=time={datetime}`` (no data duplication) — which is what's tested here.
"""

from __future__ import annotations

import datetime as dt
import sys
import urllib.parse
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pystac
import pytest
import requests

scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

import register_per_acquisition as rpa  # noqa: E402
from eopf_geozarr.stac.s1_rtc import acquisition_id  # noqa: E402

CUBE = "sentinel-1-grd-rtc-staging"  # cube collection (render endpoint)
ACQ = "sentinel-1-grd-rtc-acquisitions"  # per-acquisition collection (items go here)
RASTER = "https://api.explorer.eopf.copernicus.eu/raster"
STAC = "https://api.explorer.eopf.copernicus.eu/stac"
WHEN = dt.datetime(2026, 6, 5, 6, 9, 7, tzinfo=dt.UTC)
# sel fragment TiTiler matches against the CF-decoded datetime (colons percent-encoded)
_SEL = "sel=time=2026-06-05T06%3A09%3A07"


def _acq_item() -> pystac.Item:
    """A per-acquisition Item as build_s1_rtc_per_acquisition_items emits: single datetime, run orbit
    (descending), the orbit's γ⁰ asset + a reoriented renders.rgb, and (added by the caller) a store
    link. Decoration is what we exercise."""
    item = pystac.Item(
        id="s1-rtc-31TCH-20260605t060907",
        geometry={
            "type": "Polygon",
            "coordinates": [[[0, 42], [2, 42], [2, 43], [0, 43], [0, 42]]],
        },
        bbox=[0, 42, 2, 43],
        datetime=WHEN,
        properties={
            "sat:orbit_state": "descending",
            "renders": {
                "rgb": {
                    "title": "VV, VH, VV/VH composite",
                    "expression": "/descending:vv;/descending:vh;(/descending:vv)/(/descending:vh)",
                    "rescale": [[0.0, 0.4], [0.0, 0.1], [1.0, 15.0]],
                    "bidx": [1],
                    "tilesize": 256,
                }
            },
        },
        collection=ACQ,
    )
    item.add_asset(
        "zarr-store",
        pystac.Asset(href="https://gw/x/s1-rtc-31TCH.zarr", roles=["data"]),
    )
    item.add_link(
        pystac.Link(
            rel="store",
            target="https://gw/x/s1-rtc-31TCH.zarr",
            media_type="application/vnd.zarr+zarr",
        )
    )
    return item


def _links(d: dict) -> dict[str, str]:
    return {lk["rel"]: lk["href"] for lk in d["links"]}


def test_acquisition_id_format() -> None:
    """acquisition_id is re-exported from the data-model library (used by trigger_cdse)."""
    assert acquisition_id("31TCH", WHEN) == "s1-rtc-31TCH-20260605t060907"


def test_render_links_point_at_cube_endpoint_with_sel_datetime() -> None:
    """tilejson + viewer target the CUBE item's endpoint (not the acquisition item's), carry the
    composite render + sel=time={datetime}; never the acquisitions collection, no positional index."""
    d = rpa.decorate_acquisition_item(
        _acq_item(), tile_id="31TCH", cube_collection=CUBE, raster_api=RASTER, stac_api_url=STAC
    )
    links = _links(d)
    for rel in ("tilejson", "viewer", "xyz"):
        href = links[rel]
        assert f"/collections/{CUBE}/items/s1-rtc-31TCH" in href  # cube endpoint, not the acq item
        assert ACQ not in href
        assert "expression=" in href
        # one rescale pair per expression band (vv; vh; vv/vh ratio)
        for pair in ("rescale=0.0%2C0.4", "rescale=0.0%2C0.1", "rescale=1.0%2C15.0"):
            assert pair in href
        assert _SEL in href
        assert "sel=time=0" not in href  # not a positional index
        assert "/descending:vv" in urllib.parse.unquote(href)  # the item's own orbit
    assert "tilejson.json" in links["tilejson"]
    assert (
        "/WebMercatorQuad/map.html" in links["viewer"]
    )  # interactive viewer, not a raw tile template
    # the sole {z}/{x}/{y} template is the machine-facing rel=xyz link
    xyz_hrefs = [lk["href"] for lk in d["links"] if "{z}/{x}/{y}" in lk["href"]]
    assert xyz_hrefs == [links["xyz"]]


def test_xyz_link_shape() -> None:
    """The xyz link carries the literal {z}/{x}/{y} template (catches f-string escaping bugs),
    is image/png, ordered right after tilejson, and shares tilejson's exact query."""
    d = rpa.decorate_acquisition_item(
        _acq_item(), tile_id="31TCH", cube_collection=CUBE, raster_api=RASTER, stac_api_url=STAC
    )
    xyz = next(lk for lk in d["links"] if lk["rel"] == "xyz")
    assert "/tiles/WebMercatorQuad/{z}/{x}/{y}.png?" in xyz["href"]
    assert xyz["type"] == "image/png"
    assert xyz["title"] == "VV, VH, VV/VH composite"  # the render composite title (as on the cube)
    # ordered immediately after tilejson
    rels = [lk["rel"] for lk in d["links"]]
    assert rels.index("xyz") == rels.index("tilejson") + 1
    # query byte-identical to the tilejson link's
    tj = next(lk for lk in d["links"] if lk["rel"] == "tilejson")
    assert xyz["href"].split("?", 1)[1] == tj["href"].split("?", 1)[1]


def test_link_titles_and_order_match_cube_convention() -> None:
    """Acq visualization links mirror the cube (register_v1): order store→viewer→tilejson→xyz,
    viewer/xyz titled by the render composite, tilejson 'TileJSON for {id}'."""
    d = rpa.decorate_acquisition_item(
        _acq_item(), tile_id="31TCH", cube_collection=CUBE, raster_api=RASTER, stac_api_url=STAC
    )
    rels = [lk["rel"] for lk in d["links"]]
    # two related links (parent + sibling-collection filter) so STAC Browser groups the section
    assert rels == ["store", "viewer", "tilejson", "xyz", "via", "related", "related"]
    by_rel = {lk["rel"]: lk for lk in d["links"]}
    assert by_rel["viewer"]["title"] == "VV, VH, VV/VH composite"
    assert by_rel["xyz"]["title"] == "VV, VH, VV/VH composite"
    assert by_rel["tilejson"]["title"] == "TileJSON for s1-rtc-31TCH-20260605t060907"


def test_two_related_links_for_stac_browser_grouping() -> None:
    """Acq items carry two related links — parent cube + sibling acquisitions collection — so a rel
    group has >=2 entries and STAC Browser renders grouped 'Additional Resources' category headers."""
    d = rpa.decorate_acquisition_item(
        _acq_item(), tile_id="31TCH", cube_collection=CUBE, raster_api=RASTER, stac_api_url=STAC
    )
    related = [lk for lk in d["links"] if lk["rel"] == "related"]
    assert len(related) == 2
    titles = [lk["title"] for lk in related]
    assert titles == ["Parent tile datacube", "Per-acquisition items (filter by tile grid:code)"]
    filt = related[1]
    assert filt["href"] == f"{STAC}/collections/{ACQ}"  # the sibling acquisitions collection
    assert filt["type"] == "application/json"


def test_thumbnail_via_and_store_link_kept() -> None:
    d = rpa.decorate_acquisition_item(
        _acq_item(), tile_id="31TCH", cube_collection=CUBE, raster_api=RASTER, stac_api_url=STAC
    )
    thumb = d["assets"]["thumbnail"]
    assert thumb["type"] == "image/png"
    assert thumb["roles"] == ["thumbnail"]
    assert f"/collections/{CUBE}/items/s1-rtc-31TCH/preview" in thumb["href"]
    assert _SEL in thumb["href"]

    links = _links(d)
    assert links["via"].endswith(f"/collections/{ACQ}/items/s1-rtc-31TCH-20260605t060907")
    assert "store" in links  # the caller's cube store link is preserved
    assert "/WebMercatorQuad/map.html" in links["viewer"]  # map.html deep-link into this slice
    # the parent related link → the parent tile datacube STAC item (cube collection)
    parent = next(
        lk for lk in d["links"] if lk["rel"] == "related" and lk["title"] == "Parent tile datacube"
    )
    assert parent["href"] == f"{STAC}/collections/{CUBE}/items/s1-rtc-31TCH"


def test_no_orbit_leak() -> None:
    """A descending item's links never reference the ascending group."""
    d = rpa.decorate_acquisition_item(
        _acq_item(), tile_id="31TCH", cube_collection=CUBE, raster_api=RASTER, stac_api_url=STAC
    )
    assert "ascending" not in urllib.parse.unquote(str(d["links"]))


# ---------------------------------------------------------------------------
# _upsert_items write pattern (#352): POST, on 409 a single PUT — never DELETE
# ---------------------------------------------------------------------------


def _write_client(base_url: str = STAC) -> MagicMock:
    client = MagicMock()
    client.self_href = base_url
    return client


def _resp(status_code: int) -> Mock:
    resp = Mock(spec=requests.Response)
    resp.status_code = status_code
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(
            f"{status_code} Error", response=resp
        )
    else:
        resp.raise_for_status.return_value = None
    return resp


_ITEM = {"id": "s1-rtc-31TCH-20260605t060907", "properties": {"datetime": "2026-06-05T06:09:07Z"}}


class TestUpsertItemsWritePattern:
    def test_post_only_when_new(self):
        client = _write_client()
        client._stac_io.session.post.return_value = _resp(201)
        with patch("register_per_acquisition.stac_auth.open_client", return_value=client):
            rpa._upsert_items(STAC, ACQ, [_ITEM])

        session = client._stac_io.session
        session.post.assert_called_once()
        assert session.post.call_args.args[0] == f"{STAC}/collections/{ACQ}/items"
        assert session.post.call_args.kwargs["json"] == _ITEM
        session.put.assert_not_called()
        session.delete.assert_not_called()

    def test_409_triggers_put_replace_never_delete(self):
        client = _write_client()
        client._stac_io.session.post.return_value = _resp(409)
        client._stac_io.session.put.return_value = _resp(200)
        with patch("register_per_acquisition.stac_auth.open_client", return_value=client):
            rpa._upsert_items(STAC, ACQ, [_ITEM])

        session = client._stac_io.session
        session.put.assert_called_once()
        assert session.put.call_args.args[0] == f"{STAC}/collections/{ACQ}/items/{_ITEM['id']}"
        assert session.put.call_args.kwargs["json"] == _ITEM
        session.delete.assert_not_called()

    def test_put_failure_raises(self):
        client = _write_client()
        client._stac_io.session.post.return_value = _resp(409)
        client._stac_io.session.put.return_value = _resp(500)
        with (
            patch("register_per_acquisition.stac_auth.open_client", return_value=client),
            pytest.raises(requests.HTTPError),
        ):
            rpa._upsert_items(STAC, ACQ, [_ITEM])

        client._stac_io.session.delete.assert_not_called()
