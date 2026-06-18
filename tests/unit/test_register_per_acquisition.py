"""Unit tests for scripts/register_per_acquisition.py — per-acquisition STAC items (plan T5).

One STAC item per cube `time` slice, id `s1-rtc-{tile}-{datetime}`. Render links point at the **cube**
TiTiler endpoint with the composite render + `sel=time={datetime}` (no data duplication; the acquisition
item is a reference into the shared cube). Selecting by datetime (not a positional index) makes each item
correct regardless of the cube's physical slice order. Pure builders tested here; store I/O + upsert in
main().
"""

import datetime as dt
import json
import sys
import urllib.parse
from pathlib import Path

SCRIPT = Path(__file__).parent.parent.parent / "scripts" / "register_per_acquisition.py"


def _mod():
    sys.path.insert(0, str(SCRIPT.parent))
    import register_per_acquisition

    return register_per_acquisition


CUBE = "sentinel-1-grd-rtc-staging"  # cube collection (render endpoint)
ACQ = "sentinel-1-grd-rtc-acquisitions"  # per-acquisition collection (items go here)
RASTER = "https://api.explorer.eopf.copernicus.eu/raster"
# two acquisitions; passed in PHYSICAL (append) order — deliberately NOT chronological
_T_LATER = int(dt.datetime(2026, 6, 7, 5, 52, 48, tzinfo=dt.UTC).timestamp() * 1e9)
_T_EARLY = int(dt.datetime(2026, 6, 5, 6, 9, 7, tzinfo=dt.UTC).timestamp() * 1e9)


def _base_item() -> dict:
    """Per-tile base item as build_s1_rtc_stac_item emits: a temporal range, a `renders.rgb` composite,
    and vv/vh/zarr-store assets — preferring **ascending** so reorientation to a descending run shows."""
    return {
        "type": "Feature",
        "id": "s1-rtc-31TCH",
        "collection": CUBE,
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[0, 42], [2, 42], [2, 43], [0, 43], [0, 42]]],
        },
        "bbox": [0, 42, 2, 43],
        "properties": {
            "start_datetime": "2026-06-05T06:09:07Z",
            "end_datetime": "2026-06-07T05:52:48Z",
            "sar:product_type": "GRD",
            "proj:code": "EPSG:32631",
            "sat:orbit_state": "ascending",
            "renders": {
                "rgb": {
                    "title": "VV, VH, VV/VH composite",
                    "expression": "/ascending:vv;/ascending:vh;(/ascending:vv)/(/ascending:vh)",
                    "rescale": [[0.0, 0.1]],
                    "bidx": [1],
                    "tilesize": 256,
                }
            },
        },
        "assets": {
            "vv": {"href": "https://gw/x/s1-rtc-31TCH.zarr/ascending", "roles": ["data"]},
            "vh": {"href": "https://gw/x/s1-rtc-31TCH.zarr/ascending", "roles": ["data"]},
            "zarr-store": {"href": "https://gw/x/s1-rtc-31TCH.zarr", "roles": ["data"]},
        },
        "links": [],
    }


def _items(orbit: str = "descending", times=None):
    return _mod().per_acquisition_items(
        _base_item(),
        times if times is not None else [_T_EARLY],
        tile_id="31TCH",
        orbit=orbit,
        collection=ACQ,
        cube_collection=CUBE,
        raster_api=RASTER,
    )


def _links(item):
    return {link["rel"]: link["href"] for link in item["links"]}


# --- acquisition_id ----------------------------------------------------------


def test_acquisition_id_format():
    when = dt.datetime(2026, 6, 7, 5, 52, 48, tzinfo=dt.UTC)
    assert _mod().acquisition_id("31TCH", when) == "s1-rtc-31TCH-20260607t055248"


# --- identity + datetime -----------------------------------------------------


def test_single_datetime_no_range_and_targets_acq_collection():
    item = _items()[0]
    props = item["properties"]
    assert item["collection"] == ACQ  # item lives in the per-acquisition collection
    assert props["datetime"] == "2026-06-05T06:09:07+00:00"
    assert "start_datetime" not in props and "end_datetime" not in props
    assert props["sar:product_type"] == "GRD"


# --- render links point at the CUBE endpoint + sel=datetime (no duplication) --

# _T_EARLY = 2026-06-05T06:09:07 → the encoded sel fragment TiTiler matches by datetime
_SEL_EARLY = "sel=time=2026-06-05T06%3A09%3A07"
_SEL_LATER = "sel=time=2026-06-07T05%3A52%3A48"


def test_render_links_point_at_cube_endpoint_with_sel_datetime():
    """tilejson + xyz target the CUBE item's endpoint (not the acquisition item's), carry the
    composite render (reoriented to the run orbit) + rescale 0,0.1 + sel=time={datetime} (colons
    percent-encoded); no store path, no hardcoded 0,219, no integer index, no nearest:: syntax."""
    links = _links(_items()[0])
    for rel in ("tilejson", "xyz"):
        href = links[rel]
        assert f"/collections/{CUBE}/items/s1-rtc-31TCH" in href  # cube endpoint, not the acq item
        assert ACQ not in href  # the render link never points at the acquisitions collection
        assert "expression=" in href
        assert "rescale=0.0%2C0.1" in href and "rescale=0%2C219" not in href
        assert _SEL_EARLY in href and "nearest" not in href
        assert "sel=time=0" not in href  # not a positional index
        assert "/descending:vv" in urllib.parse.unquote(href)  # reoriented to the run orbit
    assert "tilejson.json" in links["tilejson"] and "{z}/{x}/{y}.png" in links["xyz"]


def test_thumbnail_asset_points_at_cube_endpoint_with_sel_datetime():
    thumb = _items()[0]["assets"]["thumbnail"]
    assert thumb["type"] == "image/png" and thumb["roles"] == ["thumbnail"]
    href = thumb["href"]
    assert f"/collections/{CUBE}/items/s1-rtc-31TCH/preview" in href
    assert "expression=" in href and "rescale=0.0%2C0.1" in href and _SEL_EARLY in href


def test_sel_follows_datetime_not_physical_position():
    """Each item's sel is ITS OWN datetime, independent of position in the passed (physical) list —
    so a non-monotonic cube still renders every slice correctly without reordering or re-registering."""
    items = _items(times=[_T_LATER, _T_EARLY])  # physical/append order (later appended first)
    assert items[0]["properties"]["datetime"] == "2026-06-07T05:52:48+00:00"
    assert _SEL_LATER in _links(items[0])["tilejson"]  # index 0 but its OWN (later) datetime
    assert items[1]["properties"]["datetime"] == "2026-06-05T06:09:07+00:00"
    assert _SEL_EARLY in _links(items[1])["tilejson"]  # index 1 but its OWN (earlier) datetime


# --- orbit metadata reconciliation -------------------------------------------


def test_reorients_orbit_metadata_to_run_orbit():
    """A descending run's item carries /descending metadata (sat:orbit_state, renders.rgb orbit, vv/vh
    asset group) — never the cube's preferred /ascending. vv/vh hrefs stay on the cube (group only)."""
    item = _items(orbit="descending")[0]
    props = item["properties"]
    assert props["sat:orbit_state"] == "descending"
    assert props["renders"]["rgb"]["expression"] == (
        "/descending:vv;/descending:vh;(/descending:vv)/(/descending:vh)"
    )
    for pol in ("vv", "vh"):
        assert item["assets"][pol]["href"] == "https://gw/x/s1-rtc-31TCH.zarr/descending"
    assert "ascending" not in urllib.parse.unquote(json.dumps(item))


# --- base item is never mutated ----------------------------------------------


def test_does_not_mutate_base():
    base = _base_item()
    _mod().per_acquisition_items(
        base, [_T_EARLY, _T_LATER], tile_id="31TCH", orbit="descending",
        collection=ACQ, cube_collection=CUBE, raster_api=RASTER,
    )  # fmt: skip
    assert base["id"] == "s1-rtc-31TCH"
    assert "start_datetime" in base["properties"]
    assert base["properties"]["sat:orbit_state"] == "ascending"  # not reoriented in place
    assert "thumbnail" not in base["assets"]


# --- rescale override (S1_RTC_RESCALE) ---------------------------------------


def test_apply_s1_rtc_rescale_overrides_build_default():
    """apply_s1_rtc_rescale replaces build's 0.0,0.1 with S1_RTC_RESCALE (0.0,0.2) on the rgb render."""
    from pystac import Item

    m = _mod()
    render = _base_item()["properties"]["renders"][
        "rgb"
    ]  # the build-default render (rescale 0.0,0.1)
    item = Item(
        id="s1-rtc-31TCH", geometry=None, bbox=None,
        datetime=dt.datetime(2026, 6, 7, tzinfo=dt.UTC), properties={"renders": {"rgb": render}},
    )  # fmt: skip
    assert item.properties["renders"]["rgb"]["rescale"] == [[0.0, 0.1]]
    m.apply_s1_rtc_rescale(item)
    assert item.properties["renders"]["rgb"]["rescale"] == [[0.0, 0.2]]
    assert m.S1_RTC_RESCALE == [[0.0, 0.2]]


def test_apply_s1_rtc_rescale_noop_without_render():
    """No rgb render (e.g. a non-S1 item) → apply is a safe no-op, not an error."""
    from pystac import Item

    m = _mod()
    item = Item(
        id="x", geometry=None, bbox=None,
        datetime=dt.datetime(2026, 6, 7, tzinfo=dt.UTC), properties={},
    )  # fmt: skip
    m.apply_s1_rtc_rescale(item)
    assert "renders" not in item.properties


# --- alignment with cube item: alternate/storage + store/via links -----------


def _base_item_with_alternate() -> dict:
    """base_item as main() hands it to per_acquisition_items: the build item + the cube augmentation
    (alternate-assets/storage blocks on the data assets + a `store` link)."""
    d = _base_item()
    d["stac_extensions"] = [
        "https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json",
        "https://stac-extensions.github.io/storage/v2.0.0/schema.json",
    ]
    for key in ("vv", "vh", "zarr-store"):
        suffix = "" if key == "zarr-store" else "/ascending"  # preferred orbit, as build emits
        d["assets"][key]["alternate"] = {
            "s3": {
                "href": f"s3://bkt/x/s1-rtc-31TCH.zarr{suffix}",
                "storage:scheme": {"tier": "STANDARD", "region": "de", "platform": "OVHcloud"},
            }
        }
    d["links"] = [
        {
            "rel": "store",
            "type": "application/vnd.zarr+zarr",
            "href": "https://gw/x/s1-rtc-31TCH.zarr",
        }
    ]
    return d


def test_aligned_items_carry_alternate_storage_and_store_via_links():
    """Per-acq items inherit the cube's alternate-assets/storage blocks + `store` link and gain a `via`
    link. For a descending run, the s3 alternate href is reoriented to the run orbit (zarr-store stays
    at the store root)."""
    items = _mod().per_acquisition_items(
        _base_item_with_alternate(), [_T_EARLY], tile_id="31TCH", orbit="descending",
        collection=ACQ, cube_collection=CUBE, raster_api=RASTER,
    )  # fmt: skip
    item = items[0]
    rels = {link["rel"] for link in item["links"]}
    assert {"store", "tilejson", "xyz", "via"} <= rels
    via = next(link for link in item["links"] if link["rel"] == "via")
    assert via["href"].endswith(f"/collections/{ACQ}/items/{item['id']}")
    for pol in ("vv", "vh"):
        assert item["assets"][pol]["alternate"]["s3"]["href"].endswith(".zarr/descending")
        assert item["assets"][pol]["alternate"]["s3"]["storage:scheme"]["platform"] == "OVHcloud"
    assert item["assets"]["zarr-store"]["alternate"]["s3"]["href"].endswith(".zarr")
    assert "ascending" not in json.dumps(item)  # fully reoriented (primary + alternate)
