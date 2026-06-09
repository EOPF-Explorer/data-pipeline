"""Unit tests for scripts/register_per_acquisition.py — per-acquisition STAC items (plan T5).

One STAC item per cube `time` slice, id `s1-rtc-{tile}-{datetime}`, with `sel=time` viz links
baked (explorer rendering deferred to #228). Pure builders tested here; store I/O + upsert in main().
"""

import datetime as dt
import sys
from pathlib import Path

SCRIPT = Path(__file__).parent.parent.parent / "scripts" / "register_per_acquisition.py"


def _mod():
    sys.path.insert(0, str(SCRIPT.parent))
    import register_per_acquisition

    return register_per_acquisition


def _base_item() -> dict:
    """A per-tile base item (as build_s1_rtc_stac_item would emit) to clone per acquisition."""
    return {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "s1-rtc-31TCH",
        "collection": "sentinel-1-grd-rtc-staging",
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
        },
        "assets": {
            "vh": {"href": "https://gw/.../s1-rtc-31TCH.zarr/descending", "roles": ["data"]},
            "zarr-store": {"href": "https://gw/.../s1-rtc-31TCH.zarr", "roles": ["data"]},
        },
        "links": [],
    }


# two acquisitions on the cube's time axis (ns since epoch)
_T0 = int(dt.datetime(2026, 6, 5, 6, 9, 7, tzinfo=dt.UTC).timestamp() * 1e9)
_T1 = int(dt.datetime(2026, 6, 7, 5, 52, 48, tzinfo=dt.UTC).timestamp() * 1e9)
RASTER = "https://api.explorer.eopf.copernicus.eu/raster"
ACQ = "sentinel-1-grd-rtc-acquisitions"


# --- acquisition_id ----------------------------------------------------------


def test_acquisition_id_format():
    m = _mod()
    when = dt.datetime(2026, 6, 7, 5, 52, 48, tzinfo=dt.UTC)
    assert m.acquisition_id("31TCH", when) == "s1-rtc-31TCH-20260607t055248"


# --- sel_time_tilejson -------------------------------------------------------


def test_sel_time_tilejson_carries_sel_and_item():
    m = _mod()
    when = dt.datetime(2026, 6, 5, 6, 9, 7, tzinfo=dt.UTC)
    url = m.sel_time_tilejson(RASTER, ACQ, "s1-rtc-31TCH-20260605t060907", when, "descending")
    assert ACQ in url and "s1-rtc-31TCH-20260605t060907" in url
    assert "sel=" in url and "time%3Dnearest" in url  # url-encoded "time=nearest"
    assert "tilejson.json" in url


# --- per_acquisition_items ---------------------------------------------------


def test_per_acquisition_items_one_per_time():
    m = _mod()
    items = m.per_acquisition_items(
        _base_item(),
        [_T0, _T1],
        tile_id="31TCH",
        orbit="descending",
        collection=ACQ,
        raster_api=RASTER,
    )
    assert [i["id"] for i in items] == [
        "s1-rtc-31TCH-20260605t060907",
        "s1-rtc-31TCH-20260607t055248",
    ]


def test_per_acquisition_items_single_datetime_no_range():
    m = _mod()
    items = m.per_acquisition_items(
        _base_item(), [_T0], tile_id="31TCH", orbit="descending", collection=ACQ, raster_api=RASTER
    )
    props = items[0]["properties"]
    assert props["datetime"] == "2026-06-05T06:09:07+00:00"
    assert "start_datetime" not in props and "end_datetime" not in props
    assert props["sar:product_type"] == "GRD"  # base properties preserved


def test_per_acquisition_items_collection_and_sel_link():
    m = _mod()
    items = m.per_acquisition_items(
        _base_item(), [_T0], tile_id="31TCH", orbit="descending", collection=ACQ, raster_api=RASTER
    )
    item = items[0]
    assert item["collection"] == ACQ
    tilejson = [link for link in item["links"] if link["rel"] == "tilejson"]
    assert len(tilejson) == 1
    assert "sel=" in tilejson[0]["href"] and item["id"] in tilejson[0]["href"]


def test_per_acquisition_items_does_not_mutate_base():
    m = _mod()
    base = _base_item()
    m.per_acquisition_items(
        base, [_T0, _T1], tile_id="31TCH", orbit="descending", collection=ACQ, raster_api=RASTER
    )
    assert base["id"] == "s1-rtc-31TCH"  # base untouched
    assert "start_datetime" in base["properties"]
