"""Unit tests for register_v1_s1_rtc.py -- register."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pystac
import pytest

scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

from register_v1_s1_rtc import (  # noqa: E402
    acquisitions_collection_href,
    acquisitions_collection_of,
    add_acquisition_links,
    register,
)

# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

_STORE = "s3://bucket/prefix/s1-grd-rtc-31TCH.zarr"
_COLLECTION = "sentinel-1-grd-rtc-staging"


def _make_item() -> pystac.Item:
    item = pystac.Item(
        id="s1-rtc-31TCH",
        geometry={
            "type": "Polygon",
            "coordinates": [[[0, 44], [2, 44], [2, 46], [0, 46], [0, 44]]],
        },
        bbox=[0.0, 44.0, 2.0, 46.0],
        datetime=None,
        properties={
            "start_datetime": "2023-01-15T06:12:34+00:00",
            "end_datetime": "2023-01-27T06:12:35+00:00",
            "sar:instrument_mode": "IW",
            "sar:frequency_band": "C",
            "sar:center_frequency": 5.405,
            "sar:polarizations": ["VV", "VH"],
            "sar:product_type": "GRD",
            "sat:orbit_state": "ascending",
            "proj:code": "EPSG:32631",
        },
        stac_extensions=[
            "https://stac-extensions.github.io/sar/v1.0.0/schema.json",
            "https://stac-extensions.github.io/sat/v1.0.0/schema.json",
            "https://stac-extensions.github.io/projection/v2.0.0/schema.json",
        ],
        collection=_COLLECTION,
    )
    item.add_asset(
        "zarr-store",
        pystac.Asset(href=_STORE, media_type="application/vnd.zarr; version=3", roles=["data"]),
    )
    item.add_asset(
        "vv",
        pystac.Asset(
            href=f"{_STORE}/ascending/r10m/vv",
            media_type="application/vnd.zarr; version=3",
            roles=["data"],
        ),
    )
    item.add_asset(
        "vh",
        pystac.Asset(
            href=f"{_STORE}/ascending/r10m/vh",
            media_type="application/vnd.zarr; version=3",
            roles=["data"],
        ),
    )
    return item


_MOD = "register_v1_s1_rtc"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_upserts_item_with_correct_id() -> None:
    """upsert_item must be called once with item id 's1-rtc-31TCH'."""
    with (
        patch(f"{_MOD}.build_s1_rtc_stac_item", return_value=_make_item()),
        patch(f"{_MOD}.warm_thumbnail_cache"),
        patch(f"{_MOD}.slice_coverages", return_value=[]),  # no store read in these unit tests
        patch(f"{_MOD}.upsert_item") as mock_upsert,
        patch(f"{_MOD}.stac_auth.open_client"),
        patch(f"{_MOD}.add_acquisition_links"),  # no network in register() unit tests
    ):
        result = register(
            _STORE,
            _COLLECTION,
            "https://stac.example.com",
            "https://raster.example.com",
            "https://s3.example.com",
        )

    assert result == 0
    mock_upsert.assert_called_once()
    called_item = (
        mock_upsert.call_args[1]["item"]
        if mock_upsert.call_args[1]
        else mock_upsert.call_args[0][2]
    )
    assert called_item.id == "s1-rtc-31TCH"


def test_render_rescale_propagates_to_links() -> None:
    """The cube renders at 0.0,0.2 (now emitted by the data-model builder; the old in-pipeline
    apply_s1_rtc_rescale override is gone), and the derived xyz/tilejson links inherit it."""
    item = _make_item()
    item.properties["renders"] = {
        "rgb": {
            "expression": "/ascending:vv;/ascending:vh;(/ascending:vv)/(/ascending:vh)",
            "rescale": [[0.0, 0.2]],
            "bidx": [1],
        }
    }
    item.stac_extensions.append("https://stac-extensions.github.io/render/v1.0.0/schema.json")
    with (
        patch(f"{_MOD}.build_s1_rtc_stac_item", return_value=item),
        patch(f"{_MOD}.warm_thumbnail_cache"),
        patch(f"{_MOD}.slice_coverages", return_value=[]),
        patch(f"{_MOD}.upsert_item") as mock_upsert,
        patch(f"{_MOD}.stac_auth.open_client"),
        patch(f"{_MOD}.add_acquisition_links"),  # no network in register() unit tests
    ):
        result = register(
            _STORE,
            _COLLECTION,
            "https://stac.example.com",
            "https://raster.example.com",
            "https://s3.example.com",
        )

    assert result == 0
    called_item = mock_upsert.call_args[0][2]
    assert called_item.properties["renders"]["rgb"]["rescale"] == [[0.0, 0.2]]
    hrefs = " ".join(link.href for link in called_item.links if link.rel in ("xyz", "tilejson"))
    assert "rescale=0.0%2C0.2" in hrefs and "rescale=0.0%2C0.1" not in hrefs


def test_visualization_links_called() -> None:
    """add_visualization_links must be called once with correct raster URL and collection."""
    with (
        patch(f"{_MOD}.build_s1_rtc_stac_item", return_value=_make_item()),
        patch(f"{_MOD}.warm_thumbnail_cache"),
        patch(f"{_MOD}.slice_coverages", return_value=[]),  # no store read in these unit tests
        patch(f"{_MOD}.upsert_item"),
        patch(f"{_MOD}.stac_auth.open_client"),
        patch(f"{_MOD}.add_acquisition_links"),  # no network in register() unit tests
        patch(f"{_MOD}.add_visualization_links") as mock_viz,
    ):
        register(
            _STORE,
            _COLLECTION,
            "https://stac.example.com",
            "https://raster.example.com",
            "https://s3.example.com",
        )

    mock_viz.assert_called_once()
    _, raster_url, coll = mock_viz.call_args[0]
    assert raster_url == "https://raster.example.com"
    assert coll == _COLLECTION


def test_s2_helpers_not_called() -> None:
    """S2-specific helpers must not be imported or present in the module namespace."""
    import register_v1_s1_rtc

    assert not hasattr(
        register_v1_s1_rtc, "consolidate_reflectance_assets"
    ), "consolidate_reflectance_assets must not be imported into register_v1_s1_rtc"
    assert not hasattr(
        register_v1_s1_rtc, "fix_zarr_asset_media_types"
    ), "fix_zarr_asset_media_types must not be imported into register_v1_s1_rtc"


def test_exits_nonzero_on_bad_store() -> None:
    """When build_s1_rtc_stac_item raises ValueError, register returns non-zero."""
    with (
        patch(f"{_MOD}.build_s1_rtc_stac_item", side_effect=ValueError("no acquisitions")),
        patch(f"{_MOD}.upsert_item") as mock_upsert,
    ):
        result = register(
            _STORE,
            _COLLECTION,
            "https://stac.example.com",
            "https://raster.example.com",
            "https://s3.example.com",
        )

    assert result != 0
    mock_upsert.assert_not_called()


def test_env_mismatch_rejected_before_build() -> None:
    """A staging collection paired with a tests-bucket store is the 32TLR footgun on the
    standalone register path -> reject before building or upserting anything."""
    bad_store = (
        "s3://esa-zarr-sentinel-explorer-tests/sentinel-1-grd-rtc-staging/s1-grd-rtc-31TCH.zarr"
    )
    with (
        patch(f"{_MOD}.build_s1_rtc_stac_item") as mock_build,
        patch(f"{_MOD}.upsert_item") as mock_upsert,
        pytest.raises(ValueError, match="mismatch"),
    ):
        register(
            bad_store,
            "sentinel-1-grd-rtc-staging",
            "https://stac.example.com",
            "https://raster.example.com",
            "https://s3.example.com",
        )

    mock_build.assert_not_called()
    mock_upsert.assert_not_called()


def test_matched_env_store_allowed() -> None:
    """A staging-bucket store + staging collection passes the guard and registers normally."""
    good_store = (
        "s3://esa-zarr-sentinel-explorer-s1-l1grd-staging/"
        "sentinel-1-grd-rtc-staging/s1-grd-rtc-31TCH.zarr"
    )
    with (
        patch(f"{_MOD}.build_s1_rtc_stac_item", return_value=_make_item()),
        patch(f"{_MOD}.warm_thumbnail_cache"),
        patch(f"{_MOD}.slice_coverages", return_value=[]),  # no store read in these unit tests
        patch(f"{_MOD}.upsert_item") as mock_upsert,
        patch(f"{_MOD}.stac_auth.open_client"),
        patch(f"{_MOD}.add_acquisition_links"),  # no network in register() unit tests
    ):
        result = register(
            good_store,
            "sentinel-1-grd-rtc-staging",
            "https://stac.example.com",
            "https://raster.example.com",
            "https://s3.example.com",
        )

    assert result == 0
    mock_upsert.assert_called_once()


# ---------------------------------------------------------------------------
# Cube → per-acquisition cross-link
# ---------------------------------------------------------------------------


def test_acquisitions_collection_derivation() -> None:
    assert (
        acquisitions_collection_of("sentinel-1-grd-rtc-staging")
        == "sentinel-1-grd-rtc-acquisitions-staging"
    )
    assert (
        acquisitions_collection_of("sentinel-1-grd-rtc-tests")
        == "sentinel-1-grd-rtc-acquisitions-tests"
    )


def test_acquisitions_collection_href() -> None:
    href = acquisitions_collection_href(
        "https://stac.example.com/", "sentinel-1-grd-rtc-acquisitions-staging"
    )
    # the browsable collection URL (NOT a /search?filter URL, which blanks in STAC Browser)
    assert href == "https://stac.example.com/collections/sentinel-1-grd-rtc-acquisitions-staging"


def test_register_adds_related_acquisitions_link() -> None:
    """The cube item gets one `related` link to the browsable per-acquisition collection (derived)."""
    with (
        patch(f"{_MOD}.build_s1_rtc_stac_item", return_value=_make_item()),
        patch(f"{_MOD}.warm_thumbnail_cache"),
        patch(f"{_MOD}.slice_coverages", return_value=[]),
        patch(f"{_MOD}.upsert_item") as mock_upsert,
        patch(f"{_MOD}.stac_auth.open_client"),
        patch(f"{_MOD}.add_acquisition_links"),  # no network in register() unit tests
    ):
        register(
            _STORE,
            _COLLECTION,
            "https://stac.example.com",
            "https://raster.example.com",
            "https://s3.example.com",
        )

    item = mock_upsert.call_args[0][2]
    related = [lk for lk in item.links if lk.rel == "related"]
    assert len(related) == 1
    href = related[0].href
    assert href.endswith("/collections/sentinel-1-grd-rtc-acquisitions-staging")  # derived coll
    assert "/search?" not in href  # not a search URL (renders blank in STAC Browser)


def test_register_acquisitions_collection_override() -> None:
    """--acquisitions-collection overrides the derived sibling name."""
    with (
        patch(f"{_MOD}.build_s1_rtc_stac_item", return_value=_make_item()),
        patch(f"{_MOD}.warm_thumbnail_cache"),
        patch(f"{_MOD}.slice_coverages", return_value=[]),
        patch(f"{_MOD}.upsert_item") as mock_upsert,
        patch(f"{_MOD}.stac_auth.open_client"),
        patch(f"{_MOD}.add_acquisition_links"),  # no network in register() unit tests
    ):
        register(
            _STORE,
            _COLLECTION,
            "https://stac.example.com",
            "https://raster.example.com",
            "https://s3.example.com",
            acquisitions_collection="custom-acq-coll",
        )

    item = mock_upsert.call_args[0][2]
    related = next(lk for lk in item.links if lk.rel == "related")
    assert related.href.endswith("/collections/custom-acq-coll")


def test_add_acquisition_links_enumerates_sorted() -> None:
    """add_acquisition_links lists the tile's acquisitions as related links, sorted by datetime."""
    import datetime as dt

    feats = [
        {
            "id": "s1-rtc-31TCH-20260605t060842",
            "properties": {"datetime": "2026-06-05T06:08:42Z", "sat:orbit_state": "descending"},
        },
        {
            "id": "s1-rtc-31TCH-20260604t061555",
            "properties": {"datetime": "2026-06-04T06:15:55Z", "sat:orbit_state": "ascending"},
        },
    ]

    class _Resp:
        def json(self):
            return {"features": feats}

    class _Http:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, *a, **k):
            return _Resp()

    item = pystac.Item(
        id="s1-rtc-31TCH",
        geometry=None,
        bbox=None,
        datetime=dt.datetime(2026, 6, 7, tzinfo=dt.UTC),
        properties={},
    )
    with patch(f"{_MOD}.httpx.Client", lambda *a, **k: _Http()):
        add_acquisition_links(
            item, "https://stac.example.com", "sentinel-1-grd-rtc-acquisitions-staging"
        )

    related = [lk for lk in item.links if lk.rel == "related"]
    assert len(related) == 2
    # sorted by datetime (earliest first), titled by date + orbit, href → the acquisition item
    assert "20260604t061555" in related[0].href and related[0].title.startswith(
        "Acquisition 2026-06-04 06:15Z"
    )
    assert "20260605t060842" in related[1].href
    assert related[0].href.startswith(
        "https://stac.example.com/collections/sentinel-1-grd-rtc-acquisitions-staging/items/"
    )


# ---------------------------------------------------------------------------
# Best-recent preview slice: _pin_preview_to_best_recent (reorient + sel_time)
# ---------------------------------------------------------------------------

import datetime as _dt  # noqa: E402

import numpy as _np  # noqa: E402
import zarr as _zarr  # noqa: E402
from register_v1_s1_rtc import _pin_preview_to_best_recent  # noqa: E402


def _ns(day: int) -> int:
    return int(_dt.datetime(2026, 6, day, 6, 0, tzinfo=_dt.UTC).timestamp() * 1e9)


def _write_orbit(root, orbit, masks, days):
    lvl = root.create_group(orbit).create_group("r720m")
    n, (y, x) = len(masks), masks[0].shape
    lvl.create_array("border_mask", shape=(n, y, x), dtype="uint8", fill_value=0)[:] = _np.stack(
        masks
    )
    lvl.create_array("time", shape=(n,), dtype="int64")[:] = _np.array(
        [_ns(d) for d in days], "int64"
    )


def _cube_item(store_href: str) -> pystac.Item:
    """A cube item as build_s1_rtc_stac_item emits: renders.rgb on the preferred (ascending) orbit."""
    expr = "/ascending:vv;/ascending:vh;(/ascending:vv)/(/ascending:vh)"
    item = pystac.Item(
        id="s1-rtc-31TCH",
        geometry=None,
        bbox=None,
        datetime=_dt.datetime(2026, 6, 7, tzinfo=_dt.UTC),
        properties={
            "sat:orbit_state": "ascending",
            "renders": {"rgb": {"expression": expr, "rescale": [[0.0, 0.1]], "bidx": [1]}},
        },
    )
    item.add_asset("zarr-store", pystac.Asset(href=store_href, roles=["data"]))
    for pol in ("vv", "vh"):
        item.add_asset(pol, pystac.Asset(href=f"{store_href}/ascending", roles=["data"]))
    return item


def test_pin_reorients_to_descending_best_recent(tmp_path) -> None:
    """Best-recent = most recent >80%: descending day-7 (0.81) beats ascending day-4 (1.0)."""
    store = str(tmp_path / "cube.zarr")
    root = _zarr.open_group(store, mode="w", zarr_format=3)
    full = _np.ones((4, 4))  # 1.0
    over80 = _np.zeros((4, 4))
    over80.flat[:13] = 1  # 13/16 = 0.8125 (> 0.80)
    _write_orbit(root, "ascending", [full], [4])
    _write_orbit(root, "descending", [over80], [7])

    new_item, sel_time = _pin_preview_to_best_recent(_cube_item(store), store)

    assert sel_time == "2026-06-07T06:00:00"
    assert new_item.properties["sat:orbit_state"] == "descending"
    assert "/descending:vv" in new_item.properties["renders"]["rgb"]["expression"]
    assert "/ascending" not in new_item.properties["renders"]["rgb"]["expression"]


def test_pin_noop_on_empty_cube(tmp_path) -> None:
    """No slices -> item unchanged, sel_time None (preview falls back to default)."""
    store = str(tmp_path / "empty.zarr")
    _zarr.open_group(store, mode="w", zarr_format=3)  # no orbit groups
    item = _cube_item(store)
    new_item, sel_time = _pin_preview_to_best_recent(item, store)
    assert sel_time is None
    assert new_item.properties["sat:orbit_state"] == "ascending"  # untouched


# ---------------------------------------------------------------------------
# eodash:rasterform on the cube item (issue #348)
# ---------------------------------------------------------------------------

from eodash_rasterform import RASTERFORM_BY_ORBIT  # noqa: E402
from eopf_geozarr.stac.s1_rtc import Slice  # noqa: E402


def _register_and_capture(item=None, coverages=None, coverages_error=None):  # noqa: ANN001
    """Run register() with the store read stubbed; return the item handed to upsert_item."""
    coverage_patch = (
        patch(f"{_MOD}.slice_coverages", side_effect=coverages_error)
        if coverages_error is not None
        else patch(f"{_MOD}.slice_coverages", return_value=coverages or [])
    )
    with (
        patch(f"{_MOD}.build_s1_rtc_stac_item", return_value=item or _make_item()),
        patch(f"{_MOD}.warm_thumbnail_cache"),
        coverage_patch,
        patch(f"{_MOD}.upsert_item") as mock_upsert,
        patch(f"{_MOD}.stac_auth.open_client"),
        patch(f"{_MOD}.add_acquisition_links"),
    ):
        assert (
            register(
                _STORE,
                _COLLECTION,
                "https://stac.example.com",
                "https://raster.example.com",
                "https://s3.example.com",
            )
            == 0
        )
    return (
        mock_upsert.call_args[1]["item"]
        if mock_upsert.call_args[1]
        else mock_upsert.call_args[0][2]
    )


def test_rasterform_follows_the_reoriented_orbit() -> None:
    """The form must track the preview slice's orbit, not the builder's original.

    _make_item() is ascending; the preview pins to a descending slice, so the form must flip
    with it. A setdefault-style assignment would leave the ascending form and lie.
    """
    import datetime as _dt2

    chosen = Slice(
        orbit="descending", dt=_dt2.datetime(2026, 6, 7, 6, 0, tzinfo=_dt2.UTC), coverage=0.9
    )
    with patch(f"{_MOD}.pick_slice", return_value=chosen):
        item = _register_and_capture(coverages=[chosen])
    assert item.properties["sat:orbit_state"] == "descending"
    assert item.properties["eodash:rasterform"] == RASTERFORM_BY_ORBIT["descending"]


def test_rasterform_set_even_when_coverage_read_fails() -> None:
    """A coverage-read failure skips reorientation — the form must still be emitted.

    _pin_preview_to_best_recent returns the item untouched on any store-read error, but the
    builder has already set sat:orbit_state on a single-orbit cube. Deriving the form inside
    _reorient_item_to_orbit would leave such an item live with an orbit and no form, and the
    cube is re-upserted on every ingest — so it would also silently wipe the backfill.
    """
    item = _register_and_capture(coverages_error=OSError("store unreadable"))
    assert item.properties["sat:orbit_state"] == "ascending"  # builder's, reorientation skipped
    assert item.properties["eodash:rasterform"] == RASTERFORM_BY_ORBIT["ascending"]


def test_no_rasterform_when_cube_has_no_orbit() -> None:
    """A dual-orbit cube with no pinned preview has no single render target -> emit nothing."""
    bare = _make_item()
    del bare.properties["sat:orbit_state"]
    item = _register_and_capture(item=bare)
    assert "eodash:rasterform" not in item.properties
