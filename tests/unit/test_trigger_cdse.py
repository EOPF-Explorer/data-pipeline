"""Unit tests for scripts/trigger_cdse.py (Phase-6 Task 6 — data-driven CDSE trigger)."""

from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from shapely.geometry import box, mapping

scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

from trigger_cdse import (  # noqa: E402
    MIN_TILE_COVERAGE,
    build_parser,
    collapse_same_pass,
    drop_low_coverage,
    expected_item_id,
    is_enabled_platform,
    item_exists,
    platform_of,
    query_products,
    select_new_products,
    tile_coverage,
    tile_polygon,
)

_MOD = "trigger_cdse"


# --- platform parse + allowlist (T6.1) ------------------------------------------------------


@pytest.mark.parametrize(
    ("product_id", "expected"),
    [
        ("S1A_IW_GRDH_1SDV_20260607T055248", "S1A"),
        ("S1C_IW_GRDH_1SDV_20260605T060907", "S1C"),
        ("S1D_IW_GRDH_1SDV_20260607T055248", "S1D"),
    ],
)
def test_platform_of_reads_id_prefix(product_id: str, expected: str) -> None:
    assert platform_of(product_id) == expected


def test_platform_of_malformed_id_does_not_crash() -> None:
    """Adversarial: an id with no underscore yields the upper-cased token (and is not enabled)."""
    assert platform_of("garbage") == "GARBAGE"
    assert is_enabled_platform(platform_of("garbage")) is False


@pytest.mark.parametrize("platform", ["S1A", "S1C"])
def test_is_enabled_platform_allows_s1a_s1c(platform: str) -> None:
    assert is_enabled_platform(platform) is True


@pytest.mark.parametrize("platform", ["S1D", "S1B", "ZZZ"])
def test_is_enabled_platform_rejects_others(platform: str) -> None:
    assert is_enabled_platform(platform) is False


# --- query_products (T6.2) ------------------------------------------------------------------


def _item(
    item_id: str,
    when: dt.datetime | None,
    properties: dict | None = None,
    geometry: dict | None = None,
) -> MagicMock:
    item = MagicMock()
    item.id = item_id
    item.datetime = when
    item.properties = properties or {}
    item.geometry = geometry  # explicit: an unset MagicMock attr would break shape()
    return item


def _patched_client(items: list[MagicMock]) -> MagicMock:
    client = MagicMock()
    client.search.return_value.items.return_value = iter(items)
    return client


def _covering_geometry(tile_id: str) -> dict:
    """A GeoJSON polygon that fully covers ``tile_id`` (the tile bbox grown 1° each way)."""
    minx, miny, maxx, maxy = tile_polygon(tile_id).bounds
    return mapping(box(minx - 1, miny - 1, maxx + 1, maxy + 1))


def test_query_products_keeps_full_datetime_platform_and_coverage() -> None:
    """Unlike query_cdse (date-only), the trigger keeps the per-second datetime, platform + coverage."""
    full = _covering_geometry("31TCH")
    items = [
        _item("S1A_IW_GRDH_A", dt.datetime(2026, 6, 7, 5, 52, 48, tzinfo=dt.UTC), geometry=full),
        _item("S1D_IW_GRDH_B", dt.datetime(2026, 6, 7, 5, 53, 0, tzinfo=dt.UTC), geometry=full),
    ]
    with patch(f"{_MOD}.Client.open", return_value=_patched_client(items)):
        products = query_products(
            "https://cdse/stac", [0.5, 42.4, 1.8, 43.3], "descending", 7, tile_polygon("31TCH")
        )
    assert [(p["product_id"], p["platform"], p["datetime"], p["date"]) for p in products] == [
        ("S1A_IW_GRDH_A", "S1A", "2026-06-07T05:52:48+00:00", "2026-06-07"),
        ("S1D_IW_GRDH_B", "S1D", "2026-06-07T05:53:00+00:00", "2026-06-07"),
    ]
    assert all(p["coverage"] > 0.99 for p in products)


def test_query_products_applies_orbit_and_collection_filter() -> None:
    """The CDSE search is scoped to sentinel-1-grd, the bbox, and the orbit-state filter."""
    client = _patched_client([])
    with patch(f"{_MOD}.Client.open", return_value=client):
        query_products(
            "https://cdse/stac", [0.5, 42.4, 1.8, 43.3], "descending", 7, tile_polygon("31TCH")
        )
    kwargs = client.search.call_args.kwargs
    assert kwargs["collections"] == ["sentinel-1-grd"]
    assert kwargs["bbox"] == [0.5, 42.4, 1.8, 43.3]
    assert kwargs["query"] == {"sat:orbit_state": {"eq": "descending"}}


def test_query_products_skips_item_without_datetime() -> None:
    """Adversarial: an item with no datetime and no start_datetime is skipped, not crashed on."""
    items = [
        _item("S1A_good", dt.datetime(2026, 6, 7, 5, 52, 48, tzinfo=dt.UTC)),
        _item("S1A_bad", None, properties={}),
    ]
    with patch(f"{_MOD}.Client.open", return_value=_patched_client(items)):
        products = query_products(
            "https://cdse/stac", [0.5, 42.4, 1.8, 43.3], "descending", 7, tile_polygon("31TCH")
        )
    assert [p["product_id"] for p in products] == ["S1A_good"]


# --- coverage gate (empty-tile fix) ---------------------------------------------------------


def test_tile_coverage_full_when_footprint_covers_tile() -> None:
    poly = tile_polygon("31TCH")
    assert tile_coverage(_covering_geometry("31TCH"), poly) > 0.99


def test_tile_coverage_near_zero_for_corner_graze() -> None:
    """The 30TWN case: a swath clipping a tile corner scores far below the 20% gate."""
    poly = tile_polygon("31TCH")
    minx, miny, _maxx, _maxy = poly.bounds
    sliver = mapping(box(minx - 1, miny - 1, minx + 0.02, miny + 0.02))
    assert tile_coverage(sliver, poly) < 0.05


def test_tile_coverage_zero_when_disjoint() -> None:
    poly = tile_polygon("31TCH")
    assert tile_coverage(mapping(box(100, 0, 101, 1)), poly) == 0.0


def test_tile_coverage_zero_when_geometry_missing() -> None:
    assert tile_coverage(None, tile_polygon("31TCH")) == 0.0


def test_tile_polygon_rejects_bad_tile_id() -> None:
    with pytest.raises(ValueError, match="invalid MGRS tile id"):
        tile_polygon("NOPE")


def test_drop_low_coverage_filters_below_threshold() -> None:
    products = [
        {"product_id": "keep", "coverage": 0.8},
        {"product_id": "graze", "coverage": 0.015},
        {"product_id": "edge", "coverage": 0.20},  # exactly at the gate is kept
    ]
    kept = drop_low_coverage(products, 0.20)
    assert [p["product_id"] for p in kept] == ["keep", "edge"]


# --- item-exists dedup (T6.3) ---------------------------------------------------------------


def test_expected_item_id_matches_per_acquisition_convention() -> None:
    """The dedup key is exactly the id register_per_acquisition emits (s1-rtc-{tile}-{stamp})."""
    when = dt.datetime(2026, 6, 7, 5, 52, 48, tzinfo=dt.UTC)
    assert expected_item_id("31TCH", when) == "s1-rtc-31TCH-20260607t055248"


def test_item_exists_true_when_search_yields_item() -> None:
    item = _item("s1-rtc-31TCH-20260607t055248", dt.datetime(2026, 6, 7, tzinfo=dt.UTC))
    client = _patched_client([item])
    with patch(f"{_MOD}.Client.open", return_value=client):
        exists = item_exists("https://stac", "sentinel-1-grd-rtc-acquisitions", item.id)
    assert exists is True
    kwargs = client.search.call_args.kwargs
    assert kwargs["collections"] == ["sentinel-1-grd-rtc-acquisitions"]
    assert kwargs["ids"] == ["s1-rtc-31TCH-20260607t055248"]


def test_item_exists_false_when_search_empty() -> None:
    with patch(f"{_MOD}.Client.open", return_value=_patched_client([])):
        assert item_exists("https://stac", "sentinel-1-grd-rtc-acquisitions", "nope") is False


# --- select_new_products + main wiring (T6.4) -----------------------------------------------


def _args(**overrides: object) -> object:
    ns = build_parser().parse_args(
        [
            "--tiles", "31TCH",
            "--orbit-direction", "descending",
            "--lookback-days", "7",
            "--stac-api-url", "https://eopf-target/stac",
        ]
    )  # fmt: skip
    for key, value in overrides.items():
        setattr(ns, key, value)
    return ns


def _product(product_id: str, platform: str, when: str, coverage: float = 1.0) -> dict[str, str]:
    return {
        "product_id": product_id,
        "platform": platform,
        "datetime": when,
        "date": when[:10],
        "coverage": coverage,
    }


# --- collapse_same_pass (A3 — adjacent frames of one pass -> one product) --------------------


def test_collapse_same_pass_keeps_earliest_frame_per_date_platform() -> None:
    """Two adjacent frames of one pass (same date+platform) collapse to the earliest-datetime frame."""
    frames = [
        _product("S1C_f2", "S1C", "2026-06-06T06:00:12+00:00"),
        _product("S1C_f1", "S1C", "2026-06-06T05:59:47+00:00"),
    ]
    assert [p["product_id"] for p in collapse_same_pass(frames)] == ["S1C_f1"]


def test_collapse_same_pass_keeps_distinct_dates_and_platforms() -> None:
    """Distinct acquisitions (different date OR platform) are NOT collapsed."""
    products = [
        _product("S1C_a", "S1C", "2026-06-06T06:00:12+00:00"),
        _product("S1A_a", "S1A", "2026-06-06T06:00:12+00:00"),  # same date, different platform
        _product("S1C_b", "S1C", "2026-06-07T06:00:00+00:00"),  # different date
    ]
    assert len(collapse_same_pass(products)) == 3


def test_select_collapses_same_pass_frames_to_one() -> None:
    """End-to-end: 2 same-pass S1C frames -> the trigger emits ONE product (the earliest frame)."""
    products = [
        _product("S1C_f2", "S1C", "2026-06-06T06:00:12+00:00"),
        _product("S1C_f1", "S1C", "2026-06-06T05:59:47+00:00"),
    ]
    with (
        patch(f"{_MOD}.query_products", return_value=products),
        patch(f"{_MOD}.item_exists", return_value=False),
    ):
        new = select_new_products(_args())
    assert len(new) == 1
    assert new[0]["product_id"] == "S1C_f1"
    assert new[0]["date"] == "2026-06-06"


def test_select_emits_only_unregistered_products() -> None:
    products = [
        _product("S1A_new", "S1A", "2026-06-07T05:52:48+00:00"),
        _product("S1A_old", "S1A", "2026-06-05T06:09:07+00:00"),
    ]
    with (
        patch(f"{_MOD}.query_products", return_value=products),
        patch(f"{_MOD}.item_exists", side_effect=[False, True]) as exists,
    ):
        new = select_new_products(_args())
    assert [p["product_id"] for p in new] == ["S1A_new"]
    # dedup key is the per-acquisition item id derived from the acquisition datetime
    assert exists.call_args_list[0].args[2] == "s1-rtc-31TCH-20260607t055248"


def test_select_empty_query_returns_empty() -> None:
    with patch(f"{_MOD}.query_products", return_value=[]):
        assert select_new_products(_args()) == []


def test_select_all_registered_returns_empty_idempotent() -> None:
    products = [_product("S1A_old", "S1A", "2026-06-05T06:09:07+00:00")]
    with (
        patch(f"{_MOD}.query_products", return_value=products),
        patch(f"{_MOD}.item_exists", return_value=True),
    ):
        assert select_new_products(_args()) == []


def test_select_emits_s1tiling_window_date_minus_plus_one() -> None:
    """Each emitted product carries the s1tiling window date_start=date-1 / date_end=date+1, so the
    CronWorkflow fans out child pipelines with no per-product date-math step (C0)."""
    products = [_product("S1A_new", "S1A", "2026-06-05T06:08:42+00:00")]
    with (
        patch(f"{_MOD}.query_products", return_value=products),
        patch(f"{_MOD}.item_exists", return_value=False),
    ):
        new = select_new_products(_args())
    assert new[0]["date"] == "2026-06-05"
    assert new[0]["date_start"] == "2026-06-04"
    assert new[0]["date_end"] == "2026-06-06"


def test_select_window_crosses_month_boundary() -> None:
    """Adversarial: date-1/date+1 use real date arithmetic, not string slicing (month rollover)."""
    products = [_product("S1A_eom", "S1A", "2026-05-31T06:00:32+00:00")]
    with (
        patch(f"{_MOD}.query_products", return_value=products),
        patch(f"{_MOD}.item_exists", return_value=False),
    ):
        new = select_new_products(_args())
    assert new[0]["date_start"] == "2026-05-30"
    assert new[0]["date_end"] == "2026-06-01"


def test_select_skips_s1d_at_query_time_no_dedup_call() -> None:
    """S1D is dropped before the dedup check -> never emitted, never a child Workflow."""
    products = [
        _product("S1D_skip", "S1D", "2026-06-07T05:52:48+00:00"),
        _product("S1C_keep", "S1C", "2026-06-07T17:10:00+00:00"),
    ]
    with (
        patch(f"{_MOD}.query_products", return_value=products),
        patch(f"{_MOD}.item_exists", return_value=False) as exists,
    ):
        new = select_new_products(_args())
    assert [p["product_id"] for p in new] == ["S1C_keep"]
    # dedup is only consulted for the enabled platform, not for the skipped S1D
    assert exists.call_count == 1


def test_select_carries_tile_and_orbit_per_record() -> None:
    products = [_product("S1A_new", "S1A", "2026-06-07T05:52:48+00:00")]
    with (
        patch(f"{_MOD}.query_products", return_value=products),
        patch(f"{_MOD}.item_exists", return_value=False),
    ):
        new = select_new_products(_args())
    assert new[0]["tile"] == "31TCH"
    assert new[0]["orbit"] == "descending"
    assert new[0]["platform"] == "S1A"


def test_select_drops_low_coverage_product() -> None:
    """Regression (30TWN): a product that only grazes the tile (1.5% coverage) is NOT emitted, while a
    full-coverage product on another date is. Fails on pre-gate code (both would be emitted)."""
    products = [
        _product("S1A_full", "S1A", "2026-06-07T05:52:48+00:00", coverage=1.0),
        _product("S1A_graze", "S1A", "2026-06-06T17:55:31+00:00", coverage=0.015),
    ]
    with (
        patch(f"{_MOD}.query_products", return_value=products),
        patch(f"{_MOD}.item_exists", return_value=False),
    ):
        new = select_new_products(_args())
    assert [p["product_id"] for p in new] == ["S1A_full"]


def test_select_min_coverage_override_changes_gate() -> None:
    """A higher --min-coverage drops a partially-covered product the default would keep."""
    products = [_product("S1A_partial", "S1A", "2026-06-07T05:52:48+00:00", coverage=0.30)]
    with (
        patch(f"{_MOD}.query_products", return_value=products),
        patch(f"{_MOD}.item_exists", return_value=False),
    ):
        assert select_new_products(_args(min_coverage=0.5)) == []  # 0.30 < 0.5 -> dropped
        assert len(select_new_products(_args(min_coverage=0.2))) == 1  # 0.30 >= 0.2 -> kept


def test_select_iterates_multiple_tiles() -> None:
    products = [_product("S1A_new", "S1A", "2026-06-07T05:52:48+00:00")]
    with (
        patch(f"{_MOD}.query_products", return_value=products) as q,
        patch(f"{_MOD}.item_exists", return_value=False),
    ):
        new = select_new_products(_args(tiles="31TCH,30TXM"))
    assert q.call_count == 2
    assert {p["tile"] for p in new} == {"31TCH", "30TXM"}


# --- asc+desc coverage (T7) -----------------------------------------------------------------


def test_parser_orbit_direction_accepts_both() -> None:
    """`--orbit-direction both` is a valid choice (asc+desc AOI coverage, T7)."""
    ns = build_parser().parse_args(
        ["--tiles", "31TCH", "--orbit-direction", "both",
         "--lookback-days", "7", "--stac-api-url", "https://eopf/stac"]
    )  # fmt: skip
    assert ns.orbit_direction == "both"


def test_select_orbit_both_queries_both_directions_and_tags_each() -> None:
    """`both` runs discover for ascending AND descending, tagging each product with its own orbit."""

    def _q(_stac: str, _bbox: list, orbit: str, _lookback: int, _poly: object) -> list[dict]:
        # asc/desc passes image a tile at different times -> distinct datetimes
        when = "2026-06-07T05:52:48+00:00" if orbit == "descending" else "2026-06-07T17:10:00+00:00"
        return [_product(f"S1A_{orbit}", "S1A", when)]

    with (
        patch(f"{_MOD}.query_products", side_effect=_q) as q,
        patch(f"{_MOD}.item_exists", return_value=False),
    ):
        new = select_new_products(_args(orbit_direction="both"))
    assert {call.args[2] for call in q.call_args_list} == {"ascending", "descending"}
    assert {p["orbit"] for p in new} == {"ascending", "descending"}
    assert len(new) == 2


def test_select_single_orbit_unchanged() -> None:
    """A single direction still queries once and tags that direction (no behaviour change)."""
    with (
        patch(
            f"{_MOD}.query_products",
            return_value=[_product("S1A", "S1A", "2026-06-07T05:52:48+00:00")],
        ) as q,
        patch(f"{_MOD}.item_exists", return_value=False),
    ):
        new = select_new_products(_args(orbit_direction="ascending"))
    assert q.call_count == 1 and q.call_args.args[2] == "ascending"
    assert [p["orbit"] for p in new] == ["ascending"]


def test_select_queries_cdse_catalogue_not_target_stac() -> None:
    """The acquisition query hits the CDSE source catalogue, not the EOPF target STAC."""
    from trigger_cdse import CDSE_STAC_URL

    with (
        patch(f"{_MOD}.query_products", return_value=[]) as q,
        patch(f"{_MOD}.item_exists", return_value=False),
    ):
        select_new_products(_args(stac_api_url="https://eopf-target/stac"))
    assert q.call_args.args[0] == CDSE_STAC_URL


def test_parser_acq_collection_defaults_to_tests() -> None:
    # env-split: code default is the test env; the cron passes --acq-collection …-staging
    assert _args().acq_collection == "sentinel-1-grd-rtc-acquisitions-tests"


def test_parser_min_coverage_defaults_and_overrides() -> None:
    assert _args().min_coverage == MIN_TILE_COVERAGE
    ns = build_parser().parse_args(
        ["--tiles", "31TCH", "--orbit-direction", "ascending",
         "--lookback-days", "7", "--stac-api-url", "https://eopf/stac", "--min-coverage", "0.5"]
    )  # fmt: skip
    assert ns.min_coverage == 0.5


def test_main_writes_json_array_to_output(tmp_path: Path) -> None:
    out = tmp_path / "new.json"
    from trigger_cdse import main

    argv = [
        "trigger.py", "--tiles", "31TCH", "--orbit-direction", "descending",
        "--lookback-days", "7", "--stac-api-url", "https://eopf-target/stac",
        "--output", str(out),
    ]  # fmt: skip
    products = [_product("S1A_new", "S1A", "2026-06-07T05:52:48+00:00")]
    with (
        patch.object(sys, "argv", argv),
        patch(f"{_MOD}.query_products", return_value=products),
        patch(f"{_MOD}.item_exists", return_value=False),
    ):
        main()
    written = json.loads(out.read_text())
    assert [p["product_id"] for p in written] == ["S1A_new"]
    assert written[0]["tile"] == "31TCH"
