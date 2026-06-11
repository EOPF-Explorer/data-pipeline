"""Unit tests for scripts/trigger_cdse.py (Phase-6 Task 6 — data-driven CDSE trigger)."""

from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

from trigger_cdse import (  # noqa: E402
    build_parser,
    collapse_same_pass,
    expected_item_id,
    is_enabled_platform,
    item_exists,
    platform_of,
    query_products,
    select_new_products,
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


def _item(item_id: str, when: dt.datetime | None, properties: dict | None = None) -> MagicMock:
    item = MagicMock()
    item.id = item_id
    item.datetime = when
    item.properties = properties or {}
    return item


def _patched_client(items: list[MagicMock]) -> MagicMock:
    client = MagicMock()
    client.search.return_value.items.return_value = iter(items)
    return client


def test_query_products_keeps_full_datetime_and_platform() -> None:
    """Unlike query_cdse (date-only), the trigger keeps the per-second datetime + parsed platform."""
    items = [
        _item("S1A_IW_GRDH_A", dt.datetime(2026, 6, 7, 5, 52, 48, tzinfo=dt.UTC)),
        _item("S1D_IW_GRDH_B", dt.datetime(2026, 6, 7, 5, 53, 0, tzinfo=dt.UTC)),
    ]
    with patch(f"{_MOD}.Client.open", return_value=_patched_client(items)):
        products = query_products("https://cdse/stac", [0.5, 42.4, 1.8, 43.3], "descending", 7)
    assert products == [
        {
            "product_id": "S1A_IW_GRDH_A",
            "platform": "S1A",
            "datetime": "2026-06-07T05:52:48+00:00",
            "date": "2026-06-07",
        },
        {
            "product_id": "S1D_IW_GRDH_B",
            "platform": "S1D",
            "datetime": "2026-06-07T05:53:00+00:00",
            "date": "2026-06-07",
        },
    ]


def test_query_products_applies_orbit_and_collection_filter() -> None:
    """The CDSE search is scoped to sentinel-1-grd, the bbox, and the orbit-state filter."""
    client = _patched_client([])
    with patch(f"{_MOD}.Client.open", return_value=client):
        query_products("https://cdse/stac", [0.5, 42.4, 1.8, 43.3], "descending", 7)
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
        products = query_products("https://cdse/stac", [0.5, 42.4, 1.8, 43.3], "descending", 7)
    assert [p["product_id"] for p in products] == ["S1A_good"]


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


def _product(product_id: str, platform: str, when: str) -> dict[str, str]:
    return {
        "product_id": product_id,
        "platform": platform,
        "datetime": when,
        "date": when[:10],
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


def test_select_iterates_multiple_tiles() -> None:
    products = [_product("S1A_new", "S1A", "2026-06-07T05:52:48+00:00")]
    with (
        patch(f"{_MOD}.query_products", return_value=products) as q,
        patch(f"{_MOD}.item_exists", return_value=False),
    ):
        new = select_new_products(_args(tiles="31TCH,30TXM"))
    assert q.call_count == 2
    assert {p["tile"] for p in new} == {"31TCH", "30TXM"}


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
