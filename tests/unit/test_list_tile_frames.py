"""Unit tests for scripts/list_tile_frames.py (S1 caching T7 — frame-cache pull input).

Enumerates the S1 GRD frames overlapping an MGRS tile in a date window, so the Argo
template's cache-pull pre-step knows which product ids to try restoring from the cache.
Parity only moves the cache hit-rate (a wrong id is harmless waste, a missed id just
downloads), so the tests pin the CDSE query shape + the platform/dedup filtering, not
byte-parity with S1Tiling's internal search.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

from list_tile_frames import build_parser, list_tile_frames, main  # noqa: E402

_MOD = "list_tile_frames"
_BBOX = [0.5, 42.4, 1.8, 43.3]


def _item(item_id: str) -> MagicMock:
    item = MagicMock()
    item.id = item_id
    return item


def _patched_client(items: list[MagicMock]) -> MagicMock:
    client = MagicMock()
    client.search.return_value.items.return_value = iter(items)
    return client


def test_lists_frames_over_tile_bbox_for_platform() -> None:
    items = [
        _item("S1A_IW_GRDH_1SDV_20240101T060000_20240101T060025_0522A1_01_ABCD"),
        _item("S1A_IW_GRDH_1SDV_20240101T060025_20240101T060050_0522A1_01_EF01"),
    ]
    with patch(f"{_MOD}.tile_bbox", return_value=_BBOX), patch(
        f"{_MOD}.Client.open", return_value=_patched_client(items)
    ):
        frames = list_tile_frames(
            "https://cdse/stac", "31TCH", "descending", "2024-01-01", "2024-01-02", "S1A"
        )
    assert frames == [
        "S1A_IW_GRDH_1SDV_20240101T060000_20240101T060025_0522A1_01_ABCD",
        "S1A_IW_GRDH_1SDV_20240101T060025_20240101T060050_0522A1_01_EF01",
    ]


def test_scopes_search_to_collection_bbox_window_and_orbit() -> None:
    client = _patched_client([])
    with patch(f"{_MOD}.tile_bbox", return_value=_BBOX), patch(
        f"{_MOD}.Client.open", return_value=client
    ):
        list_tile_frames(
            "https://cdse/stac", "31TCH", "descending", "2024-01-01", "2024-01-02", "S1A"
        )
    kwargs = client.search.call_args.kwargs
    assert kwargs["collections"] == ["sentinel-1-grd"]
    assert kwargs["bbox"] == _BBOX
    assert kwargs["query"] == {"sat:orbit_state": {"eq": "descending"}}
    # window is [date_start 00:00, date_end+1 day 00:00) so the whole date_end day is covered
    assert kwargs["datetime"] == "2024-01-01T00:00:00+00:00/2024-01-03T00:00:00+00:00"


def test_drops_other_platform_frames() -> None:
    items = [
        _item("S1A_IW_GRDH_1SDV_20240101T060000_20240101T060025_0522A1_01_ABCD"),
        _item("S1D_IW_GRDH_1SDV_20240101T060000_20240101T060025_0522A1_01_DEAD"),
    ]
    with patch(f"{_MOD}.tile_bbox", return_value=_BBOX), patch(
        f"{_MOD}.Client.open", return_value=_patched_client(items)
    ):
        frames = list_tile_frames(
            "https://cdse/stac", "31TCH", "descending", "2024-01-01", "2024-01-02", "S1A"
        )
    assert frames == ["S1A_IW_GRDH_1SDV_20240101T060000_20240101T060025_0522A1_01_ABCD"]


def test_dedups_repeated_ids_preserving_order() -> None:
    dup = "S1A_IW_GRDH_1SDV_20240101T060000_20240101T060025_0522A1_01_ABCD"
    other = "S1A_IW_GRDH_1SDV_20240101T060025_20240101T060050_0522A1_01_EF01"
    items = [_item(dup), _item(other), _item(dup)]
    with patch(f"{_MOD}.tile_bbox", return_value=_BBOX), patch(
        f"{_MOD}.Client.open", return_value=_patched_client(items)
    ):
        frames = list_tile_frames(
            "https://cdse/stac", "31TCH", "descending", "2024-01-01", "2024-01-02", "S1A"
        )
    assert frames == [dup, other]


def test_invalid_tile_id_raises() -> None:
    with patch(
        f"{_MOD}.tile_bbox", side_effect=ValueError("invalid MGRS tile id: 'ZZ'")
    ), pytest.raises(ValueError, match="invalid MGRS tile id"):
        list_tile_frames(
            "https://cdse/stac", "ZZ", "descending", "2024-01-01", "2024-01-02", "S1A"
        )


def test_main_prints_ids_one_per_line(capsys: pytest.CaptureFixture[str]) -> None:
    items = [
        _item("S1A_IW_GRDH_1SDV_20240101T060000_20240101T060025_0522A1_01_ABCD"),
        _item("S1A_IW_GRDH_1SDV_20240101T060025_20240101T060050_0522A1_01_EF01"),
    ]
    argv = [
        "--tile-id", "31TCH",
        "--orbit-direction", "descending",
        "--date-start", "2024-01-01",
        "--date-end", "2024-01-02",
        "--platform", "S1A",
    ]
    with patch(f"{_MOD}.tile_bbox", return_value=_BBOX), patch(
        f"{_MOD}.Client.open", return_value=_patched_client(items)
    ):
        rc = main(argv)
    assert rc == 0
    out = capsys.readouterr().out.strip().splitlines()
    assert out == [
        "S1A_IW_GRDH_1SDV_20240101T060000_20240101T060025_0522A1_01_ABCD",
        "S1A_IW_GRDH_1SDV_20240101T060025_20240101T060050_0522A1_01_EF01",
    ]


def test_parser_requires_core_args() -> None:
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([])
