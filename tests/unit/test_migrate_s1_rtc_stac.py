"""Unit tests for migrate_s1_rtc_stac.py — work enumeration + command construction (no live writes)."""

from __future__ import annotations

import argparse
import io
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

import migrate_s1_rtc_stac as m  # noqa: E402

_MOD = "migrate_s1_rtc_stac"
STAC = "https://stac.example.com"


def _resp(payload: dict) -> MagicMock:
    cm = MagicMock()
    cm.__enter__.return_value = io.BytesIO(json.dumps(payload).encode())
    cm.__exit__.return_value = False
    return cm


def _args(**over: object) -> argparse.Namespace:
    base: dict[str, object] = {
        "stac_api_url": STAC,
        "raster_api_url": "https://raster.example.com",
        "s3_endpoint": "https://s3.example.com",
        "cube_collection": "sentinel-1-grd-rtc-staging",
        "acq_collection": "sentinel-1-grd-rtc-acquisitions-staging",
        "tile": None,
        "limit": None,
        "orbit_direction": None,
        "dry_run": True,
        "allow_nonstaging": False,
    }
    base.update(over)
    return argparse.Namespace(**base)


def test_list_cube_items_follows_pagination() -> None:
    page1 = {
        "features": [
            {"id": "s1-rtc-30TWN", "assets": {"zarr-store": {"href": "https://x/a.zarr"}}}
        ],
        "links": [{"rel": "next", "href": f"{STAC}/page2"}],
    }
    page2 = {
        "features": [
            {"id": "s1-rtc-31TEG", "assets": {"zarr-store": {"href": "https://x/b.zarr"}}}
        ],
        "links": [],
    }
    with patch(f"{_MOD}.urllib.request.urlopen", side_effect=[_resp(page1), _resp(page2)]):
        items = m.list_cube_items(STAC, "sentinel-1-grd-rtc-staging")
    assert items == [("s1-rtc-30TWN", "https://x/a.zarr"), ("s1-rtc-31TEG", "https://x/b.zarr")]


def test_command_construction() -> None:
    args = _args()
    cube = m._cube_cmd("s3://b/s1-rtc-30TWN.zarr", args)
    assert cube[1].endswith("register_v1_s1_rtc.py")
    assert "--store" in cube and "s3://b/s1-rtc-30TWN.zarr" in cube
    assert "--collection" in cube and "sentinel-1-grd-rtc-staging" in cube

    peracq = m._peracq_cmd("s3://b/s1-rtc-30TWN.zarr", "30TWN", "descending", args)
    assert peracq[1].endswith("register_per_acquisition.py")
    assert "--tile-id" in peracq and "30TWN" in peracq
    assert "--orbit-direction" in peracq and "descending" in peracq
    assert "--cube-collection" in peracq and "sentinel-1-grd-rtc-staging" in peracq
    assert "--reregister-all" in peracq  # migration re-registers every slice


def test_dry_run_emits_cube_then_per_orbit_commands(capsys: pytest.CaptureFixture[str]) -> None:
    with (
        patch(f"{_MOD}.list_cube_items", return_value=[("s1-rtc-30TWN", "https://x/a.zarr")]),
        patch(f"{_MOD}.store_orbits", return_value=["ascending", "descending"]),
        patch(f"{_MOD}.subprocess.run") as mock_run,
    ):
        rc = m.migrate(_args(dry_run=True))
    out = capsys.readouterr().out
    assert rc == 0
    mock_run.assert_not_called()  # dry-run never executes
    assert "register_v1_s1_rtc.py" in out  # cube
    assert out.count("register_per_acquisition.py") == 2  # one per orbit


def test_tile_filter_scopes_smoke() -> None:
    with (
        patch(
            f"{_MOD}.list_cube_items",
            return_value=[("s1-rtc-30TWN", "https://a"), ("s1-rtc-31TEG", "https://b")],
        ),
        patch(f"{_MOD}.store_orbits", return_value=["descending"]),
        patch(f"{_MOD}._run", return_value=0) as mock_run,
    ):
        m.migrate(_args(tile="30TWN"))
    # one cube + one per-acq command for the single matched tile only
    stores = [call.args[0][call.args[0].index("--store") + 1] for call in mock_run.call_args_list]
    assert all(s in ("https://a",) for s in stores)
    assert len(mock_run.call_args_list) == 2


def test_main_refuses_non_staging_without_flag() -> None:
    argv = [
        "migrate",
        "--stac-api-url", STAC,
        "--raster-api-url", "https://r",
        "--s3-endpoint", "https://s",
        "--cube-collection", "sentinel-1-grd-rtc-prod",
        "--acq-collection", "sentinel-1-grd-rtc-acquisitions-prod",
    ]  # fmt: skip
    with patch.object(sys, "argv", argv), pytest.raises(SystemExit, match="non-…-staging"):
        m.main()


def test_store_orbits_skips_all_zero_time(tmp_path: Path) -> None:
    """A corrupt orbit (all-zero r10m time) is skipped so the migration never emits 1970-epoch items."""
    import numpy as np
    import zarr

    store = str(tmp_path / "s1-rtc-30TYN.zarr")
    root = zarr.open_group(store, mode="w", zarr_format=3)
    for orbit, times in (
        ("ascending", [0, 0, 0]),  # corrupt: time axis lost
        ("descending", [1781244056000000000, 1780639747000000000]),  # valid
    ):
        r10m = root.create_group(orbit).create_group("r10m")
        r10m.create_array("time", shape=(len(times),), dtype="int64")[:] = np.array(times, "int64")
    zarr.consolidate_metadata(store, zarr_format=3)

    assert m.store_orbits(store) == ["descending"]
