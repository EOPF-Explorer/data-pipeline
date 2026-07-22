"""Unit tests for scripts/wipe_s1rtc_tiles.py (plan T1/T3, issue #306 geoid reprocess).

Covers the destructive boundary: dry-run deletes nothing, --execute deletes BOTH the cube item and
every per-acquisition item (both orbits), the delete is idempotent (404 = already gone), other HTTP
errors propagate, and enumeration is tile-scoped (grid:code = MGRS-<tile> server filter + a client-side
id.startswith("s1-rtc-<tile>-") guard, never an orbit filter). No network — the HTTP session is
injected and the STAC item dicts are faked.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

import wipe_s1rtc_tiles as w  # noqa: E402


class _Resp:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _Session:
    """Records DELETE calls; returns a configurable status."""

    def __init__(self, status: int = 204) -> None:
        self.status = status
        self.deleted: list[str] = []

    def delete(self, url: str, timeout: int | None = None) -> _Resp:
        self.deleted.append(url)
        return _Resp(self.status)


def test_cube_item_id():
    assert w.cube_item_id("30UVU") == "s1-rtc-30UVU"


def test_grid_code_filter_is_tile_scoped_no_orbit():
    # Both-orbit guarantee: the filter constrains only the tile, never sat:orbit_state.
    assert w.grid_code_filter("30UVU") == {
        "op": "=",
        "args": [{"property": "grid:code"}, "MGRS-30UVU"],
    }


def test_item_delete_url_shape():
    url = w.item_delete_url("https://api/stac", "acq", "s1-rtc-30UVU-20260611t180505")
    assert url == "https://api/stac/collections/acq/items/s1-rtc-30UVU-20260611t180505"


def test_filter_tile_items_guards_prefix():
    """Client-side safety net: keep only this tile's per-acq ids (both orbits), drop everything else."""
    items = [
        {"id": "s1-rtc-30UVU-20260611t180505"},  # ascending
        {"id": "s1-rtc-30UVU-20260613t060000"},  # descending, same tile -> both orbits kept
        {"id": "s1-rtc-30UWB-20260611t180505"},  # other tile
        {"id": "s1-rtc-30UVUX-20260611t180505"},  # superstring tile, must NOT match
        {"id": "s1-rtc-30UVU"},  # the cube item id, not a per-acq id
    ]
    assert w.filter_tile_items(items, "30UVU") == [
        "s1-rtc-30UVU-20260611t180505",
        "s1-rtc-30UVU-20260613t060000",
    ]


def test_dry_run_deletes_nothing():
    sess = _Session()
    n = w.delete_items(sess, "https://api/stac", "acq", ["a", "b"], execute=False)
    assert sess.deleted == []  # nothing deleted
    assert n == 2  # but reports the in-scope count


def test_execute_deletes_each_item():
    sess = _Session(204)
    n = w.delete_items(sess, "https://api/stac", "coll", ["a", "b"], execute=True)
    assert sess.deleted == [
        "https://api/stac/collections/coll/items/a",
        "https://api/stac/collections/coll/items/b",
    ]
    assert n == 2


def test_idempotent_on_404():
    sess = _Session(404)  # already gone
    assert w.delete_items(sess, "https://api/stac", "coll", ["gone"], execute=True) == 1


def test_other_http_error_propagates():
    sess = _Session(500)
    with pytest.raises(RuntimeError):
        w.delete_items(sess, "https://api/stac", "coll", ["boom"], execute=True)


def test_wipe_tile_deletes_cube_then_all_acq_both_orbits():
    sess = _Session(204)
    res = w.wipe_tile(
        sess,
        "https://api/stac",
        cube_collection="cube",
        acq_collection="acq",
        cube_id="s1-rtc-30UVU",
        acq_ids=["s1-rtc-30UVU-20260611t180505", "s1-rtc-30UVU-20260613t060000"],
        execute=True,
    )
    assert sess.deleted == [
        "https://api/stac/collections/cube/items/s1-rtc-30UVU",
        "https://api/stac/collections/acq/items/s1-rtc-30UVU-20260611t180505",
        "https://api/stac/collections/acq/items/s1-rtc-30UVU-20260613t060000",
    ]
    assert res == {"cube": 1, "acq": 2}


def test_wipe_tile_dry_run_deletes_nothing():
    sess = _Session(204)
    res = w.wipe_tile(
        sess,
        "https://api/stac",
        cube_collection="cube",
        acq_collection="acq",
        cube_id="s1-rtc-30UVU",
        acq_ids=["s1-rtc-30UVU-20260611t180505"],
        execute=False,
    )
    assert sess.deleted == []
    assert res == {"cube": 1, "acq": 1}  # reports in-scope counts, deletes nothing
