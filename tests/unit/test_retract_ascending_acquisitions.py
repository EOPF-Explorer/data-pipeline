"""Unit tests for scripts/retract_ascending_acquisitions.py (plan T2, issue #306).

Covers the destructive boundary: dry-run deletes nothing, --execute deletes each in-scope item,
the delete is idempotent (404 = already gone), other HTTP errors propagate, and the scope filter
targets only ascending items. No network — the HTTP session is injected.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

import retract_ascending_acquisitions as r  # noqa: E402


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


def test_item_delete_url_shape():
    url = r.item_delete_url("https://api/stac", "acq-coll", "s1-rtc-30UVU-20260611t180505")
    assert url == "https://api/stac/collections/acq-coll/items/s1-rtc-30UVU-20260611t180505"


def test_orbit_filter_targets_orbit_state():
    assert r.orbit_filter("ascending") == {
        "op": "=",
        "args": [{"property": "sat:orbit_state"}, "ascending"],
    }


def test_dry_run_deletes_nothing():
    sess = _Session()
    n = r.retract(sess, "https://api/stac", "acq", ["a", "b", "c"], execute=False)
    assert sess.deleted == []  # nothing deleted
    assert n == 3  # but reports the in-scope count


def test_execute_deletes_each_item():
    sess = _Session(204)
    n = r.retract(sess, "https://api/stac", "acq", ["a", "b"], execute=True)
    assert sess.deleted == [
        "https://api/stac/collections/acq/items/a",
        "https://api/stac/collections/acq/items/b",
    ]
    assert n == 2


def test_idempotent_on_404():
    sess = _Session(404)  # already gone
    n = r.retract(sess, "https://api/stac", "acq", ["gone"], execute=True)
    assert n == 1  # no raise


def test_other_http_error_propagates():
    sess = _Session(500)
    with pytest.raises(RuntimeError):
        r.retract(sess, "https://api/stac", "acq", ["boom"], execute=True)
