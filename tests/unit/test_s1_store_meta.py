"""Unit tests for scripts/s1_store_meta.py — the S1 RTC datamodel-migration low-level helpers.

Slice 1 covers the R5 writer-pin guard (`assert_writer_pinned`): the migration re-derives vv/vh +
overviews with the data-model writer's private `_downsample_2d`/`OVERVIEW_CHAIN`, so it must refuse to
run unless the writer is at the pinned, value-identical behavior (eopf-geozarr 0.10.1 == data-model
f882a3f).
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import eopf_geozarr
import pytest
from eopf_geozarr.conversion import s1_ingest

# Load the script module by path (scripts/ is not an importable package).
_SPEC = importlib.util.spec_from_file_location(
    "s1_store_meta", Path(__file__).resolve().parents[2] / "scripts" / "s1_store_meta.py"
)
s1_store_meta = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(s1_store_meta)  # type: ignore[union-attr]


def test_passes_on_the_pinned_env() -> None:
    """The worktree pins f882a3f / 0.10.1, so the guard must accept it (no raise)."""
    s1_store_meta.assert_writer_pinned()


def test_rejects_wrong_eopf_geozarr_version(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(eopf_geozarr, "__version__", "9.9.9")
    with pytest.raises(RuntimeError, match="eopf-geozarr"):
        s1_store_meta.assert_writer_pinned()


def test_rejects_changed_float32_nan_fill_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(s1_ingest, "FLOAT32_NAN_FILL_VALUE", "WRONG==")
    with pytest.raises(RuntimeError, match="FLOAT32_NAN_FILL_VALUE"):
        s1_store_meta.assert_writer_pinned()


def test_rejects_changed_overview_chain(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(s1_ingest, "OVERVIEW_CHAIN", s1_ingest.OVERVIEW_CHAIN[:-1])
    with pytest.raises(RuntimeError, match="OVERVIEW_CHAIN"):
        s1_store_meta.assert_writer_pinned()
