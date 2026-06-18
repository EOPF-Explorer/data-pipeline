"""Unit tests for scripts/register_v1_s1_rtc.py::pick_slice — the cube-preview slice selector.

Rule (per plan): pick the **most recent acquisition with coverage > 0.80**; if none clears 0.80,
pick the one with the **highest coverage** (ties broken by most recent). Spans both orbit groups.
Pure function — no I/O — so exercised exhaustively here, including the adversarial boundaries.
"""

import datetime as dt
import sys
from pathlib import Path

SCRIPT = Path(__file__).parent.parent.parent / "scripts" / "register_v1_s1_rtc.py"


def _mod():
    sys.path.insert(0, str(SCRIPT.parent))
    import register_v1_s1_rtc

    return register_v1_s1_rtc


def _s(orbit: str, day: int, coverage: float):
    return _mod().Slice(
        orbit=orbit, dt=dt.datetime(2026, 6, day, 6, 0, tzinfo=dt.UTC), coverage=coverage
    )


def test_most_recent_above_threshold_wins_even_if_older_has_more_coverage():
    m = _mod()
    chosen = m.pick_slice(
        [
            _s("ascending", 4, 0.99),  # older, higher coverage
            _s("descending", 7, 0.85),  # most recent above threshold -> winner
            _s("ascending", 6, 0.90),
        ]
    )
    assert (chosen.orbit, chosen.dt.day, chosen.coverage) == ("descending", 7, 0.85)


def test_falls_back_to_max_coverage_when_none_above_threshold():
    m = _mod()
    chosen = m.pick_slice(
        [
            _s("ascending", 7, 0.40),  # most recent but low coverage
            _s("descending", 5, 0.75),  # highest coverage (still <= 0.80) -> winner
            _s("ascending", 4, 0.50),
        ]
    )
    assert (chosen.orbit, chosen.dt.day, chosen.coverage) == ("descending", 5, 0.75)


def test_exact_80_percent_is_not_above_threshold():
    m = _mod()
    # 0.80 is NOT > 0.80 -> excluded from the "good" set; the 0.81 older slice wins instead.
    chosen = m.pick_slice(
        [
            _s("ascending", 7, 0.80),  # most recent, exactly at threshold -> excluded from "good"
            _s("descending", 4, 0.81),  # only one strictly above -> winner
        ]
    )
    assert (chosen.dt.day, chosen.coverage) == (4, 0.81)


def test_max_coverage_tie_broken_by_most_recent():
    m = _mod()
    chosen = m.pick_slice(
        [
            _s("ascending", 4, 0.60),
            _s("descending", 8, 0.60),  # same (max) coverage, more recent -> winner
            _s("ascending", 6, 0.60),
        ]
    )
    assert chosen.dt.day == 8


def test_single_slice_returned():
    m = _mod()
    only = _s("ascending", 5, 0.10)
    assert m.pick_slice([only]) == only


def test_empty_returns_none():
    m = _mod()
    assert m.pick_slice([]) is None
