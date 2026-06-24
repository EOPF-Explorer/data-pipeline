"""Low-level reuse helpers + the writer-pin guard for the S1 RTC datamodel migration.

The migration (`migrate_s1_rtc_datamodel.py`) re-derives each legacy cube's vv/vh + overviews **in
place** so they are value-identical to a fresh re-ingest. To stay value-identical it reuses the
data-model writer's own constants and the private `_downsample_2d`/`OVERVIEW_CHAIN` rather than
re-implementing the overview math. That reuse is only safe at the pinned writer (eopf-geozarr 0.10.1
== data-model `f882a3f`, the live `v0.8.0-s1rtc-rc2`): a later bump could silently change overview
values with no semver guard. `assert_writer_pinned()` is the R5 mitigation — refuse to run unless the
writer is at the asserted behavior.
"""

from __future__ import annotations

import json
from pathlib import Path

# Pinned writer invariants (R5). `f882a3f` == eopf-geozarr 0.10.1; the float32 NaN fill is the S2-parity
# encoding (data-model #201); the overview chain is the level/factor ladder the re-derive walks.
PINNED_EOPF_GEOZARR_VERSION = "0.10.1"
EXPECTED_FLOAT32_NAN_FILL_VALUE = "AAAAAAAA+H8="
EXPECTED_OVERVIEW_CHAIN = [
    ("r10m", None, 1),
    ("r20m", "r10m", 2),
    ("r60m", "r20m", 3),
    ("r120m", "r60m", 2),
    ("r360m", "r120m", 3),
    ("r720m", "r360m", 2),
]


def assert_writer_pinned() -> None:
    """Refuse to run unless the data-model writer is at the pinned, value-identical behavior (R5).

    Reads the live values at call time (not import time) so a drifted dependency is caught on every
    run. Raises ``RuntimeError`` naming the invariant that drifted.
    """
    import eopf_geozarr
    from eopf_geozarr.conversion import s1_ingest

    if eopf_geozarr.__version__ != PINNED_EOPF_GEOZARR_VERSION:
        raise RuntimeError(
            f"eopf-geozarr {eopf_geozarr.__version__} != pinned {PINNED_EOPF_GEOZARR_VERSION} "
            "(data-model f882a3f); the re-derive is only value-identical to a fresh re-ingest at the "
            "pinned writer. Re-pin or re-validate before migrating."
        )
    if s1_ingest.FLOAT32_NAN_FILL_VALUE != EXPECTED_FLOAT32_NAN_FILL_VALUE:
        raise RuntimeError(
            f"FLOAT32_NAN_FILL_VALUE {s1_ingest.FLOAT32_NAN_FILL_VALUE!r} != expected "
            f"{EXPECTED_FLOAT32_NAN_FILL_VALUE!r}; the float32 NaN encoding changed."
        )
    if list(s1_ingest.OVERVIEW_CHAIN) != EXPECTED_OVERVIEW_CHAIN:
        raise RuntimeError(
            f"OVERVIEW_CHAIN changed: {list(s1_ingest.OVERVIEW_CHAIN)} != {EXPECTED_OVERVIEW_CHAIN}; "
            "overview levels/factors differ from the pinned writer."
        )


def drop_consolidated_metadata(store_path: str | Path) -> int:
    """Strip Zarr-v3 consolidated metadata from every group node of a local store; return the count.

    Reopening a consolidated store ``mode="r+"`` serves the stale consolidated array metadata to
    writers, so the migration must drop it before re-deriving (mirrors ``ingest_v1_s1_rtc.py``'s
    pre-append drop). ``eopf_geozarr`` consolidates at the orbit-group level (not just the root), so
    strip it from *every* group node; ``consolidate_s1_store`` re-consolidates at the end.
    """
    dropped = 0
    for zj in Path(store_path).rglob("zarr.json"):
        meta = json.loads(zj.read_text())
        if meta.get("node_type") == "group" and meta.pop("consolidated_metadata", None) is not None:
            zj.write_text(json.dumps(meta))
            dropped += 1
    return dropped
