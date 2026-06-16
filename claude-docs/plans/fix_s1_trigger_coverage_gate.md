# Plan: S1 trigger footprint-coverage gate (empty-tile fix)

**Goal**: Stop the CDSE trigger from scheduling tile/product pairs the swath only grazes, which
produce all-nodata tiles (e.g. `s1-rtc-30TWN`), and remove the existing bogus tile(s).
**Constraint**: change confined to `scripts/trigger_cdse.py` + its unit test; emitted-record schema
unchanged. **Revised:** `shapely==2.1.2` was only in the `notebooks` dep group → **promoted to runtime
`dependencies`** (the trigger image needs it) — the one scope delta from the original "no new dep".

## Status (shipped 2026-06-16)
T1 ✅ + T2 ✅ (41 trigger tests, full suite 536 passed; ruff + mypy clean). Checkpoint A ✅ (five-axis
review, no Critical/Important). T3 ✅ — proven via the AOI replay below (30TWN asc 1.55% excluded).
T4 → **folded into the post-soak reconcile** (deleting 30TWN now would race the running ungated soak,
which still selects its graze). Deploy (trigger image pin-bump) is a follow-up.

## Soak contamination finding (drives the reconcile)
The running Pyrenees soak (`s1rtc-soak-pyrenees-vpmmr`, trigger image `sha-0ecdafc`, **pre-gate**)
mints empties at scale. Replaying the AOI (14 tiles, both orbits, S1A/S1C, same-pass collapsed,
lookback 14 d) through the gate: **74 selected → 40 real (≥20%) / 34 empty (<20%, many 0.00%)** —
~46%. The gate excludes exactly these. Post-soak reconcile = delete the all-zero cubes (the gate
replay is the delete manifest), incl. 30TWN.

## Root cause (evidenced 2026-06-16)
`trigger_cdse.query_products` selects CDSE products by **bbox intersection** (`bbox=tile_bbox(tile)`,
`trigger_cdse.py:80-85`). An S1 IW GRD footprint is a slanted parallelogram, so its *bounding box*
overlaps a tile even when the actual *footprint polygon* barely clips a corner. For `30TWN`,
ascending, 2026-06-06:
- `S1A_…_064852_…` (rel-orbit **30**) footprint covers **1.5%** of the tile → got processed.
- `S1D_…_003119_…` (rel-orbit 103) covers **100%** → skipped (S1D not in `ENABLED_PLATFORMS`, by design).

s1tiling projected the 1.5%-coverage S1A scene onto the `30TWN` MGRS grid → an all-zero cube
(verified: `vv`/`vh`/`gamma_area` exactly `0.0` at every overview incl. full 120 m read; `border_mask`
all 0). The tile was then ingested + registered as a dark STAC item.

## Current state
| Resource | Status |
|----------|--------|
| `scripts/trigger_cdse.py` | bbox-only selection; no coverage check |
| `tests/unit/test_trigger_cdse.py` | covers platform/dedup/collapse/orbit; **no coverage test** |
| `shapely==2.1.2` | already a declared dep (no add) |
| `mgrs` | already used by `watch_cdse_and_process.tile_bbox` |
| STAC `sentinel-1-grd-rtc-staging/s1-rtc-30TWN` | all-zero item, to be removed |
| S3 `…/tests-output/sentinel-1-grd-rtc-staging/s1-rtc-30TWN.zarr` | all-zero store, to be removed |

## Design
Add a true-geometry coverage gate to the trigger (the boundary where per-tile/per-product scheduling
is decided and the product geometry is already in hand — filtering here prevents the wasted s1tiling
run entirely, vs. detecting an empty cube downstream).

- `tile_polygon(tile_id) -> shapely.Polygon` — the MGRS 100 km square as a WGS84 polygon (true
  corners, not the axis-aligned bbox), mirroring `tile_bbox`'s MGRS-corner approach.
- `tile_coverage(geometry, tile_poly) -> float` — `footprint ∩ tile / tile.area` in [0, 1]; missing
  geometry ⇒ `0.0`.
- `query_products(... , tile_poly)` — attach a `coverage` float to each product (it already iterates
  the CDSE items, so geometry is free here; the scalar keeps geometry out of the product dict).
- `drop_low_coverage(products, min_coverage)` — filter (logged) **before** `collapse_same_pass`, so a
  grazing frame can't become a pass's representative.
- `MIN_TILE_COVERAGE = 0.20`; `--min-coverage` CLI override threaded through `select_new_products`.
- Emitted-record schema unchanged (`coverage`/geometry are internal-only).

**Why filter before collapse**: collapse keeps the earliest frame per `(date, platform)`. Dropping
sub-threshold frames first prevents a sliver frame from being chosen as the representative; a pass
whose frames are *all* sub-threshold is dropped entirely (the 30TWN case → no s1tiling run scheduled).

## Dependency graph
```
T1 (coverage helpers + gate in trigger) ──► T2 (unit tests green) ──► Checkpoint A (code review)
                                                                          │
T3 (live trigger replay: 30TWN excluded, a covered tile kept) ◄──────────┘
Checkpoint A ──► T4 (delete bogus 30TWN STAC item + S3 store)  ── independent of T3, gated on A
```

## Tasks

### Task 1 — Coverage gate in trigger_cdse.py  <status: NEXT>
**What**: add `MIN_TILE_COVERAGE`, `tile_polygon`, `tile_coverage`, `drop_low_coverage`; give
`query_products` a `tile_poly` param that attaches `coverage`; call `drop_low_coverage(...,
args.min_coverage)` before `collapse_same_pass` in `select_new_products`; add `--min-coverage`
(default `MIN_TILE_COVERAGE`) to the parser. Import `mgrs`, `shapely.geometry.{shape, Polygon}`.
**Verify**: `uv run ruff check scripts/trigger_cdse.py` clean; module imports.
**Acceptance criteria**:
- [ ] `tile_coverage` returns ~1.0 for a footprint covering the tile, ~0.0 for a corner sliver / disjoint / missing geometry.
- [ ] `query_products` output carries a `coverage` field; emitted final records do **not** (schema intact).
- [ ] sub-threshold products dropped (logged) before collapse; `--min-coverage` overrides the default.
**Files**: `scripts/trigger_cdse.py`. **Scope**: S

### Task 2 — Unit tests  <status: ready>
**What**: add tests for `tile_coverage` (full/sliver/disjoint/missing), `drop_low_coverage`, a
select-level regression encoding the 30TWN bug (a 0.015-coverage product is **not** emitted; a 1.0 one
is), and a `--min-coverage` default/override test. Update existing tests for the new signature:
`_item` gains a `geometry`; `_product` gains `coverage=1.0` (so existing select tests still pass the
gate); `query_products` test callers pass a `tile_poly`; the `both`-orbit `_q` side-effect gains the
`tile_poly` param.
**Verify**: `uv run pytest tests/unit/test_trigger_cdse.py -q`.
**Acceptance criteria**:
- [ ] new coverage/gate tests pass; the 30TWN-style regression fails on `main` code and passes with T1.
- [ ] all pre-existing trigger tests still pass (signature updates only — no assertion semantics weakened).
- [ ] full `uv run pytest -q` green (no collateral breakage).
**Files**: `tests/unit/test_trigger_cdse.py`. **Scope**: S

### Checkpoint A — code review
- [ ] Five-axis review of the diff; every changed line traces to the gate. Plan reflects what shipped.

### Task 3 — Live replay proof (the real gate)  <status: ready>
**What**: run the trigger against the real CDSE catalogue for `30TWN` ascending over a window
covering 2026-06-06 with `--min-coverage 0.20`; confirm the 1.5% S1A product is **excluded** from the
emitted JSON. Spot-check a known-covered tile (e.g. `31TCH`) still emits.
**Verify**: `uv run python scripts/trigger_cdse.py --tiles 30TWN --orbit-direction ascending
--lookback-days <N> --stac-api-url <eopf-stac> --min-coverage 0.20` → no `30TWN` record for that pass;
trigger logs the skip with the coverage %.
**Acceptance criteria**:
- [ ] emitted array contains no `30TWN` record for the orbit-30 2026-06-06 pass.
- [ ] log shows `skip … covers 1.x% of tile (< 20% min)`.
**Files**: none (live run). **Scope**: S

### Task 4 — Remove the bogus 30TWN tile  <status: ready>
**What**: delete the all-zero `s1-rtc-30TWN` STAC item from `sentinel-1-grd-rtc-staging` and its zarr
store at `…/tests-output/sentinel-1-grd-rtc-staging/s1-rtc-30TWN.zarr`. (Decision: user opted to clean
up now.) Needs STAC-admin + S3 credentials/endpoint available to the session.
**Verify**: `GET …/collections/sentinel-1-grd-rtc-staging/items/s1-rtc-30TWN` → 404; S3 prefix listing empty.
**Acceptance criteria**:
- [ ] STAC item returns 404; no other staging item is affected.
- [ ] zarr store prefix removed from S3.
**Dependencies**: Checkpoint A (don't delete before the producing bug is fixed). **Scope**: S (ops)

## Open questions
1. **T4 credentials** — does this session have the STAC transaction (admin) token + the OVH S3
   creds/endpoint for the staging bucket, or is T4 a hand-off back to your cluster/S3 ops? (Owner: user.)
2. **Backfill of 30TWN** — once the gate is deployed, should a *correctly covered* `30TWN` be
   re-triggered (a different orbit, or S1C), or left to the normal cron AOI sweep? (Owner: user; default:
   leave to cron.)

## Done definition
Trigger no longer emits a tile/product pair below `--min-coverage`; the 30TWN regression test proves
it; live replay confirms the real 30TWN sliver is excluded; the existing all-zero `s1-rtc-30TWN` STAC
item + store are gone. Deploy (pin-bump the trigger image) is a follow-up outside this plan.
