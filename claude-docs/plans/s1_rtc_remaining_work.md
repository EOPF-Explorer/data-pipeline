# Implementation Plan: S1 RTC — remaining work (margin fix · #246 closure · CP-A · cleanup)

## Overview
Close out the open S1 RTC threads from this session: (1) the **31TCG DEM-coverage** bug (margin too
tight), (2) the **#246 tests-output** workaround (PRs raised — needs merge + orphan cleanup + acceptance),
(3) **T7 Phase-1 CP-A** closure (2/3 tiles passed; 31TCG blocked on the margin fix), and (4) **cleanup** of
the throwaway test scaffolding. Each code change ships as its own revertable, tracked PR.

## Current state (2026-06-15)
- **Merged:** data-pipeline **#261** (#246 direct-write) merged into `feat--s1_grd_phase6` (HEAD `6f32275`,
  2026-06-15). **Open:** platform-deploy #240 (DEM→workspace handoff), #241 (#246 ingest path).
  data-model `f43d567` on `feat/s1-rtc-stac-builder` (#180).
- **Images:** the current integration build (with #261's direct-write) is **`sha-6f32275`** — use this for
  go-forward pin bumps. ⚠ `sha-b629416` and `sha-4c1b942` are **pre-#261** (no direct-write) — do NOT use for
  #246. A3 uses `sha-e83f83c` (the #261 PR build, same direct-write code).
- **Validated:** workspace DEM handoff (single-tile CP-A green; 2/3 scale-up). #246 direct-write (unit-tested;
  A3 live preview in progress).
- **Broken/pending:** 31TCG (DEM margin); STAC orphans `s1-rtc-31TCJ`/`s1-rtc-31TDH`; in-cluster scaffolding.

## Architecture decisions (already taken, for context)
- DEM staged into the per-workflow **workspace RWO PVC** (not the S3-FUSE cache) — reliable handoff (#240).
- #246: **direct-write** the cube at titiler's render path (not #250 auto-copy) — couples store/item, no orphans.
- Margin fix is a **permanent** bug fix (not TEMPORARY); footprint-derived DEM is the deferred robust follow-up.

## Dependency graph
```
A3 (#246 preview test, #261 image) — independent, validates #246 PRE-merge ──┐ (de-risks Phase B)
                                                                             │
Phase A: A1 margin fix ─► CI image ─► A2 prove (test image) ─► A2b pin bump ─┤
                                                                             ├─► Phase C (CP-A 3-tile, needs #240 merge + A)
#240 merge (DEM workspace) ──────────────────────────────────────────────────┘
#261 MERGED ✓ ; #241 + data-model merge ─► Flux sync ─► Phase B (B2 orphan cleanup ─► B3 re-ingest orphans + preview-200)
                                                                             │
All of A/B/C green ───────────────────────────────────────────────────────────► Phase D (cleanup test scaffolding)
```

---

## Phase A — 31TCG DEM coverage (margin fix)  *(data-pipeline, own PR)*

### Task A1: Widen the DEM swath latitude margin
**Description:** `ensure_dem.py` derives DEM cells from `tile_bbox ± (MARGIN_LON, MARGIN_LAT)`. `MARGIN_LAT=1.5`
covers 31TCH but is 1° short for 31TCG (swath needs N44 from a 42.45°N tile). Widen to 3.0 (N45 headroom).
**Acceptance criteria:**
- [ ] `MARGIN_LAT = 3.0`; comment explains the swath-vs-tile span (31TCG needed N44 from 42.45°N).
- [ ] unit test: `tiles_to_fetch("31TCG", …)` includes the full **N44** row (incl. `N44_W001/W002`).
- [ ] existing margin/ocean/idempotency tests still pass.
**Verification:** `uv run python -m pytest tests/unit/test_ensure_dem.py -q` green.
**Dependencies:** None. **Files:** `scripts/ensure_dem.py`, `tests/unit/test_ensure_dem.py`. **Scope:** S

### Task A2: Prove it end-to-end (the real gate)
**Description:** s1tiling reports DEM gaps incrementally and aborts, so the unit test isn't sufficient proof —
a live render is. Re-run 31TCG and confirm OTB succeeds with the wider margin.
**⚠ Image dependency (Gap 1):** `ensure_dem.py` runs in the s1tiling `ensure-dem` step via
`pipeline_image_version` — the working-tree change is NOT exercised until it's in a *built image*. So A2 must
run against the **A1-PR-branch CI build** (`sha-<A1 commit>`), set on a throwaway test s1tiling template
(same pattern A3 uses with `sha-e83f83c`). Do NOT test against `main`/integration — that has the old margin.
**Acceptance criteria:**
- [ ] 31TCG workflow **Succeeds**, `aggFail=0`, GeoTIFFs produced — running the **A1-image** (verify the tag).
- [ ] if a *new* gap appears, widen/iterate (don't declare fixed on a green unit test alone).
**Verification:** confirm CI built the A1 commit; submit 31TCG (desc, 2026-06-05→07) on a test template pinned
to that image; monitor to terminal; capture `argo` evidence.
**Dependencies:** A1 + its CI image build. **Files:** none (live run). **Scope:** S

### Task A2b: Deploy the margin fix (post-merge pin bump)
**Description:** Once A1 merges, the integration build carries the fix, but the deployed s1tiling template still
pins the old `pipeline_image_version`. Bump it so cron/sensor runs use the margin fix.
**Acceptance criteria:**
- [ ] `eopf-explorer-s1tiling-template` `pipeline_image_version` bumped to the post-merge integration build
      (`sha-<merged commit>`, e.g. the `6f32275`-lineage build that also carries #261) — own platform-deploy PR.
**Verification:** deployed template references the new tag; `argo lint` clean.
**Dependencies:** A1 merged + CI image. **Files:** `eopf-explorer-s1tiling-template.yaml`. **Scope:** XS

### Task A3: Validate the #246 titiler workaround pre-merge (independent of A1/A2)
**Description:** Prove the #246 direct-write actually makes titiler render, *before* merging #261/#241. Uses an
already-rendering tile (31TCH) so it needs no margin fix. Mirrors the test-template pattern: a throwaway test
ingest template carrying #241's path + the #261 image (`sha-e83f83c`, has direct-write + the `f43d567` parse),
run against 31TCH's existing GeoTIFFs → cube at `tests-output/sentinel-1-grd-rtc-tests/s1-rtc-31TCH.zarr` →
register (no auto-copy) → titiler preview.
**Acceptance criteria — DONE ✅ (2026-06-15, image `sha-b629416`):**
- [x] ingest+register Succeeded; cube present at `…-fra/tests-output/sentinel-1-grd-rtc-tests/s1-rtc-31TCH.zarr`.
- [x] STAC item `s1-rtc-31TCH` registered in `sentinel-1-grd-rtc-tests` (no `_titiler_render_copy`).
- [x] titiler `/info` + `tilejson` + `/preview` → **HTTP 200** (`/preview` = image/jpeg 42 926 B). Recorded on #246.
**Image note:** the correct integration image is the PR-merge sha **`sha-b629416`** (= `pr-232`), NOT the
branch-head `sha-6f32275` (not pushed) nor the PR-head `sha-e83f83c` (not found). See memory
`reference_datapipeline_image_tags`.
**Note:** disposable `-tests` data; cleaned up in Phase D.

### Checkpoint A
- [ ] A1 unit test green; A2 live render Succeeds; **A3 preview HTTP 200** → open the margin-fix PR; #246 de-risked.

---

## Phase B — #246 closure  *(after #261 + #241 + data-model merge)*

### Task B1: Merge + Flux-sync the direct-write workaround
**Status:** data-pipeline **#261 MERGED** (integration `6f32275`). Remaining: platform-deploy **#241** + the
data-model commit reachable from the pinned branch.
**Acceptance criteria:**
- [x] #261 merged; data-model `f43d567` on `feat/s1-rtc-stac-builder`.
- [ ] #241 merged; Flux reconciles the ingest template; cron/sensor unchanged.
**Verification:** deployed ingest template `s3_zarr_store` == `…/tests-output/{collection}/s1-rtc-{tile}.zarr`.
**Dependencies:** #241 review. **Scope:** XS (review/merge)

### Task B2: Clean up orphaned STAC items + stale stores
**Description:** `s1-rtc-31TCJ`, `s1-rtc-31TDH` are registered with no store at the render path; a misplaced
copy sits under `tests-output/…-staging/`. Remove the orphan items + stale/misplaced stores so STAC matches S3.
**Acceptance criteria:**
- [ ] orphan items deleted from STAC (`sentinel-1-grd-rtc-tests`); stale `tests-output/…-staging/` copies removed.
- [ ] no STAC item points at a non-existent store.
**Verification:** STAC list vs S3 `tests-output/sentinel-1-grd-rtc-tests/` reconcile (every item has a store).
**Dependencies:** B1. **Scope:** S (cluster/S3 ops)

### Task B3: Re-ingest the orphaned tiles on the merged path (Gap 2: scope narrowed)
**Description:** A3 already proves the #246 preview path end-to-end (31TCH, pre-merge), so this is **not** a
re-test of the mechanism — it's re-ingesting the *orphans* (`31TCJ`, `31TDH`) through the merged pipeline so
they resolve, plus a post-merge confirmation on one tile. (31TCJ has GeoTIFFs from the scale-up; 31TDH may need
a fresh s1tiling run — and if so, the margin fix A, since 31TDH is in the same swath family as 31TCG.)
**Acceptance criteria:**
- [ ] `31TCJ` (and `31TDH`) re-ingested; each store present at `…-fra/tests-output/sentinel-1-grd-rtc-tests/s1-rtc-{tile}.zarr`.
- [ ] their `/preview` returns **HTTP 200** with no manual copy → #246 acceptance boxes ticked + comment on #246.
**Verification:** curl titiler `/preview` for each → 200.
**Dependencies:** B1 (#241 merge), B2 (cleanup first), and A for 31TDH if it lacks DEM coverage.
**Dependencies:** B1, B2, and A (if a re-ingested tile needs the margin fix). **Scope:** S

### Checkpoint B
- [ ] Preview 200 with no copy → #246 acceptance met (issue stays open only for the titiler-eopf#108 revert).

---

## Phase C — T7 Phase-1 CP-A closure  *(after A + #240 merge)*

### Task C1: Merge #240 (DEM→workspace) + re-run the 3-tile scale-up
**Description:** Scale-up was 2/3 (31TCG failed on the margin bug). With A merged, re-run 31TCH+31TCJ+31TCG
concurrently to get **3/3** on the real (merged) template.
**Acceptance criteria:**
- [ ] #240 merged + Flux-synced (real `eopf-explorer-s1tiling` template carries the workspace handoff).
- [ ] 3 concurrent tiles all **Succeed** (`aggFail=0`, diff-tile parallel, per-tile DEM).
- [ ] CP-A marked done in `claude-docs/plans/subissue_T7_multitile_aoi.md`.
**Verification:** submit 3 tiles via the merged template; monitor to 3× Succeeded.
**Dependencies:** A (margin), #240. **Scope:** S (live run)

---

## Phase D — Cleanup test scaffolding  *(after A/B/C green)*

### Task D1: Tear down throwaway in-cluster test resources
**Acceptance criteria:**
- [ ] delete test templates: `workflowtemplate/eopf-explorer-s1tiling-test`,
      `workflowtemplate/eopf-explorer-ingest-v1-s1rtc-test` (A3), `configmap/s1grd-rtc-cfg-base-test`.
- [ ] delete the test/failed workflows (`cpa-31tch-desc-*`, `diag-31tcg-*`, `scale-31tc*`, `t246-ingest-*` (A3)).
- [ ] A3 leftovers: if the A3 test wrote `tests-output/sentinel-1-grd-rtc-tests/s1-rtc-31TCH.zarr` + a STAC
      item that B3 doesn't keep, remove them (disposable `-tests` data).
- [ ] resolve the `s1-dem-cache` SealedSecret/PVC/Secret: #240 removes them from git — confirm Flux prunes
      them, else delete the manually-applied in-cluster objects; remove the `s3://…/dem-cache` test objects.
- [ ] restore the stashed data-pipeline `uv.lock` + `claude-docs/`/`tasks/` when returning to the query-stac branch.
**Verification:** `kubectl get workflowtemplate,configmap,sealedsecret,pvc -n devseed-staging | grep -iE 'test|dem-cache'` empty.
**Dependencies:** A/B/C (don't delete scaffolding still in use). **Scope:** S

---

## Phase E — Follow-up (separate issue, not now)
- **Footprint-derived DEM** — **filed as #264.** Replace the tile+margin heuristic: derive cells from the
  actual S1 product footprint (thread the geometry from the trigger, which already queries CDSE) and fetch
  exactly `footprint ∩ GLO-30 ∩ land-gpkg`. Eliminates per-tile margin guessing + over-fetch. Not urgent
  (lat±3° works — CP-A 3/3).

## Risks & mitigations
| Risk | Impact | Mitigation |
|------|--------|------------|
| Margin 3.0 still short / a new gap on 31TCG | Med | A2 live re-run is the gate; iterate before declaring fixed |
| Orphan cleanup deletes a still-referenced store | High | B2 reconciles STAC↔S3 first; delete only items with no store |
| Flux doesn't prune the removed `s1-dem-cache` PVC | Low | D1 explicitly checks + manually deletes if prune is off |
| Re-ingest needs the margin fix for some tiles | Med | sequence B3 after A; pick tiles with known DEM coverage |

## Open questions
1. Should the data-model `f43d567` be split into its own PR (vs the commit on #180)? Needs a force-push of the
   shared branch — only if you want strict per-change PR symmetry.
2. Which tiles for the B3 re-ingest acceptance — reuse 31TCH (known-good) or a fresh `-tests` tile?
