# Plan: S1 GRD RTC Productionization — Phase 6 (`claude-docs/specs/s1_grd_phase6_productionization.md`)

**Goal**: an automated, multi-tile, in-cluster S1 GRD RTC service on **staging** — a data-driven
trigger discovers new CDSE S1A/S1C products, auto-provisions the DEM per tile, runs
s1tiling → ingest → quality-gate → register, storing each tile as a **multi-temporal datacube** with
**per-acquisition STAC items** (P8). **Tracked in #226.**
**Constraint**: keep Script A/B + merged Argo templates as the single source of pipeline logic (new
work *orchestrates*, *gates*, and *accumulates the cube*); path-guard every destructive op; serialise
per-tile cube writes; tests at each new-code boundary; **staging only** (prod = Phase 7).

> **Repo split**: plan authored here; new **scripts** (`ensure_dem.py`, `validate_s1_rtc.py`, cube
> time-slice insert, trigger entrypoint) + the **`eopf-geozarr` per-acquisition builder** ship in
> data-pipeline / the data-model repo; new **Argo manifests** (CronWorkflow, ensure-dem + quality-gate
> steps, per-tile mutex) ship in **platform-deploy**. Branch off `main` once phase-5 (#186 +
> platform-deploy #207/#208) is merged.

---

## Current state

| Resource | Status |
|----------|--------|
| `eopf-explorer-s1tiling` / `ingest-v1-s1rtc` WorkflowTemplates | ✅ merged (PR#207) |
| s1tiling cfg render (tile/orbit/date) | ✅ exists; `platform_list` not parametrized (T2) |
| `s1-dem` PVC (31TCH only, manual) + EGM2008 geoid | ✅ bound; no auto-fetch (T3) |
| Ingest = **fresh store per run** | ⚠️ → **append scene to per-tile cube** (T4); append validated by PoC |
| `eopf_geozarr.build_s1_rtc_stac_item` = one item per store | ✅ reuse with **per-acquisition ids** + `sel=time` links (T5; no upstream change) |
| titiler store resolution | ⚠️ reconstructs + ignores href (Spike 3); **I2/option 2** (`ecde99c`) to fix it — not yet effective; blocks per-acq rendering |
| Local watcher query/bbox/dedup logic | ✅ port; dedup → per-acquisition STAC item-exists (T6) |
| Blind daily cron + sensor (sub-issue 8) | ✅ shipped, suspended — replaced by the data-driven trigger (T6) |

---

## Dependency graph

```
merged templates + spec P1–P8
   ├── T1 quality-gate step (ingest) ─────────────┐
   ├── T2 platform S1A+S1C (render) ──────────────┤
   ├── T3 ensure-dem auto-fetch ──────────────────┤
   ├── T4 datacube ingest (append to per-tile cube + mutex) ─┤   ┐ P8 cube + per-acq items
   └── T5 per-acquisition catalogue (sel=time; needs I2/opt-2) ┘ (T4 → T5)
                         │
                         ▼
        T6 trigger → Argo (dedup = per-acq item-exists + cube time-present)
                         │                                   │
                         ├── T7 multi-tile AOI ──────────────┤
                         └── T8 bounded backfill ────────────┴──► T9 acceptance soak
   CP-A after T1–T5 (one acquisition → cube slice + per-acq item, gated, any tile, S1A/S1C)
   CP-B after T8   (automated multi-tile data-driven trigger + backfill; cube accumulates)
   CP-C  = T9      (acceptance soak → phase done)
```

Vertical slices: T1–T3 improve the existing pipeline independently; **T4+T5 deliver the datacube
model** (P8); T6 adds the trigger; T7/T8 scale; T9 proves it.

---

## Tasks

### Task 1 — Quality-gate step (gate register on validation FAIL)  <status: ready>
**What**: `scripts/validate_s1_rtc.py` CLI (wrap the notebook checks) → exit **0=PASS/1=WARN/2=FAIL**.
Add a `quality-gate` step in the ingest template *between* ingest and register: 2 → fail (no register)
+ alert; 1 → register + annotate; 0 → register. (P1; alert path = OQ #3.)
**Verify**: `uv run pytest tests/unit/test_validate_s1_rtc.py`; cluster: good store registers, corrupted store fails (no item).
**Acceptance**:
- [ ] CLI returns 0/1/2, unit-tested on good + corrupted fixtures
- [ ] FAIL → no item registered; PASS → registered; WARN → registered + annotated
- [ ] FAIL alerts via OQ #3

### Task 2 — Enable S1A + S1C (platform render) + off-platform-download tolerance  <status: ready>
**What**: (a) parametrize `platform_list` in the s1tiling template `sed` render (default `S1A S1C`);
prove s1tiling handles S1C by a direct submit (query-side S1D skip = T6). (b) **Tolerate off-platform
download failures** — the PoC showed s1tiling downloads *all* platforms in the window and exits
non-zero if S1D/S1C downloads fail, even when the requested platform produced output (and that
non-zero exit then skips the S3 sync). Change the s1tiling step's success contract to **"requested-
platform GeoTIFFs present in `data_out/{tile}`"** rather than "S1Processor exit 0".
**Verify**: cluster — live S1C scene (clean window) → A→B → item + `validate_s1_rtc` PASS; a window with a failing S1D download still succeeds (target output synced).
**Acceptance**:
- [ ] S1C scene processes A→B end-to-end; item queryable + PASS
- [ ] `platform_list` rendered from a param (default `S1A S1C`), not hardcoded
- [ ] s1tiling step succeeds (and syncs) when the requested platform produced output despite
      off-platform download failures

### Task 3 — `ensure-dem` auto-fetch step  <status: blocked by OQ #4>
**What**: `scripts/ensure_dem.py` — tile → swath bbox → fetch missing GLO-30 COGs from public
`copernicus-dem-30m` (HTTPS, anon) → rename to `Product10` → rebuild `DEM_Union.gpkg`; idempotent.
Argo `ensure-dem` step before s1tiling, writing the `s1-dem` PVC. Geoid pre-staged.
**Verify**: `uv run pytest tests/unit/test_ensure_dem.py` (bbox/tile-name derivation; skip-existing; bad tile rejected); cluster: a NEW tile provisions + processes; re-run skips.
**Acceptance**:
- [ ] tile→swath-bbox→tile-name derivation unit-tested (malformed tile rejected)
- [ ] previously-unprovisioned tile runs end-to-end, no manual DEM upload
- [ ] idempotent re-run fetches nothing; OQ #4 resolved

### Task 4 — Datacube ingest: append scene to the per-tile cube  <status: ready (PoC-validated)>
**What**: ingest **appends the scene as a new `time` slice into the per-tile cube**
(`s1-rtc-{tile}.zarr`, `sentinel-1-grd-rtc-staging`) via the existing `ingest_s1tiling_acquisition`
(`mode=r+`, PoC-validated); **skip a `time` already present**; serialise per-tile writes with an **Argo
`synchronization` mutex keyed on the tile**. *(Fallback if I2/option 2 stalls: also write a
per-acquisition single-time store named = item-id for reconstruction-based rendering — dual storage.)*
**Verify**: `uv run pytest tests/unit/test_ingest_cube.py` (append into empty/existing cube; duplicate-time skip); cluster: 2 scenes same tile → a 2-time cube; concurrent submit → no corruption.
**Acceptance**:
- [ ] A 2nd acquisition appends a `time` slice (cube has N times), unit-tested
- [ ] Re-ingesting an existing time is a no-op
- [ ] Concurrent same-tile writes serialised (mutex) — no corruption
- [ ] `xr.open_dataset(cube)` exposes all scenes on `time`

### Task 5 — Per-acquisition catalogue (renders via `sel=time`)  <status: blocked by I2/option 2>
**What**: emit **one STAC item per acquisition** (`s1-rtc-{tile}-{datetime}`, single `datetime`,
collection `sentinel-1-grd-rtc-acquisitions`) pointing at the **per-tile cube** via asset href, with
viz/xyz/tilejson links carrying `sel=time=nearest::{datetime}`. **Renders once I2/option 2** (titiler
resolves the store from the href; platform-deploy `ecde99c`) **is deployed + verified** (re-tested
2026-06-08: not yet effective). Reuse the phase-5 builder with per-acquisition ids; also (re)register
the per-tile cube item (`sentinel-1-grd-rtc-staging`) for analysis. No upstream data-model change.
**Verify**: `uv run pytest tests/unit/test_register_v1_s1_rtc.py` (per-acquisition ids + `sel=time` links); cluster (after option 2 live): each item's XYZ/preview renders its slice (HTTP 200); distinct dates → distinct slices.
**Acceptance**:
- [ ] One item per acquisition (id `s1-rtc-{tile}-{datetime}`, single `datetime`), `sel=time` links
- [ ] Per-tile cube item present for analysis; per-acquisition items are the time-series index
- [ ] Renders 200 **once I2/option 2 is live** (until then: blocked, or dual-storage fallback)

> **CP-A (after T1–T5)**: a single discovered product (any tile, S1A/S1C) is quality-gated and
> appended to the tile cube (openable as a datacube), and gets a per-acquisition item that renders its
> slice via `sel=time` **once I2/option 2 is live** (else dual-storage fallback). Model proven on one product.

### Task 6 — Port the CDSE watcher to an Argo CronWorkflow (data-driven trigger)  <status: blocked by 1–5>
**What**: trigger entrypoint (reuse `tile_bbox`/`query_cdse`) queries CDSE for a tile+window, filters
platform to {S1A,S1C} (**skips S1D**, logged), and emits **new** products — dedup = **per-acquisition
STAC item-exists** + cube time-present (P2). CronWorkflow (**6 h**) submits one child Workflow per new
product (decision B), **replacing** the suspended blind cron.
**Verify**: `uv run pytest tests/unit/test_trigger.py` (query→dedup, mocked STAC); cluster: data-bearing window submits only new products; no-data → nothing; re-run → 0.
**Acceptance**:
- [ ] Submits only for acquisitions with no existing per-acquisition item (dedup), unit-tested
- [ ] No-data window → 0 submissions; re-run → 0 (idempotent)
- [ ] S1D filtered at query time (logged), no child Workflow
- [ ] Suspended sub-issue-8 cron retired (one trigger of record)

### Task 7 — Multi-tile AOI iteration  <status: blocked by 3,6; OQ #1>
**What**: CronWorkflow iterates the configured AOI; each tile self-provisions DEM (T3), accumulates its
own cube (T4), queried/deduped independently (T6); per-tile failure isolation.
**Verify**: cluster — cron over ≥2-tile AOI → per-tile child Workflows; new tile auto-provisions DEM; one failure ≠ whole-run failure.
**Acceptance**:
- [ ] Every AOI tile processed; per-tile isolation
- [ ] New AOI tile auto-provisions DEM; AOI config-driven (OQ #1)

### Task 8 — Bounded backfill on enable  <status: blocked by 6; OQ #2>
**What**: trigger accepts a one-time **backfill lookback** (distinct from the 6 h window); dedup +
cube time-present prevent reprocessing; per-tile mutex (T4) serialises the burst into each cube.
**Verify**: cluster — enable with backfill=N days → historical window processed once into the cubes; later runs only forward.
**Acceptance**:
- [ ] Configured lookback processed without duplicate items/time slices
- [ ] Backfill respects the semaphore + per-tile mutex (no thundering herd / no cube corruption)
- [ ] OQ #2 (window) set

> **CP-B (after T8)**: automated, data-driven, multi-tile trigger + backfill; tile cubes accumulate
> over time. Functionally complete — ready to soak.

### Task 9 — Acceptance soak  <status: blocked by 7,8; OQ #1,#3>
**What**: run the full system on staging for the soak window; track success rate.
**Verify**: cluster — ≥14 days × 5 soak tiles; tally success/fail from Argo + STAC; spot-check a tile opens as a multi-time cube.
**Acceptance**:
- [ ] **5 tiles × 14 days × ≥ 95% success** on staging
- [ ] Each soak tile opens as a cube with all its scenes; failures alert (OQ #3) + triaged
- [ ] Outcome recorded on #226; phase done (staging)

---

## Open questions (pinned to tasks)

| OQ | Question | Blocks | Owner |
|----|----------|--------|-------|
| 1 | AOI tile set (+ 5 soak tiles) | T7, T9 | Loïc / science |
| 2 | Backfill lookback window (days) | T8 | Loïc |
| 3 | Alerting path (quality-gate FAIL + persistent failures) | T1, T9 | Loïc / infra |
| 4 | `ensure-dem` discovery (eodag vs direct) + PVC cache layout | T3 | Loïc / me (during T3) |
| 5 | **I2 / option 2** — deploy + verify titiler resolves the store from the item asset href (platform-deploy `ecde99c` adds `TITILER_EOPF_API_ROOT_URL`; re-tested 2026-06-08 **NOT yet effective**). Blocks per-acquisition rendering; fallback = dual storage. | T5 | Loïc / infra (E. Mathot) |

*(Resolved: P8 = shared per-tile cube + per-acquisition items rendering via `sel=time` (option 2); DEM source = public `copernicus-dem-30m`; Spike 3 = titiler currently reconstructs the store + ignores href, so option 2 (`ecde99c`) must land to enable per-acquisition rendering — dual storage is the fallback.)*

---

## Risks & mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Concurrent same-tile cube writes corrupt the Zarr | **High** | per-tile Argo `synchronization` mutex (T4); region writes; time-present skip |
| **I2/option 2 (titiler href resolution) not effective** → per-acquisition rendering blocked | **High** (T5) | `ecde99c` adds `TITILER_EOPF_API_ROOT_URL`; deploy + re-verify (re-tested 2026-06-08: still reconstructs). Fallback = dual storage (per-acq stores named = item-id render via reconstruction). Cube + xarray load unaffected |
| Dual-storage fallback cost (each scene written twice) | Low–Med | only if option 2 stalls; per-acq store is small, cube append incremental |
| S1C has an unforeseen RTC issue despite the orbit table | Med | T2 is a live S1C verify gate before relying on it; fall back to S1A-only |
| s1tiling exits non-zero when off-platform (S1C/S1D) downloads fail, even though target succeeded | Med | tolerate off-platform download failures in the s1tiling step (don't fail the run if the wanted platform produced output) — observed in the PoC scene-2 run |
| `ensure-dem` swath/tile-name derivation wrong → missing DEM | High (silent bad RTC) | unit-test vs known 31TCH set; s1tiling errors on <100% DEM coverage |
| STAC-existence dedup races indexing latency | Low | cube time-present check is the backstop |
| Backfill thundering herd | Med | semaphore + per-tile mutex; cap backfill concurrency (T8) |
| `copernicus-dem-30m` anon HTTPS unavailable/rate-limited | Med (T3) | retry/backoff; PVC cache = one-time per tile |
| Cross-repo drift (image SHA vs template pin) | Med | pin `pipeline_image_version` to merged-`main` SHA (#186 follow-up) |

---

## Done definition

CronWorkflow (6 h, data-driven) submits child Workflows only for new S1A/S1C products across the AOI;
each tile auto-provisions its DEM; ingest **appends each scene to the per-tile cube** (per-tile writes
serialised); ingest is quality-gated (FAIL ⇒ no register); each acquisition has a **per-acquisition
STAC item that renders its slice via `sel=time`** once **I2/option 2** (titiler href resolution) is
live (dual-storage fallback otherwise); a tile opens as a datacube (xarray) regardless; S1D is skipped;
backfill ran once on enable; the acceptance soak (5 × 14 × ≥95%) passes on staging; the blind cron is
retired. Prod promotion is Phase 7.
