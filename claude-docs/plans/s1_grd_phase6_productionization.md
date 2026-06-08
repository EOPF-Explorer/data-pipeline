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
| Ingest = **fresh store per run** | ⚠️ → **dual**: per-acq store + append to per-tile cube (T4); append validated by PoC |
| `eopf_geozarr.build_s1_rtc_stac_item` = one item per store | ✅ reuse as-is with **per-acquisition ids** (T5; no upstream change) |
| titiler store resolution | ⚠️ reconstructs `{base}/{collection}/{item_id}.zarr`, ignores href (Spike 3) → stores must be named = item-id |
| Local watcher query/bbox/dedup logic | ✅ port; dedup → per-acquisition STAC item-exists (T6) |
| Blind daily cron + sensor (sub-issue 8) | ✅ shipped, suspended — replaced by the data-driven trigger (T6) |

---

## Dependency graph

```
merged templates + spec P1–P8
   ├── T1 quality-gate step (ingest) ─────────────┐
   ├── T2 platform S1A+S1C (render) ──────────────┤
   ├── T3 ensure-dem auto-fetch ──────────────────┤
   ├── T4 dual ingest (per-acq store + cube append + mutex) ─┤   ┐ P8 dual storage
   └── T5 per-acquisition catalogue (renders directly) ──────┘ (T4 → T5)
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

### Task 2 — Enable S1A + S1C (platform render)  <status: ready>
**What**: parametrize `platform_list` in the s1tiling template `sed` render (default `S1A S1C`); prove
s1tiling handles S1C by a direct submit. (Query-side S1D skip = T6.)
**Verify**: cluster — live S1C scene (31TCH 2026-06-06) → A→B → item + `validate_s1_rtc` PASS.
**Acceptance**:
- [ ] S1C scene processes A→B end-to-end; item queryable + PASS
- [ ] `platform_list` rendered from a param (default `S1A S1C`), not hardcoded

### Task 3 — `ensure-dem` auto-fetch step  <status: blocked by OQ #4>
**What**: `scripts/ensure_dem.py` — tile → swath bbox → fetch missing GLO-30 COGs from public
`copernicus-dem-30m` (HTTPS, anon) → rename to `Product10` → rebuild `DEM_Union.gpkg`; idempotent.
Argo `ensure-dem` step before s1tiling, writing the `s1-dem` PVC. Geoid pre-staged.
**Verify**: `uv run pytest tests/unit/test_ensure_dem.py` (bbox/tile-name derivation; skip-existing; bad tile rejected); cluster: a NEW tile provisions + processes; re-run skips.
**Acceptance**:
- [ ] tile→swath-bbox→tile-name derivation unit-tested (malformed tile rejected)
- [ ] previously-unprovisioned tile runs end-to-end, no manual DEM upload
- [ ] idempotent re-run fetches nothing; OQ #4 resolved

### Task 4 — Dual-storage ingest: per-acquisition store + per-tile cube append  <status: ready (PoC-validated mechanics)>
**What**: ingest writes **two** artifacts per scene (P8): (a) a **per-acquisition single-time store**
named = its item id (`s1-rtc-{tile}-{datetime}.zarr`) for rendering, and (b) **append the scene as a
new `time` slice into the per-tile cube** (`s1-rtc-{tile}.zarr`) for analysis (Zarr region/append;
**skip a time already present**). Serialise cube writes with an **Argo `synchronization` mutex keyed on
the tile**. The PoC proved the append works via the existing `ingest_s1tiling_acquisition` (`mode=r+`);
the per-acquisition store is the proven phase-5 single-acquisition store.
**Verify**: `uv run pytest tests/unit/test_ingest_dual.py` (per-acq store + cube append; duplicate-time skip); cluster: 2 scenes same tile → 2 per-acq stores + a 2-time cube; concurrent submit → no corruption.
**Acceptance**:
- [ ] Each scene yields its own single-time store (name = item id) **and** a `time` slice in the cube
- [ ] Re-ingesting an existing time is a no-op (no duplicate slice/store)
- [ ] Concurrent same-tile cube writes serialised (mutex) — no corruption
- [ ] `xr.open_dataset(cube)` exposes all scenes on `time`

### Task 5 — Per-acquisition catalogue (renders directly)  <status: ready>
**What**: emit **one STAC item per acquisition** (`s1-rtc-{tile}-{datetime}`, single `datetime`)
pointing at its **per-acquisition store** → **renders directly** (store-name = item-id matches titiler
reconstruction; **no `sel=time`, no titiler change** — Spike 3). Reuse the phase-5 builder with
per-acquisition ids; `register_v1_s1_rtc.py` upserts each. Also (re)register the per-tile cube item
for analysis. **No upstream data-model change required.**
**Verify**: `uv run pytest tests/unit/test_register_v1_s1_rtc.py` (per-acquisition ids); cluster: each item's XYZ/preview renders (HTTP 200); distinct dates → distinct items.
**Acceptance**:
- [ ] One item per acquisition (id `s1-rtc-{tile}-{datetime}`, single `datetime`), renders directly (200)
- [ ] Per-tile cube item present for analysis; per-acquisition items are the time-series index
- [ ] No `sel=time` / titiler dependency on the per-acquisition render path

> **CP-A (after T1–T5)**: a single discovered product (any tile, S1A/S1C) yields a per-acquisition
> store + item that **renders directly**, is quality-gated, and is also appended to the tile cube
> (openable as a datacube). The dual-storage model is proven on one product.

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
| 5 | Per-acquisition store path must match titiler's reconstructed `{base}/{collection}/{item_id}.zarr` — confirm the titiler store base + collection for the per-acquisition collection | T4, T5 | Loïc / infra |

*(Resolved: P8 = dual storage; DEM source = public `copernicus-dem-30m`; Spike 3 = titiler reconstructs store from `{base}/{collection}/{item_id}.zarr` (ignores href) → per-acq stores named = item-id render directly, no titiler change. Dual-storage write cost accepted.)*

---

## Risks & mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Concurrent same-tile cube writes corrupt the Zarr | **High** | per-tile Argo `synchronization` mutex (T4); region writes; time-present skip |
| Dual-storage cost (each scene written twice) | Med | accepted (P8); per-acq store is small; cube append is incremental. If titiler later resolves the asset href (I-8 Option A), collapse to single storage |
| Per-acquisition store not at titiler's reconstructed path → 500 | **High** | stores MUST land at `{titiler-base}/{collection}/{item_id}.zarr` (store-name = item-id); validate one render early (PoC showed the reconstruction rule) |
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
each tile auto-provisions its DEM; ingest writes a **per-acquisition store** (renders directly) **and
appends to a per-tile cube** (per-tile writes serialised); ingest is quality-gated (FAIL ⇒ no
register); each acquisition has a **per-acquisition STAC item that renders directly** (store-name =
item-id, no titiler change); S1D is skipped; backfill ran once on enable; the acceptance soak
(5 × 14 × ≥95%) passes on staging and a tile opens as a datacube; the blind cron is retired. Prod
promotion is Phase 7.
