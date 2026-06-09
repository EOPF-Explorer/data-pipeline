# Plan: S1 GRD RTC Productionization — Phase 6 (`claude-docs/specs/s1_grd_phase6_productionization.md`)

**Goal**: an automated, multi-tile, in-cluster S1 GRD RTC service on **staging** — a data-driven
trigger discovers new CDSE S1A/S1C products, auto-provisions the DEM per tile, runs
s1tiling → ingest → quality-gate → register, storing each tile as a **multi-temporal datacube** with
**per-acquisition STAC items** (P8). **Tracked in #226.**
**Constraint**: keep Script A/B + merged Argo templates as the single source of pipeline logic (new
work *orchestrates*, *gates*, and *accumulates the cube*); path-guard every destructive op; serialise
per-tile cube writes; tests at each new-code boundary; **staging only** (prod = Phase 7).

> **Repo split**: plan authored here; new **scripts** (`ensure_dem.py`, `validate_s1_rtc.py`, cube
> time-slice append, per-acquisition register, trigger entrypoint) ship in **data-pipeline** (reusing
> the existing `eopf_geozarr` builder with per-acquisition ids — no upstream data-model change); new
> **Argo manifests** (CronWorkflow, ensure-dem + quality-gate steps, per-tile mutex) ship in
> **platform-deploy**. Branch off `main` once phase-5 (#186 + platform-deploy #207/#208) is merged.

---

## Current state

| Resource | Status |
|----------|--------|
| `eopf-explorer-s1tiling` / `ingest-v1-s1rtc` WorkflowTemplates | ✅ merged (PR#207) |
| s1tiling cfg render (tile/orbit/date) | ✅ exists; `platform_list` not parametrized (T2) |
| `s1-dem` PVC (31TCH only, manual) + EGM2008 geoid | ✅ bound; **`scripts/ensure_dem.py` auto-fetch shipped** (T3 branch); Argo `ensure-dem` step + cluster run pending |
| Ingest = **fresh store per run** | ⚠️ → **append scene to per-tile cube** (T4); append validated by PoC |
| `eopf_geozarr.build_s1_rtc_stac_item` = one item per store | ✅ reuse with **per-acquisition ids** + `sel=time` links (T5; no upstream change) |
| `scripts/validate_s1_rtc.py` quality-gate CLI (T1) | ✅ shipped (#229); unit-tested + **verified tile-agnostic** on 31TDH; Argo step pending |
| `scripts/register_per_acquisition.py` per-acq register (T5) | ✅ shipped (#229); unit-tested + live (31TCH ×2 items); P-3 multi-time notebook pending |
| titiler store resolution | ⚠️ reconstructs + ignores href (Spike 3); explorer rendering **deferred (#228)** — not a phase blocker (validation is via notebook) |
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
   └── T5 per-acquisition catalogue + notebook validation ────┘ (T4 → T5; render deferred)
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

### Task 1 — Quality-gate step (gate register on validation FAIL)  <status: CLI shipped (#229); Argo step pending>
**What**: `scripts/validate_s1_rtc.py` CLI (wrap the notebook checks) → exit **0=PASS/1=WARN/2=FAIL**.
Add a `quality-gate` step in the ingest template *between* ingest and register: 2 → fail (no register)
+ alert; 1 → register + annotate; 0 → register. (P1; alert path = OQ #3.)
**Verify**: `uv run pytest tests/unit/test_validate_s1_rtc.py`; cluster: good store registers, corrupted store fails (no item).
**Acceptance**:
- [x] CLI returns 0/1/2, unit-tested on good + corrupted fixtures — `scripts/validate_s1_rtc.py`
      (17 tests in `test_validate_s1_rtc.py`); **verified tile-agnostic** on a live cross-tile run:
      31TDH store → OVERALL WARN (EPSG:32631, vv/vh 100% finite, dB p2..p98 plausible, orbit auto-
      detected; WARN only from benign `S1RtcRoot` coord drift — same signature as 31TCH). 2026-06-09.
- [ ] FAIL → no item registered; PASS → registered; WARN → registered + annotated *(Argo step — pending)*
- [ ] FAIL alerts via OQ #3 *(Argo step — pending)*

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

### Task 3 — `ensure-dem` auto-fetch step  <status: script shipped (T3 branch); Argo step + cluster run pending>
**What**: `scripts/ensure_dem.py` — tile → swath bbox (lon±4°/lat±1.5° margin) → integer GLO-30 cells
→ keep the land cells the **static `eotile` `DEM_Union.gpkg`** knows (ocean cells don't exist in
GLO-30) → skip cells already on disk → download the rest from public anon `copernicus-dem-30m`,
renaming each COG to the `Product10` stem s1tiling matches. (`DEM_Union.gpkg` is *staged once* from
`eotile`, not rebuilt per tile.) Argo `ensure-dem` step before s1tiling, writing the `s1-dem` PVC.
Geoid pre-staged.
**Verify**: `uv run pytest tests/unit/test_ensure_dem.py` (10 tests: derivation, naming, gpkg read, skip-existing, bad tile rejected); cluster: a NEW tile provisions + processes; re-run skips.
**Acceptance**:
- [x] tile→swath-bbox→tile-name derivation unit-tested (malformed tile rejected) — `scripts/ensure_dem.py`;
      convention ground-truthed vs the real `eotile` gpkg: 31TCH → **41 land cells incl N44W001/N44W002**
      (the cells phase-5 saw the swath need), matching the ≥31-tile 31TCH PVC; anon HEAD on
      `copernicus-dem-30m` confirmed key/region (42.6 MB tiff). 2026-06-09.
- [ ] previously-unprovisioned tile runs end-to-end, no manual DEM upload *(cluster — pending)*
- [x] idempotent re-run fetches nothing — `tiles_to_fetch` excludes on-disk stems (verified: full set present → `[]`)
- [x] **OQ #4 resolved** (script-side): direct anon HTTPS from `copernicus-dem-30m`; static `eotile`
      gpkg (no eodag); configurable margin (lon±4/lat±1.5); skip-existing for idempotency. PVC cache
      *layout* is an Argo-manifest detail (platform-deploy).

### Task 4 — Datacube ingest: append scene to the per-tile cube  <status: ready (PoC-validated)>
**What**: ingest **appends the scene as a new `time` slice into the per-tile cube**
(`s1-rtc-{tile}.zarr`, `sentinel-1-grd-rtc-staging`) via the existing `ingest_s1tiling_acquisition`
(`mode=r+`, PoC-validated); **skip a `time` already present**; serialise per-tile writes with an **Argo
`synchronization` mutex keyed on the tile**. Single shared cube (rendering deferred → no per-acquisition
stores).
**Verify**: `uv run pytest tests/unit/test_ingest_cube.py` (append into empty/existing cube; duplicate-time skip); cluster: 2 scenes same tile → a 2-time cube; concurrent submit → no corruption.
**Acceptance**:
- [ ] A 2nd acquisition appends a `time` slice (cube has N times), unit-tested
- [ ] Re-ingesting an existing time is a no-op
- [ ] Concurrent same-tile writes serialised (mutex) — no corruption
- [ ] `xr.open_dataset(cube)` exposes all scenes on `time`

### Task 5 — Per-acquisition catalogue + notebook validation  <status: register CLI shipped (#229); P-3 multi-time notebook pending>
**What**: emit **one queryable STAC item per acquisition** (`s1-rtc-{tile}-{datetime}`, single
`datetime`, collection `sentinel-1-grd-rtc-acquisitions`) indexing its cube slice (asset href + time);
also (re)register the per-tile cube item (`sentinel-1-grd-rtc-staging`). Reuse the phase-5 builder with
per-acquisition ids (no upstream change). **Validate via the `validate_s1_grd_rtc` notebook** (open the
cube → PASS) + STAC queries. **Explorer rendering deferred (I2/option 2)** — bake `sel=time` links into
the items now so they render later when option 2 lands, with **no data change**.
**Verify**: `uv run pytest tests/unit/test_register_per_acquisition.py` (per-acquisition ids); cluster: items queryable in STAC; `validate_s1_grd_rtc` notebook returns PASS against the cube; distinct dates → distinct items.
**Acceptance**:
- [x] One queryable item per acquisition (id `s1-rtc-{tile}-{datetime}`); per-tile cube item present —
      `scripts/register_per_acquisition.py` (6 tests in `test_register_per_acquisition.py`); live:
      registered `s1-rtc-31TCH-20260605t060907` + `s1-rtc-31TCH-20260607t055248` in
      `sentinel-1-grd-rtc-acquisitions`, per-tile `s1-rtc-31TCH` present in `…-staging`. Builder is
      tile/orbit-parametrized (tile-agnostic by construction; confirmed alongside T1's 31TDH run).
- [ ] **`validate_s1_grd_rtc` confirmed/extended for a *multi-time* cube** (it was written for a
      single-acquisition store — verify it validates every `time`, or update it) and returns **PASS** *(P-3 — pending)*
- [x] `sel=time` links baked in (explorer render deferred to #228; no later data change) —
      `sel_time_tilejson()` / `per_acquisition_items()`, unit-tested

> **CP-A (after T1–T5)**: a single discovered product (any tile, S1A/S1C) is quality-gated, **appended
> to the tile cube** (which opens as a datacube), and gets a **queryable per-acquisition STAC item**;
> the `validate_s1_grd_rtc` notebook **PASSes** against the cube. Model proven on one product.
> (Explorer rendering deferred — #228.)
>
> **"Run the whole chain on a *different* tile" lives here (CP-A).** It was gated solely on the DEM:
> T1 (gate) is tile-agnostic (verified on 31TDH), T5 (register) is tile-parametrized, and **T3 now
> auto-provisions the DEM for any tile** — so a new tile no longer needs a manual DEM upload. A
> *single-acquisition* CP-A on a new tile needs only **T3** (fresh cube, no append); making it a true
> *multi-time* datacube exercises **T4** on the 2nd acquisition. **CP-A-local** (run before the Argo
> wiring): `ensure_dem.py` → `run_s1tiling.py` → ingest → `validate_s1_rtc.py` → `register_per_acquisition.py`
> on a chosen tile (tile choice = OQ #1).

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
**Verify**: cluster — ≥14 days × 5 soak tiles; tally success/fail from Argo + STAC; run `validate_s1_grd_rtc` on each soak tile's cube.
**Acceptance**:
- [ ] **5 tiles × 14 days × ≥ 95% success** on staging
- [ ] Each soak tile opens as a cube with all its scenes **and `validate_s1_grd_rtc` PASSes**;
      failures alert (OQ #3) + triaged
- [ ] Outcome recorded on #226; phase done (staging)

---

## Open questions (pinned to tasks)

| OQ | Question | Blocks | Owner |
|----|----------|--------|-------|
| 1 | AOI tile set (+ 5 soak tiles) | T7, T9 | Loïc / science |
| 2 | Backfill lookback window (days) | T8 | Loïc |
| 3 | Alerting path (quality-gate FAIL + persistent failures) | T1, T9 | Loïc / infra |
| 4 | ~~`ensure-dem` discovery (eodag vs direct) + PVC cache layout~~ **RESOLVED (script-side, 2026-06-09)**: direct anon HTTPS from `copernicus-dem-30m`; static `eotile` `DEM_Union.gpkg` (no eodag); margin lon±4/lat±1.5; skip-existing. PVC cache *layout* → Argo manifest (platform-deploy). | T3 ✅ | Loïc / me |
| 5 | **Titiler rendering (I2/option 2) — DEFERRED**, tracked in its own follow-up issue (see below). Not a phase-6 blocker; per-acquisition items render later with no data change. | — (deferred) | Loïc / infra |

*(Resolved: P8 = shared per-tile cube + per-acquisition items (queryable index), validated by the `validate_s1_grd_rtc` notebook; explorer/titiler rendering **deferred** off the critical path; DEM source = public `copernicus-dem-30m`; Spike 3 = titiler reconstructs the store + ignores href, so a titiler change (I2/option 2, `ecde99c`) is needed for rendering — tracked separately.)*

---

## Deferred follow-ups (create tracking issues)

### Task D1 — **Create a tracking issue: "S1 GRD RTC — titiler rendering support (I2/option 2)"**  <status: DONE — #228>
**What**: open a GitHub issue (linked from #226) capturing everything needed to turn on explorer
rendering of the per-acquisition items, deferred out of phase 6. The issue body should list:
- **Make titiler resolve the store from the STAC item asset href** (not path reconstruction) — verify
  whether platform-deploy `ecde99c` (`TITILER_EOPF_API_ROOT_URL`) achieves it once deployed
  (re-tested 2026-06-08: still reconstructs `…fra/tests-output/{collection}/{item_id}.zarr`); if not,
  land titiler-eopf I-8 Option A (resolve from `asset_info["href"]`/`alternate`).
- **Verify `sel=time` slice rendering** end-to-end: a per-acquisition item renders *its* slice of the
  multi-time cube (HTTP 200, correct slice) — the phase-6 items already bake `sel=time` links.
- **Per-mission bucket addressing**: confirm titiler can resolve stores across per-mission buckets
  (the single `TITILER_EOPF_STORE_URL` base can't — this is why href resolution is needed).
- Re-run `analysis/poc_s1_datacube_hybrid.py` render checks as the acceptance gate.
**Verify**: issue exists, linked from #226, with the checklist above.
**Acceptance**:
- [x] Tracking issue created (#228) + linked from #226; referenced in the spec

---

## Risks & mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Concurrent same-tile cube writes corrupt the Zarr | **High** | per-tile Argo `synchronization` mutex (T4); region writes; time-present skip |
| Titiler rendering (I2/option 2) not yet effective | **Low** (deferred) | rendering is out of scope for phase 6 — tracked in Task D1; per-acquisition items pre-bake `sel=time` so they render later with no data change; cube + xarray + notebook validation are unaffected |
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
serialised); ingest is quality-gated (FAIL ⇒ no register); each acquisition has a **queryable
per-acquisition STAC item**; a tile **opens as a datacube (xarray)** and the **`validate_s1_grd_rtc`
notebook returns PASS**; S1D is skipped; backfill ran once on enable; the acceptance soak
(5 × 14 × ≥95%) passes on staging. Explorer/titiler rendering is **deferred** (tracked in Task D1);
prod promotion is Phase 7. The blind cron is retired.
