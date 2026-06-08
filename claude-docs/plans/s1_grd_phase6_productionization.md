# Plan: S1 GRD RTC Productionization — Phase 6 (`claude-docs/specs/s1_grd_phase6_productionization.md`)

**Goal**: an automated, multi-tile, in-cluster S1 GRD RTC service on **staging** — a data-driven
trigger discovers new CDSE S1A/S1C products, auto-provisions the DEM per tile, and runs
s1tiling → ingest → quality-gate → register. **Tracked in #226.**
**Constraint**: keep Script A/B + the merged Argo templates as the single source of pipeline logic
(new work only *orchestrates* and *gates*); path-guard every destructive op; tests at each new-code
boundary; **staging only** (prod = Phase 7).

> **Repo split** (as in `subissue_8_cron_sensor.md`): plan authored here (spec lives here); new
> **scripts** (`ensure_dem.py`, `validate_s1_rtc.py`, trigger entrypoint) ship in **data-pipeline**
> and bake into the image; new **Argo manifests** (CronWorkflow, ensure-dem step, quality-gate step)
> ship in **platform-deploy** (`workspaces/devseed-staging/data-pipeline/`). Branch off `main` in each
> repo *after* the phase-5 PRs (#186 + platform-deploy #207/#208) are merged.

---

## Current state

| Resource | Status |
|----------|--------|
| `eopf-explorer-s1tiling` / `ingest-v1-s1rtc` WorkflowTemplates | ✅ merged (PR#207) — child Workflows for decision B |
| s1tiling cfg render (tile/orbit/date) in template | ✅ exists; **platform_list not yet parametrized** (Task 2) |
| `s1-dem` PVC (31TCH swath only, manual) + EGM2008 geoid | ✅ bound; **no auto-fetch** (Task 3) |
| Local watcher `watch_cdse_and_process.py` (query/bbox/dedup) | ✅ logic to port; dedup is file-based → STAC-existence (Task 4) |
| `validate_s1_grd_rtc` notebook (PASS/WARN/FAIL checks) | ✅ logic to wrap as a CLI gate (Task 1) |
| Blind daily cron + webhook sensor (sub-issue 8) | ✅ shipped, **suspended** — replaced by the data-driven trigger (Task 4) |
| Data-driven trigger / ensure-dem / quality-gate steps | ❌ this plan |

---

## Dependency graph

```
merged templates (s1tiling, ingest)            spec decisions (P1–P7)
        │                                              │
        ├── Task 1  quality-gate step (ingest)  ───────┐
        ├── Task 2  platform S1A+S1C (render)   ───────┤
        ├── Task 3  ensure-dem auto-fetch       ──┐    │
        │                                         ▼    ▼
        └── Task 4  trigger → Argo (query→dedup→submit) ──► Task 5  multi-tile AOI ──┐
                                  │                                                  ├─► Task 7  soak
                                  └── Task 6  bounded backfill ──────────────────────┘
        CP-A after T1–T3 (single product, any tile, S1A+S1C, gated)
        CP-B after T6   (full automated data-driven multi-tile + backfill)
        CP-C  = Task 7  (acceptance soak → phase done)
```

Build order favours **vertical slices that ship value early**: each of T1–T3 improves the *existing*
single-tile pipeline independently; T4 adds the trigger; T5/T6 scale it; T7 proves it.

---

## Tasks

> Each task: code/manifest + tests; cluster `Verify` steps are run by an operator (no cluster in the
> dev env). Unit tests for new **scripts** run in CI (`uv run pytest`).

### Task 1 — Quality-gate step (gate register on validation FAIL)  <status: ready>
**What**: wrap the `validate_s1_grd_rtc` checks into `scripts/validate_s1_rtc.py` — a CLI taking a
store URI, exiting **0=PASS / 1=WARN / 2=FAIL** with a structured summary. Add a `quality-gate` step
to the **ingest template** *between* ingest and register: exit 2 → fail the workflow (no register) +
alert; exit 1 → register + annotate; exit 0 → register. (Mirrors spec P1; **needs OQ #3** for the
alert path.)
**Verify**:
```bash
uv run pytest tests/unit/test_validate_s1_rtc.py          # PASS/WARN/FAIL on fixtures
# cluster: run ingest on a good store (registers) and a corrupted store (fails, no item)
```
**Acceptance criteria**:
- [ ] CLI returns 0/1/2 for PASS/WARN/FAIL, unit-tested on good + corrupted fixtures
- [ ] FAIL → workflow fails, **no STAC item registered**; PASS → item registered
- [ ] WARN → item registered with an annotation
- [ ] FAIL emits an alert via the OQ #3 path

### Task 2 — Enable S1A + S1C (platform render)  <status: ready>
**What**: parametrize `platform_list` in the s1tiling template's `sed` render (default `S1A S1C`), so
s1tiling will process an S1C scene. (Spec §3; spike confirms S1C is in the orbit table, S1D is not.)
The query-side **S1D skip** lives in the trigger (Task 4) — this task only proves s1tiling itself
handles S1C, by a direct submit.
**Verify**:
```bash
# cluster: submit s1tiling for a live S1C scene (e.g. 31TCH 2026-06-06) → A→B → item + validation PASS
```
**Acceptance criteria**:
- [ ] An **S1C** scene processes A→B end-to-end; item queryable + `validate_s1_rtc` PASS
- [ ] `platform_list` is rendered from a param (not hardcoded), default `S1A S1C`

### Task 3 — `ensure-dem` auto-fetch step (any tile)  <status: blocked by OQ #4>
**What**: `scripts/ensure_dem.py` — given a tile, compute the S1 IW **swath** bbox, fetch missing
GLO-30 COGs from the public **`copernicus-dem-30m`** bucket over HTTPS (anon), rename to the
`Product10` convention, (re)build `DEM_Union.gpkg`; **idempotent** (skip cached tiles). Add an
`ensure-dem` step before s1tiling writing to the `s1-dem` PVC. Geoid is pre-staged (not fetched).
**Verify**:
```bash
uv run pytest tests/unit/test_ensure_dem.py     # tile→bbox→tile-name derivation; skip-existing; bad tile rejected
# cluster: run for a NEW tile (not 31TCH) → DEM tiles appear, s1tiling proceeds; re-run skips
```
**Acceptance criteria**:
- [ ] Tile → swath-bbox → GLO-30 tile-name derivation, unit-tested (incl. malformed tile rejected)
- [ ] A previously-unprovisioned tile runs end-to-end with **no manual DEM upload**
- [ ] Idempotent: re-run fetches nothing; `DEM_Union.gpkg` consistent
- [ ] OQ #4 resolved (discovery method; PVC cache layout) and reflected in the step

> **CP-A (after T1–T3)**: a single discovered product, *any* tile, S1A or S1C, runs end-to-end on
> staging and is quality-gated. The pipeline is no longer 31TCH/S1A-bound. Smallest shippable phase-6 slice.

### Task 4 — Port the CDSE watcher to an Argo CronWorkflow (data-driven trigger)  <status: blocked by 1,2,3>
**What**: a trigger entrypoint (reuse `tile_bbox`/`query_cdse` from `watch_cdse_and_process.py`) that
queries CDSE for a tile+window, **filters platform to {S1A,S1C} (skips S1D, logged)**, and emits the
**new** products — dedup switched from the local JSON state file to a **STAC item-exists check**
(spec P2). A **CronWorkflow** (every **6 h**) runs the query step and submits one child `Workflow` per
new product via `workflowTemplateRef` (decision B), **replacing** the suspended blind cron.
**Verify**:
```bash
uv run pytest tests/unit/test_trigger.py        # query→dedup (mocked STAC); new vs already-registered
# cluster: argo submit --from cronworkflow/... over a data-bearing window → submits only NEW products;
#          no-data window → submits nothing; immediate re-run → 0 submitted (existence-dedup)
```
**Acceptance criteria**:
- [ ] Submits a child Workflow **only** for products with no existing STAC item (dedup), unit-tested
- [ ] No-data window submits nothing (zero blind s1tiling runs)
- [ ] Re-run over the same window submits 0 (idempotent)
- [ ] S1D products are filtered out at query time with a logged reason (no child Workflow)
- [ ] The suspended sub-issue-8 cron is retired/replaced (one trigger of record)

### Task 5 — Multi-tile AOI iteration  <status: blocked by 3,4; OQ #1>
**What**: the CronWorkflow iterates the configured **AOI** tile set; each tile self-provisions its DEM
(T3) and is queried/deduped/submitted independently (T4). Per-tile failure must not abort the others.
**Verify**:
```bash
# cluster: cron over a ≥2-tile AOI → per-tile child Workflows; a never-seen tile auto-provisions DEM;
#          one tile failing does not stop the others
```
**Acceptance criteria**:
- [ ] Cron processes every tile in the AOI; per-tile isolation (one failure ≠ whole-run failure)
- [ ] A new AOI tile auto-provisions its DEM (no manual step)
- [ ] AOI is config-driven (OQ #1 set)

### Task 6 — Bounded backfill on enable  <status: blocked by 4; OQ #2>
**What**: the trigger accepts a **backfill lookback** (distinct from the steady-state 6 h window) used
once on enable; existence-dedup (P2) prevents reprocessing already-registered items.
**Verify**:
```bash
# cluster: enable with backfill=N days → the historical window is processed once;
#          subsequent scheduled runs only pick up forward products
```
**Acceptance criteria**:
- [ ] The configured lookback is processed on enable **without duplicating** forward items
- [ ] Backfill volume respects the concurrency limits (no thundering herd) — semaphore honoured
- [ ] OQ #2 (window size) set

> **CP-B (after T6)**: full automated, data-driven, multi-tile trigger with backfill running on
> staging. Functionally complete — ready to soak.

### Task 7 — Acceptance soak  <status: blocked by 5,6; OQ #1,#3>
**What**: run the full system on staging for the soak window; track success rate per the spec bar.
**Verify**:
```bash
# cluster: observe ≥ 14 days across the 5 soak tiles; tally workflow success/fail from Argo + STAC
```
**Acceptance criteria**:
- [ ] **5 tiles × 14 days × ≥ 95% success** on staging
- [ ] Failures alert via the OQ #3 path and are triaged (no silent drops)
- [ ] Outcome recorded on #226; phase declared done (staging)

---

## Open questions (pinned to tasks)

| OQ | Question | Blocks | Owner |
|----|----------|--------|-------|
| 1 | AOI tile set (+ the 5 soak tiles) | T5, T7 | Loïc / science |
| 2 | Backfill lookback window (days) | T6 | Loïc |
| 3 | Alerting path (quality-gate FAIL + persistent failures) | T1, T7 | Loïc / infra |
| 4 | `ensure-dem` discovery (eodag earth_search vs direct names) + PVC cache layout | T3 | Loïc / me, during T3 |

*(Resolved in spec §7: DEM source = public `copernicus-dem-30m` over HTTPS, no auth.)*

---

## Risks & mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| S1C has an unforeseen RTC issue despite being in the orbit table | Med — Task 2 stalls | Task 2 is a *live* S1C verify gate before relying on it; fall back to S1A-only if it fails |
| `ensure-dem` swath-bbox / tile-name derivation wrong → missing DEM coverage | High — silent bad RTC | Unit-test the derivation against the known 31TCH tile set; s1tiling already errors on <100% DEM coverage |
| STAC-existence dedup races indexing latency → duplicate submit | Low | ingest skip-gate is the backstop; child Workflow self-skips (exit-2 contract) |
| Backfill thundering herd overwhelms the cluster | Med | honour the `v1-s1rtc`/cron semaphores; cap backfill concurrency (Task 6) |
| `copernicus-dem-30m` anon HTTPS unavailable/rate-limited in-cluster | Med — Task 3 | retry/backoff in `ensure_dem.py`; cache on the PVC so it's a one-time cost per tile |
| Cross-repo drift (data-pipeline image vs platform-deploy template pin) | Med | pin `pipeline_image_version` to the merged-`main` SHA; document the bump (per #186 follow-up) |

## Done definition

CronWorkflow (6 h, data-driven) submits child Workflows only for new S1A/S1C products across the AOI;
each tile auto-provisions its DEM; ingest is quality-gated (FAIL ⇒ no register); S1D is skipped;
backfill ran once on enable; the acceptance soak (5 × 14 × ≥95%) passes on staging; the blind cron is
retired. Prod promotion is Phase 7.
