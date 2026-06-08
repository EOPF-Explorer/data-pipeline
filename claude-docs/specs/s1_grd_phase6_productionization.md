# Spec: S1 GRD RTC Productionization (Phase 6)

**Status**: Draft (Define stage) — 2026-06-08
**Builds on**: `s1_grd_phase5_subissues.md` (local prototype + Argo templates),
`s1_grd_STACregisration_and_argo_pipelines.md`, `spike_s1tiling_platform_support.md` (S1C/S1D finding).

---

## 1. Objective

Move the S1 GRD RTC pipeline from the verified **single-tile (31TCH) / S1A** prototype to an
**automated, multi-tile, in-cluster production service**: a data-driven trigger discovers new CDSE
S1 GRD products, provisions the DEM for any requested tile automatically, and runs
s1tiling → ingest → register, promoting outputs along a staging → prod path.

**Target users**: the EOPF Explorer platform (STAC catalogue + raster viewer consumers) and the
devseed operators who run/monitor the pipeline.

### Resolved decisions (2026-06-08, confirmed with Loïc)
| # | Decision | Implication |
|---|----------|-------------|
| Goal | Productionize S1 GRD RTC in-cluster (automated, multi-tile, staging→prod) | Drives the whole spec |
| Platform | **S1A + S1C**, skip **S1D** | S1C enabled config-only; S1D skipped until upstream s1tiling support (no release/dev branch has it — `spike_s1tiling_platform_support.md`) |
| Trigger | **Port the CDSE watcher into Argo** (data-driven) | A CronWorkflow runs query→dedup→submit; no blind-day s1tiling |
| Tiles/DEM | **Automated DEM-fetch step** | A pipeline step fetches/builds the Copernicus DEM GLO-30 for any requested MGRS tile; no manual DEM upload |

---

## 2. Scope

### In scope
- **Data-driven trigger in Argo**: a CronWorkflow that runs the watcher logic (per-tile bbox → CDSE
  STAC query → dedup → submit a child Workflow per *new* product). Reuses sub-issue 8's composition
  decision B (child `Workflow`s via `workflowTemplateRef`).
- **Automated DEM-fetch**: a template/step that ensures the DEM swath + `dem_db` for the requested
  tile exists before s1tiling (fetches Copernicus DEM GLO-30 tiles intersecting the MGRS square).
- **Multi-platform**: render `platform_list = "S1A S1C"` into the s1tiling cfg; the trigger skips S1D
  (logged), so daily runs don't attempt-and-fail S1D scenes.
- **Multi-tile**: trigger iterates a configured tile set (AOI); each tile self-provisions its DEM.
- **Bounded historical backfill** on enable (configurable lookback — OQ #4), alongside forward
  processing (P5).
- **In-cluster dedup/idempotency** via STAC item-exists check (P2 — replaces the local JSON state).
- **Quality gate** before register (P1).

### Out of scope (deferred, flagged)
- **Staging → prod promotion** — **staging-only this phase** (P3); prod bucket/collection + cutover
  authority deferred to a later phase.
- **S1D** — no s1tiling version supports it; track an upstream issue. Trigger skips it meanwhile.
- **Other missions (S2/S3/S4)** — generalizing the trigger/ingest pattern is a separate effort;
  design with reuse in mind but do not build it here.

---

## 3. Architecture (proposed)

```
CronWorkflow (data-driven trigger, every N hours)
  └─ for each tile in AOI:
       query CDSE STAC (bbox, lookback, orbit, platform∈{S1A,S1C})  ──► new products?
          └─ for each NEW product (not already in STAC):           ──► submit child Workflow:
               ┌─────────────────────────────────────────────────────────────┐
               │ ensure-dem (fetch GLO-30 for tile)  →  s1tiling  →  ingest  →  register │
               │                                                     └─ quality gate ┘   │
               └─────────────────────────────────────────────────────────────┘
```

- **Trigger**: the local `watch_cdse_and_process.py` query/dedup logic is ported into a container
  step the CronWorkflow runs; it submits child Workflows rather than calling subprocesses. The blind
  daily cron (sub-issue 8) is retired/replaced once this is live.
- **ensure-dem step**: automates the phase-1 DEM recipe per tile — compute the S1 IW **swath** bbox
  for the MGRS tile, fetch the GLO-30 COGs from the **public AWS Open Data bucket
  `s3://copernicus-dem-30m/`** over HTTPS (no auth — see §7 Resolved), rename to the `Product10`
  convention, rebuild `DEM_Union.gpkg`, and cache on the `s1-dem` PVC (idempotent: skip tiles already
  cached). The EGM2008 geoid is a one-time shared asset, not per-tile.
- **Platform**: cfg render extended to set `platform_list`; trigger filters its CDSE query to
  `S1A, S1C` and explicitly skips S1D with a log line.
- **Storage**: GeoTIFFs + Zarr to the S1 staging bucket; prod promotion path per P3.

---

## 4. Success criteria (proposed — review)

- [ ] Data-driven trigger submits a child Workflow **only** for genuinely new CDSE products
      (zero s1tiling runs on no-data days).
- [ ] A **previously-unprocessed tile** runs end-to-end with **no manual DEM upload** (ensure-dem works).
- [ ] An **S1C** scene processes A→B end-to-end; item queryable + validation PASS.
- [ ] **S1D** scenes are skipped with a logged reason; no failed workflows.
- [ ] **Idempotent**: re-trigger over the same window creates no duplicate work / items.
- [ ] **Quality gate** blocks a deliberately-corrupted output from being registered (P1).
- [ ] **Backfill**: the configured lookback window is processed on enable without duplicating
      forward items (P5).
- [ ] **Phase acceptance soak**: **5 tiles × 14 days × ≥ 95% success** on staging (no prod cutover
      this phase — P3, P4).

---

## 5. Resolved operational decisions (2026-06-08, confirmed with Loïc)

- **P1 — Quality gate**: run the `validate_s1_grd_rtc` checks (CRS, grid_mapping, NaN fraction, dB
  ranges) as an automated **post-ingest gate**. **FAIL → do not register, fail the workflow + alert;
  WARN → register with an annotation.**
- **P2 — Dedup**: **STAC "does this item exist?" check** (+ the ingest's own skip gate) is the dedup
  authority — no persistent PVC/ConfigMap state. *Risk: STAC indexing latency could allow a brief
  re-submit window; acceptable, mitigated by the ingest skip-gate.*
- **P3 — Promotion**: **staging only this phase.** Register to the S1 staging bucket/collection;
  **prod bucket/collection + cutover are out of scope** (deferred to a later phase). The §4 soak is
  the phase acceptance bar, not a prod gate.
- **P4 — Acceptance soak**: **5 tiles × 14 days × ≥ 95% success** on staging.
- **P5 — Temporal**: **forward + bounded backfill.** Process new acquisitions *and* a configurable
  historical lookback window on enable. Backfill window size = **OQ #4**.
- **P6 — Cadence**: trigger **every 6 h**.
- **P7 — Reprocessing**: **overwrite (upsert)** on reprocess (orbit/DEM/data-model change); no item
  versioning yet (revisit only if a consumer needs history).

---

## 6. Boundaries

**Always**: keep Script A/B and the Argo templates as the single source of pipeline logic (trigger
only orchestrates); path-guard any destructive op; validate before register (P1); use per-mission
buckets.
**Ask first**: enabling **prod** writes; changing the STAC collection schema (consumer contract);
any S1D work; cross-repo changes to the merged s1tiling/ingest templates.
**Never**: register an item that failed the quality gate; process S1D until upstream support lands;
hardcode credentials; blind-run s1tiling on no-data days.

---

## 7. Open questions (owner)

1. **AOI definition** — the concrete tile set (and the 5 soak tiles). *Owner: Loïc / science.*
2. **Backfill window** (P5) — how far back on enable (e.g. 30 / 90 days)? Sizes throughput + cost.
   *Owner: Loïc.*
3. **Alerting path** — where do quality-gate FAILs and persistent workflow failures notify? *Owner: Loïc / infra.*
4. **ensure-dem sub-choices** — DEM discovery via `eodag earth_search` vs. direct tile-name
   derivation; cache reuse of the existing `s1-dem` PVC. *Owner: Loïc — confirm during planning.*

### Resolved
- **DEM source/auth (2026-06-08)** — replicate phase-1: Copernicus DEM **GLO-30** COGs from the
  **public AWS Open Data bucket `s3://copernicus-dem-30m/`** over HTTPS
  (`https://copernicus-dem-30m.s3.amazonaws.com/`), **no credentials / not requester-pays**
  (`s1_grd_phase5_subissues.md:169-242`). NOT CDSE (`cop_dataspace` lacks the collection / is
  auth'd-requester-pays). This makes the automated DEM-fetch viable; cache on the `s1-dem` PVC.
- **P1–P7** — see §5. Prod cutover authority deferred with the prod path (P3).

---

## 8. Done definition (Define stage)

Objectives, scope, and success criteria are unambiguous and agreed; P1–P7 defaults are confirmed or
amended; open questions §7 have owners. Then proceed to a Plan (`claude-docs/plans/`) breaking this
into atomic tasks (trigger port, ensure-dem, platform render, quality gate, promotion, soak).
