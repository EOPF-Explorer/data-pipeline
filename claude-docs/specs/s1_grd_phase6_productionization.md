# Spec: S1 GRD RTC Productionization (Phase 6)

**Status**: Draft (Define stage) — 2026-06-08 · **Tracked in #226**
**Builds on**: `s1_grd_phase5_subissues.md` (local prototype + Argo templates),
`s1_grd_STACregisration_and_argo_pipelines.md`, `spike_s1tiling_platform_support.md` (S1C/S1D finding).

---

## 1. Objective

Move the S1 GRD RTC pipeline from the verified **single-tile (31TCH) / S1A** prototype to an
**automated, multi-tile, in-cluster production service**: a data-driven trigger discovers new CDSE
S1 GRD products, provisions the DEM for any requested tile automatically, and runs
s1tiling → ingest → register, writing to **staging** (staging→prod promotion is a later phase — P3).

**Hard requirement**: a user can load *all scenes in a tile as a datacube* — met by a **per-tile
multi-temporal cube** (xarray). The catalogue exposes **per-acquisition STAC items** that render their
slice from the cube via TiTiler `sel=time`. This rendering **depends on I2 / option 2** — titiler
resolving the store from the item's asset href (platform-deploy `ecde99c`; **not yet effective**, §7
Spike 3, P8). **Dual storage** (per-acquisition stores) is the documented fallback if option 2 stalls.

**Target users**: the EOPF Explorer platform (STAC catalogue + raster viewer consumers) and the
devseed operators who run/monitor the pipeline.

### Resolved decisions (2026-06-08, confirmed with Loïc)
| # | Decision | Implication |
|---|----------|-------------|
| Goal | Productionize S1 GRD RTC in-cluster (automated, multi-tile, **staging-only**; prod deferred) | Drives the whole spec |
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
- **Multi-platform**: render `platform_list = "S1A S1C"` into the s1tiling cfg; the trigger skips
  submitting S1D-targeted workflows (logged). **Note (PoC finding)**: s1tiling downloads *all*
  platforms in the window (filter applies at processing, not download) and exits non-zero if an
  off-platform (S1D/S1C) download fails — so the s1tiling step must **tolerate off-platform download
  failures** (succeed when the requested platform produced output). See §7 + plan T2.
- **Multi-tile**: trigger iterates a configured tile set (AOI); each tile self-provisions its DEM.
- **Bounded historical backfill** on enable (configurable lookback — OQ #2), alongside forward
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
CronWorkflow (data-driven trigger, every 6 h)
  └─ for each tile in AOI:
       query CDSE STAC (bbox, lookback, orbit, platform∈{S1A,S1C})  ──► new products?
          └─ for each NEW product (not already in STAC):           ──► submit child Workflow:
               ┌──────────────────────────────────────────────────────────────────────┐
               │ ensure-dem → s1tiling → ingest(append scene as a time slice to the      │
               │   per-tile cube) → quality-gate(FAIL⇒stop) → register(per-acquisition   │
               │   item → cube via asset href + sel=time)                                │
               └──────────────────────────────────────────────────────────────────────┘
   (cube append serialised by an Argo synchronization mutex keyed on the tile;
    per-acquisition rendering depends on titiler href resolution — I2/option 2)
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
- **Storage (P8)**: ingest **appends the scene as a new `time` slice into the per-tile cube**
  (`s1-rtc-{tile}.zarr`, collection `sentinel-1-grd-rtc-staging`) on the S1 **staging** bucket;
  the append is **serialised by an Argo `synchronization` mutex keyed on the tile** and skips a `time`
  already present. (Fallback only, if I2/option 2 stalls: also write a per-acquisition single-time
  store named = item-id for reconstruction-based rendering — dual storage.) Prod is out of scope (P3).
- **Catalogue (P8)**: `register` upserts **one STAC item per acquisition**
  (`s1-rtc-{tile}-{datetime}`, collection `sentinel-1-grd-rtc-acquisitions`), pointing at the cube via
  asset href; TiTiler renders the right slice with `sel=time=nearest::{datetime}` **once I2/option 2
  (href resolution) is live**. The per-tile cube item (`sentinel-1-grd-rtc-staging`) is the analysis
  entry — a user loads the whole tile by opening the cube (xarray). The per-acquisition items are the
  queryable time-series index (dedup authority).
- **Geoid**: the EGM2008 model is assumed pre-staged on the `s1-dem` PVC (as in phase-1); `ensure-dem`
  does not fetch it per run.

---

## 4. Success criteria (agreed)

- [ ] Data-driven trigger submits a child Workflow **only** for genuinely new CDSE products
      (zero s1tiling runs on no-data days).
- [ ] A **previously-unprocessed tile** runs end-to-end with **no manual DEM upload** (ensure-dem works).
- [ ] An **S1C** scene processes A→B end-to-end; item queryable + validation PASS.
- [ ] **S1D** scenes are skipped with a logged reason; no failed workflows.
- [ ] **Per-acquisition rendering (P8)**: each acquisition has a STAC item that renders its slice from
      the cube via `sel=time` (depends on **I2/option 2** — titiler href resolution being live).
- [ ] **Datacube (P8)**: after ≥2 acquisitions for a tile, the per-tile cube opens as a datacube with
      all scenes on the `time` axis (xarray) — "load all scenes in a tile" works (independent of titiler).
- [ ] **Concurrency**: two scenes for the same tile processed together both land in the cube without
      corruption (per-tile write serialised).
- [ ] **Idempotent**: re-trigger over the same window creates no duplicate stores / items / time slices.
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
- **P2 — Dedup**: the **automated trigger** skips a product whose **per-acquisition STAC item**
  (`s1-rtc-{tile}-{datetime}`) already exists; the ingest also skips a `time` already present in the
  cube. No persistent PVC/ConfigMap state — STAC is the index (P8). *Risk: STAC indexing latency could
  allow a brief re-submit window; acceptable, mitigated by the cube time-present check.*
  → **Interaction with P7**: existence-dedup means the trigger **never reprocesses** an already-
  registered acquisition. Reprocessing is an **explicit, out-of-band path** (see P7).
- **P3 — Promotion**: **staging only this phase.** Register to the S1 staging bucket/collection;
  **prod bucket/collection + cutover are out of scope** (deferred to a later phase). The §4 soak is
  the phase acceptance bar, not a prod gate.
- **P4 — Acceptance soak**: **5 tiles × 14 days × ≥ 95% success** on staging.
- **P5 — Temporal**: **forward + bounded backfill.** Process new acquisitions *and* a configurable
  historical lookback window on enable. Backfill window size = **OQ #2**.
- **P6 — Cadence**: trigger **every 6 h**.
- **P7 — Reprocessing**: an explicit/manual path (e.g. a `force` flag bypassing the P2 check)
  **overwrites the acquisition's `time` slice in the cube (region write) and upserts its item**; no
  versioning yet. The **automated trigger never reprocesses** (P2). *Force-path UX is a plan detail,
  not required for the soak.*
- **P8 — Data model & identity** *(decided 2026-06-08; hard requirement)*: a **per-tile multi-temporal
  Zarr cube** (one store per tile, scenes appended on the `time` axis) backs both access paths:
  1. **Analysis** — a user loads all scenes in a tile by opening the cube (`s1-rtc-{tile}.zarr`,
     collection `sentinel-1-grd-rtc-staging`) with xarray. Works regardless of titiler.
  2. **Rendering** — **per-acquisition STAC items** (`s1-rtc-{tile}-{datetime}`, one per scene,
     collection `sentinel-1-grd-rtc-acquisitions`) point at the cube via asset href and render their
     slice via TiTiler `sel=time=nearest::{datetime}`.
  STAC is the per-acquisition index (no separate tracking system). **Rendering depends on I2 (option 2,
  chosen 2026-06-08)**: titiler must resolve the store from the STAC item's **asset href** — platform-
  deploy `ecde99c` adds `TITILER_EOPF_API_ROOT_URL` toward this, but **re-tested 2026-06-08 it is NOT
  yet effective** (titiler still reconstructs `{base}/{collection}/{item_id}.zarr`, ignores href; §7
  Spike 3). Per-acquisition **rendering is blocked until option 2 deploys + is verified**; the cube
  storage/append + xarray load are unaffected.
  **Fallback** (if option 2 stalls): **dual storage** — also write per-acquisition single-time stores
  named = item-id, which render via titiler's reconstruction today (cost: each scene written twice +
  cube-append serialisation). Single shared cube is preferred once option 2 is verified.

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
5. **I2 / option 2 — titiler href resolution (blocks per-acquisition rendering)**: deploy + verify that
   titiler resolves the store from the STAC item asset href (platform-deploy `ecde99c` adds
   `TITILER_EOPF_API_ROOT_URL`; re-tested 2026-06-08 — **not yet effective**). Until verified, fall back
   to dual storage (per-acquisition stores named = item-id). *Owner: Loïc / infra (E. Mathot).*
6. **s1tiling off-platform-download tolerance** — make the s1tiling step succeed when the requested
   platform produced output despite S1D/S1C download failures (PoC finding). *Owner: plan T2.*

### Resolved
- **Spike 1 — TiTiler `sel` param (2026-06-08)**: titiler-eopf **0.9.0** exposes `sel` on all render
  endpoints (`sel=time={value}` / `{method}::{value}`). `sel=time=nearest::…` returns 200. *(Caveat:
  the 200 was against a store sitting at titiler's reconstructed path — it did not prove href
  resolution; see Spike 3.)*
- **Spike 2 — per-acquisition builder sizing (2026-06-08)**: the per-acquisition item builder is a
  **small** change (the phase-5 builder already emits per-store items; just use per-acquisition ids);
  viz links are a few lines in `scripts/register_v1.py`.
- **Spike 3 — titiler store resolution (2026-06-08, PoC, decisive)**: the datacube PoC proved
  titiler-eopf **reconstructs the store path as `s3://esa-zarr-sentinel-explorer-fra/tests-output/
  {collection}/{item_id}.zarr` and IGNORES the asset href** (`/info` → *"No group found in store …
  {item_id}.zarr"*); there is **no URL-based render endpoint**. The PoC *did* validate the cube
  **append** works (2 acquisitions → 2-time cube).
- **I2 decision = option 2, in flight (2026-06-08)**: make titiler **resolve the store from the STAC
  item asset href** so per-acquisition items can render from the shared cube (collapses the dual-storage
  fallback to a single cube). platform-deploy **`ecde99c`** adds
  `TITILER_EOPF_API_ROOT_URL: …/stac` toward this. **Re-tested 2026-06-08 — NOT yet effective**: the
  per-acquisition item still 500s with the reconstructed `fra/tests-output/…` path. → option 2 is a
  **dependency to deploy + verify** (OQ #5); until then per-acquisition rendering is blocked (cube
  append + xarray load are fine), and **dual storage** (per-acquisition stores named = item-id) is the
  fallback that renders via reconstruction today.
- **DEM source/auth (2026-06-08)** — replicate phase-1: Copernicus DEM **GLO-30** COGs from the
  **public AWS Open Data bucket `s3://copernicus-dem-30m/`** over HTTPS
  (`https://copernicus-dem-30m.s3.amazonaws.com/`), **no credentials / not requester-pays**
  (`s1_grd_phase5_subissues.md:169-242`). NOT CDSE (`cop_dataspace` lacks the collection / is
  auth'd-requester-pays). This makes the automated DEM-fetch viable; cache on the `s1-dem` PVC.
- **P1–P8** — see §5. Prod cutover authority deferred with the prod path (P3).

---

## 8. Done definition (Define stage)

Objectives, scope, and success criteria are unambiguous and agreed; P1–P8 confirmed (§5); open
questions §7 have owners. Then proceed to the Plan (`claude-docs/plans/s1_grd_phase6_productionization.md`),
which breaks this into atomic tasks (quality gate, platform render S1A+S1C, ensure-dem, **datacube
ingest/append**, **per-acquisition catalogue**, trigger port, multi-tile AOI, backfill, acceptance soak).
