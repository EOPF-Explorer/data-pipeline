# Spec: S1 RTC input-data caching (eliminate redundant CDSE downloads)

**Stage:** 1 — Define (spec). Storage spike (OQ1) **resolved 2026-06-26** → §5/§7. Next stage = a plan
in `claude-docs/plans/s1_input_data_caching.md`.
**Author context:** surfaced 2026-06-25 while validating the RTC gamma-area fix on the cluster;
upgraded from the original problem brief to a spec 2026-06-25 after studying the live wiring in
`../platform-deploy`.
**Out of scope:** the RTC gamma-area / ascending geocoding shift (#306, `use_resampled_dem` /
OTB sensor model) — a *separate* issue, tracked in `claude-docs/reports/s1_306_phase1_audit.md`
and the `project_s1_306_*` memories. Do not conflate.

---

## 1. Objective

A given Sentinel-1 GRD frame (and DEM cell) needed by the S1 RTC pipeline should be **fetched from
CDSE at most once per AOI + time window**, and per-tile processing should become **compute-bound, not
download-bound**. Today every per-tile workflow independently re-downloads the same frames from CDSE,
which dominates wall-time and CDSE rate-limit pressure and caps how far the AOI / revisit frequency
can scale.

This spec defines the problem, the **corrected** current state, observable success criteria, the
candidate architectures with their trade-offs, and the decisions that must be resolved (a storage
spike) before a plan is written. **This spec does not commit to an implementation** — it scopes the
work and surfaces the forks.

---

## 2. Current state (corrected against the live wiring)

> ⚠️ The original brief said `s1_images` is a *per-pod ephemeral `emptyDir`*. **That is no longer
> true.** The template was reworked (commit lineage "T7 Task 0") to a **per-workflow RWO PVC**. The
> cache already survives *retries within a tile's workflow*; the remaining gap is purely
> **cross-tile / cross-workflow** sharing. The redundancy is real, but its precise shape matters.

### Fan-out model (`../platform-deploy/.../cronwf/eopf-explorer-cronwf-s1rtc.yaml`)
- Prod = Argo **CronWorkflow `eopf-explorer-s1rtc`** (ns `devseed-staging`), every 4 h from 21:00 UTC.
  Currently `suspend: true` (datacube-maintenance window) and **descending-only** (ascending #306).
- A `discover` step runs `scripts/trigger_cdse.py`, which **per tile** queries CDSE, then
  `collapse_same_pass()` keeps **one representative product per `(tile, date, platform)`**. The DAG
  `withParam`-fans-out **one `s1tiling → ingest` chain per emitted item**. `parallelism: 6`, globally
  bounded by the `v1-s1rtc-limit: 6` semaphore (the parent cron is additionally gated to 1 by
  `cron-s1rtc-limit` + `concurrencyPolicy: Forbid`).
- **The fan-out unit is the tile.** A single IW GRD frame (~250 × 170 km) overlaps ~a dozen adjacent
  100 km MGRS tiles, so that one frame is emitted under ~12 different tile items, and **each tile's
  `s1tiling` workflow re-downloads the frame(s) overlapping it, independently.** That is the redundancy.

### Per-tile processing (`../platform-deploy/.../eopf-explorer-s1tiling-template.yaml`)
- Each child workflow provisions a **per-workflow `workspace` PVC** via `volumeClaimTemplates`:
  `accessModes: [ReadWriteOnce]`, `storageClassName: csi-cinder-high-speed`, `40Gi`, mounted at
  `/data`. Three sequential steps share it (RWO reattaches step→step, all on the `pipeline` pool):
  1. **`ensure-dem`** (data-pipeline image) → `scripts/ensure_dem.py` stages the tile's GLO-30 cells
     into `/data/dem/COP_DEM_GLO30` **per workflow**. Reads the static `DEM_Union.gpkg` + geoid from
     the `s1-dem` **ReadOnlyMany csi-rclone** mount — and has to **copy the gpkg to local `/tmp`
     first** because sqlite can't read off the S3-FUSE mount (random-access/locking).
  2. **`s1processor`** (OTB image `s1tiling:1.4.0-ubuntu-otb9.1.1`, requests `16Gi`/`4 cpu`) renders
     the per-run cfg from the `s1grd-rtc-cfg-base` ConfigMap, applies the EODAG-4 patch, runs
     `S1Processor`. Raw SAFEs land in `/data/data_raw` (`s1_images`), orbit files in `/data/eof`.
  3. **`upload-geotiffs`** (data-pipeline image) s3fs-uploads the GeoTIFFs and verifies.
- Relevant cfg (`s1grd-rtc-cfg-base` ⟵ `config/S1GRD_RTC.cfg`): `s1_images : /data/data_raw`,
  `download : True`, `nb_parallel_downloads : 2`, `platform_list` set per-run; no `relative_orbit_list`.
- **Why S1D is pulled despite `platform_list : S1A`** (verified in `S1FileManager.py` 1.4.0): the search
  *does* pass `platformSerialIdentifier=S1A` (`_search_products:528`), but cop_dataspace OData appears to
  ignore it (same class as `relativeOrbitNumber`, which the eodag-4 patch had to remove). The platform
  **post-filter runs only for `len(platform_list) > 1`** (`:553`) and on eodag-4 filters by the legacy
  `platformSerialIdentifier` property, which no longer exists (products carry STAC `platform`, e.g.
  `sentinel-1a`) → it matches nothing (the documented "`S1A S1C` → 0 products" bug). Net: with one
  platform **nothing filters S1D before download** → S1D is fetched then discarded. (P0 fix = make the
  post-filter use STAC `platform` and run for `len>=1`; plan T1.)
- **Skip-if-present seam** (verified): `_filter_products_to_download:627` drops any product whose
  identifier is already in `_product_list`, which `_refresh_s1_product_list` builds by **scanning
  `s1_images` for `S1*_IW_GRD*` dirs with a valid `manifest.safe`** (layout
  `{prod_id}/{prod_id}.SAFE/manifest.safe`). So a shared cache reuses with **no s1tiling code change** —
  but it must restore the **extracted SAFE tree**, not a marker file.

### Storage reality (the crux — `../platform-deploy` has only two storage classes)
- **`csi-cinder-high-speed`** — OVH block storage, **RWO, zone-bound (nova)**. Cannot be shared
  concurrently across pods. This is what `workspace` uses.
- **`csi-rclone`** — **S3-FUSE**. Supports RWX, but **poor random-access + weak locking** (already
  broke `AgglomerateDEM` cross-pod listing and the gpkg sqlite read — hence the local-copy
  workaround, and hence the DEM read path was deliberately moved *off* FUSE).
- **There is no Manila / CephFS / NFS / JuiceFS RWX POSIX filesystem** — confirmed at the **cluster**
  level by the §5 spike (`kubectl get storageclass`/`csidrivers` = only cinder RWO + csi-rclone), not
  just absent from the repo.
- The `s1-geotiff-out` PVC (`ReadWriteMany`, csi-rclone) exists but is **unused** — the upload step
  was switched to direct s3fs precisely because the FUSE VFS writeback dropped in-flight uploads.
  Precedent: **S3-FUSE is avoided in the hot paths.**

### Evidence (this session — single tile 30UVU ascending)
- eodag queued **4 products** for one tile run: 2 needed (S1A datatake `082E88`) + **2 redundant S1D**
  (`relative_orbit_list` empty ⇒ other platforms pulled too). ~1.75 GB each.
- CDSE throttles to **~6 MB/s** per file ⇒ **~35+ min of wall-time was download**, vs minutes of compute.
- Fleet math: 163-tile AOI × frames overlapping ~N tiles each ⇒ the same frame fetched from CDSE ~N×.

---

## 3. Scope

**In scope** — a **phased roadmap** (decision 1a), each phase independently shippable:

| Phase | Goal | New infra |
|-------|------|-----------|
| **P0 — cheap per-run wins** | Cut redundant download *volume per run* (drop the S1D pull, restrict orbits, tune parallelism). | none |
| **P1 — download-once** | Each frame / DEM cell fetched from CDSE **once per AOI+window**, served to tile pods from an **in-region cache**. The spine. | a cache substrate (see §5; substrate chosen by the spike) |
| **P2 — shared POSIX cache (optional)** | Download-once **and** copy-once via a true shared RWX `s1_images`. | RWX filesystem (gated on the spike) |

**Out of scope:** the #306 gamma-area/ascending bug; the output-upload path (already durable via
s3fs); any data-model / ingest change (the GeoTIFF→Zarr ingest chain is unchanged by caching).

**Explicitly deferred to the plan, not decided here:** the cache substrate (S3-bucket vs RWX PVC),
the concurrency model, the eviction policy, and the fan-out-unit choice (§6). This spec presents the
trade-offs; the plan commits after the spike.

---

## 4. Success criteria (observable)

- [ ] **Download-once (target):** each distinct CDSE frame id is fetched from CDSE **once per
      AOI+window** — exactly-once with the stage-before-fan-out variant; with the lazy v1 (plan default)
      it is **≈once, bounded above by the concurrency width** (a frame first-touched by K concurrent
      tiles may be fetched ≤K×). Measure: count CDSE GETs per frame id across the run's pods.
- [ ] **Compute-bound, not download-bound:** in the `s1processor` pods, download time is a **minority**
      of wall-time (target: < 30%), where today it is the majority (~35 min of ~40).
- [ ] **CDSE egress per cron run drops ~N×** (N = avg tiles per frame across the AOI) vs the per-tile
      baseline (measure: total CDSE bytes/time per run, from pod node timings + the Argo archive —
      cf. `project_s1rtc_cron_timing_model`, `tests/fixtures/wf_perf/`).
- [ ] **No regression:** the empty-coverage `exit 0` contract, per-tile retry isolation
      (`continueOn`/`retryStrategy`), and the downstream ingest chain still pass on a real run.
- [ ] **DEM:** GLO-30 cells for the AOI are not re-fetched once-per-tile (lower priority — DEM is
      smaller and comes from anonymous AWS Open Data, not the throttled CDSE).

---

## 5. Candidate architectures (P1 substrate)

The download-once spine needs somewhere to put a frame that **all** tile pods can read.

> ### ✅ Spike result (2026-06-26) — OQ1 RESOLVED → **Substrate (i)** recommended
> Ran on `eoxhub_ovh` (devseed-staging, `general-c3-128` pool), 1.7 GB object, isolated `__spike_perf__/`
> prefix on the `s1-l1grd-staging` bucket (cleaned up). Numbers:
>
> | Path | Throughput (1.7 GB) | Notes |
> |------|--------------------|-------|
> | Local block write (emptyDir) | **897 MB/s** | OTB's local I/O ceiling — fast |
> | csi-rclone **S3-FUSE cold read** (fresh pod, other node) | **31 MB/s** | md5 matched ⇒ async write was durable *this run* |
> | boto3 **in-region S3 cold read** (single object, 10-thread TransferManager) | **28 MB/s** | ≈ same as FUSE ⇒ the limit is the **per-object cold S3 stream**, not FUSE |
> | boto3 **3× concurrent** (different streams) | **323 MB/s aggregate** (107/stream) | endpoint scales with concurrency; warm objects far faster |
> | CDSE (baseline) | ~6 MB/s | per-file throttle |
>
> **Findings:** (1) **No RWX POSIX filesystem exists** — `kubectl get storageclass`/`csidrivers` show
> only `cinder.csi.openstack.org` (RWO block) + `csi-rclone` (S3-FUSE); **Manila/CephFS/NFS is not
> installed**, so Substrate (ii) would first require standing up OVH Manila CSI. (2) A single **cold**
> object stream is ~30 MB/s **either way** — FUSE is not the bottleneck for one read; the OVH S3
> per-object cold rate is. (3) **Parallelism across _different_ frames is the lever** (323 MB/s at
> 3 streams); boto3's internal multipart did *not* speed a single cold object. (4) Even the
> conservative cold single-stream (~30 MB/s) is **~5× CDSE**; parallel/warm is 50×+.
>
> **⇒ Recommendation: Substrate (i).** Same cold per-stream cost as FUSE, but OTB's heavy *random* I/O
> then runs on **local block (897 MB/s), never through FUSE** — and the cache-fill parallelizes across
> frames to ~320 MB/s. Substrate (ii) buys only copy-elimination (the copy is cheap) at the cost of
> putting OTB's random reads behind a 31 MB/s FUSE mount + needing new RWX infra. Not worth it unless a
> future need installs Manila anyway.

### Substrate (i) — in-region S3 cache bucket  *(works within today's primitives — RECOMMENDED)*
The cache stores each frame as **one object = a tar of the extracted SAFE tree** (keeps the spike's
single-object throughput; a SAFE is hundreds of small files). A tile pod's pre-step pulls the SAFEs it
needs from **in-region S3** (parallel, not CDSE-throttled) and untars them into its local RWO
`workspace` at `data_raw/{prod_id}/{prod_id}.SAFE/`, so s1tiling's disk scan skips their download. The
plan keeps `download : True` so genuine cache **misses** are still fetched from CDSE by s1tiling, then a
post-step uploads the new SAFEs (lazy populate-on-miss).
- **Pros:** uses only primitives that exist (S3 + RWO block); shifts the bottleneck from CDSE egress
  (~6 MB/s) to in-region S3; keeps the OTB read path on **local block storage** (no FUSE in the hot
  path — respects the hard-won DEM lesson); reuses s1tiling's existing skip seam (no s1tiling change).
- **Cons:** still **copies** each frame per-tile (egress deduped, copy not); cache populate/skip +
  eviction is net-new code; tar/untar adds local CPU (cheap vs the CDSE saving).

### Substrate (ii) — shared RWX POSIX cache PVC  *(DEFERRED — RWX infra not present + FUSE too slow for OTB)*
Mount a shared RWX volume as `s1_images`; s1tiling's disk scan skips a product whose extracted SAFE is
already present ⇒ **zero s1tiling code change**, and it dedups **both** egress and copy.
- **Pros:** simplest conceptually; download-once *and* copy-once; no staging glue.
- **Cons (confirmed by the spike):** the cluster has **no RWX POSIX backend** (Manila/CephFS/NFS not
  installed) — only `csi-rclone` S3-FUSE, which read the 1.7 GB object at **31 MB/s sequentially** and
  would force OTB's *random* reads through FUSE for the whole job (worse, and has already bitten us:
  gpkg, AgglomerateDEM, upload writeback). Revisit only if OVH Manila gets installed for another reason.

### Concurrency model (orthogonal to substrate — for the plan)
- **(c) lazy populate-on-miss [PLAN DEFAULT]:** no pre-stage, no lock — each tile keeps `download : True`;
  its pre-step pulls cache **hits**, s1tiling fetches **misses** from CDSE, a post-step uploads new SAFEs.
  Lowest blast radius (no cron-DAG change), but ≤K concurrent first-touches of an uncached frame
  double-fetch it (K = concurrency width, 6). Ship + measure; promote to (a) if material.
- **(a) stage-before-fan-out:** a job computes the AOI+window frame set, downloads each **once**
  (parallel — spike: ~320 MB/s aggregate vs ~30 for one) before the fan-out; tiles then run pure
  cache-reads. **Exactly-once**, race-free, but adds a cron-DAG step + the un-collapsed frame-set computation.
- **(b) per-product lock/marker:** the first tile to need a frame downloads it; others wait/reuse via a
  lock object. Between (c) and (a); races on partial files need careful locking.

### DEM
Same pattern, lower priority: pre-populate/share GLO-30 cells on a cache prefix (the `s1-dem` prefix
already exists) instead of `ensure_dem.py` re-staging per workflow.

---

## 6. Fan-out unit — two designs, pros & cons (decision 3: compare, do not commit)

The redundancy ultimately stems from the **per-tile fan-out unit**. Two end-states; the plan chooses
after the spike, because the chosen P1 substrate changes how much Design B's advantage is worth.

### Design A — keep per-tile fan-out + a shared cache (additive)
Leave the one-workflow-per-tile model intact; solve redundancy with the §5 cache layer (a pre/post cache
step around `s1processor` — §5 concurrency option (c), `download : True` kept for misses).
- **Pros:** minimal blast radius — the validated trigger / `withParam` / ingest chain is unchanged;
  s1tiling's per-tile mosaic logic is unchanged; per-tile retry/`continueOn` isolation is preserved;
  s1tiling's existing skip-on-present seam makes Substrate (ii) literally zero-code; ships on top of P0.
- **Cons:** with Substrate (i) it **relocates** the duplication (per-tile copy) rather than removing
  it; the cache concurrency + eviction machinery is net-new and must be correct.

### Design B — process per S1 frame / datatake (structural)
Make the **frame/datatake** the unit of work: download it once (by construction), then tile/mosaic
into the N MGRS tiles it covers.
- **Pros:** download-once **by construction** — no cross-pod cache, no concurrency/eviction problem at
  all; fewer, larger pods; attacks the root cause.
- **Cons:** **large blast radius** — rewrites the trigger fan-out (today per-tile) and the
  frame→tile mapping; the tile fan-OUT moves *downstream* (one frame-pod must emit N tile outputs, and
  ingest is per-tile/per-acq today); loses the clean per-tile retry/`continueOn` isolation; s1tiling is
  driven `roi_by_tiles`, so a frame→multi-tile invocation needs rework; meaningful risk to a working,
  recently-validated pipeline.

**Recommendation (non-binding):** **A is the lower-risk path to the success criteria; B is the
structurally cleaner end-state.** Post-spike update: there is **no good RWX substrate** (so the
"A + Substrate (ii) = near-zero-code download-once *and* copy-once" option is off the table), which
slightly strengthens B's "no cache needed" appeal — but A + Substrate (i) still meets every success
criterion in §4 with far less blast radius, so **A remains the recommended v1**; keep B as the noted
structural follow-up. Final call at plan time.

---

## 7. Open questions (with owner; resolve before/within the plan)

1. **[owner: user]** Does an OVH-side S1 mirror bucket already exist, or is one cheaper/faster than
   CDSE egress? (If yes, P1 staging may read from it instead of CDSE.)
2. **[owner: P0 investigation]** Is the ~6 MB/s throttle per-file or per-account? Does raising
   `nb_parallel_downloads` raise *aggregate* throughput, or just split the same ceiling?
3. **[owner: P0 investigation]** Does `relative_orbit_list` help, given the same dual issue as platform
   (search param ignored by cop_dataspace + post-filter only `len>1`)? The trigger already collapses
   same-pass, so this is often moot.
4. **[owner: plan]** Cache sizing + eviction/retention policy for the AOI + lookback window.
5. **[owner: plan]** Concurrency model: the plan picks (c) lazy populate-on-miss; (a) stage-before-fan-out
   is the exactly-once follow-up if (c)'s double-fetch is material (§5).
6. **[owner: user/plan]** Fan-out unit — Design A vs B (§6); spike leaves A as recommended v1.

### Resolved
- **OQ1 (storage spike) — RESOLVED 2026-06-26.** Cluster `eoxhub_ovh` has **no RWX POSIX filesystem**
  (only `cinder.csi.openstack.org` RWO + `csi-rclone` S3-FUSE; Manila/CephFS/NFS not installed).
  Measured (1.7 GB object, in-region S3): cold single-stream ≈ **30 MB/s via either FUSE or boto3**
  (per-object cold S3 rate, not a FUSE limit); **323 MB/s aggregate at 3 concurrent streams**; local
  block 897 MB/s; CDSE ~6 MB/s. **Impact:** Substrate (ii) deferred (no RWX backend + FUSE too slow for
  OTB random reads); **Substrate (i) in-region S3 cache bucket adopted as the P1 substrate**, with
  parallel multi-frame staging as the throughput lever. Full table + reasoning in §5.

---

## 8. Boundaries

**Always**
- Preserve the empty-coverage `exit 0` contract and the per-tile retry/`continueOn` isolation.
- Keep the OTB DEM/SAFE **random-access read path on local block storage** — never behind S3-FUSE
  without a perf gate (it has already broken AgglomerateDEM + the gpkg sqlite read).
- Keep the per-workflow RWO `workspace` for *outputs*; caching changes the *input* path only.

**Ask first**
- Provisioning new OVH storage (Manila/CephFS) or a new S3 cache bucket (cost/region implications).
- Adopting Design B (changing the fan-out unit) — large blast radius on a validated chain.
- Anything that crosses into the data-model / ingest repos.

**Never**
- Interpolate a raw CDSE `product_id` into a submitted Argo manifest (injection boundary — the trigger
  already enforces this).
- Commit `.env`, `eodag.yml`, kubeconfig, or any token/password literal.
- Put a csi-rclone S3-FUSE mount in the OTB hot read path on the strength of "it mounts" alone — a
  green mount is not a perf proof.

---

## 9. Validation & measurement (how the plan will prove success)

**Primary method = a flag-gated A/B canary** (not a one-sided "after" measurement). The cache ships behind
`enable_frame_cache` (default off); validation runs the **same 6 tiles** twice — control (off) vs cached
(on) — and compares. This both proves the win and is the safe rollout (canary → ramp → cron; rollback =
flag off).

- **Tile selection (critical):** the 6 canary tiles must be **contiguous / frame-sharing** (a block
  covered by common frames) so cross-tile reuse exists to measure; verify the shared-frame overlap. A
  scattered 6 has little reuse and would understate the benefit.
- **Cold vs warm:** report the cached arm as **two** runs — a one-time **cold-fill** (pays the CDSE
  misses to populate) and a **warm** run (the steady-state per-revisit number). The warm run is the win.
- **Fairness:** run the arms **separated in time** (not simultaneously on the same frames — CDSE
  server-side warming/throttle cross-contaminates); hold image / window / node pool / resources constant.
- **Isolation:** canary arms are **GeoTIFF-only (skip ingest)** to an isolated output prefix — perf
  comparison, no prod cube/STAC writes.
- **Metrics (A vs B-cold vs B-warm):** per-frame CDSE GET count; total CDSE bytes + download seconds;
  download/wall-time fraction; per-tile + total wall-time. Capture via pod-node timings + the Argo
  archive (`project_s1rtc_cron_timing_model`); fixtures/precedent: `tests/fixtures/wf_perf/`.
- **No-regression:** outputs A vs B equivalent (same acquisitions + per-band pixel data); empty-coverage
  tiles still exit 0; per-tile retry isolation intact.

---

## 10. Affected components / pointers

- **platform-deploy** (`workspaces/devseed-staging/data-pipeline/`):
  - `cronwf/eopf-explorer-cronwf-s1rtc.yaml` — trigger fan-out, `parallelism`, semaphore.
  - `eopf-explorer-s1tiling-template.yaml` — the 3-step workflow + the `workspace` RWO PVC.
  - `s1grd-rtc-cfg-base-configmap.yaml` — `s1_images`, `download`, `nb_parallel_downloads`.
  - `s1-rclone-pvcs.yaml` — `s1-dem` (RO) + the unused `s1-geotiff-out` (RWX csi-rclone).
  - `eopf-workflow-concurrency-configmap.yaml` — `v1-s1rtc-limit`, `cron-s1rtc-limit`.
- **data-pipeline:**
  - `config/S1GRD_RTC.cfg` — source of the cfg ConfigMap (P0 knobs at lines 31–32, 39, 44–45).
  - `scripts/trigger_cdse.py` — `collapse_same_pass()`, per-tile emission (fan-out unit).
  - `scripts/ensure_dem.py` — DEM staging (margins, Product10 naming, idempotency).
  - `scripts/run_s1tiling.py` — the S1D success-contract (why off-platform pulls are tolerated).
  - `analysis/s1tiling_eodag4_patch.py` — eodag-4 search-time orbit/platform handling.
- **Related memories:** `project_s1tiling_perf_findings` (OTB-bound 66%, procs/threads),
  `project_s1_aoi_france` (163 tiles), `project_s1rtc_cron_timing_model` (where timings live).
