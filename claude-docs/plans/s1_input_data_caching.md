# Plan: S1 RTC input-data caching (spec `claude-docs/specs/s1_input_data_caching.md`)

**Goal**: A given S1 GRD frame is fetched from CDSE **once per AOI+window** (bounded by concurrency in
v1; exactly-once with T11), and per-tile pods become compute-bound, not download-bound — via P0 cheap
wins (no infra) then a P1 in-region S3 frame cache.
**Constraint**: minimum code; **no new RWX infra** (storage spike: only cinder-RWO + csi-rclone exist);
keep Design A (per-tile fan-out) + the validated trigger/ingest chain; 2-repo lockstep
(`data-pipeline` code ⇄ `platform-deploy` configmap/template copies).

## Decisions carried from the spec/spike (do not re-litigate)
- **Substrate = (i) in-region S3 cache bucket** (spike: S3-FUSE 31 MB/s is wrong for OTB random reads;
  client-copy to local block keeps OTB on 897 MB/s; cache-fill parallelizes to ~320 MB/s).
- **Cache unit = one tar object per frame** = the extracted SAFE tree. s1tiling's skip is a disk scan
  of `data_raw` for `S1*_IW_GRD*` dirs with a valid `manifest.safe` (`_filter_products_to_download:627`
  + `_refresh_s1_product_list`), **not** an eodag `.downloaded` marker — so the cache restores the
  extracted SAFE at `data_raw/{prod_id}/{prod_id}.SAFE/`. One tar keeps the spike's single-object
  throughput (a SAFE is hundreds of small files).
- **Concurrency = lazy populate-on-miss** (spec §5 option (c); Design A, additive pre/post step, **no
  cron-DAG rewrite**). v1 does **not** strictly meet spec §4 "download-once": ≤K concurrent first-touches
  of an uncached frame double-fetch it (K = concurrency width, here 6). Strict exactly-once is T11, built
  only if T10 shows the double-fetch is material. Rationale: lowest blast radius; ship + measure (ai-eng §3/§7).
- **Prod path = the Argo template's inline bash + `S1Processor`**, NOT `scripts/run_s1tiling.py` (local
  prototype only). Changes land in `platform-deploy` template/configmaps + the eodag patch.
- **Rollout = flag-gated A/B canary, then ramp.** The cache pre/post steps ship behind a template param
  `enable_frame_cache` (default **false** → zero prod behaviour change). Validation is a **6-tile A/B
  canary** (control = flag off, cached = flag on) on **NEW frame-sharing tiles further east, outside the
  prod AOI**, GeoTIFF-only (no ingest), isolated output bucket — runs in parallel with the existing
  workflow without touching it (see **Isolation invariants** above). Compares CDSE bytes/time +
  download-fraction + wall-time (T10). Ramp by widening the flag (T12); rollback = flip it off. Same
  template, no fork.

**Explicitly deferred (not this plan):** DEM caching (spec §5 — GLO-30 comes from anon AWS Open Data, is
small, already staged per-workflow; *not* the CDSE bottleneck) and Substrate (ii)/RWX. This plan attacks
**redundant CDSE S1-frame egress** only.

## Isolation invariants — the canary must NOT touch the live cron or already-ingested tiles
Validate the cache **in parallel, on NEW tiles further east**, designed to be safe **even if the cron is
running** (do not rely on it being suspended). The cron (`eopf-explorer-s1rtc`) and a canary share more than
"flag-off" implies — hold these invariants:

- **Canary tiles are NEW + further east, outside the current AOI.** The prod AOI is ConfigMap
  `s1rtc-aoi-tiles` key `western-europe` (163 MGRS tiles, bbox `[-5.2,42,13.5,51.2]`, ocean denylist in
  `scripts/gen_aoi_tiles.py:60-72`). Pick a contiguous, same-zone/orbit, **frame-sharing block >13.5°E**
  (e.g. eastern Austria / Slovenia / NE Italy / Czechia — UTM 32U/33T not in the list) that is **not in that
  list and not yet cubed**. New tiles ⇒ no per-tile ingest-mutex collision (`s1rtc-ingest-{tile_id}`), no
  reprocessing of already-ingested tiles, and clean cross-tile frame reuse for the cache test.
- **Output isolation is NOT the default — enforce it.** A bare template run writes GeoTIFFs to the prod
  bucket `esa-zarr-sentinel-explorer-s1-l1grd-staging` and (if ingest runs) into the prod-staging cube +
  `sentinel-1-grd-rtc(-acquisitions)-staging` collections. The canary MUST **override `s3_output_bucket`**
  to an isolated canary prefix AND run **GeoTIFF-only (skip the ingest template)** — zero cube/STAC writes.
- **T1's configmap is SHARED, not isolatable.** `s1tiling-eodag4-patch` is read by every cron child, so a
  T1 change hits the live cron the instant Flux deploys (merge = deploy). T1 cannot be A/B'd per-tile via
  the configmap — gate it on the throwaway-pod real-search validation (Task 1) and land it while the cron is
  **verified-suspended**, separate from the P1 cache canary.
- **T7's template edit is SHARED.** The pre/post cache steps land in the same `eopf-explorer-s1tiling`
  template the cron's children use; the flag-OFF path must be proven behaviour-identical (T7 criterion)
  before any cron child could execute it.
- **Verify the cron's LIVE suspend state — don't assume.** Git HEAD has `suspend: false` (re-enabled
  2026-06-27, #306); Flux may reconcile it back on. Confirm with `argo cron list -n devseed-staging` before
  any canary. The new-tile + isolated-output invariants above keep the canary safe regardless.
- **Residual shared resources:** the `v1-s1rtc-limit: 6` ingest semaphore and the throttled CDSE account —
  both moot for a GeoTIFF-only canary while the cron is suspended; if the cron is live, run the canary
  off-peak so it doesn't split CDSE bandwidth with a scheduled tick.

## Current state
| Resource | Status | Notes |
|----------|--------|-------|
| `eopf-explorer-s1tiling` template | per-workflow RWO `workspace` PVC; `ensure-dem→s1processor→upload` | cron **suspended** (maintenance) |
| `s1grd-rtc-cfg-base` configmap | `download:True`, `nb_parallel_downloads:2`, no `relative_orbit_list` | **copy** of `config/S1GRD_RTC.cfg` |
| `s1tiling-eodag4-patch` configmap | **pre-#310 baseline — NO platform post-filter.** The #310 T1 fix (STAC-`platform` value-map) landed on branch `fix/s1rtc-stac-collection-bbox`, dropped the wanted S1A product in prod → reverted #311 (coverage loss). This branch never carried it; `_rewrite_platform_postfilter` does not exist here. T1 needs a redo — see `s1_caching_t1_HANDOFF.md`. | **copy** of `analysis/s1tiling_eodag4_patch.py` |
| S1D redundancy | search passes `platformSerialIdentifier` (ignored by cop_dataspace) + no post-filter for `len==1` → S1D downloaded then discarded | cited `S1FileManager.py:511,528,553` — **in-container only; not verifiable from this checkout. UNCONFIRMED in the prod narrow-window path (HANDOFF: live run saw 1 S1A, 0 S1D). Gated by Task 0/C3.** |
| Skip seam | s1tiling scans `data_raw` for `S1*_IW_GRD*`+`manifest.safe`; skips products already in `_product_list` | cited `:627`, `_refresh_s1_product_list` — **in-container only (S1FileManager.py not on disk); pin image by digest + capture excerpt. Live-confirmed by T8 gate.** |
| In-region S3 read | ~30 MB/s cold/stream, ~320 MB/s @3 concurrent (spike) | local block 897 MB/s |

## Dependency graph
```
RISK-FIRST (upfront, read-only/cheap):
  T0 baseline ──┐        T8 skip-seam + frame-set live-confirm ──┐  (gates the whole cache design)
                │                                                │
P0 (no infra):  T1 platform post-filter fix ──► ships alone (kills S1D pull)
                │   T2 relative_orbit (opt) · T3 nb_parallel (opt)
                │                                                │
P1 (cache):     T4 cache bucket+creds ─► T5 cache_frames(pull) ─► T7 wire pre/post (flag-gated)
                                         T6 populate(push) ─────┘        │
                                         (T8 GATES T5,T7)                ▼
                                         T9 eviction          T10 A/B canary (6 frame-sharing tiles, no-ingest)
                                                                         │ green
                                              T11 stage-before-fan-out ◄─┤ (only if double-fetch material)
                                                                         ▼
                                                              T12 ramp flag → cron (rollback = flag off)
```

## Tasks

> Tasks are listed in **dependency order** (per the graph above), not numeric order — e.g. T8 precedes T4, and T11 (conditional) follows T12.

### Task 0 — Baseline measurement instrument  <status: NEXT>
**What**: From the existing prod path (read-only; cron stays suspended — submit ONE manual s1tiling
workflow for a known multi-frame tile), capture (a) products eodag pulls (count + platforms + ids),
(b) total CDSE bytes + download seconds, (c) download/wall-time fraction. The **before** for §4.
**Verify**:
```
argo submit -n devseed-staging --from workflowtemplate/eopf-explorer-s1tiling \
  -p tile_id=<multi-frame tile> -p orbit_direction=descending -p platform=S1A \
  -p date_start=<d> -p date_end=<d> --watch
kubectl -n devseed-staging logs <s1processor-pod> | grep -iE "remote S1 product|download|\.SAFE"
```
**Acceptance criteria**:
- [ ] Captured: # products, platforms, ids, ~bytes, download/compute split → `claude-docs/reports/s1_input_caching_baseline.md`.
- [ ] **S1D redundancy reproduced in the *descending* path** (the §spec evidence was ascending; cron is descending-only) — else pick the orbit/tile that does, so T1's "0 S1D" claim is testable.
      **HARD GATE for T1:** the HANDOFF's live run saw only 1 S1A, 0 S1D in the narrow-window prod path. Use
      the HANDOFF's wide-window throwaway-pod diagnostic; **also dump the real product `properties`
      (`platform`, `platformSerialIdentifier`, `id`) into a fixture** so T1's filter is built on observed
      data, not prose. **If no real off-platform (S1D) pull can be reproduced → T1 is solving a non-problem;
      rescope or drop P0 before rebuilding** (HANDOFF §"deeper open question").
- [ ] N (avg frames/tile) estimated for the AOI (sets the target ~N× egress drop).

### Task 1 — Fix the platform post-filter (kill the redundant S1D download)  <status: REVERTED (#311 — coverage loss) · needs redo via product-id PREFIX · BLOCKED on the Task 0 "redundancy exists?" gate>
**GATE:** Do not rebuild T1 until Task 0's hard gate confirms a real off-platform (S1D) pull exists in the
prod narrow-window path (HANDOFF §"deeper open question"). If it doesn't, this task is moot — rescope/drop P0.

**What**: The S1D pull is **not** a missing filter. `_search_products:528` already passes
`platformSerialIdentifier=S1A`, but cop_dataspace OData ignores it (same class as `relativeOrbitNumber`,
which the patch removed); the platform post-filter (`:553`) runs only for `len(platform_list) > 1` and
filters by `platformSerialIdentifier`, a property eodag-4 dropped (products carry STAC `platform`, e.g.
`sentinel-1a`) → it matches nothing (the documented "`S1A S1C` → 0 products" bug). **Fix** in
`analysis/s1tiling_eodag4_patch.py`: rewrite the platform post-filter to (a) match the **product-id PREFIX**
(`id.startswith("S1A"/"S1C"/…)` — guaranteed present and format-stable), **NOT** the
`properties["platform"]` value. The `S1A→sentinel-1a` value-map approach shipped in #310 and **dropped the
wanted S1A product in prod (coverage loss → reverted #311)** because `filter_property(platform="sentinel-1a")`
matched nothing at runtime; do not repeat it (HANDOFF §"What happened" / §"how to redo T1 correctly"). And
(b) run for `len(platform_list) >= 1`. This drops off-platform (S1D) products **before download**,
independent of whether cop_dataspace honoured the search param (belt-and-suspenders), and fixes
multi-platform as a bonus. **Validate against a REAL cop_dataspace search in a throwaway pod BEFORE
regenerating/merging the configmap** (merge = Flux deploy to live — HANDOFF §"Process lesson"). Mirror into
the configmap.
**Verify**:
```
uv run pytest tests/unit/test_s1tiling_eodag4_patch.py   # assert the post-filter rewrite (idempotent)
# cluster: a manual S1A run pulls 0 S1D products (compare to T0)
```
**Acceptance criteria**:
- [ ] Patch rewrites the post-filter via **product-id prefix** (`len>=1`); unit test covers rewrite +
      idempotency + raises-on-anchor-drift. **NOT done on this branch.** The #310 attempt used the
      STAC-`platform` value-map (wrong approach), landed on branch `fix/s1rtc-stac-collection-bbox`, and was
      reverted by #311 for coverage loss. `_rewrite_platform_postfilter` does **not** exist in this working
      tree (the local patch is the pre-#310 baseline; `test_s1tiling_eodag4_patch.py` has only the
      stream-timeout tests).
- [ ] A real S1A run pulls **0 S1D** products (was present at T0) — log evidence. *(blocked: needs a manual
      cluster run; cron suspended. Pair with T0 baseline.)*
- [ ] Requested-platform outputs still produced (**no coverage loss — the exact failure #311 hit**);
      multi-platform list no longer yields 0. *(blocked: same manual run.)*
- [ ] Filter validated against a **REAL cop_dataspace search in a throwaway pod BEFORE** the configmap is
      regenerated/merged (merge = Flux deploy). *(HANDOFF process lesson — the gate that was missing in #310.)*
- [ ] configmap copy regenerated + committed in lockstep, embedded script **byte-identical** to source.
      **NOT done on this branch** — the deployed configmaps still carry the pre-#310 baseline.
- [ ] If the prefix approach proves wrong on cop_dataspace → record it, revert, don't ship a blind no-op.
      *(the patch must fail loud on anchor drift rather than no-op'ing.)*

### Task 2 — `relative_orbit_list` viability (OQ4)  <status: ready · optional>
**What**: Same dual issue as platform (search param removed; post-filter only `len>1`). Decide whether
restricting relative orbits drops redundant pulls **without losing coverage** (the trigger already
collapses same-pass, so per-tile this is usually moot), or document it not-viable and rely on T1+cache.
**Verify**: a tile run with the list set pulls only the matching orbit(s) and produces the same GeoTIFFs.
**Acceptance criteria**:
- [ ] Documented yes/no with evidence; if yes, the per-tile orbit source is named (trigger emits it).
- [ ] No coverage regression vs T0.

### Task 3 — `nb_parallel_downloads` tuning (OQ3)  <status: ready · optional>
**What**: Measure CDSE aggregate throughput at `nb_parallel_downloads` 2 vs 4 vs 6 on a multi-frame
tile; determine per-file vs per-account throttle; pick the value at the knee. Config-only (configmap copy).
**Verify**: download wall-time for the same product set at each setting.
**Acceptance criteria**:
- [ ] Chosen value justified by measured wall-time (not a guess); configmap updated in lockstep.

### Task 8 — Skip-seam + frame-set live confirmation (RETIRE THE DESIGN RISK FIRST)  <status: NEXT (with T0)>
**What**: Source analysis already established the seam (see Current state). **Confirm live** the two
assumptions the whole cache rests on: (1) a manually pre-placed extracted SAFE tree at
`data_raw/{prod_id}/{prod_id}.SAFE/` makes a real s1tiling run **skip** that product's CDSE download;
(2) the frame set `cache_frames` will compute equals the products s1tiling actually downloads for a tile.
Decide the frame-list **source** for T5: reuse s1tiling's own `S1FileManager._search_products` (exact
parity, heavier) vs `trigger_cdse.query_products` (already in-repo, must prove parity).
**Verify**:
```
# place one extracted SAFE into data_raw, run s1processor, grep the log for "0 ... will be downloaded" for it
# diff cache_frames' computed frame list vs s1processor's "remote S1 product(s) ... downloaded" for >=2 tiles
```
**Acceptance criteria**:
- [ ] Live skip confirmed (pre-placed SAFE ⇒ that product not re-downloaded).
- [ ] Frame-list source chosen + parity shown on ≥2 tiles (one multi-frame).
- [ ] **If parity fails** → caching is defeated as designed; stop and rethink before T4/T5.

### Task 4 — Provision the in-region S3 frame cache  <status: ✅ DONE 2026-07-01 (dedicated bucket, reuse creds; scoped-key hardening → T4b)>
**What**: Stand up the cache in `platform-deploy`: an in-region S3 **cache prefix** (decide: dedicated
bucket vs a `frame-cache/` prefix in an existing in-region bucket) + a **dedicated least-privilege key
scoped to that prefix** (do **not** reuse the broad `geozarr-s3-credentials` output-bucket key). No RWX
PVC. Endpoint `https://s3.de.io.cloud.ovh.net`.
**Verify**: a throwaway pod with the dedicated key can PUT/GET under the prefix and is **denied** outside it.

**RESOLVED 2026-07-01 — dedicated in-region bucket + reuse existing creds.** User provided a dedicated bucket
**`esa-zarr-sentinel-explorer-cache`** (region `DE`, OVH project `bcc5927763514f499be7dff5af781d57`) — cleaner
than a staging prefix (independent lifecycle for T9, isolated cost). Creds **reused**, not a new scoped key:
the cache pre/post steps run **inside the s1tiling pod, which already mounts `geozarr-s3-credentials`** → reuse
adds **zero new blast radius** (a scoped key only helps in a different trust context, which this isn't). Keeps
T4 to a config choice — no Terraform / new OVH user / kubeseal / Flux — unblocking T5/T7/T10 immediately.
- **Cache location:** bucket `esa-zarr-sentinel-explorer-cache`, prefix `frame-cache/` (objects
  `frame-cache/{prod_id}.tar` = `cache_frames.py` `DEFAULT_PREFIX`). Endpoint `https://s3.de.io.cloud.ovh.net`,
  region `de`. T7 passes `--bucket esa-zarr-sentinel-explorer-cache`; no code change.
- **Creds:** reuse `geozarr-s3-credentials` (in-cluster `devseed-staging`, already mounted by s1tiling).
- **Evidence (live, throwaway pod `t4-cache-probe2`, `amazon/aws-cli` + geozarr key):** `s3 ls` + PUT/GET/DELETE
  of `frame-cache/.t4-probe.txt` all `ok`; probe deleted; pod deleted. (Earlier `t4-cache-probe` also ok on
  `…-s1-l1grd-staging` + `…-tests` — reuse of the geozarr key is broadly valid.)

**Acceptance criteria**:
- [x] Cache location + creds documented; put/get works under the prefix (live-verified on the dedicated
      bucket). *("denied elsewhere" is N/A for the reuse path — moved to the scoped-key follow-up T4b.)*
- [x] Decision recorded: **dedicated bucket** `esa-zarr-sentinel-explorer-cache` (user-provided) over a
      staging prefix — independent lifecycle (T9), isolated cost, zero-new-blast-radius creds.

### Task 4b — (hardening) dedicated least-privilege cache-bucket key  <status: ready · pre-T12, not a canary blocker>
**What**: Replace the reused `geozarr-s3-credentials` with a dedicated OVH S3 user + key scoped to
`arn:aws:s3:::esa-zarr-sentinel-explorer-cache` + `/*` (whole dedicated bucket → simpler than prefix-scoping;
Terraform block in `cloud-infra-deploy/infra/obj_storage.tf` per the `argo-logs` pattern: `_user` +
`_user_s3_credential` + `_user_s3_policy` + a `_storage_object_bucket_lifecycle_configuration` for T9), then
`kubeseal` a `frame-cache-s3-credentials-sealed.yaml` per `platform-deploy/.../SEALED-SECRETS.md`; flip the T7
template to mount it. (Bucket is console-created → import block or just user+policy resources.)
**Verify**: throwaway pod with the scoped key PUT/GET on `esa-zarr-sentinel-explorer-cache` **ok**, PUT to
another bucket **denied**.
**Acceptance criteria**:
- [ ] Scoped key created + sealed + committed; put/get on the cache bucket ok, denied on other buckets.
- [ ] T7 template mounts `frame-cache-s3-credentials` instead of `geozarr-s3-credentials` for the cache steps.

### Task 5 — `cache_frames.py` (pull cache-present SAFEs into `data_raw`)  <status: blocked: T4,T8>
**What**: New `scripts/cache_frames.py`: given the tile's needed frames (source per T8) and the cache
creds, for each frame **present in the cache**, pull its tar object (parallel boto3 across frames = the
spike's lever) and untar into `data_raw/{prod_id}/{prod_id}.SAFE/` so s1tiling's scan skips it; print the
miss list. **Validate `prod_id` as a safe S3 key** (reject path-traversal / unexpected chars). No s1tiling change.
**Verify**:
```
uv run pytest tests/unit/test_cache_frames.py   # frame listing, key validation, hit→untar layout, miss→reported, idempotent
```
**Acceptance criteria**:
- [ ] Unit tests: frame listing, `prod_id` key validation, hit→`{prod_id}/{prod_id}.SAFE/` layout, miss reported, idempotent.
- [ ] Parallel pull (≥3 frames) verified faster than serial.
- [ ] Untarred tree passes s1tiling's `manifest.safe` check (ties to T8).

### Task 6 — populate-on-miss (push new SAFEs to the cache)  <status: blocked: T4>
**What**: After s1processor, tar each SAFE that s1tiling freshly downloaded from CDSE (a cache miss) from
`data_raw/{prod_id}/{prod_id}.SAFE/` and upload the tar to the cache prefix; skip frames already cached
(pulled by T5). Verify upload integrity (no silent partial like the old csi-rclone writeback).
**Verify**: after a tile run with a miss, the frame's tar exists under the prefix and re-extracts cleanly.
**Acceptance criteria**:
- [ ] New SAFEs uploaded (as tar); already-cached frames not re-uploaded; upload size/integrity verified.

### Task 7 — Wire pre/post cache steps into the s1tiling template  <status: 🟢 BUILT + statically validated 2026-07-01 · flag-on runtime test folded into T10 (needs merged image)>
**What**: In `platform-deploy` `eopf-explorer-s1tiling-template.yaml`, add a **pre-step** (`cache_frames.py`)
before `s1processor` and a **post-step** (T6) after it, **both gated by a new template param
`enable_frame_cache` (default `false`)** via Argo `when:` — so the template ships cache-OFF (zero prod
behaviour change) and the canary/ramp flips it per submit. Both steps use the **data-pipeline image** (has
boto3, like `ensure-dem`) and the **`pipeline` nodeSelector + toleration** so the RWO `workspace`
reattaches step→step (zone-binding has bitten this project before). `s1processor` keeps `download:True`
(fetches only misses). No cron-DAG change.
**Verify**: with the flag off, a run executes neither new step (identical to today); with it on, two
adjacent tiles sharing a frame — second after first — the second cache-hits + does **0 CDSE re-fetch**.

**BUILT 2026-07-01 (branches `feat/s1-caching-t7-wiring` [data-pipeline] + `feat/s1-caching-t7-template`
[platform-deploy]).** Two pieces:
1. **Frame-list producer** `scripts/list_tile_frames.py` (+ `tests/unit/test_list_tile_frames.py`, 7 tests
   green, ruff clean) — the pull step's input the plan's "add a pre-step (cache_frames.py)" assumed but
   nothing produced: `cache_frames.py pull` takes an explicit id list, the cron's `discover` collapses to
   one representative/pass, and s1processor searches internally. `list_tile_frames` enumerates the tile's
   overlapping GRD frames via `tile_bbox(tile_id)` + a CDSE STAC query over `[date_start, date_end]`
   (uncollapsed, platform-scoped). **Frame-list source = Option A (user-chosen).** Parity only moves the
   HIT-RATE — s1tiling only reuses a pre-placed SAFE whose id is in its own search, so a wrong/extra id is
   harmless waste and a missed id just downloads.
2. **Template wiring** — additive (219 insert, 0 delete): 2 params (`enable_frame_cache` default `false`,
   `frame_cache_bucket` default `esa-zarr-sentinel-explorer-cache`) + `cache-pull` step (ensure-dem→**pull**
   →process) + `cache-populate` step (upload→**populate**), both gated `when: '…enable_frame_cache' == 'true'`,
   both **best-effort (always exit 0)** so a list/pull/populate error only degrades to a normal CDSE fetch,
   never a wrong output or a red run. Both templates = data-pipeline image + `pipeline` pool + `fsGroup:1000`
   + `geozarr-s3-credentials` (T4 reuse). `populate` mounts `workspace` read-only.
- **Static validation:** `argo lint` clean; `kubectl apply --dry-run=server` accepted by the live Argo CRD;
  YAML step order confirmed `ensure-dem → cache-pull(gated) → process → upload → cache-populate(gated)` — so
  **flag-off is behaviour-identical by construction** (both gated steps skipped).
- **⚠️ Flag-ON dependency (blocks the runtime cache-hit demo → T10):** the deployed pipeline image
  (`v0.8.0-s1rtc-rc7`) does **NOT** contain `list_tile_frames.py` (new) or `cache_frames.py` (merged after
  rc7). Flag-on with rc7 degrades gracefully (both steps WARN + exit 0 → normal download, no cache benefit).
  The flag-on cache-hit test therefore needs a **data-pipeline image built from `feat--s1_grd_phase5` at/after
  the T7 merge** — pinned by T10's canary. Flag-off (default/cron) is unaffected and needs no new image.

**Acceptance criteria**:
- [x] `enable_frame_cache=false` (default): neither pre/post step runs; behaviour identical to today.
      *(proven by construction: additive diff, both steps `when`-gated; argo-lint + server-dry-run clean.)*
- [ ] `enable_frame_cache=true`, 2 frame-sharing tiles: tile-2 logs cache-hit + 0 CDSE bytes for the shared
      frame. *(→ T10 canary — needs the merged pipeline image; graceful no-op proven with rc7.)*
- [ ] Empty-coverage `exit 0` + per-tile retry/`continueOn` isolation still pass (no regression).
      *(populate no-ops on absent/empty `data_raw`; gated steps don't alter the process/upload contract → T10.)*
- [ ] Outputs equivalent to a no-cache run: **same acquisition set/count + per-band pixel data equal**
      (NOT byte-for-byte — OTB embeds processing timestamps, so byte identity is not expected). *(→ T10.)*

### Task 9 — Cache eviction / retention  <status: blocked: T4>
**What**: A retention policy on the cache prefix (S3 lifecycle rule or a small GC step) keyed to the AOI
+ lookback window (e.g. keep last `lookback_days + margin`); the cache must not grow unbounded.
**Verify**: objects older than the window are removed; in-window retained.
**Acceptance criteria**:
- [ ] Policy defined + applied; a dry-run lists exactly the stale objects it would remove.

### Task 10 — A/B canary: cached vs current, 6 frame-sharing tiles  <status: blocked: T7>
**What**: Prove the win by A/B comparison on a **6-tile canary**, not by re-asserting §4. **Both arms are
GeoTIFF-only (skip the ingest template) and override `s3_output_bucket` to an isolated canary prefix** —
perf comparison, no prod cube/STAC writes (see **Isolation invariants**). Both use the same image / window /
`pipeline` pool / resources. Runs **in parallel with the existing cron without interfering** with it.
- **Tile selection (critical):** pick **6 contiguous, frame-sharing tiles** (a block covered by common
  frames) so cross-tile reuse exists — verify the chosen tiles share ≥1 frame via the frame→tile overlap;
  a scattered 6 would show ~no cache benefit and understate the result. **These MUST be NEW tiles outside
  the current `western-europe` AOI — further east, >13.5°E (e.g. a same-zone vertical run in eastern
  Austria / Slovenia / NE Italy / Czechia), not yet cubed** — so the canary never re-processes or contends
  with an already-ingested prod tile.
- **Arm A (control):** the 6 tiles, `enable_frame_cache=false` → current behaviour.
- **Arm B (cached):** the same 6 tiles, `enable_frame_cache=true`, reported as **two** runs: a **cold-fill**
  run (first population — pays the CDSE misses once) and a **warm** run (cache pre-populated) = the
  steady-state per-revisit number.
- **Fairness:** run A and B **separated in time** (not simultaneously on the same frames — CDSE
  server-side warming/throttle would cross-contaminate); note run order.
**Verify** (reuse the perf-capture machinery — pod-node timings + Argo archive, `project_s1rtc_cron_timing_model`,
`tests/fixtures/wf_perf/`): per-frame CDSE GET count, total CDSE bytes + download seconds, download/wall-time
fraction, per-tile + total wall-time — tabulated A vs B(cold) vs B(warm) → `claude-docs/reports/s1_input_caching_canary.md`.
**Acceptance criteria**:
- [ ] 6 frame-sharing tiles selected with the shared-frame overlap shown — **all outside the prod AOI
      (>13.5°E) and not yet cubed**; confirmed none collide with cron/ingested tiles.
- [ ] Isolation held: GeoTIFF-only, `s3_output_bucket` overridden to a canary prefix → **zero writes to the
      prod cube / `…-staging` STAC collections** (verified, e.g. no new items in the prod collection).
- [ ] B(warm): each distinct frame fetched from CDSE **≈once** (≤ concurrency-width); A re-fetches shared frames per tile.
- [ ] B(warm): download **< 30%** of s1processor wall-time (was the majority in A); CDSE egress down **~N×** vs A.
- [ ] B(cold-fill) one-time cost quantified (so the break-even over a revisit window is clear).
- [ ] Outputs (A vs B) equivalent: same acquisitions + per-band pixel data; no-regression checks green.
- [ ] If B's double-fetch under concurrency is material → open T11; else proceed to ramp (T12).

### Task 12 — Ramp the flag (canary → wider → cron)  <status: blocked: T10 green>
**What**: After T10 validates, ramp in stages, **least-blast-radius first**: (1) re-enable **ingest on the
new east canary tiles** (now added to the AOI) and confirm correct cubes/STAC there — the first
ingest-enabled cache run, still off the existing prod tiles; (2) widen `enable_frame_cache` to a larger
**manual** slice of existing prod tiles; (3) only then flip the cron param / template default to `true`.
Staged; **rollback = flip the flag off** (instant, no redeploy).
**Verify**: each stage shows the canary's per-frame ≤1× + download-fraction holding, with ingest producing
correct cubes/STAC; the cron tick (last) reproduces it at AOI scale.
**Acceptance criteria**:
- [ ] Stage 1 (new east tiles, ingest on) produces correct cubes/STAC before any prod tile is touched.
- [ ] Ramp steps recorded; the cache win holds at wider scale; rollback (flag off) verified to restore current behaviour.
- [ ] **Live cron suspend state confirmed** (`argo cron list`) before the default flip; git HEAD shows
      `suspend: false`, so do not assume idle. Cron flipped on only after a clean wider run, and **stays
      suspended until explicitly asked**.

### Task 11 — (conditional) stage-before-fan-out pre-warm  <status: blocked: T10 says material>
**What**: Only if T10 shows the lazy double-fetch is significant: add a `stage-frames` task between the
cron's `discover` and `process-products` that downloads the **union of un-collapsed frames** once
(parallel) to the cache before the fan-out, making tile pods pure cache-readers (race-free, exactly-once).
**Verify**: a cron run fetches each frame from CDSE exactly once across the AOI.
**Acceptance criteria**:
- [ ] Per-frame CDSE GET count == 1 across a full AOI run.

## Open questions
- **OQ2 [owner: user]** — is there an OVH-side S1 mirror bucket (cheaper/faster than CDSE for the cache
  fill)? Would change T6's *source* (mirror vs CDSE), not the design.
- **OQ3/OQ4** — folded into T3/T2.
- **Frame-list source** (s1tiling search vs trigger query) — decided in T8 before T5 lands.

## Done definition
The **6-tile A/B canary (T10)** — run **on new tiles further east, outside the prod AOI, GeoTIFF-only on an
isolated bucket, in parallel with (and without interfering with) the existing cron** — shows the cached arm
(warm) fetches each frame from CDSE **≈once**
(≤ concurrency-width; exactly-once only with T11), is compute-bound (download <30% wall-time), and cuts
CDSE egress ~N× vs the control arm — with the one-time cold-fill cost quantified — while producing
equivalent GeoTIFFs (same acquisitions + pixel data; not byte-identical) and holding the empty-coverage +
retry contracts. The cache ships behind `enable_frame_cache` (default off) and self-evicts; rollout ramps
the flag (T12) with rollback = flag off. P0's platform-filter fix ships first and stands alone (no S1D
pulls). All cluster verification on manual runs; the cron stays suspended until explicitly asked.
