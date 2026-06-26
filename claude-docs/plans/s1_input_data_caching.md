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
  canary** (control = flag off, cached = flag on) on **frame-sharing** tiles, GeoTIFF-only (no ingest),
  comparing CDSE bytes/time + download-fraction + wall-time (T10). Ramp by widening the flag (T12);
  rollback = flip it off. Same template, no fork.

**Explicitly deferred (not this plan):** DEM caching (spec §5 — GLO-30 comes from anon AWS Open Data, is
small, already staged per-workflow; *not* the CDSE bottleneck) and Substrate (ii)/RWX. This plan attacks
**redundant CDSE S1-frame egress** only.

## Current state
| Resource | Status | Notes |
|----------|--------|-------|
| `eopf-explorer-s1tiling` template | per-workflow RWO `workspace` PVC; `ensure-dem→s1processor→upload` | cron **suspended** (maintenance) |
| `s1grd-rtc-cfg-base` configmap | `download:True`, `nb_parallel_downloads:2`, no `relative_orbit_list` | **copy** of `config/S1GRD_RTC.cfg` |
| `s1tiling-eodag4-patch` configmap | **T1 FIXED in source+configmap file** (post-filter now matches STAC `platform`, runs `len>=1`); deploy pending (cron suspended) | **copy** of `analysis/s1tiling_eodag4_patch.py` (regen byte-identical) |
| S1D redundancy | search passes `platformSerialIdentifier` (ignored by cop_dataspace) + no post-filter for `len==1` → S1D downloaded then discarded | verified in `S1FileManager.py:511,528,553` |
| Skip seam | s1tiling scans `data_raw` for `S1*_IW_GRD*`+`manifest.safe`; skips products already in `_product_list` | verified `:627`, `_refresh_s1_product_list` |
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
- [ ] N (avg frames/tile) estimated for the AOI (sets the target ~N× egress drop).

### Task 1 — Fix the platform post-filter (kill the redundant S1D download)  <status: code+tests LANDED; cluster validation pending (cron suspended)>
**What**: The S1D pull is **not** a missing filter. `_search_products:528` already passes
`platformSerialIdentifier=S1A`, but cop_dataspace OData ignores it (same class as `relativeOrbitNumber`,
which the patch removed); the platform post-filter (`:553`) runs only for `len(platform_list) > 1` and
filters by `platformSerialIdentifier`, a property eodag-4 dropped (products carry STAC `platform`, e.g.
`sentinel-1a`) → it matches nothing (the documented "`S1A S1C` → 0 products" bug). **Fix** in
`analysis/s1tiling_eodag4_patch.py`: rewrite the platform post-filter to (a) match the eodag-4 STAC
`platform` property with an `S1A→sentinel-1a` value map, and (b) run for `len(platform_list) >= 1`. This
drops off-platform (S1D) products **before download**, independent of whether cop_dataspace honoured the
search param (belt-and-suspenders), and fixes multi-platform as a bonus. Mirror into the configmap.
**Verify**:
```
uv run pytest tests/unit/test_s1tiling_eodag4_patch.py   # assert the post-filter rewrite (idempotent)
# cluster: a manual S1A run pulls 0 S1D products (compare to T0)
```
**Acceptance criteria**:
- [x] Patch rewrites the post-filter (STAC `platform`, `len>=1`); unit test covers the rewrite + idempotency.
      → `_rewrite_platform_postfilter` in `analysis/s1tiling_eodag4_patch.py`; 6 tests in
      `tests/unit/test_s1tiling_eodag4_patch.py::TestRewritePlatformPostfilter` (rewrite, S1x→sentinel-1x
      value map, relative-orbit filter untouched, patched block compiles, idempotent, raises on drift) —
      all green. Verified the transform applies + the full file byte-compiles against the **real** vendored
      S1FileManager.py for both s1tiling 1.4.0 and 1.4.1.
- [ ] A real S1A run pulls **0 S1D** products (was present at T0) — log evidence. *(blocked: needs a manual
      cluster run; cron suspended. Pair with T0 baseline.)*
- [ ] Requested-platform outputs still produced (no coverage loss); multi-platform list no longer yields 0.
      *(blocked: same manual run.)*
- [x] configmap copy regenerated + committed in lockstep (regen command in the PR).
      → regenerated `platform-deploy .../s1tiling-eodag4-patch-configmap.yaml` via the header's
      `kubectl create configmap … --from-file=… --dry-run=client -o yaml`; embedded script verified
      **byte-identical** to the source. (Commit lands in the platform-deploy PR.)
- [ ] If the property/value map proves wrong on cop_dataspace → record it, revert, don't ship a blind no-op.
      *(conditional, gated on the manual run; the patch fails loud on anchor drift rather than no-op'ing.)*

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

### Task 4 — Provision the in-region S3 frame cache  <status: ready>
**What**: Stand up the cache in `platform-deploy`: an in-region S3 **cache prefix** (decide: dedicated
bucket vs a `frame-cache/` prefix in an existing in-region bucket) + a **dedicated least-privilege key
scoped to that prefix** (do **not** reuse the broad `geozarr-s3-credentials` output-bucket key). No RWX
PVC. Endpoint `https://s3.de.io.cloud.ovh.net`.
**Verify**: a throwaway pod with the dedicated key can PUT/GET under the prefix and is **denied** outside it.
**Acceptance criteria**:
- [ ] Cache location + dedicated least-privilege creds documented; put/get works under the prefix, denied elsewhere.
- [ ] Decision recorded: dedicated bucket vs prefix (cost/region/lifecycle).

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

### Task 7 — Wire pre/post cache steps into the s1tiling template  <status: blocked: T5,T6,T8>
**What**: In `platform-deploy` `eopf-explorer-s1tiling-template.yaml`, add a **pre-step** (`cache_frames.py`)
before `s1processor` and a **post-step** (T6) after it, **both gated by a new template param
`enable_frame_cache` (default `false`)** via Argo `when:` — so the template ships cache-OFF (zero prod
behaviour change) and the canary/ramp flips it per submit. Both steps use the **data-pipeline image** (has
boto3, like `ensure-dem`) and the **`pipeline` nodeSelector + toleration** so the RWO `workspace`
reattaches step→step (zone-binding has bitten this project before). `s1processor` keeps `download:True`
(fetches only misses). No cron-DAG change.
**Verify**: with the flag off, a run executes neither new step (identical to today); with it on, two
adjacent tiles sharing a frame — second after first — the second cache-hits + does **0 CDSE re-fetch**.
**Acceptance criteria**:
- [ ] `enable_frame_cache=false` (default): neither pre/post step runs; behaviour identical to today.
- [ ] `enable_frame_cache=true`, 2 frame-sharing tiles: tile-2 logs cache-hit + 0 CDSE bytes for the shared frame.
- [ ] Empty-coverage `exit 0` + per-tile retry/`continueOn` isolation still pass (no regression).
- [ ] Outputs equivalent to a no-cache run: **same acquisition set/count + per-band pixel data equal**
      (NOT byte-for-byte — OTB embeds processing timestamps, so byte identity is not expected).

### Task 9 — Cache eviction / retention  <status: blocked: T4>
**What**: A retention policy on the cache prefix (S3 lifecycle rule or a small GC step) keyed to the AOI
+ lookback window (e.g. keep last `lookback_days + margin`); the cache must not grow unbounded.
**Verify**: objects older than the window are removed; in-window retained.
**Acceptance criteria**:
- [ ] Policy defined + applied; a dry-run lists exactly the stale objects it would remove.

### Task 10 — A/B canary: cached vs current, 6 frame-sharing tiles  <status: blocked: T7>
**What**: Prove the win by A/B comparison on a **6-tile canary**, not by re-asserting §4. **Both arms are
GeoTIFF-only (skip ingest) and write to an isolated canary output prefix** — perf comparison, no prod
cube/STAC writes. Both use the same image / window / `pipeline` pool / resources.
- **Tile selection (critical):** pick **6 contiguous, frame-sharing tiles** (a block covered by common
  frames) so cross-tile reuse exists — verify the chosen tiles share ≥1 frame via the frame→tile overlap;
  a scattered 6 would show ~no cache benefit and understate the result.
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
- [ ] 6 frame-sharing tiles selected with the shared-frame overlap shown.
- [ ] B(warm): each distinct frame fetched from CDSE **≈once** (≤ concurrency-width); A re-fetches shared frames per tile.
- [ ] B(warm): download **< 30%** of s1processor wall-time (was the majority in A); CDSE egress down **~N×** vs A.
- [ ] B(cold-fill) one-time cost quantified (so the break-even over a revisit window is clear).
- [ ] Outputs (A vs B) equivalent: same acquisitions + per-band pixel data; no-regression checks green.
- [ ] If B's double-fetch under concurrency is material → open T11; else proceed to ramp (T12).

### Task 12 — Ramp the flag (canary → wider → cron)  <status: blocked: T10 green>
**What**: After T10 validates, widen `enable_frame_cache` from the 6-tile canary to a larger manual slice,
then to the cron (set the cron param / template default to `true`). Staged; **rollback = flip the flag off**
(instant, no redeploy). Re-enable ingest for the ramped tiles (the canary skipped it).
**Verify**: a wider run (then a cron tick) shows the canary's per-frame ≤1× + download-fraction holding,
with ingest producing correct cubes/STAC.
**Acceptance criteria**:
- [ ] Ramp steps recorded; the cache win holds at wider scale; rollback (flag off) verified to restore current behaviour.
- [ ] Cron flipped on only after a clean wider run (cron stays suspended until explicitly asked).

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
The **6-tile A/B canary (T10)** shows the cached arm (warm) fetches each frame from CDSE **≈once**
(≤ concurrency-width; exactly-once only with T11), is compute-bound (download <30% wall-time), and cuts
CDSE egress ~N× vs the control arm — with the one-time cold-fill cost quantified — while producing
equivalent GeoTIFFs (same acquisitions + pixel data; not byte-identical) and holding the empty-coverage +
retry contracts. The cache ships behind `enable_frame_cache` (default off) and self-evicts; rollout ramps
the flag (T12) with rollback = flag off. P0's platform-filter fix ships first and stands alone (no S1D
pulls). All cluster verification on manual runs; the cron stays suspended until explicitly asked.
