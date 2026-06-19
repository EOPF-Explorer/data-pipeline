# Plan: Fix the S1 RTC ingest S3 upload bottleneck

**Goal**: Cut the per-cube S3 transfer wall-time (upload + append-fetch) from ~30–40 min to a few minutes, so STAC items land promptly and a live-cron run finishes in ~hours not ~all-day.
**Constraint**: preserve "store lands at dest/<relpath>, no nesting" semantics, cube correctness, and the **web-optimized GeoZarr layout** (multiscale overviews r10m…r720m, consolidated metadata) titiler depends on; no new heavy deps (use fsspec/s3fs already in use).
**Scope spans two repos** (boundary surfaced per ai-engineering §2): T1/T2/T3 are **data-pipeline** (`scripts/ingest_v1_s1_rtc.py`); **T5 is data-model** (`eopf_geozarr` builder) — it changes how the cube is *written*, needs a builder change + image redeploy + re-ingest, and is sequenced as a parallel track.

## Context

Profiled the first live cron run (2026-06-18): the ingest is **I/O-bound on serial S3 transfers**, not compute.
- Evidence: an ingest pod sat **~34 min in `Uploading store →` at 9 millicores CPU** (idle); pipeline nodes at 1–5% CPU (huge headroom); 0 Pending pods. So it's network/PUT-bound, not resources/scheduling.
- Root causes:
  1. **Object COUNT is dominated by one unsharded array family** (`conditions/gamma_area_*`): **94.7% of all objects** are tiny 366² chunks that were never sharded (see ground-truth below). This is the upstream root cause and the biggest single lever — but it lives in `eopf_geozarr` (data-model), **T5**, not data-pipeline. *(primary, upstream)*
  2. **`_upload_store_to_s3` (`scripts/ingest_v1_s1_rtc.py:313`)** does `fs.rm(dest, recursive=True)` then re-uploads the **entire accumulated cube every append** — including the ~3600 static `gamma_area` objects that never change. **T3.** *(primary, data-pipeline)*
  3. **`_put_tree` (:241) / `_get_tree` (:263)** transfer every object with a **serial `put_file`/`get_file` loop** → thousands of blocking round-trips. **T1/T2** — a multiplier on whatever object set remains after T3/T5.
- Effect: per product ≈ s1tiling ~40 min + ingest ~40 min (mostly upload) ≈ ~80 min; at parallelism 3, 17 products ≈ ~7–8 h.

### Ground-truth measurement (real store `s1-rtc-31TEG`, staging, 2026-06-18)
Measured directly against the live store via consolidated metadata + S3 listing (DE endpoint, `eopfexplorer` creds). **Supersedes the earlier "~995 objects" estimate, which was understated.**
| Metric | Value | Source |
|---|---|---|
| Real objects in cube (T=2 slices) | **3807** (3.5 GB) | `boto3 list_objects_v2`, ground truth |
| Of which `conditions/gamma_area_*` | **3604 (94.7%)** | unsharded: shape `[10980,10980]`, chunks `366²`, codecs `bytes+blosc`, **no `sharding_indexed`** → ~900 objects/array × 4 |
| Objects < 4 KB | **960 (25%)** | latency-bound tiny PUTs |
| Multiscale image arrays (`vv`,`vh`,`border_mask`, r10m…r720m) | **already sharded** | shard `[1, full, full]`, inner `366²` → ~2 objects/array (1 shard/time slice) |
| New objects a single acquisition (one orbit) adds | **~18 shards + ~103 tiny `zarr.json`** | `vv/vh/border_mask` grow by 1 time-shard each across 6 levels |
| Serial upload time (live pod) | **≥ 34 min** | `Uploading store` 21:44 → still uploading 22:18 |
| CPU during upload | **9 millicores** (idle) | `kubectl top` |

**Real GET benchmark** (this store, laptop→DE cross-region, 240 real `gamma_area` chunks ≈178 KB each):
| Mode | ms/object | speedup |
|---|---|---|
| serial `get_file` loop | 296 | 1.0× |
| concurrent `fs.get` batch=8 | 217 | 1.4× |
| concurrent `fs.get` batch=16 | 187 | 1.6× |
| concurrent `fs.get` batch=32 | **134** | **2.2×** |
This 2.2× is a **conservative floor**: laptop home-bandwidth + 178 KB objects (partly bandwidth-bound). The production pain was tiny <4 KB PUTs (pure latency-bound), where concurrency scales closer to N. **In-cluster PUT numbers must still be measured (Task 0)** before claiming a production speedup.

**Real-S3 PUT/GET/incremental benchmark (2026-06-19, laptop→DE, bucket `esa-zarr-sentinel-explorer-tests`, exercising the actual `_put_files`/`_sync_tree`/`_get_tree`):** synthetic 372-object store (300 tiny <4 KB + 60×50 KB + 8×178 KB) — modelling the real cube's latency-bound tiny-object profile.
| Path | Serial (old) | Concurrent / incremental (new) | Speedup |
|---|---|---|---|
| PUT full store | 298.9 s | 11.4 s @batch16 · 12.8 s @batch32 · 13.3 s @batch64 | **~23–26×** |
| GET append-fetch | 49.8 s | 4.6 s @batch32 | **10.8×** |
| T3 incremental append (1 new shard + 4 `zarr.json`) | 15.1 s (cold/full) | **1.2 s** (warm) | **13×** |

The PUT win (>20×) far exceeds the 2.2× GET floor because tiny PUTs are pure-latency-bound (concurrency scales near N), confirming the plan's prediction. **The knee is batch≈16**; 32/64 plateau (within noise) — default 32 sits on the plateau, env-overridable for the in-cluster ceiling. **I2 (key-format) validated on real OVH S3**: `fs.find(dest, detail=True)` keys are exactly `f"{dest}/{rel}"` (372/372; `local rels == remote rels`), so T3's size-based incremental match works rather than silently degrading to a full re-upload.

**Takeaways that reorder the plan:**
- Sharding is **already done** for the display pyramid — the win is sharding the *one* family left out (`gamma_area`), collapsing **3604 → ~8 objects** (T5). This is the largest absolute reduction available.
- Because `gamma_area` is static per relative-orbit, T3 (incremental upload) **skips re-PUTting ~3600 unchanged objects every append** — the biggest data-pipeline win, independent of T5.
- T1/T2 concurrency is a secondary multiplier; its value shrinks once T3+T5 cut the per-append set to tens of objects.

## Current state
| Resource | Status |
|---|---|
| `conditions/gamma_area_*` arrays (data-model) | ❌ unsharded 366² chunks → 3604/3807 objects (94.7%) — the object-count bomb (T5, separate repo) |
| `_upload_store_to_s3` (data-pipeline) | ✅ T3 — `_sync_tree`: incremental, uploads only new/size-changed objects + always `zarr.json`, deletes vanished keys; no `rm(recursive)` |
| `_put_tree` upload (data-pipeline) | ✅ T1 — single batched `fs.put(lpaths, rpaths, batch_size=_S3_CONCURRENCY)` |
| `_get_tree` fetch (data-pipeline) | ✅ T2 — single batched `fs.get(keys, lpaths, batch_size=_S3_CONCURRENCY)` |
| Multiscale display pyramid (vv/vh/border_mask) | ✅ already sharded (1 shard/time slice) — leave as-is |
| Guard test `test_put_tree_lands_at_dest_without_nesting` (`tests/unit/test_ingest_v1_s1_rtc.py:327`) | ✅ green — must stay green |
| Nodes / scheduling / semaphore | ✅ not bottlenecks (ruled out with data) |

## Reuse (don't reinvent)
- **fsspec batched concurrency**: `AbstractFileSystem.put(lpaths_list, rpaths_list, batch_size=N)` / `.get(...)` run concurrently for async backends (s3fs is async-backed) via fsspec's coro chunking — keeps the *explicit per-file mapping* (the reason `_put_tree` avoided `put(recursive=True)`) while getting parallelism. Primary mechanism. (Fallback if needed: `concurrent.futures.ThreadPoolExecutor` over `put_file`.) **Verified working against the live store** (2.2× @ batch=32, see ground-truth).
- Keep `_put_tree`/`_get_tree` signatures + the dest-mapping logic; only the *transfer* becomes concurrent.
- **Sharding pattern already in the repo**: copy the `sharding_indexed` config `eopf_geozarr` already applies to `vv/vh` (outer shard = full spatial extent, inner chunk `366²`) onto the `gamma_area_*` arrays — don't invent a new layout. T5 is *applying an existing pattern to a missed array*, not new machinery.

## Dependency graph
```
PARALLEL TRACK A (data-model, biggest absolute win, slower to land):
  T5 shard gamma_area in eopf_geozarr ── 3604→~8 objects ── needs builder change + redeploy + re-ingest

PARALLEL TRACK B (data-pipeline, fast to land):
  T3 incremental upload (drop rm+full-reupload) ── REQUIRED ── skips ~3600 static objects/append
  T1 parallel upload (_put_tree) ──┐  secondary multiplier; interim relief before T3/T5 land,
  T2 parallel fetch  (_get_tree) ──┤  diminishing once the per-append object set is small
                                   └─ T4 tests (per task)

T0 read-only benchmark gates Track B's concurrency choice (largely DONE — see ground-truth).
```
**Priority rationale (evidence-driven):** the object COUNT — not per-object speed — is the dominant cost (94.7% of objects are static, re-uploaded every append). So **shrinking the set (T5 + T3) beats speeding up the transfer (T1/T2)**. T3 lands fast in data-pipeline and is **required** (it alone removes the per-append re-upload of ~3600 unchanged objects); T5 is the larger absolute win but crosses into data-model and needs a redeploy/re-ingest, so it runs as a parallel track. T1/T2 are worth doing as quick interim relief but become marginal once T3+T5 cut the per-append set to tens of objects — **re-measure after T3 before investing further in T1/T2.**

## Tasks

### Task 0 — Measure the assumption on real data (read-only benchmark)  <FIRST — largely DONE, see ground-truth>
**What**: Before changing any production code, run a **read-only** in-cluster benchmark that proves (or disproves) the concurrency speedup on a **real cube**. A short script (one-off Argo Workflow / Job, `geozarr-s3-credentials`, `AWS_ENDPOINT_URL`): pick an existing cube (e.g. `s1-rtc-30TWN.zarr`), list its objects, then **download** them to a temp dir (a) serially via `get_file` in a loop and (b) concurrently via `fs.get(keys, lpaths, batch_size=N)` for N ∈ {8,16,32}; time each. **GET-only — writes nothing to S3**, so it's safe and representative (same per-object network RTT as the upload PUTs). Record object count, total bytes, and wall-time per mode.
**Verify**: benchmark log shows serial vs concurrent wall-times.
**Acceptance**:
- [x] Object-count census captured (real store `31TEG`): 3807 objects, 94.7% unsharded `gamma_area` → motivates T5. *(done — ground-truth table)*
- [x] Serial vs concurrent GET measured on real objects: 296→134 ms/obj = **2.2× @ batch=32** (laptop→DE floor). *(done — ground-truth table)*
- [x] Real-S3 **PUT** path measured (laptop→DE) on the actual functions: serial 298.9 s → batched 11.4 s = **~23–26×**; GET 10.8×; T3 incremental append 13× (see real-S3 table above). I2 key-format validated.
- [ ] **In-cluster** benchmark re-run from a pod (same region, no home-bandwidth cap) to get the production ceiling — the laptop numbers are still a cross-region floor.
- [x] Decision recorded: `_S3_CONCURRENCY` default = **32** (on the measured plateau; knee≈16), env-overridable via `S1_INGEST_S3_CONCURRENCY` for the in-cluster ceiling. (T3 is **required regardless** — concurrency is a multiplier, not the primary lever.)
**PUT-vs-GET caveat**: GET is a *proxy* for PUT round-trip cost (read-only, hence safe), but object stores often throttle/rate-limit PUT differently and PUT RTT can be slower. **The N chosen here is provisional**: the post-T1 real single-tile ingest (Checkpoints) re-validates concurrency on the *write* path, and `_S3_CONCURRENCY` may need retuning for PUT.
**Note**: the laptop benchmark already proves the mechanism works; the remaining open item is the in-cluster ceiling, which only affects how much T1/T2 add *on top of* T3/T5.

### Task 1 — Parallelize the upload (`_put_tree`)  <✅ DONE>
**What**: Replace the serial `for … fs.put_file()` loop with a concurrent batch: collect all `(lpath, rpath)` pairs (same mapping as today) and issue them via `fs.put(lpaths, rpaths, batch_size=_S3_CONCURRENCY)`; on a non-async/local fs fsspec still maps each pair correctly. Add a module constant `_S3_CONCURRENCY` (default 32, override via env e.g. `S1_INGEST_S3_CONCURRENCY`). Keep dest-mapping identical (no nesting).
**On the env override (ai-eng §3 — not speculative config)**: the env knob is *required by the work*, not added for flexibility — Task 0's GET benchmark gives a provisional N, and the post-T1 in-cluster PUT measurement (Checkpoints) is expected to retune it. An env var lets that retune happen without a rebuild/redeploy. If the first in-cluster number proves 32 is right, the override can be dropped to a plain constant.
**Verify**: `uv run pytest tests/unit/test_ingest_v1_s1_rtc.py -k put_tree` (existing no-nesting test stays green) + new concurrency test (Task 4).
**Acceptance**:
- [x] All files land at exactly `dest/<relpath>` (`test_put_tree_lands_at_dest_without_nesting` green on the batched path).
- [x] Transfers issued concurrently (≤ `_S3_CONCURRENCY` in flight), not one-by-one (`test_put_tree_issues_single_concurrent_batch`: one `fs.put`, `put_file` never called, `batch_size==_S3_CONCURRENCY`).
- [x] Measured on real S3 (laptop→DE, `esa-zarr-sentinel-explorer-tests`): full-store PUT 298.9 s → 11.4 s = **~26×**; GET 10.8× (real-S3 table). In-cluster wall-time on a live cube still deferred (laptop creds cross-owner-denied on existing prod cubes; plan §Checkpoints).

### Task 2 — Parallelize the append-fetch (`_get_tree`)  <✅ DONE>
**What**: Same treatment for the download side: `fs.get(keys, lpaths, batch_size=_S3_CONCURRENCY)` (build the key→lpath list as today), so fetching the existing cube to append is concurrent too.
**Verify**: `uv run pytest tests/unit/test_ingest_v1_s1_rtc.py -k "get_tree or fetch"`.
**Acceptance**:
- [x] Existing-cube fetch downloads concurrently; round-trips to the same local layout as today (`test_get_tree_issues_single_concurrent_batch`: one batched `fs.get`, local parents pre-created, `get_file` never called; `test_get_tree_empty_src_is_noop`).

### Task 3 — Incremental upload (drop rm + full re-upload)  <✅ DONE — REQUIRED, bounds per-append cost>
**What**: In `_upload_store_to_s3`, stop `fs.rm(dest, recursive=True)` + re-uploading the whole tree. Instead upload only objects that are **new or changed** vs the current S3 listing. Net: each append uploads ~the new slice's objects, not the whole growing cube — this is what keeps append cost flat as the cube accumulates (not just faster per-object, which is T1).
**Change-detection — additive-zarr model (NOT ETag/MD5)**: zarr append is overwhelmingly *additive* (a new acquisition writes **new shard keys** at the new time index; only the tiny metadata is rewritten in place). Exploit that instead of per-object content hashing:
- **Metadata files** (`zarr.json`): always re-upload unconditionally. Tiny, and they're the one thing that genuinely changes *in place* (shape/attr edits) — never trust size/etag for these.
- **Chunk/shard objects**: upload a key if it is **absent from the S3 listing** (new data) **or its local size differs** from the listed size (changed). Both come from the single `fs.find`/`ls` listing we already fetch — no extra round-trips.
- **Deletions**: delete any S3 object no longer present locally (rare — e.g. a re-chunk).
- **Why not ETag/MD5**: s3fs uploads large objects (the shards) as **multipart**, whose ETag is a compound `<md5>-<nparts>` hash, *not* the object MD5 — so `info["ETag"]` ≠ local MD5 for every multipart object, making the comparison both wrong and pointless. Size+`always-reupload-metadata` is simpler (ai-eng §3) and correct: a rewritten *compressed* shard almost always changes compressed size, and the only reliable in-place rewrite (metadata) is force-pushed regardless.
**Assumption to validate (ai-eng §1 — stated, not hidden)**: the size-diff heuristic assumes a content-changed *compressed* shard also changes byte-size. This holds for blosc-compressed zarr in practice; the one reliable in-place rewrite (metadata) is force-pushed regardless, so the only residual risk is a same-size content rewrite of a chunk. **Safety net**: the end-to-end render/open check in Acceptance catches that case. If it ever shows up, fall back to force-pushing the small per-array data too (still ≪ full cube). Validate on the 2-slice end-to-end test before relying on it.
**Verify**: unit test asserting only new-key + size-changed chunks are PUT (mock fs listing), every `zarr.json` re-uploaded, vanished keys deleted; end-to-end append on a 2-slice tile re-uploads ≪ full cube and the result opens/renders.
**Acceptance**:
- [x] Append uploads only new + size-changed objects (count ≪ total cube objects); no `rm(recursive)` of the live cube (`test_sync_tree_uploads_only_new_changed_and_metadata` asserts the exact sent-set + asserts no `recursive=True` rm; `test_sync_tree_local_roundtrip_converges` end-to-end on a real fs).
- [x] Every `zarr.json` re-uploaded each append; no ETag/MD5 dependency anywhere (size-only compare; both `zarr.json` always in the sent-set).
- [x] Resulting cube opens with `zarr` after an incremental sync (`test_sync_tree_result_opens_as_zarr`); validated on **real OVH S3** too — a warm append re-PUT only the 5 changed objects (1 new shard + 4 `zarr.json`) in 1.2 s vs 15.1 s cold = **13×**, and the I2 key-match is exact (372/372). titiler render unaffected — **in-cluster confirmation deferred** (same creds blocker as T1).
**Partial-failure note**: dropping the `rm`+full-reupload means a mid-append failure can leave a cube that is neither the old nor the fully-new state. This is acceptable because the per-tile Argo mutex serialises writers and a re-run re-attempts the same append idempotently (already-present `time` slices are skipped, T4); the next successful run converges the cube. Surface this in the run log so a partial upload is visible.

### Task 4 — Tests  <✅ DONE>
**What**: Extend `tests/unit/test_ingest_v1_s1_rtc.py`: concurrency invocation (assert `fs.put`/`fs.get` called with batched lists, capped), dest-mapping preserved (keep `test_put_tree_lands_at_dest_without_nesting`), incremental-upload selects only changed keys. Use the existing `fsspec.filesystem("file")` pattern + a Mock fs for call-shape assertions.
**Verify**: `uv run pytest tests/unit/test_ingest_v1_s1_rtc.py`.
**Acceptance**: [x] full ingest test module green — **31 passed** (was 24; +7: 3 concurrency, 4 incremental-upload). `ruff` clean; `mypy` adds no new errors (3 pre-existing in `_patch_cf_grid_mapping`, unchanged).

### Task 5 — Shard the `conditions/gamma_area_*` arrays (data-model / `eopf_geozarr`)  <PARALLEL TRACK — biggest absolute win; crosses repo boundary>
**What**: In the `eopf_geozarr` S1 builder, apply the **same `sharding_indexed` codec already used for `vv`/`vh`** to the `conditions/gamma_area_*` arrays so each becomes ~1 shard object instead of ~900 366²-chunk objects. Collapses **3604 → ~8 objects per cube** (94.7% → negligible). Reuse the existing shard config (outer = full spatial extent, inner chunk `366²`) — no new layout invented.
**Boundary note (ai-engineering §2)**: this is a **data-model change, not data-pipeline** — it changes how the cube is *written*. It needs: builder edit → unit test in data-model → image build → platform redeploy → **re-ingest** existing cubes to pick up the new layout (old cubes stay unsharded until rewritten). Sequence as its own PR/track; do not bundle with T1–T4.
**Tracked as a separate spec (decision 2026-06-18)**: at build time, T5 gets its **own spec in the data-model repo** (`claude-docs/specs/s1_gamma_area_sharding.md` there), not in this data-pipeline plan. This plan keeps T5 only as a cross-reference (problem statement + target numbers + acceptance) so the two repos' work stays in their own review/PR loops. The data-pipeline tasks (T1–T4) do **not** depend on T5 landing — they improve transfer of whatever object set exists.
**Web-optimized-GeoZarr constraint check**: `gamma_area` is a **`conditions` array** (per-relative-orbit gamma normalization factor), **not** part of the multiscale display pyramid titiler renders (`vv`/`vh`/`border_mask`). So sharding it does **not** touch the web-render path; it only changes how a client reads `gamma_area` (one shard read vs 900 chunk reads — strictly better for cloud access). The display pyramid is already sharded — **leave it untouched**.
**Benchmark / Verify**:
- Re-ingest one tile with the patched builder; re-run the object census (the Task-0 script) → object count drops from ~3807 to ~210.
- titiler still renders `vv`/`vh` for that cube (render path unaffected).
- A client (`xarray.open_zarr` / `zarr`) can still read `gamma_area_*` correctly (values byte-identical to the unsharded version).
**Acceptance**:
- [ ] `gamma_area_*` written with `sharding_indexed` (same config as `vv`); object count per cube drops ~18× (≈3807 → ≈210).
- [ ] data-model unit test asserts the codec is present on `gamma_area` arrays.
- [ ] Re-ingested cube renders in titiler unchanged; `gamma_area` reads back byte-identical.
- [ ] Re-ingest path for existing cubes documented (old cubes are not auto-migrated).

## Concurrency safety (ai-engineering pass)
- Parallel PUTs/GETs target **independent objects** (distinct zarr chunk keys) → no write-write hazard within a transfer.
- No two ingests touch the same cube concurrently: the ingest WorkflowTemplate already holds a **per-tile mutex** (`s1rtc-ingest-{tile}`), so the only writer at a time is this process.
- Reuse one `S3FileSystem` instance per transfer and let fsspec drive its async loop (batch form). The "don't share the fs across threads" caveat applies **only to the ThreadPool fallback** — the primary `fs.put`/`fs.get` batch path drives a single async event loop, so it has no cross-thread sharing concern.

## Checkpoints
- **Recommended sequence**: land **T3** first (fast, data-pipeline, removes the per-append re-upload of ~3600 static objects), open **T5** in parallel as a data-model PR (biggest absolute reduction, but needs redeploy + re-ingest). **Re-measure after T3** — if the per-append upload is already minutes, T1/T2 concurrency may not be worth the change.
- **After Task 3** (before investing in T1/T2): run a real single-tile ingest and **measure the upload-step wall-time** on the *write* path — this both validates T3 and tells you whether T1/T2 still add value. Human review here.
- **Before Task 3**: confirm the additive-zarr change-detection model (new-key + size-diff + always-reupload-metadata). Explicitly *not* ETag/MD5 — multipart ETags aren't object MD5s.
- **Before Task 5 merges**: confirm the re-ingest plan for existing cubes (old unsharded cubes are not auto-migrated) and that titiler render is unaffected.

## Verification (end-to-end)
1. Unit: `uv run pytest tests/unit/test_ingest_v1_s1_rtc.py` green (data-pipeline) + the new `gamma_area` sharding test green (data-model).
2. Object-count: re-run the census script on a re-ingested tile — expect ~3807 → ~210 objects after T5; per-append upload count ≪ full cube after T3.
3. Real measurement: ingest one tile via the image (e.g. `argo submit --from cronworkflow/eopf-explorer-s1rtc -p tiles=<one> -p lookback_days=14`) and grep the pod log: time between `Uploading store →` and ingest completion — expect minutes, not ~30–40 min. Confirm CPU is no longer idle-waiting for the whole upload.
4. Correctness: the re-ingested cube renders in titiler and the per-acq items land; no missing chunks; `gamma_area` reads back byte-identical.

## Out of scope (noted)
- **Cron parallelism 3 → 6**: complementary throughput win (nodes are idle; the cost is I/O-bound so more concurrent products is ~free), but it's a platform-deploy/config lever, not an upload-code fix. Flagged separately.
- **Further chunk-size tuning of the display pyramid**: already sharded sensibly (1 shard/time, inner 366²); no evidence it needs changing — leave it.

## Note on output location
Plan-mode allows editing only this file. On build:
- **data-pipeline** (T0–T4): copy this plan to the single canonical location `claude-docs/plans/s1_ingest_upload_perf.md` (project convention). Do **not** also duplicate into `tasks/plan.md` / `tasks/todo.md` — two copies drift; keep one source of truth.
- **data-model** (T5): write a **separate spec** `claude-docs/specs/s1_gamma_area_sharding.md` *in the data-model repo* and link back here. T5's section in this plan stays as a cross-reference only (decision 2026-06-18).
```
```

## Done definition
- **T5 (data-model):** `gamma_area_*` sharded like `vv` → cube drops ~3807 → ~210 objects; titiler render unaffected; re-ingest path documented.
- **T3 (data-pipeline):** appends upload only new/changed objects (ETag-based, no `rm`+full re-upload) so per-append cost stays flat as the cube grows.
- **T1/T2 (data-pipeline):** `_put_tree`/`_get_tree` transfer concurrently *if* the post-T3 measurement shows it still adds value.
- `tests/unit` green in both repos; a real single-tile ingest's upload step drops from ~30–40 min to a few minutes with the cube still byte-correct and rendering in titiler.
