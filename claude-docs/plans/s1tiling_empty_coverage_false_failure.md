# Plan: s1tiling empty-coverage tiles falsely fail the cron run

**Goal**: A cron tick that hits a tile with no S1 coverage in the requested orbit ends **Succeeded** (that tile skipped), instead of marking the whole run **Failed**.
**Constraint**: Surgical. Treat "0 acquisitions" as a legitimate success across the three pipeline stages that touch it; keep genuine failures red and alertable. No new Argo output-parameter plumbing.
**Status**: Code changes **BUILT + unit-verified** (not yet deployed). Branch `fix--s1tiling-empty-coverage` in both repos.

> Two repos: the s1tiling WorkflowTemplate is in **platform-deploy**; the upload step's script is in **data-pipeline**. Plan filed in data-pipeline `claude-docs/` per the 2-repo precedent of `s1tiling_compute_download_perf.md`.

---

## Problem & root cause (evidence)

**Symptom.** Every cron run since **Sun 2026-06-21 05:00 UTC** shows the parent `eopf-explorer-s1rtc-*` as `Failed`. (Earlier runs Succeeded and were GC'd by `ttlStrategy.secondsAfterSuccess=300`; Failed runs are retained 7 days, so the list begins exactly when the failures began.)

**Not a regression, not the append-time fix.** Per run, **47–61 Pods succeed and exactly 2 fail**; **0 `ingest-v1-s1rtc` children ever failed**. The two failures are always the same two `submit-s1tiling` branches:

| Tile · orbit | Failing source granule (identical every run) |
|---|---|
| `30TXT` · descending | `S1A_IW_GRDH_1SDV_20260615T062423…_08304A_6925` |
| `31TGJ` · descending | `S1A_IW_GRDH_1SDV_20260619T055222…_08325C_5E00` |

(`30TXQ` ascending failed once, Sun 17:00, then succeeded — transient, out of scope.)

**Authoritative log** (Argo artifact bucket `esa-zarr-sentinel-explorer-argo-logs`, key `…/s1processor-*/main.log`), identical for both tiles:
```
DEBUG - Summary of 0 tasks related to S1 -> S2 transformations of 31TGJ
INFO  -  -> Nothing has been executed
INFO  - Situation: 0 computations errors. 0 search failures. 0 download failures. 0 download timeouts
ERROR: S1Processor produced no acquisition GeoTIFFs in /data/data_out/31TGJ/   <- the contract, exit 1
```
s1tiling reports **"no error detected"** but generates **0 tasks → 0 GeoTIFFs** for these `(tile, descending)` pairs — genuine **no descending coverage** (in the same run, 5 other descending tiles succeeded: `30UUU 30UXU 30UXV 31TEK 31UDP`). `discover` over-selects on a coarse STAC bbox; s1tiling's precise MGRS-vs-footprint geometry is the real authority and correctly yields nothing.

**Why the run goes red.** Three stages each treat "empty" as fatal/expected-fatal — but only the *ingest* stage was ever hardened:

| Stage | Where | Empty-input behaviour (before) |
|---|---|---|
| 1. s1tiling `process` | `eopf-explorer-s1tiling-template.yaml:347-354` | `exit 1` ("empty-day contract") |
| 2. s1tiling `upload` | `data-pipeline scripts/upload_s1tiling_outputs.py:71-77` | `return 1` ("No GeoTIFFs found") |
| 3. ingest | `data-pipeline scripts/ingest_v1_s1_rtc.py:163-165` → `run_ingest_register.py:109-111` | **`return 2` → `return 0`** ✅ already clean (PR #265, the 30TWQ edge-tile fix) |

The s1tiling `exit 1` was meant to be swallowed by the parent's `continueOn: {failed: true}` — but in current Argo, `continueOn` lets the DAG branch proceed (ingest *is* correctly gated off) yet the **workflow still ends `Failed`**. So: (a) **false-alarm `Failed`** every tick a no-coverage tile appears (masks real failures); (b) **wasted compute** — `exit 1` trips the s1processor `retryStrategy` (`limit:3`, 2m/6m/18m), re-running the deterministic empty result **4×**, re-downloading 30TXT's **1.76 GB each time** (the template comment at `:253` already flagged this).

**Fix.** Align stages 1 & 2 with the already-hardened stage 3: empty is a **success-with-no-output**, not a failure. The empty prefix then flows cleanly to ingest, which no-ops it. No acq_count output-parameter threading is needed (an earlier draft proposed it; resolving OQ-2 showed ingest already handles empty, so that machinery was dropped).

## No-regression analysis (every path traced against real code)

| Scenario | `process` | `upload` | `ingest` | child wf | **parent run** |
|---|---|---|---|---|---|
| **Empty (no coverage)** | exit 0 *(new)* | `not files → return 0` *(new)* | empty prefix → `return 2` → orch `return 0` | **Succeeded** | **🟢 Succeeded** |
| Normal (has data) | exit 0 | upload N → 0 | ingest → 0 | Succeeded | 🟢 Succeeded *(unchanged)* |
| Real OTB/download crash | S1Processor ≠0 → `set -e` aborts | skipped (steps halt) | not reached | Failed | 🔴 Failed *(alertable, unchanged)* |
| Upload S3 error / verify mismatch | exit 0 | `return 1` *(kept)* | not reached | Failed | 🔴 Failed *(unchanged)* |
| Duplicate basenames | exit 0 | `return 1` *(kept)* | not reached | Failed | 🔴 Failed *(unchanged)* |

Key guarantees:
- Empty is established by `process` itself (counts `data_out/{TILE}/*.tif`); `upload` re-checks the **same** dir, so a `return 0` there cannot mask "files produced but in the wrong place."
- ingest returns 2 **immediately after discover, before any cube fetch/append** (`ingest_v1_s1_rtc.py:162-165`) — it never touches the existing cube. The extra ingest pod on an empty tile is a few-second no-op.
- All genuine-error exit codes (1 for OTB/upload/dup, non-zero for S1Processor) are untouched → failure detection is preserved.

## Current state
| Resource | Status |
|---|---|
| Cron `eopf-explorer-s1rtc` (`devseed-staging`) | Active, `0 1,5,9,13,17,21 * * *`, `ttlStrategy` at original `{success:300, failure/completion:604800}` |
| `eopf-explorer-s1tiling-template.yaml` | **EDITED** (branch off platform-deploy `main`): `process` empty ⇒ `exit 0`; comments at `:125`, `:253`, `:347` updated. `argo lint` clean. |
| `upload_s1tiling_outputs.py` | **EDITED** (branch off data-pipeline `feat--s1_grd_phase5`): empty ⇒ `return 0`; test flipped to `returns_0_clean_skip`. |
| `ingest_v1_s1_rtc.py` | unchanged — already `return 2` on empty prefix |
| Tests | `uv run pytest tests/unit/test_upload_s1tiling_outputs.py tests/unit/test_ingest_v1_s1_rtc.py` → **39 passed** |

## Dependency graph
```
T1 data-pipeline: upload empty ⇒ return 0 (+test)   ── DONE
T2 platform-deploy: s1tiling process empty ⇒ exit 0 ── DONE
        │
T3 deploy coordination: build pipeline image, bump pipeline_image_version, merge template
        │
T4 live verify on next tick that re-selects 30TXT/31TGJ descending (read-only)

T5 (do NOT do blindly) discover-side prevention — see Open Questions
```

## Tasks

### Task 1 — data-pipeline: upload empty ⇒ return 0  ✅ DONE
**What**: `upload_s1tiling_outputs.py:71` — no-files branch `return 1` → `return 0` with an INFO log (empty = no coverage, not an error). All other `return 1` paths (dupes `:84`, verify mismatch `:108`, permanent failure `:100`) unchanged. Test `test_upload_outputs_no_files_returns_1` → `test_upload_outputs_no_files_returns_0_clean_skip` asserting `rc == 0` and nothing written.
**Verify**: `uv run pytest tests/unit/test_upload_s1tiling_outputs.py -q`
**Acceptance criteria**:
- [x] Empty output dir ⇒ `upload_outputs` returns 0, writes nothing
- [x] Dup / verify-mismatch / permanent-failure still return 1 (regression guard) — 39 passed

### Task 2 — platform-deploy: s1tiling process empty ⇒ exit 0  ✅ DONE
**What**: `eopf-explorer-s1tiling-template.yaml:347-354` — empty-day `exit 1` → `exit 0` with a clear "no S1 coverage … not an error" log. `set -euo pipefail` kept, so a real `S1Processor` non-zero still aborts the step above the check. Stale comments at `:125-127` and `:253-256` updated to the new contract.
**Verify**: `argo lint --offline …/eopf-explorer-s1tiling-template.yaml`
**Acceptance criteria**:
- [x] Empty `data_out/{TILE}` ⇒ step exits 0
- [x] `set -e` preserved so genuine S1Processor failure still exits non-zero
- [x] `argo lint` clean; only one template copy in repo (no prod overlay to mirror)

### Task 3 — deploy coordination  <status: NEXT (needs owner approval)>
**What**: The upload `return 0` ships in the **data-pipeline image**; the cron's `upload-geotiffs` step runs `pipeline_image_version`. So full effect needs: (1) build/publish the data-pipeline image from the fix branch, (2) platform-deploy bump of `pipeline_image_version` to that sha, (3) merge the template change. **Rollout is order-safe** — with only one side deployed, an empty tile still fails (at a different step) exactly as today, so no mid-rollout regression; both are required for green.
**Verify**: After Flux sync, `kubectl get cronworkflow eopf-explorer-s1rtc -o yaml` shows the new `pipeline_image_version`; the template hash matches.
**Acceptance criteria**:
- [ ] data-pipeline image built with the upload fix; `pipeline_image_version` bumped; template merged

### Task 4 — live verification (read-only)  <status: blocked by T3>
**What**: Watch the next tick that re-selects 30TXT/31TGJ descending. Confirm parent = Succeeded; those branches show `process` Succeeded + `upload` Succeeded (no files) + ingest Succeeded (exit 2 skip); and **one** s1processor attempt for 30TXT (retry eliminated).
**Verify**: `kubectl get wf <next eopf-explorer-s1rtc-*> -o jsonpath='{.status.phase}'` == `Succeeded`; 30TXT has a single s1processor pod.
**Acceptance criteria**:
- [ ] First post-deploy tick touching 30TXT/31TGJ desc ends `Succeeded`
- [ ] 30TXT downloads once (no 4× retry)

## Open questions (resolved)
1. **(RESOLVED) Does ingest no-op on an empty prefix?** Yes — `ingest_v1_s1_rtc.py:163-165` returns 2 on empty discovery; `run_ingest_register.py:109-111` maps exit 2 → `return 0` ("skipping register"). This is why the acq_count threading from the first draft was **dropped** — unnecessary complexity. (Resolved 2026-06-22.)
2. **(RESOLVED) Argo resource-output timing (OQ-3 in the first draft).** N/A — no output parameters are threaded in the final design.

## Rejected / deferred alternatives
- **acq_count output-param gating (first draft T2/T3).** Threaded the count s1processor→child→parent and gated `submit-ingest` on `acq_count > 0`. Dropped: ingest already no-ops on empty, so this was unearned complexity, and the `valueFrom.path` value (`echo` trailing newline) would have broken the `when: … > 0` numeric comparison.
- **Static `(tile, orbit)` denylist for 30TXT/31TGJ descending (review suggestion S1).** **Deliberately NOT done — it risks a real regression.** S1 descending passes over these tiles vary by relative-orbit/cycle; a blanket descending denylist could permanently drop *legitimate future* descending coverage. The empty result is per-date/geometry, not "this tile never has descending data." The tolerate fix (above) is the safe, data-preserving approach. A *geometry-accurate* discover pre-filter (replicating s1tiling's MGRS-vs-footprint check) would save 30TXT's recurring 1.76 GB/tick but is high-effort; revisit only if that download cost is shown to matter.

## Done definition
A cron tick whose `discover` selects a tile with no S1 coverage in the requested orbit ends **Succeeded**: `process` exits 0, `upload` no-ops, ingest skips (exit 2), no retry/re-download, the existing cube is untouched, and a genuine s1tiling failure still turns the run **Failed**. Verified live on the first post-deploy tick that re-selects 30TXT/31TGJ descending.

## Commit / PR notes
- **data-pipeline** (`fix--s1tiling-empty-coverage` → `feat--s1_grd_phase5`): `fix(s1tiling): treat empty-coverage upload as a clean skip (exit 0), not an error` — body: explain the 30TXT/31TGJ descending no-coverage case, the 3-stage contract, and that ingest already handled it (PR #265); link this plan + the archived-log evidence.
- **platform-deploy** (`fix--s1tiling-empty-coverage` → `main`): `fix(s1rtc): s1tiling exits 0 on no-coverage tiles so the cron run stays green` — body: continueOn doesn't green the run in current Argo; empty is not a failure; pairs with the data-pipeline upload fix + image bump.
