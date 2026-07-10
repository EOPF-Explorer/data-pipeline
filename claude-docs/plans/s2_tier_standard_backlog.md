# Plan 1: Move S2 backlog (registered since Nov 2025, older than 3 months) to STANDARD storage

**Goal**: All prod `sentinel-2-l2a` items **registered** (`properties.created`) between 2025-11-01 and (today − 3 months) have their S3 objects in STANDARD storage class and STAC storage metadata synced.
**Constraint**: Reuse the existing PR #118 machinery (`submit_storage_tier_workflows.py` → webhook → `eopf-storage-tier-batch-job`); only code change is a `--date-field` option; only platform change is one prod Sensor manifest. No new deps.

Companion plan: [historical_data_cleanup.md](historical_data_cleanup.md) (Plan 2 — expiry-driven retention). **This plan should complete before Plan 2 enables any real deletion drain** — otherwise we tier-change items that are about to be deleted, or worse, skip tier-changing data that survives.

## Current state

| Resource | Status |
|---|---|
| data-pipeline PR #118 (`feat--add-storage-tier-change-events-script`) | **MERGED 2026-07-08** (`1639baf`). |
| CQL2 date-field pattern to copy | `scripts/query_stac.py:136-141` — `{"op":"between","args":[{"property":"updated"},start,end]}`, `filter_lang=cql2-json` |
| platform-deploy prod (`devseed`) | webhook EventSource ✅, `templates/eopf-storage-tier-batch-job.yaml` ✅, storage-tier **Sensor ✅** — [platform-deploy#306](https://github.com/EOPF-Explorer/platform-deploy/pull/306) merged (`0258776`), Flux-synced, Healthy (confirmed by user 2026-07-08). |
| Prod storage-tier-optimizer cron | transitions at `age_days=180` by sensing-datetime drain window — misses backlog outside its rolling window; hence this manual run |
| Staging | optimizer runs at `age_days=7` → staging already drains to STANDARD; staging NOT in scope here |

## Dependency graph

```
T1 rebase #118 ──► T2 --date-field ──► T3 merge #118 ──┐
T4 prod Sensor (platform-deploy, independent) ─────────┴──► T5 dry-run ──► T6 live run
```

## Tasks

### Task 1 — Rebase PR #118 on main  ✅ DONE
**What**: Rebase `feat--add-storage-tier-change-events-script` (2 commits) onto current `main`; expect at most README/pyproject conflicts.
**Verify**: `uv run pytest tests/unit/test_submit_storage_tier_workflows.py tests/unit/test_change_storage_tier_commands.py`
**Evidence (2026-07-08)**: Rebased onto `origin/main` in worktree `data-pipeline-worktrees/s2-tier-standard-backlog`. Conflicts were exactly as predicted — `operator-tools/README.md` section-numbering only (main added `migrate_catalog.py` §3; renumbered the added sections 5/6/7); `pyproject.toml` auto-merged (its version bump was already on main). Commit `46035e4` (CLI command) auto-dropped as already-applied via merged PR #103. Branch now ahead 1 / behind 0. Tests: **43 passed** (`test_submit_storage_tier_workflows.py` + `test_change_storage_tier_commands.py`).
**Acceptance criteria**:
- [x] Branch rebased, force-pushed (`--force-with-lease`, `5997970...bc65a8a`); PR #118 CI green — all lint/test/security checks pass (**Pre-commit and tests**, Bandit, Trivy, Analyze(python), Security audit, zizmor). "Build and publish image" is the only job still running (Docker build, unaffected by this Python-only change). PR is `MERGEABLE`.

### Task 2 — Add `--date-field` option to `submit_storage_tier_workflows.py`  ✅ DONE (committed `bc65a8a`, not pushed)
**What**: `--date-field` choice `datetime|created|updated` (default `datetime` — backward compatible). For non-default fields, replace the `datetime=` kwarg with a CQL2 `between` filter on the chosen property (pattern from `scripts/query_stac.py:136-141`). No changes to webhook payload, Sensor, or batch template — they consume resolved `item_ids` and are date-agnostic.
**Verify**: `uv run pytest tests/unit/test_submit_storage_tier_workflows.py`
**Evidence (2026-07-08)**: Implemented TDD (4 failing tests → green). Full gate on commit `bc65a8a`: ruff/ruff-format/mypy/pytest all Passed via pre-commit; **43 passed**. CLI `--help` shows `--date-field {datetime,created,updated}`. CQL2 filter uses bare property name `{"property": "created"}` — matches the proven `scripts/query_stac.py` pattern against the same prod STAC API (pgstac exposes `created`/`updated` as bare queryables, not `properties.*`).
**Acceptance criteria**:
- [x] `--date-field created` produces CQL2 `between` on `created` — asserted via mocked `Client.search` kwargs (`test_date_field_created_uses_cql2_between`)
- [x] Default invocation behavior unchanged — existing `datetime=` range test still green (`test_default_date_field_uses_datetime_range` + original `test_returns_item_ids`)
- [x] `operator-tools/README.md` gains the registered-window example (`--date-field created --start-date 2025-11-01 ... --storage-class STANDARD`) + `--date-field` row in options table
- [x] Nit: `--storage-class` default flipped `STANDARD_IA` → `STANDARD`; documented commands stay explicit
- [x] Extra (in-scope): added `timeout=30` to the webhook POST — fixes ruff S113 on the PR's own `submit_batch`, unblocking CI, and prevents a hung endpoint stalling the per-window loop

### Task 3 — Merge PR #118  ✅ DONE
**What**: Update PR description (mention `--date-field` + the planned prod backlog run — this is #182 follow-up territory, keep coordination#183 out of it); request review; merge.
**Evidence (2026-07-08)**: Merged by user, squash commit `1639baf`.
**Acceptance criteria**:
- [x] PR #118 merged

### Task 4 — Deploy storage-tier Sensor to prod  ✅ DONE
**What**: Copy `workspaces/devseed-staging/data-pipeline/eopf-explorer-storage-tier-sensor.yaml` → `workspaces/devseed/data-pipeline/`, changing only `metadata.namespace: devseed` and the trigger's workflow `metadata.namespace: devseed`. EventSource + batch WorkflowTemplate already exist in prod.
**Verify**: `kubectl -n devseed get sensor eopf-explorer-storage-tier` Healthy; port-forward `kubectl -n devseed port-forward svc/eopf-explorer-webhook-eventsource-svc 12000:12000`, POST a 1-item test payload, confirm a `wh-storage-tier-batch-*` workflow spawns with correct params.
**Evidence (2026-07-08)**: [platform-deploy#306](https://github.com/EOPF-Explorer/platform-deploy/pull/306) merged (`0258776`). Diffed against the staging source before merge — only the two `namespace: devseed-staging` → `devseed` lines differed. Flux sync + in-cluster Healthy/test-payload verification confirmed by user (no kubectl access from this machine to independently re-check).
**Acceptance criteria**:
- [x] PR #306 merged and Flux-synced
- [x] Sensor Healthy in `devseed`; test payload spawns batch workflow (confirmed by user)

### Task 5 — Staging smoke test + dry-run over the registration window  (blocked by T3 + T4)
**Smoke test first** (Sensor already live in staging): one live run with a 1-day window against `sentinel-2-l2a-staging` and `--date-field created` to exercise the new query path end-to-end at trivial cost, then the prod dry-run:
**What**:
```bash
uv run operator-tools/submit_storage_tier_workflows.py \
    --date-field created \
    --start-date 2025-11-01 --end-date <today - 3 months> \
    --collection sentinel-2-l2a --storage-class STANDARD \
    --process-all-assets --dry-run
```
Review per-window item counts; hand-verify a few returned items' `properties.created` values fall in the window; sanity-check total volume.
**Evidence (2026-07-08, session 1) — query path + tier machinery proven on staging:**
- New `--date-field created` query path exercised **live** against `sentinel-2-l2a-staging` (dry-run, 1-day window 2026-06-10): CQL2 `between` filter executed, 28 items returned, well-formed batch payload. Read-only.
- Tier-change + STAC-sync machinery proven end-to-end via the single-item tool: `manage_item.py change-storage-tier` moved `S2A_MSIL2A_20210917T115221_N0500_R123_T28RBS_20230110T165456` STANDARD→STANDARD_IA; verified 1231/1231 objects STANDARD_IA in S3 (OVH) **and** STAC assets/schemes updated (`objects_per_storage_class: STANDARD_IA`, ref `glacier` = STANDARD_IA scheme). Reverted back to STANDARD later the same day (see handoff).

**Evidence (2026-07-08, session 2) — prod dry-run complete:**
```bash
uv run operator-tools/submit_storage_tier_workflows.py \
    --date-field created --start-date 2025-11-01 --end-date 2026-04-08 \
    --collection sentinel-2-l2a --storage-class STANDARD \
    --process-all-assets --dry-run
```
Run against **prod** STAC (`api.explorer.eopf.copernicus.eu/stac`, collection `sentinel-2-l2a`). This machine's tool-call timeout kills any single shell invocation around ~480-600s wall-clock, so the 158-window run (would take ~15-20 min straight through) was split into 6 sequential sub-ranges covering the identical date span with no gaps (one 1-day overlap between chunks 2/3, deduplicated below); each sub-range's own `Done. Submitted: N, Failed: 0` was captured. This is purely an artifact of this session's tooling — **T6's actual invocation is the single unmodified command above**, run once.
- **Total: 158/158 daily windows queried, 0 query failures.** ~91,000 items found in range before dedup; **90,829 unique items** after removing the one duplicated day (2026-02-08, 940 items counted twice across chunk boundary).
- Per-chunk breakdown: `2025-11-01→2026-01-20` (80 windows, 12,580 items) · `2026-01-20→2026-02-09` (20 windows, 16,501 items) · `2026-02-08→2026-02-23` (15 windows, 9,872 items, submitted 10 batches) · `2026-02-23→2026-03-10` (15 windows, 21,539 items, submitted 14) · `2026-03-10→2026-03-20` (10 windows, 14,045 items, submitted 10) · `2026-03-20→2026-04-08` (19 windows, 17,232 items, submitted 14).
- **Spot-checks** (3 items via direct `pystac_client` lookup against prod): all `properties.created` values fell inside their queried window, confirming the CQL2 filter is correct. Notably `S2A_MSIL2A_20251031T101201_..._T32TQQ_...` has sensing `datetime` **2025-10-31** (before `--start-date`) but `created` **2025-11-01** (inside window) — proves `--date-field created` is genuinely filtering on registration date, not silently falling back to sensing date.
- **End-date boundary verified**: last window queried was `2026-04-07T00:00:00Z → 2026-04-08T00:00:00Z` (19/19, exact match to `--end-date 2026-04-08`), no overreach past the boundary.
- Volume note: ~90.8k items is a large batch (recall each item fans out to N per-item Argo tasks inside each of the ~158 window-workflows) — consistent with T6's "likely days of wall-clock" expectation.
**Acceptance criteria**:
- [x] Query path + tier-change/STAC-sync proven on staging (single-item route)
- [x] Prod dry-run report reviewed; window counts plausible; spot-checked `created` values in range

### Task 6 — Live run + verification  (IN PROGRESS — run 1 crashed at window 123/158, resuming)
**What**: Same command without `--dry-run` (port-forward active). Batch workflows fan out per 24h window (semaphore `batch-storage-tier-limit: "1"` — windows run sequentially; intra-window `parallelism: 30`, per-workflow deadline 4h). **Duration expectation**: ~5 months ≈ up to ~160 sequential window workflows — likely days of wall-clock; acceptable for a background migration. If it drags, the lever is temporarily raising `batch-storage-tier-limit` in the prod `eopf-workflow-concurrency` ConfigMap. Monitor to completion; re-run is idempotent (already-STANDARD objects are skipped).
**Verify**: spot-check ≥5 items: `uv run operator-tools/manage_item.py info sentinel-2-l2a <ITEM_ID> --s3-stac-info` shows STANDARD in both S3 and `storage:schemes`.

**Run 1 (2026-07-09, user's machine, full range in one process) — crashed at window 123/158**:
```bash
kubectl -n devseed port-forward svc/eopf-explorer-webhook-eventsource-svc 12000:12000 &
uv run operator-tools/submit_storage_tier_workflows.py \
    --date-field created --start-date 2025-11-01 --end-date 2026-04-08 \
    --collection sentinel-2-l2a --storage-class STANDARD --process-all-assets
```
- **Windows 1–122 (2025-11-01 → 2026-03-03) submitted successfully** — window 122 (`2026-03-02→2026-03-03`, 669 items) completed and its webhook POST landed (confirmed by the `Handling connection for 12000` line on the port-forward immediately after) before the crash. Real batch-change-storage-tier events already dispatched to Argo for these windows — **do not resubmit**.
- Crashed while **querying** window 123 (`2026-03-03→2026-03-04`), before any submission for that window: `kubectl port-forward` died (`error: lost connection to pod`) at the same moment the direct HTTPS call to the prod STAC API got `ConnectionResetError` — looks like a local network blip on the user's machine, not a data-pipeline or Sensor bug (the STAC query bypasses the port-forward entirely; only the webhook POST uses it).
- **Remaining: windows 123–158 (2026-03-03 → 2026-04-08, 35 windows)**, not yet submitted.

**Run 2 (2026-07-09 09:05–09:33, full-range command re-run from scratch) — crashed again at window 123/158, same spot**:
- Re-ran the *unmodified* full-range command (not the Blocks A/B/C resume below) — windows 1–122 were **resubmitted a second time** (idempotent per-object, but wasted ~28 min + cluster cycles re-processing already-STANDARD windows).
- Per-window item counts for windows 1–122 matched Run 1 / the T5 dry-run exactly (spot-checked several, e.g. window 30 "Found 344 items", window 100 "Found 940 items") — confirms determinism, no drift in the backlog.
- Crashed identically at window 123 (`2026-03-03T00:00:00Z→2026-03-04T00:00:00Z`) with the same dual failure: `kubectl port-forward` → `error: lost connection to pod`, and the direct HTTPS STAC query → `ConnectionResetError: [Errno 54]`.
- **Same crash window, same ~28 min elapsed (09:05:43→09:33:48), two separate days** — this is now suspicious enough to treat as systematic (an idle/session timeout in the network path around the 28–30 min mark: VPN, corporate proxy, or the port-forward stream itself), not a one-off blip. Not yet root-caused; the self-healing port-forward loop below sidesteps it regardless of cause.
- **Lesson for next attempt: do NOT re-run the full-range command again** — it will resubmit 1–122 a third time. Resume directly from window 123 (`--start-date 2026-03-03`) via the Blocks A/B/C below.

**Resuming — decided to run in blocks** (smaller blast radius per connection drop) with a self-healing port-forward:
```bash
while true; do
  kubectl -n devseed port-forward svc/eopf-explorer-webhook-eventsource-svc 12000:12000
  echo "port-forward dropped, reconnecting in 2s..."
  sleep 2
done &

# Block A
uv run operator-tools/submit_storage_tier_workflows.py --date-field created \
    --start-date 2026-03-03 --end-date 2026-03-13 \
    --collection sentinel-2-l2a --storage-class STANDARD --process-all-assets
# Block B
uv run operator-tools/submit_storage_tier_workflows.py --date-field created \
    --start-date 2026-03-13 --end-date 2026-03-24 \
    --collection sentinel-2-l2a --storage-class STANDARD --process-all-assets
# Block C
uv run operator-tools/submit_storage_tier_workflows.py --date-field created \
    --start-date 2026-03-24 --end-date 2026-04-08 \
    --collection sentinel-2-l2a --storage-class STANDARD --process-all-assets
```
Status: not yet run — next session picks up here.

**Acceptance criteria**:
- [ ] Windows 123–158 (blocks A/B/C above) submitted with 0 failures
- [ ] All submitted batch workflows Succeeded (failed windows re-submitted)
- [ ] Spot-checks show STANDARD in S3 + STAC metadata for ≥5 items across different windows (incl. at least one from the run-1 batch, windows 1–122)

## Open questions

All resolved 2026-07-08 (owner: user).

1. **Exact end date** — confirmed **2026-04-08** (today−3mo). Used in T5/T6 commands above.
2. **Anything before Nov 2025?** — confirmed no widening; keep `--start-date 2025-11-01` as planned.
3. **Staging test item `S2A_…T28RBS`** — confirmed revert to STANDARD. Done: `manage_item.py change-storage-tier sentinel-2-l2a-staging S2A_MSIL2A_20210917T115221_N0500_R123_T28RBS_20230110T165456 --storage-class STANDARD --s3-endpoint https://s3.de.io.cloud.ovh.net -y` — 1231/1231 objects succeeded, 0 failed. Verified: S3 objects back to STANDARD, STAC assets show no `storage:refs` (matches the item's pre-test state — STANDARD is the implicit default, only non-default tiers get an explicit ref).

## Done definition

PR #118 merged with `--date-field`; prod Sensor live; every prod S2 item registered in [2025-11-01, today−3mo] verified in STANDARD (S3 + STAC metadata), evidenced by dry-run report + spot-check output.
