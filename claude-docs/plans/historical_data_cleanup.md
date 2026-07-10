# Plan 2: Historical Data Cleanup — expiry-driven S2 retention (coordination#183)

**Goal**: Every S2 item carries a STAC timestamps-extension `expires` property (registration-stamped + backfilled); a suspended, dry-run-default monthly CronWorkflow drains expired items (S3 zarr delete → validate 0 remaining → STAC delete) with a JSONL audit trail.
**Constraint**: No new Python deps; no refactor beyond moving 3 functions out of `manage_item.py`; nothing enabled in prod without documented approval (issue #183 gates).

Companion plan: [s2_tier_standard_backlog.md](s2_tier_standard_backlog.md) (Plan 1 — backlog → STANDARD). **Plan 1 must complete before this plan enables any real deletion drain** (with the strict backfill, Nov–Dec 2025 items are past-expiry immediately).

## Context

Cloud S3 storage is bloating: old S2 data accumulates indefinitely (#182 solved tier *transition*; #183 — cleanup — is open). The issue-183 discussion converged on stamping items with `expires` so manually-ingested demo data (e.g. 2021 scenes) is structurally protected, while pipeline data gets deleted past retention.

**Decisions (2026-07-08)**: retention **6 months (183 days)**; **expiry-driven** cleanup; scope **S2 prod + staging**; backfill is **strict** (`expires = created + 183d` — backlog drains immediately, batch-limited).

## Verified facts the design rests on

- `scripts/` is baked into the pipeline image (`docker/Dockerfile:44`); `operator-tools/` is NOT → cron code must live in `scripts/`.
- Recursive zarr S3-delete + validated STAC-delete (PR #89) lives at `operator-tools/manage_item.py:394-582` (`extract_s3_urls_from_item`, `delete_s3_objects_for_item` — 200-key batches, NoSuchKey tolerated; `count_s3_objects_for_item`) and `delete_item:143-221` (S3-first, validate-0, then STAC DELETE).
- `manage_item.py:24-29` already sys.path-imports from `scripts/`; `manage_collections.py` imports the helpers `from manage_item` → moving functions to `scripts/` + re-import keeps both CLIs unchanged.
- `register_v1.py` (`run_registration:646-756`) does NOT set `expires`; extension-append pattern at `:407-416`; pgstac stamps `created`/`updated` server-side on POST.
- Migration framework (PR #110) = `operator-tools/migrate_catalog.py` + `_migrate_catalog/` (pure `dict→dict|None` transforms, dry-run, history, `verify`) — home for the backfill.
- platform-deploy: semaphore key `cron-cleanup-limit: "5"` already exists in BOTH namespaces, zero consumers; single-pod S3-deletion cron template = `cronwf/eopf-explorer-cronwf-frame-cache-evict.yaml` (dry_run param, `geozarr-s3-credentials`).
- STAC writes are app-side unauthenticated on main; in-flight branch `feat--stac-transactions-auth` adds `scripts/stac_auth.py` (OIDC bearer, no-op without env) — keep a 2-line integration seam, don't block on it.

## Dependency graph

```
T0 spec + issue #183 update (early, parallel)
T1 shared S3-delete module ──► T3 cleanup script ──► T6 staging cron ──► T7 staging gates ──► T8 prod cron ──► T10 prod enable
T2 register expires ────────────────────────────────► (steady state for new items)
T4 backfill migration ──(after T2 + T7 dry-runs)──► T9 backfill staging → prod
T5 docs — parallel
Plan 1 complete ─────────────────────────────────────────────────────► gate for T9/T10 real deletion
data-pipeline PR-A(T1..T5) → release image vX → platform PR(T6) → gates → platform PR(T8) → enable PR(T10)
```

## Tasks

### Task 0 — Spec + update coordination issue #183  (ready — draft for user approval before posting)
**What**: (a) Materialize the spec at `claude-docs/specs/historical_data_cleanup.md` (condensed from this plan's Context/Goal — CLAUDE.md Stage 1 deliverable). (b) Post a design-decision comment on #183: adopts the timestamps-extension `expires` approach from the issue discussion; retention **6 months** (records the "retention period approved" prerequisite — team can object here); strict backfill `expires = created + 183d`; registration stamps `expires = now + 183d`; demo-data protection = explicit exclude list (primary) + created−datetime gap review (secondary); reuses PR #89 deletion method as suggested in the comments. Amend the task list: tick `Update eopf-workflow-concurrency ConfigMap` (key already exists in both namespaces); add new items (stamp expires at registration; backfill migration; `expires` queryable check; DeleteObject-permission check).
**Acceptance criteria**:
- [ ] Spec file exists in `claude-docs/specs/`
- [ ] Comment posted after user reviews the draft; retention decision on the record

### Task 1 — Extract shared S3-delete module `scripts/s3_item_cleanup.py`  (ready)
**What**: Move `extract_s3_urls_from_item` (manage_item.py:394-423), `delete_s3_objects_for_item` (:426-515), `count_s3_objects_for_item` (:518-582) into new `scripts/s3_item_cleanup.py`; replace `click.progressbar` with `logger.info` batch progress (no click in `scripts/`); keep NoSuchKey-as-deleted + 200-key batching. `manage_item.py` re-imports them (sys.path bootstrap exists), keeping `manage_collections.py` unchanged. Also define `DEFAULT_RETENTION_DAYS = 183` here (single source for T2/T4).
**Verify**: `uv run pytest tests/unit/test_s3_item_cleanup.py tests/unit/`; `uv run operator-tools/manage_item.py --help`
**Acceptance criteria**:
- [ ] Module exists w/o click; existing tests pass
- [ ] New tests cover zarr-prefix expansion, batching, NoSuchKey-as-deleted, count validation (mocked boto3)

### Task 2 — Stamp `expires` at registration (`scripts/register_v1.py`)  (ready)
**What**: New `add_expires(item, retention_days)`: sets `properties.expires = now_utc + timedelta(days=retention_days)` (ISO Z) + appends `https://stac-extensions.github.io/timestamps/v1.1.0/schema.json` (append-if-missing pattern :407-416). Call between step 12 (`add_derived_from_link` :748) and upsert. Retention via env `EXPIRES_RETENTION_DAYS` (default `DEFAULT_RETENTION_DAYS`; `0` disables) — env avoids plumbing a param through 3 conversion sensors in platform-deploy.
⚠️ Stamping is unconditional on upsert — any re-registration resets the clock, and re-registering a protected demo item would *give* it an expiry. Docs (T5) must state that manual/demo registrations run with `EXPIRES_RETENTION_DAYS=0`; the T3 runtime denylist is the backstop.
**Verify**: `uv run pytest tests/unit/test_register_v1.py -k expires`
**Acceptance criteria**:
- [ ] Item has expires=now+183d + ext URL exactly once; `=0` → neither; existing tests unaffected
- [ ] Retention constant shared from `s3_item_cleanup.py`, not duplicated

### Task 3 — Cleanup script `scripts/cleanup_expired_items.py`  (blocked by T1)
**What**: Single-pod discovery+delete (frame-cache-evict model — one coherent JSONL audit log, no fan-out; `cron-cleanup-limit` + `concurrencyPolicy: Forbid` bound concurrency).
1. Discovery: CQL2 `{"op":"<","args":[{"property":"expires"},<now>]}` scoped `--collection`, `sortby=+properties.expires`, cap `--max-items` (default 100).
2. Per item, in order: re-fetch fresh → **guard: skip unless `expires` exists and < now** (items without expires are structurally undeletable) → **guard: skip if item ID in optional `--exclude-file` runtime denylist** (same format as T4's `EXPIRES_EXCLUDE_FILE`) → **guard: every s3:// asset URL must be in `--allowed-bucket`** (default `esa-zarr-sentinel-explorer-fra`) → `delete_s3_objects_for_item` → `count==0` else skip STAC delete (`s3_validation_failed`) → STAC DELETE (404 = success, idempotent; **401/403 = distinct loud `auth_required` status** — expected once stac-auth-proxy enforcement lands; signals "uncomment the OIDC env", not a per-item flake).
3. **Dry-run is the default; real deletion requires explicit `--execute`** (deliberate inversion for a destructive tool). Dry-run still reports per-item S3 object counts.
4. Audit: one JSON line per item (`ts,event,dry_run,collection,item_id,expires,s3_objects_deleted,s3_objects_failed,s3_remaining,stac_deleted,status`) + summary line; exit 1 on any failure.
5. Auth seam: single `_session()` helper — 2-line change to wire `stac_auth` bearer when `feat--stac-transactions-auth` merges.
6. **Queryable proof**: pgstac may not expose `expires` as a queryable — discovery could 400 or silently ignore the filter. First live dry-run must assert every returned item has `expires < now`; if not queryable → add `expires` to eoapi queryables config (platform-deploy prerequisite for T6).
**Verify**: `uv run pytest tests/unit/test_cleanup_expired_items.py`; live dry-run against `sentinel-2-l2a-staging` with `--max-items 5`
**Acceptance criteria**:
- [ ] Dry-run makes zero delete calls (asserted); CQL2/sort/cap asserted
- [ ] No-expires item never deleted; excluded ID skipped; wrong bucket refused; validation-failure retains STAC item
- [ ] All stdout lines parse as JSON; fixtures `tests/fixtures/cleanup_expired/` (expired / no-expires / wrong-bucket items)

### Task 4 — Backfill migration `stamp_expires`  (blocked by T2)
**What**: New migration in `operator-tools/_migrate_catalog/migrations/`, registered in `MIGRATIONS`. Skip if `expires` present. **Explicit exclude list is the PRIMARY demo protection** (env `EXPIRES_EXCLUDE_FILE`, newline item IDs, always skipped); the `created − datetime` gap heuristic is secondary and its threshold **data-driven, not assumed** — ⚠️ bulk catalog-conversion campaigns also produce large gaps, so a naive 30d heuristic would leave exactly the oldest/most-bloating data unstamped forever. Dry-run must emit a skip-reason histogram (+ gap distribution); team reviews and either tunes the threshold, switches to a pipeline-era rule (`created >= <bulk-conversion-end-date>` ⇒ stamp regardless of gap), or enumerates demo items in the exclude file. Else `expires = created + DEFAULT_RETENTION_DAYS` + ext URL (STRICT — drain bounded by T3 `--max-items`). Log every skip with reason.
**Verify**: `uv run pytest tests/unit/test_migrate_catalog.py -k stamp_expires`; staging `--dry-run`
**Acceptance criteria**:
- [ ] Unit tests: stamped / already-stamped / demo-gap / excluded / ext-URL cases
- [ ] `migrate_catalog.py list` shows it; `verify` reports unstamped items; dry-run outputs skip-reason histogram
- [ ] Pre-run check: `properties.created` survives the framework's DELETE-then-POST round-trip (open Q2)

### Task 5 — Docs  (parallel)
**What**: `scripts/README_cleanup_expired_items.md` (flags, audit format, safety model, manual drain runbook); update `operator-tools/README_MIGRATIONS.md` (stamp_expires, exclude file, heuristic, created-roundtrip precheck); `EXPIRES_RETENTION_DAYS` in register docs incl. the `=0` rule for manual/demo registrations.
**Acceptance criteria**:
- [ ] Docs exist; drain runbook includes the exact suspended-cron manual-submit command

### — platform-deploy —

### Task 6 — Staging cleanup CronWorkflow  (blocked by PR-A merged + image released)
**What**: `workspaces/devseed-staging/data-pipeline/cronwf/eopf-explorer-cronwf-historical-cleanup.yaml`, modeled on frame-cache-evict: `schedule "0 3 1 * *"` (monthly), `concurrencyPolicy Forbid`, **`suspend: true`**, semaphore → `cron-cleanup-limit` (first consumer), **ttlStrategy all 604800** (audit = log; success TTL deliberately NOT 300s), params `collection=sentinel-2-l2a-staging`, `stac_api_url`, `s3_endpoint`, `allowed_bucket=esa-zarr-sentinel-explorer-fra`, `max_items_per_run="25"`, **`dry_run="true"`** (container adds `--execute` only when `"false"`), `pipeline_image_version=<release>`; env from `geozarr-s3-credentials` (+ commented `stac-auth-oidc` OIDC block until auth branch lands); `activeDeadlineSeconds 3600` (provisional); SA `operate-workflow-sa`. Header documents operator-paced backlog drain via `argo submit --from cronwf/... -p dry_run=false -p max_items_per_run=200`.
**Pre-check**: prove `geozarr-s3-credentials` can `DeleteObject` on `esa-zarr-sentinel-explorer-fra` (delete a scratch key from a staging pod) — today it's only exercised for `copy_object`. If it can't, or least-privilege preferred, mint a scoped `cleanup-s3-credentials` sealed secret.
**Verify**: Flux reconciles; `suspend=true`; manual dry-run submit emits parseable JSONL + summary; semaphore visible; DeleteObject pre-check evidenced.
**Acceptance criteria**:
- [ ] Cron exists suspended; dry-run manual submit produces audit JSONL + summary; DeleteObject confirmed

### — operations + prod rollout (map to issue #183 checklist) —

### Task 7 — Staging validation gates  (blocked by T6)
**What**: ≥5 dry-runs (varied batch sizes), team log review; **synthetic real-delete test** (register 2-3 throwaway items with past `expires`, run `dry_run=false`, verify S3 empty + STAC 404 + audit complete; measure wall-time per item to size prod params); zero non-expired candidates across runs. Evidence linked in issue #183.
**Acceptance criteria**:
- [ ] 5 dry-run logs linked in #183; synthetic delete evidenced; per-item wall-time recorded

### Task 8 — Prod cleanup CronWorkflow  (blocked by T7 gates; platform-deploy PR)
**What**: Same manifest under `workspaces/devseed/`, `collection=sentinel-2-l2a`, `max_items_per_run` sized from T7 measurements (100 provisional), still suspended + dry-run.
**Acceptance criteria**:
- [ ] Deployed suspended/dry-run; first manual dry-run reviewed

### Task 9 — Backfill execution  (blocked by T4 + T7 + Plan 1 complete)
**What**: Staging — backup clone → dry-run → review skip/stamp histogram + finalize exclude file → real run → `verify`; then prod same. Spot-check 2021 demo scenes remain unstamped.
**Acceptance criteria**:
- [ ] Backups exist before each real run; `verify` clean; demo items provably unstamped

### Task 10 — Prod enablement  (blocked by T8 + T9)
**What**: 2–4 weeks of prod dry-runs (manual weekly submits, cron suspended); documented stakeholder approval in #183; final PR flips `dry_run="false"` + `suspend: false` (staging first, one reconcile apart); operator-paced backlog drain before trusting the monthly schedule.
**Acceptance criteria**:
- [ ] Approval link in issue; storage growth stabilizes; zero unintended deletions

## Open questions

1. 183 days vs calendar 6 months (`relativedelta`) — assume 183d unless team objects. (owner: team)
2. **Does `properties.created` survive migrate_catalog's DELETE-then-POST?** pgstac may re-stamp — spot-check one staging item before T9; if re-stamped, preserve original `created` in POST body and confirm honored. (owner: implementer; blocks T9)
3. Demo-protection rule — exclude list primary; heuristic threshold vs pipeline-era `created` cutoff decided from the T4 dry-run skip histogram (bulk-conversion items must NOT be left unstamped). (owner: team; blocks T9)
4. Prod `max_items_per_run` + `activeDeadlineSeconds` — sized from T7 measured wall-time and drain-rate math. (owner: team)
5. S3 bucket versioning on `esa-zarr-sentinel-explorer-fra` as delete-reversibility backstop — decide before T10 (issue #183 prerequisite). (owner: platform)
6. **Is `expires` a pgstac queryable on this deployment?** If rejected/ignored, add to eoapi queryables config (platform-deploy prerequisite for T6). (owner: implementer; verified at first T3 live dry-run)
7. **Can `geozarr-s3-credentials` DeleteObject on the S2 bucket — or should the cleanup cron get a scoped credential?** (owner: platform; blocks T7 real-delete test)

## Verification (end-to-end)

- `uv run pytest` green in data-pipeline after each task.
- Staging live dry-run: cleanup script lists only past-expiry items, zero mutations, JSONL parses (also proves `expires` is queryable — open Q6).
- Synthetic-data real delete on staging proves the full S3 → validate-0 → STAC chain (T7).
- Storage growth curve on `esa-zarr-sentinel-explorer-fra` stabilizes post-T10 (success criterion from #183).

## Done definition

New registrations carry `expires` (now+183d, timestamps ext); non-demo historical items backfilled (`verify` clean; demo items provably unstamped); `eopf-explorer-historical-cleanup` crons live in both namespaces with JSONL audit retained ≥7d; prod enabled only after documented approval in #183; storage growth curve stabilizes.
