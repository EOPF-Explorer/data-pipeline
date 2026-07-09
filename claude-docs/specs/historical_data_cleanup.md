# Spec: Historical Data Cleanup — expiry-driven S2 retention

**Issue**: [coordination#183](https://github.com/EOPF-Explorer/coordination/issues/183)
**Plan**: [../plans/historical_data_cleanup.md](../plans/historical_data_cleanup.md)
**Status**: draft (Stage 1)

## Problem

Cloud S3 storage on `esa-zarr-sentinel-explorer-fra` grows without bound: old
S2 data accumulates indefinitely and nothing deletes it. Issue #182 solved tier
*transition* (STANDARD → cheaper tiers); #183 — actual cleanup — is still open.

A naive "delete everything older than N months" job is unsafe here: some data
was ingested **manually for demo purposes** (e.g. 2021 scenes) and must never be
deleted, even though it is old. Age alone cannot distinguish demo data from
disposable pipeline output.

## Goal

Every S2 item carries a STAC [timestamps-extension](https://github.com/stac-extensions/timestamps)
`expires` property (stamped at registration and backfilled onto existing items).
A suspended, dry-run-by-default monthly CronWorkflow drains expired items:
S3 zarr delete → validate 0 objects remain → STAC delete, with a JSONL audit
trail. Nothing is enabled in production without documented approval on #183.

## Decisions (2026-07-08)

- **Retention: 6 months (183 days).** Resolves the "retention period approved"
  prerequisite. Supersedes the earlier "registration + 2 months" idea from the
  issue thread.
- **Mechanism: expiry-driven, not age-driven.** Deletion selects on
  `expires < now`, not on object/creation age. Items without `expires` are
  structurally undeletable — this is what protects demo data.
- **Scope: S2 prod + staging** (`sentinel-2-l2a`, `sentinel-2-l2a-staging`).
- **Backfill is strict**: `expires = created + 183d`. Because Nov–Dec 2025
  pipeline items are then already past-expiry, the backlog drains immediately
  and must be batch-limited (`--max-items`). **Plan 1 (tier → STANDARD backlog)
  must complete before any real deletion is enabled.**
- **Demo protection = explicit exclude list (primary)** + a data-driven
  `created − datetime` gap review (secondary). The gap threshold is **not
  assumed** — bulk catalog-conversion campaigns also produce large gaps, so a
  naive heuristic would leave the oldest, most-bloating data unstamped forever.
- **Reuse the PR #89 deletion method** (recursive S3 delete + validated STAC
  delete), as suggested in the issue comments.

## Approach

1. **Shared module** `scripts/s3_item_cleanup.py` — recursive S3 delete +
   count-validation helpers, extracted from `operator-tools/manage_item.py` so
   the cron (in `scripts/`, baked into the image) can share them. *(Task 1 — done.)*
2. **Stamp at registration** — `register_v1.py` sets
   `expires = now + EXPIRES_RETENTION_DAYS` (default 183; `0` disables for
   manual/demo registrations) and appends the timestamps extension.
3. **Backfill migration** `stamp_expires` (PR #110 migration framework) —
   stamps existing items `expires = created + 183d`, skipping already-stamped
   items and the exclude list.
4. **Cleanup script** `scripts/cleanup_expired_items.py` — CQL2 discovery on
   `expires < now`, per-item guards (expires-exists, exclude-list, allowed-
   bucket), S3 delete → validate 0 → STAC delete, JSONL audit. **Dry-run is the
   default; real deletion requires `--execute`.**
5. **CronWorkflows** (platform-deploy) — monthly, `concurrencyPolicy: Forbid`,
   `suspend: true`, `dry_run: true`, staging first then prod, 7-day log TTL.

## Out of scope

- **Orphan S3 objects without STAC items** (objects left behind by pre-PR-#89
  collection deletes). Raised in the #183 thread but not addressed by the
  expires mechanism — tracked separately. *(Confirm with team; candidate
  follow-up issue.)*
- Non-S2 missions (S1/S3): the mechanism generalises but rollout is S2-only here.

## Success criteria

- New registrations carry `expires = now + 183d` (timestamps ext, exactly once).
- Non-demo historical items backfilled; `migrate_catalog verify` clean; 2021
  demo scenes provably unstamped.
- `eopf-explorer-historical-cleanup` crons live in both namespaces, suspended +
  dry-run, JSONL audit retained ≥ 7 days.
- Prod deletion enabled only after documented approval on #183.
- Storage-growth curve on `esa-zarr-sentinel-explorer-fra` stabilises.

## Key safety properties

- No-`expires` item is never deleted (structural).
- Every s3:// asset must be under `--allowed-bucket` or the item is skipped.
- S3 delete is validated (`count == 0`) before the STAC item is removed.
- Cron ships suspended + dry-run; real deletion is an explicit opt-in at two
  levels (`--execute` flag and `dry_run=false` param).
```
