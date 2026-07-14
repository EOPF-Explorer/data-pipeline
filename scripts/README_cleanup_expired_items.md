# Expired-item cleanup (`cleanup_expired_items.py`)

Expiry-driven retention for S2 data ([coordination#183](https://github.com/EOPF-Explorer/coordination/issues/183)).
Items carry a STAC `expires` timestamp; this script deletes the ones whose
`expires` is in the past — S3 objects first, then the STAC item.

> ⚠️ **Destructive.** Dry-run is the default. Real deletion requires `--execute`.

## How an item gets an `expires`

`expires` uses the [timestamps extension](https://github.com/stac-extensions/timestamps).
There are two ways it lands on an item:

1. **At registration** — `register_v1.py` stamps `expires = now + EXPIRES_RETENTION_DAYS`.
   - `EXPIRES_RETENTION_DAYS` defaults to **183** (6 months), shared from
     `s3_item_cleanup.DEFAULT_RETENTION_DAYS`.
   - **`EXPIRES_RETENTION_DAYS=0` disables stamping** for a whole run.
   - **`EXPIRES_EXCLUDE_FILE` protects specific ids.** `register_v1` reads the
     **same demo denylist** the cleanup honors, and never stamps `expires` on an
     id in it. So re-registering or **reconverting** a demo scene keeps it with
     **no `expires`** (structurally undeletable) — the one list is the single
     source of truth for demo protection at both register-time and cleanup-time,
     and there's no window where an upsert re-arms a demo scene.
2. **Backfill** — the `stamp_expires` migration stamps existing items
   (`expires = datetime + retention`, keyed off acquisition age; items acquired
   before its `EXPIRES_MIN_DATETIME` floor are left unstamped). See
   [operator-tools/README_MIGRATIONS.md](../operator-tools/README_MIGRATIONS.md).

## Safety model

- **No `expires` ⇒ never deleted.** The primary protection for demo data.
- **`--exclude-file`** — a newline-delimited item-ID denylist, always skipped
  (same format as the migration's `EXPIRES_EXCLUDE_FILE`; `#` comments allowed).
- **`--allowed-bucket`** — every `s3://` asset URL must live under this bucket
  or the item is skipped (`wrong_bucket`). Default `esa-zarr-sentinel-explorer-fra`.
- **Validate-before-delete** — S3 objects are deleted, then re-counted; the STAC
  item is removed **only** if 0 remain. Otherwise the item is retained with
  status `s3_validation_failed`.
- **Dry-run default** — real deletion needs `--execute`. Dry-run still reports
  the S3 object count that *would* be deleted.

## Flags

| Flag | Default | Meaning |
|------|---------|---------|
| `--stac-api-url` | (required) | STAC API base URL |
| `--collection` | (required) | Collection to scan |
| `--s3-endpoint` | `AWS_ENDPOINT_URL` env | S3 endpoint URL |
| `--allowed-bucket` | `esa-zarr-sentinel-explorer-fra` | Assets outside it are skipped |
| `--max-items` | `100` | Cap on items processed per run |
| `--exclude-file` | `EXPIRES_EXCLUDE_FILE` env | Item-ID denylist |
| `--execute` | off (dry-run) | Actually delete |

## Audit log

One JSON line per item on stdout, then a summary line. Fields per item:

```
ts, event, dry_run, collection, item_id, expires,
s3_objects_deleted, s3_objects_failed, s3_remaining, stac_deleted, status
```

`status` is one of: `dry_run`, `deleted`, `s3_validation_failed`,
`auth_required` (STAC DELETE returned 401/403 — expected once the
stac-auth-proxy enforcement lands; wire the bearer in `_session()`),
`already_gone` (re-fetch got 404 — already deleted, idempotent success),
`refetch_failed` (re-fetch errored — the item is skipped rather than acted on
with stale data), `no_expires`, `not_expired`, `excluded`, `wrong_bucket`.

Exit code is `1` if any item ended in `s3_validation_failed`, `auth_required`,
`refetch_failed`, or a `stac_delete_http_*` status. `already_gone` is a success.

## Notes on the discovery query (verified live 2026-07-10)

- `expires` is **filterable** even though it is not an advertised queryable — the
  collection schema is `additionalProperties: true`, so pgstac filters it via
  JSONB. Because both `register_v1` and `stamp_expires` emit a single fixed
  `%Y-%m-%dT%H:%M:%SZ` format, string ordering equals chronological ordering, so
  `expires < now` selects correctly. **The fixed timestamp format is load-bearing**
  — do not introduce a second format.
- ⚠️ The STAC `POST /search` API requires `sortby` as a **list**
  (`[{"field": "properties.expires", "direction": "asc"}]`); a bare string
  `"+properties.expires"` returns **HTTP 400**. This script goes through
  `pystac_client`, which converts the string form for us — but any **direct
  API / curl caller** (e.g. a future Argo raw-HTTP step) must send the array form.

## Local dry-run

```bash
uv run scripts/cleanup_expired_items.py \
  --stac-api-url https://api.explorer.eopf.copernicus.eu/stac \
  --collection sentinel-2-l2a-staging \
  --max-items 5
```

## Operator-paced backlog drain (production)

The monthly `eopf-explorer-historical-cleanup` CronWorkflow ships **suspended**
and **dry-run**. To drain a backlog by hand, submit one-off runs from the cron
template (this bypasses the schedule without un-suspending it):

```bash
# Dry-run a large batch first and review the JSONL:
argo submit --from cronwf/eopf-explorer-cronwf-historical-cleanup \
  -p dry_run=true -p max_items_per_run=200 -n <namespace>

# Then, once reviewed, the real drain:
argo submit --from cronwf/eopf-explorer-cronwf-historical-cleanup \
  -p dry_run=false -p max_items_per_run=200 -n <namespace>
```

Real deletion in production also requires the tier→STANDARD backlog (Plan 1) to
be complete and documented stakeholder approval on coordination#183.

### Throughput

Deletion is bound by S3's per-object delete rate — measured ~75 objects/sec on
OVH (2026-07-14), roughly `13 s` for a ~1000-object item, single-pod and
sequential by design (one coherent audit log). Batch size (`delete_objects`
sends 1000 keys/call) doesn't move this; it's server-side. If a large backlog is
ever too slow to drain this way, deletes parallelise ~2× at 4 concurrent workers
(same measurement) — deliberately not implemented, to keep the delete path
simple and auditable. Revisit only if the backlog drain becomes a real pain
point.
