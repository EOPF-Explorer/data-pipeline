# STAC Catalogue Migration Tool

`migrate_catalog.py` is a reusable operator tool for applying catalogue-wide fixes to STAC items.
Each migration is a pure function, tracked in a local history file to prevent accidental re-runs.

Items are fetched page-by-page from the STAC API (default page size: 100) and processed
immediately as they arrive — no full collection load into memory. A progress bar with total count
is shown when the API supports `numberMatched`.

## Available Migrations

| Name | Description |
|---|---|
| `fix_url_encoding` | Replace `+` with `%20` in asset/link href query strings (RFC 3986 compliance) |
| `fix_zarr_media_type` | Fix zarr media types (`vnd+zarr` → `vnd.zarr`, `version=2` → `version=3`, add missing `version=3`) and remove `zipped_product` asset |
| `stamp_expires` | Backfill `properties.expires = datetime (acquisition) + retention` (timestamps ext); skips already-stamped, excluded, and items acquired before the floor. See [stamp_expires](#stamp_expires-backfill-retention-expiry) below |

## `stamp_expires` (backfill retention expiry)

Backfills `properties.expires` onto existing items so the cleanup cron can drain
them ([coordination#183](https://github.com/EOPF-Explorer/coordination/issues/183)).
New items are stamped at registration; this migration covers the pre-existing
catalogue. Companion doc: [scripts/README_cleanup_expired_items.md](../scripts/README_cleanup_expired_items.md).

**Rule**: `expires = datetime + retention` (default 183 days, from
`s3_item_cleanup.DEFAULT_RETENTION_DAYS`), plus the timestamps extension URL.
Retention is measured from **acquisition** (`properties.datetime`), not
`created`. `created` records when an item was *converted/registered*, and the
catalogue holds multiple bulk-conversion cohorts — items acquired the same week
can carry `created` dates months apart, and a re-conversion resets `created`, so
a `created`-based expiry is unstable and disconnected from data age. Acquisition
`datetime` is stable across re-conversions. Strict by design — items older than
the window become immediately past-expiry, so the first cleanup runs drain a
backlog (bounded by the cron's `--max-items`).

**Configuration (environment):**

| Env | Default | Effect |
|---|---|---|
| `EXPIRES_RETENTION_DAYS` | `183` | Retention window added to `datetime` |
| `EXPIRES_MIN_DATETIME` | (unset) | Floor: skip items acquired before this (`before_floor`). RFC3339 timestamp or bare `YYYY-MM-DD` (→ midnight UTC). Inclusive of its own instant. |
| `EXPIRES_EXCLUDE_FILE` | (unset) | Newline-delimited item-ID denylist, never stamped (`#` comments ok) |

**Demo-data protection is layered — the exclude file is the real protection:**

- **Primary — the exclude file (`EXPIRES_EXCLUDE_FILE`).** Point it at
  `scripts/demo_exclude_ids.txt` (the same canonical list `register_v1` and the
  cleanup honor). Demo scenes are **scattered across 2021→2026 and interleaved
  with pipeline data** — several are acquired *after* any pipeline-era floor — so
  enumerating their IDs is the only complete protection. Excluded IDs are never
  stamped, carry no `expires`, and are structurally undeletable; the check runs
  *before* the floor, so an excluded ID is safe regardless of its acquisition
  date. **A floor-only run (exclude file unset) will stamp — and later delete —
  any demo acquired after the floor.** After a run, check the report for the
  `exclude-file id(s) matched no item` warning: it means a listed ID is a typo or
  a stale/reconverted id and is protecting nothing.
- **Secondary — the acquisition floor (`EXPIRES_MIN_DATETIME`).** A single date
  floor (e.g. `2025-11-01`) skips every item acquired before it (`before_floor`),
  never stamping them. Its job is to **bound the first cleanup's blast radius**
  and coarsely cover the pre-pipeline tail — not to protect demos. Run a dry-run
  first and read the outcome histogram (`stamped` vs `before_floor`, logged +
  tallied) to confirm the split.

Note: because the rule keys off `properties.datetime` (a mandatory core STAC
field, not a server-managed audit timestamp), it does not depend on `created`
surviving the framework's DELETE-then-POST round-trip.

```bash
# Dry-run: review the skip-reason / stamped histogram in the logs first.
# EXPIRES_EXCLUDE_FILE protects the demo scenes (mandatory — several are acquired
# after the floor); EXPIRES_MIN_DATETIME bounds the backlog / blast radius.
EXPIRES_EXCLUDE_FILE=scripts/demo_exclude_ids.txt \
EXPIRES_MIN_DATETIME=2025-11-01 \
uv run operator-tools/migrate_catalog.py run --migration stamp_expires \
  sentinel-2-l2a-staging --dry-run

# Confirm coverage afterwards (exits 1 while unstamped items remain).
uv run operator-tools/migrate_catalog.py verify --migration stamp_expires \
  sentinel-2-l2a-staging
```

## Safe Migration Procedure

### Pre-flight

```bash
# Check STAC API is accessible
curl https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a-staging | jq .id

# Review what will change
uv run operator-tools/migrate_catalog.py run --migration fix_url_encoding sentinel-2-l2a-staging --dry-run
uv run operator-tools/migrate_catalog.py run --migration fix_zarr_media_type sentinel-2-l2a-staging --dry-run
```

### Step 1: Backup

```bash
# Create a backup collection before modifying anything
uv run operator-tools/migrate_catalog.py clone sentinel-2-l2a-staging sentinel-2-l2a-staging-backup-20260312 --yes
```

### Step 2: Migrate

```bash
# Apply migrations in-place on the original collection
uv run operator-tools/migrate_catalog.py run --migration fix_url_encoding sentinel-2-l2a-staging
uv run operator-tools/migrate_catalog.py run --migration fix_zarr_media_type sentinel-2-l2a-staging

# Or apply both in a single pass (composable migrations)
uv run operator-tools/migrate_catalog.py run --migration fix_url_encoding --migration fix_zarr_media_type sentinel-2-l2a-staging
```

### Step 3: Verify

```bash
# Confirm all items are fixed (exits 0 if fully applied, 1 if items remain)
uv run operator-tools/migrate_catalog.py verify --migration fix_url_encoding sentinel-2-l2a-staging
uv run operator-tools/migrate_catalog.py verify --migration fix_zarr_media_type sentinel-2-l2a-staging
```

### Step 4: Cleanup

```bash
# Delete backup once satisfied (use manage_collections.py)
uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging-backup-20260312 --yes
uv run operator-tools/manage_collections.py delete sentinel-2-l2a-staging-backup-20260312 --yes
```

## Restore Procedure

If something goes wrong after migration:

```bash
# 1. Remove all items from the damaged collection
uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging --yes

# 2. Clone the backup back
uv run operator-tools/migrate_catalog.py clone sentinel-2-l2a-staging-backup-20260312 sentinel-2-l2a-staging --yes

# 3. Verify restore
uv run operator-tools/migrate_catalog.py verify --migration fix_zarr_media_type sentinel-2-l2a-staging  # should show items still need migration

# 4. Delete the backup
uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging-backup-20260312 --yes
uv run operator-tools/manage_collections.py delete sentinel-2-l2a-staging-backup-20260312 --yes
```

## Recovery Files

Each non-dry-run migration writes a `.migration_recovery_<timestamp>.jsonl` file alongside
`.migration_history.json`. Each line is the migrated version of an item that was submitted to
the API. If a run is interrupted after some DELETEs but before the corresponding POSTs complete,
use this file to re-POST the affected items manually.

## CLI Reference

```
uv run operator-tools/migrate_catalog.py [--api-url URL] [--history-file PATH] COMMAND [OPTIONS]

Global options:
  --api-url TEXT        STAC API URL (or STAC_API_URL env var)
  --history-file PATH   Migration history JSON file (default: .migration_history.json)

Commands:
  list                                     List available migrations
  run COLLECTION_ID                        Apply one or more migrations to a collection
    --migration TEXT  (repeatable)         Migration(s) to run — repeat to compose
    --dry-run                              Preview changes without updating
    --yes / -y                             Skip confirmation prompt
    --page-size INT   (default: 100)       Items fetched per API page
  verify COLLECTION_ID                     Check if migration is fully applied
    --migration TEXT  (repeatable)         Migration(s) to verify
    --page-size INT   (default: 100)       Items fetched per API page
  clone SOURCE_ID TARGET_ID               Clone a collection (metadata + all items)
    --yes / -y                             Skip confirmation prompt
    --page-size INT   (default: 100)       Items fetched per API page
  history                                  Show past migration runs
    --migration TEXT                       Filter by migration name
    --collection TEXT                      Filter by collection ID
```

## History Tracking

Each non-dry-run is appended to `.migration_history.json` in the `operator-tools/` directory.
The tool warns (but does not block) if you re-run a migration that was already applied.
Add `.migration_history.json` to `.gitignore` if you don't want it tracked in git.

View history:
```bash
uv run operator-tools/migrate_catalog.py history
uv run operator-tools/migrate_catalog.py history --migration fix_url_encoding --collection sentinel-2-l2a-staging
```

## Writing New Migrations

1. Create a new file `operator-tools/_migrate_catalog/migrations/my_migration.py`:

   ```python
   from typing import Any
   from _migrate_catalog.migrations._registry import migration
   from _migrate_catalog.types import apply_item_transform

   def _transform(item: dict[str, Any]) -> bool:
       """Mutate item in place. Return True if any change was made."""
       changed = False
       # ... make changes ...
       return changed

   @migration("my_migration", "Human-readable description")
   def my_migration(item: dict[str, Any]) -> dict[str, Any] | None:
       """Return modified copy, or None if item already conforms."""
       return apply_item_transform(item, _transform)
   ```

2. Register it by adding an import to `operator-tools/_migrate_catalog/migrations/__init__.py`:
   ```python
   from _migrate_catalog.migrations import my_migration as _  # noqa: F401
   ```

3. Add unit tests in `tests/unit/test_migrate_catalog.py`.

4. Run: `uv run operator-tools/migrate_catalog.py run --migration my_migration <collection_id> --dry-run`



**Design rules for migration functions:**
- `apply_item_transform` handles the deepcopy — `_transform` receives a copy, mutate freely
- Return `None` from the public function when no changes are needed (enables idempotency checks)
- Keep functions pure — no HTTP calls, no side effects
