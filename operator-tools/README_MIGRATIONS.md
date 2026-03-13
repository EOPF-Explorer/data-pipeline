# STAC Catalogue Migration Tool

`migrate_catalog.py` is a reusable operator tool for applying catalogue-wide fixes to STAC items.
Each migration is a pure function, tracked in a local history file to prevent accidental re-runs.

## Available Migrations

| Name | Description |
|---|---|
| `fix_url_encoding` | Replace `+` with `%20` in asset/link href query strings (RFC 3986 compliance) |
| `fix_zarr_media_type` | Replace `application/vnd+zarr` with `application/vnd.zarr` (MIME convention) |

## Safe Migration Procedure

### Pre-flight

```bash
# Check STAC API is accessible
curl https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a | jq .id

# Review what will change
python migrate_catalog.py run --migration fix_url_encoding sentinel-2-l2a --dry-run
python migrate_catalog.py run --migration fix_zarr_media_type sentinel-2-l2a --dry-run
```

### Step 1: Backup

```bash
# Create a backup collection before modifying anything
python migrate_catalog.py clone sentinel-2-l2a sentinel-2-l2a-backup-20260312 --yes
```

### Step 2: Migrate

```bash
# Apply migrations in-place on the original collection
python migrate_catalog.py run --migration fix_url_encoding sentinel-2-l2a
python migrate_catalog.py run --migration fix_zarr_media_type sentinel-2-l2a

# Or apply both in a single pass (composable migrations)
python migrate_catalog.py run --migration fix_url_encoding --migration fix_zarr_media_type sentinel-2-l2a
```

### Step 3: Verify

```bash
# Confirm all items are fixed (exits 0 if fully applied, 1 if items remain)
python migrate_catalog.py verify --migration fix_url_encoding sentinel-2-l2a
python migrate_catalog.py verify --migration fix_zarr_media_type sentinel-2-l2a
```

### Step 4: Cleanup

```bash
# Delete backup once satisfied (use manage_collections.py)
python manage_collections.py clean sentinel-2-l2a-backup-20260312 --yes
python manage_collections.py delete sentinel-2-l2a-backup-20260312 --yes
```

## Restore Procedure

If something goes wrong after migration:

```bash
# 1. Remove all items from the damaged collection
python manage_collections.py clean sentinel-2-l2a --yes

# 2. Clone the backup back
python migrate_catalog.py clone sentinel-2-l2a-backup-20260312 sentinel-2-l2a --yes

# 3. Verify restore
python migrate_catalog.py verify --migration fix_zarr_media_type sentinel-2-l2a  # should show items still need migration

# 4. Delete the backup
python manage_collections.py clean sentinel-2-l2a-backup-20260312 --yes
python manage_collections.py delete sentinel-2-l2a-backup-20260312 --yes
```

## Recovery Files

Each non-dry-run migration writes a `.migration_recovery_<timestamp>.jsonl` file alongside
`.migration_history.json`. Each line is the migrated version of an item that was submitted to
the API. If a run is interrupted after some DELETEs but before the corresponding POSTs complete,
use this file to re-POST the affected items manually.

## CLI Reference

```
migrate_catalog.py [--api-url URL] [--history-file PATH] COMMAND [OPTIONS]

Global options:
  --api-url TEXT        STAC API URL (or STAC_API_URL env var)
  --history-file PATH   Migration history JSON file (default: .migration_history.json)

Commands:
  list                                     List available migrations
  run COLLECTION_ID                        Apply one or more migrations to a collection
    --migration TEXT  (repeatable)         Migration(s) to run — repeat to compose
    --dry-run                              Preview changes without updating
    --yes / -y                             Skip confirmation prompt
  verify COLLECTION_ID                     Check if migration is fully applied
    --migration TEXT  (repeatable)         Migration(s) to verify
  clone SOURCE_ID TARGET_ID               Clone a collection (metadata + all items)
    --yes / -y                             Skip confirmation prompt
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
python migrate_catalog.py history
python migrate_catalog.py history --migration fix_url_encoding --collection sentinel-2-l2a
```

## Writing New Migrations

1. Create a new file `operator-tools/migrate_catalog/migrations/my_migration.py`:

   ```python
   from typing import Any
   from migrate_catalog.migrations._registry import migration
   from migrate_catalog.types import apply_item_transform

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

2. Register it by adding an import to `operator-tools/migrate_catalog/migrations/__init__.py`:
   ```python
   from migrate_catalog.migrations import my_migration as _  # noqa: F401
   ```

3. Add unit tests in `tests/unit/test_migrate_catalog.py`.

4. Run: `python migrate_catalog.py run --migration my_migration <collection_id> --dry-run`

**Design rules for migration functions:**
- `apply_item_transform` handles the deepcopy — `_transform` receives a copy, mutate freely
- Return `None` from the public function when no changes are needed (enables idempotency checks)
- Keep functions pure — no HTTP calls, no side effects
