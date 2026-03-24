# Query STAC Items Needing Storage Tier Migration

## Overview

The `query_storage_tier_items.py` script queries a STAC collection for items that need their S3 storage tier changed. It searches a 36h time window around a target age, filters out items already at the target storage tier (using `storage:refs` metadata), and caps the output at a configurable batch size.

This script is designed for the **chunked drain pattern**: an Argo CronWorkflow runs it hourly, each run processes a fixed batch, and the backlog drains across multiple runs.

**Output**: JSON array of item IDs to stdout (for Argo `withParam`).

## Requirements

The script requires the following Python packages:
- `pystac` - STAC item handling
- `pystac-client` - STAC API client (search and pagination)

All dependencies are managed via `uv` and will be automatically installed when running the script.

## Usage

### CLI Arguments

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--stac-api-url` | Yes | -- | STAC API base URL |
| `--collection` | Yes | -- | STAC collection ID to query |
| `--age-days` | Yes | -- | Target age in days for tier transition |
| `--target-storage-ref` | Yes | -- | Expected `storage:refs` value for the target tier |
| `--max-batch-size` | No | `100` | Maximum number of items to return |

### Storage Ref Values

The `--target-storage-ref` value corresponds to the `storage:refs` field written by `update_stac_storage_tier.py`:

| S3 Storage Class | `storage:refs` value | `--target-storage-ref` |
|------------------|---------------------|----------------------|
| `STANDARD` | `standard` | `standard` |
| `STANDARD_IA` | `glacier` | `glacier` |
| `EXPRESS_ONEZONE` | `performance` | `performance` |

### Basic Usage

```bash
uv run python scripts/query_storage_tier_items.py \
    --stac-api-url https://api.explorer.eopf.copernicus.eu/stac \
    --collection sentinel-2-l2a-staging \
    --age-days 1 \
    --target-storage-ref glacier \
    --max-batch-size 100
```

Output:
```json
["S2A_MSIL2A_20250317T100631_...", "S2B_MSIL2A_20250317T103019_..."]
```

### Verbose Logging

```bash
LOG_LEVEL=DEBUG uv run python scripts/query_storage_tier_items.py \
    --stac-api-url https://api.explorer.eopf.copernicus.eu/stac \
    --collection sentinel-2-l2a-staging \
    --age-days 7 \
    --target-storage-ref glacier
```

## How It Works

1. **Calculate time window**: A 36h window from `(age_days + 1)` days ago to `(age_days - 0.5)` days ago, ensuring items at the target age are captured with a buffer.

2. **Query STAC**: Searches the collection for all items within the time window using `pystac_client`.

3. **Filter already-migrated items**: For each item, checks if ALL assets with `alternate.s3` have `storage:refs` matching the target. If so, the item is skipped.
   - Items with **no** `alternate.s3` assets are treated as needing work (safe default for newly ingested items).
   - Items with **mixed** storage refs (some matching, some not) are treated as needing work.

4. **Cap output**: Returns at most `max_batch_size` items.

5. **Output**: Writes JSON array of item IDs to stdout. All logging goes to stderr.

## Chunked Drain Pattern

This script is designed for hourly execution by an Argo CronWorkflow:

```
Run 0:  query -> 700 items, filter -> 700 need work -> return 100
Run 1:  query -> 700 items, filter -> 600 need work -> return 100
...
Run 6:  query -> 700 items, filter -> 100 need work -> return 100
Run 7:  query -> 700 items, filter ->   0 need work -> return []
```

Between runs, the `change_storage_tier.py` and `update_stac_storage_tier.py` scripts process each item and update its `storage:refs` metadata. The next run's filter then skips those items.

## Error Handling

- Exit code `0`: Success (including when 0 items need work -- outputs `[]`)
- Exit code `1`: Failure (STAC API error, network issue, etc.)

## Related Scripts

- `change_storage_tier.py` - Changes S3 storage class for objects in a STAC item
- `update_stac_storage_tier.py` - Updates STAC metadata with current storage tier info
- `storage_tier_utils.py` - Shared utilities for storage tier operations
- `query_stac.py` - Similar script for querying items for data conversion (different use case)
