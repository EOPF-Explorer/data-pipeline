# Operator Tools

This directory contains tools for operators to submit STAC items to the data pipeline for processing.

## Overview

The data pipeline processes STAC items from the EOPF STAC catalog. These tools allow operators to:

- Submit individual test items for debugging/validation
- Search for STAC items by area and time range
- Batch submit multiple items for processing
- **[NEW]** Manage STAC collections with S3 data cleanup
- **[NEW]** Monitor S3 storage usage across collections

## Setup

### Environments

The data pipeline operates in two Kubernetes namespaces:

- **`devseed-staging`** - Testing and validation environment
- **`devseed`** - Production data pipeline

Examples below use `devseed-staging`. For production, replace with `devseed`.

### AWS/S3 Credentials (Required for S3 Features)

For S3 cleanup and statistics features, configure AWS credentials:

```bash
# Option 1: Environment variables
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_ENDPOINT_URL="https://s3.de.io.cloud.ovh.net"

# Option 2: AWS CLI configuration
aws configure
```

**Note**: S3 features (`--clean-s3`, `--s3-stats`) require these credentials. Other features work without them.

### Port Forwarding

Before using these tools, you need to set up port forwarding to access the webhook service:

#### For staging environment:
```bash
# Port forward from the webhook eventsource service (staging)
kubectl port-forward -n devseed-staging svc/eopf-explorer-webhook-eventsource-svc 12001:12000 &
```
This makes the webhook endpoint available at `http://localhost:12001/samples`.

#### For prod environment:
```bash
# Port forward from the webhook eventsource service (production)
kubectl port-forward -n devseed svc/eopf-explorer-webhook-eventsource-svc 12000:12000 &
```

This makes the webhook endpoint available at `http://localhost:12000/samples`.


## Available Tools

### 1. `manage_item.py` - Single Item Management Tool 🆕

**Purpose**: Manage individual STAC items with full S3 operation support.

**Use cases:**
- Debug problematic items before processing collections
- View detailed S3 statistics for a specific item
- Delete single items with S3 cleanup and validation
- Test item operations before scaling to collections

**Quick Examples:**
```bash
# View item details with S3 stats
uv run operator-tools/manage_item.py info sentinel-2-l2a-staging ITEM_ID --s3-stats

# View storage tier statistics from STAC metadata
uv run operator-tools/manage_item.py info sentinel-2-l2a-staging ITEM_ID --s3-stac-info

# Combine both statistics
uv run operator-tools/manage_item.py info sentinel-2-l2a-staging ITEM_ID --s3-stats --s3-stac-info

# Debug S3 URL extraction
uv run operator-tools/manage_item.py info sentinel-2-l2a-staging ITEM_ID --s3-stats --debug

# Sync storage tiers for a single item (dry run)
uv run operator-tools/manage_item.py sync-storage-tiers sentinel-2-l2a-staging ITEM_ID \
    --s3-endpoint https://s3.de.io.cloud.ovh.net --dry-run

# Delete single item with S3 cleanup (dry run)
uv run operator-tools/manage_item.py delete sentinel-2-l2a-staging ITEM_ID --clean-s3 --dry-run

# Actually delete the item
uv run operator-tools/manage_item.py delete sentinel-2-l2a-staging ITEM_ID --clean-s3 -y
```

**Key Features:**
- Detailed item inspection with S3 statistics
- Storage tier statistics from STAC metadata
- Sync storage tiers with S3 (single item)
- Debug mode for S3 URL extraction troubleshooting
- Delete with automatic S3 validation
- Dry-run mode for safe testing

**When to use**: Always start with `manage_item.py` to debug individual items before running batch operations with `manage_collections.py`.

### 2. `manage_collections.py` - Collection Management Tool

Comprehensive tool for managing STAC collections using the Transaction API, **now with validated S3 data cleanup and comprehensive storage statistics**.

**🔄 Refactored**: Now uses `manage_item.py` for all item-level operations, making the code more maintainable and easier to debug.

**Use cases:**
- Clean collections (remove all items)
- Clean collections with validated S3 data deletion (removes items AND all S3 objects)
- View comprehensive S3 storage statistics (works with any S3 asset structure)
- View storage tier statistics from STAC metadata (all items processed)
- Automatic validation ensures S3 cleanup succeeds before removing STAC items
- Create/update collections from templates
- Batch operations on multiple collections
- View collection information and statistics

**Prerequisites:**

- STAC API access to `https://api.explorer.eopf.copernicus.eu/stac`
- Write permissions for collection management operations
- **[NEW]** AWS credentials for S3 features (cleanup and statistics)

**Quick Start:**

```bash
# See all available commands
uv run operator-tools/manage_collections.py --help

# View collection info
uv run operator-tools/manage_collections.py info sentinel-2-l2a-staging

# View collection with S3 storage statistics
uv run operator-tools/manage_collections.py info sentinel-2-l2a-staging --s3-stats

# View storage tier statistics from STAC metadata
uv run operator-tools/manage_collections.py info sentinel-2-l2a-staging --s3-stac-info

# Combine both statistics
uv run operator-tools/manage_collections.py info sentinel-2-l2a-staging --s3-stats --s3-stac-info

# Debug S3 URL extraction
uv run operator-tools/manage_collections.py info sentinel-2-l2a-staging --s3-stats --debug

# Sync storage tiers for entire collection (dry run)
uv run operator-tools/manage_collections.py sync-storage-tiers sentinel-2-l2a-staging \
    --s3-endpoint https://s3.de.io.cloud.ovh.net --dry-run

# Clean a collection (dry run first!)
uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging --dry-run
uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging

# Clean collection AND delete S3 data (with validation)
uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging --clean-s3 --dry-run
uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging --clean-s3 -y

# Create/update collection from template
uv run operator-tools/manage_collections.py create stac/sentinel-2-l2a.json
uv run operator-tools/manage_collections.py create stac/sentinel-2-l2a.json --update

# Batch create collections
uv run operator-tools/manage_collections.py batch-create stac/
```

**Key Features:**
- **Validated S3 cleanup** - Verifies all S3 objects deleted before removing STAC items
- **Comprehensive S3 support** - Handles individual files, directories, and Zarr stores
- **Sync storage tiers** - Keep STAC metadata in sync with S3 storage classes
- **Debug mode** - Detailed S3 URL extraction and validation info
- **Safety first** - STAC items preserved if S3 cleanup fails

**Documentation:** See [README_collections.md](./README_collections.md) for detailed usage and examples.

### 3. `submit_test_workflow_wh.py` - HTTP Webhook Submission

Submits a single test STAC item via HTTP webhook endpoint.

**Use case:** Testing the pipeline with a known item

**Prerequisites:**

- Pipeline webhook service running on `localhost:12000`
- `requests` Python package installed

**Usage:**

```bash
uv run submit_test_workflow_wh.py
```

**Configuration:**

Edit the script to change:

- `source_url`: STAC item URL to process
- `collection`: Target collection name
- `action`: Processing action (e.g., `convert-v1-s2-hp`, or `convert-v1-s2-hp`)

### 4. `submit_stac_items_notebook.ipynb` - Interactive STAC Search & Submit

Jupyter notebook for searching and batch submitting STAC items.

**Use case:** Bulk processing multiple items from a specific area/time range

**Prerequisites:**

- Jupyter notebook environment
- Python packages: `pystac-client`, `pandas`, `requests`
- Optional: `python-dotenv` for credential management
- Pipeline webhook service running on `localhost:12000`

**Usage:**

```bash
uv run jupyter notebook submit_stac_items_notebook.ipynb
```

**Features:**

- Browse available STAC collections
- Define area of interest (bounding box) and time range
- Search and preview matching items
- Submit all or selected items to the pipeline via HTTP webhook
- Track submission success/failure

## Debugging Workflow: When to Use Which Tool 🔍

The refactored tools follow a "single item → collection" debugging workflow:

### Step 1: Debug with `manage_item.py`

When you encounter issues with collection operations, **always start by examining individual items**:

```bash
# 1. Identify a problematic item from collection operation output
python manage_collections.py clean test-coll --clean-s3 -y
# Output shows: "⚠️  Item S2A_MSIL2A_... skipped due to S3 failures"

# 2. Debug that specific item
python manage_item.py info test-coll S2A_MSIL2A_... --s3-stats --debug

# This shows:
# - Exact S3 URLs extracted from the item
# - Which asset fields contain S3 URLs
# - Object counts and sizes
# - Any extraction or access issues

# 3. Test deletion on single item
python manage_item.py delete test-coll S2A_MSIL2A_... --clean-s3 --dry-run

# 4. If dry-run looks good, actually delete
python manage_item.py delete test-coll S2A_MSIL2A_... --clean-s3 -y
```

### Step 2: Scale to Collection

Once individual items work correctly, apply to the entire collection:

```bash
# Preview collection operation
python manage_collections.py clean test-coll --clean-s3 --dry-run

# Execute if preview looks good
python manage_collections.py clean test-coll --clean-s3 -y
```

### When to Use Each Tool

| Tool | Use When | Example |
|------|----------|---------|
| `manage_item.py` | Debugging single items | `python manage_item.py info coll-id item-id --debug` |
| `manage_item.py` | Testing operations on one item | `python manage_item.py delete coll-id item-id --dry-run` |
| `manage_item.py` | Investigating S3 issues | `python manage_item.py info coll-id item-id --s3-stats` |
| `manage_collections.py` | Viewing collection statistics | `python manage_collections.py info coll-id --s3-stats` |
| `manage_collections.py` | Batch operations on all items | `python manage_collections.py clean coll-id --clean-s3 -y` |
| `manage_collections.py` | Collection lifecycle management | `python manage_collections.py create/delete` |

### Benefits of This Workflow

✅ **Faster debugging** - Test on single items instead of entire collections
✅ **Less risk** - Validate operations work before scaling
✅ **Better visibility** - Debug mode shows exactly what's happening
✅ **Easier fixes** - Fix item-level issues before batch processing

## S3 Data Management

The collection management tool can now interact with S3 storage with comprehensive deletion and validation:

### View S3 Storage Statistics

Check how much S3 storage a collection is using:

```bash
# View collection info with S3 statistics
uv run operator-tools/manage_collections.py info sentinel-2-l2a-staging --s3-stats

# View storage tier statistics from STAC metadata
uv run operator-tools/manage_collections.py info sentinel-2-l2a-staging --s3-stac-info

# Combine both statistics
uv run operator-tools/manage_collections.py info sentinel-2-l2a-staging --s3-stats --s3-stac-info

# With debug output (shows detailed URL extraction)
uv run operator-tools/manage_collections.py info sentinel-2-l2a-staging --s3-stats --debug
```

**Output includes:**
- Sample S3 URLs from item assets
- Object count and total size for sampled items
- Estimated total storage across all items
- Works with **any S3 asset structure** (individual files, Zarr stores, directories)

**Storage Tier Statistics (`--s3-stac-info`):**
- Processes all items in the collection
- Shows distribution of storage tiers (STANDARD, STANDARD_IA, EXPRESS_ONEZONE, MIXED)
- Detailed breakdowns for mixed storage tiers
- Reads from STAC metadata (no S3 queries required)

**Example:**
```
S3 Storage Statistics:
Sampling 5 of 43 items...

  Sample S3 URLs:
    • s3://bucket/product.zarr/measurements/reflectance
    • s3://bucket/product.zarr/quality/atmosphere
    ...

  Sample statistics:
    Objects: 6,235
    Size: 11.7 GB

  Estimated total (all 43 items):
    Objects: ~53,621
    Size: ~100.5 GB
```

### Clean S3 Data (with Validation)

Remove both STAC items and their associated S3 data with automatic validation:

```bash
# Preview what would be deleted (RECOMMENDED FIRST STEP)
uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging --clean-s3 --dry-run

# Delete items and S3 data (with validation)
uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging --clean-s3 -y
```

**⚠️ CRITICAL WARNING:**
- S3 data deletion is **permanent** and **cannot be undone**
- Each item may reference thousands of S3 objects
- Always use `--dry-run` first to verify what will be deleted
- Consider checking S3 statistics before cleanup to understand impact

**How it works:**
1. Extracts S3 URLs from each item's assets (`alternate.s3.href` or main `href`)
2. Deletes all S3 objects (handles individual files, directories, and Zarr stores)
3. **Validates deletion** - verifies all S3 objects were removed
4. **Only if validation succeeds** - removes the STAC item from catalog
5. **If validation fails** - preserves STAC item and shows warning

**Validation & Safety:**
- Counts S3 objects before and after deletion
- Only removes STAC item if all S3 objects were deleted
- Preserves STAC metadata if S3 cleanup fails
- Reports skipped items with detailed warnings

**Typical workflow:**
```bash
# 1. Check current storage
uv run operator-tools/manage_collections.py info test-collection --s3-stats

# 2. Preview deletion (see object counts)
uv run operator-tools/manage_collections.py clean test-collection --clean-s3 --dry-run

# Review output:
#   Would delete 10 items
#   S3 objects to delete: ~12,470
#   Sample URLs shown for verification

# 3. Proceed if safe
uv run operator-tools/manage_collections.py clean test-collection --clean-s3 -y

# Output:
#   Deleting items and S3 data  [####################################]  10/10
#   ✅ Deleted 10 STAC items
#   ✅ Deleted 12,470 S3 objects

# 4. Verify
uv run operator-tools/manage_collections.py info test-collection
```

**When S3 Cleanup Fails:**

If some items encounter S3 deletion failures:

```
⚠️  Item S2A_...: Failed to delete 3 S3 objects
⚠️  Skipping STAC item deletion for S2A_... due to S3 cleanup failures

✅ Deleted 8 STAC items (2 skipped due to S3 failures)
✅ Deleted 10,123 S3 objects (3 failed)

⚠️  WARNING: 2 items were NOT deleted from STAC catalog because
    their S3 data could not be fully removed.
```

Fix the S3 access issues and re-run cleanup to process skipped items.

## Target Collections

Common target collections for processing:

- `sentinel-2-l2a-staging` - Staging environment for S2 L2A
- `sentinel-2-l2a-dp-test` - Test environment for S2 L2A

## Payload Format

All tools submit payloads with these fields:

- `source_url`: Full STAC item URL (self link) - Must be a STAC API URL, not direct zarr
- `collection`: Target collection for processed data
- `action`: (Optional) Processing action/trigger to use

See [main README payload examples](../README.md#payload-format) for correct/incorrect formats.

## Common Actions

- `convert-v1-s2` - Standard Sentinel-2 conversion
- `convert-v1-s2-hp` - High-priority Sentinel-2 conversion

## Troubleshooting

### HTTP Webhook Not Responding

```bash
# Verify port-forward is active
ps aux | grep "port-forward.*12000"

# If not running, start port-forward (staging)
kubectl port-forward -n devseed-staging svc/eopf-explorer-webhook-eventsource-svc 12000:12000 &

# Test webhook connectivity
curl http://localhost:12000
```

If issues persist, check the [platform-deploy troubleshooting guide](https://github.com/EOPF-Explorer/platform-deploy/tree/main/workspaces/devseed-staging/data-pipeline) (or `/devseed/` for production).

### STAC Search Returns No Items

- Verify the bounding box coordinates (format: `[min_lon, min_lat, max_lon, max_lat]`)
- Check the date range format (`YYYY-MM-DDTHH:MM:SSZ`)
- Confirm the collection exists: <https://stac.core.eopf.eodc.eu/>

### S3 Access Errors

```bash
# Verify AWS credentials are set
echo $AWS_ACCESS_KEY_ID
echo $AWS_SECRET_ACCESS_KEY
echo $AWS_ENDPOINT_URL

# Or check AWS CLI configuration
aws configure list

# Set credentials if missing
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_ENDPOINT_URL="https://s3.de.io.cloud.ovh.net"
```

### S3 Cleanup Taking Too Long

If S3 cleanup is slow:
- Each item can reference thousands of S3 objects
- Large collections take time (progress bar shows status)
- Consider using `--dry-run` first to estimate total objects
- Typical deletion speed: ~100-500 objects/second
- Large collections (100+ items) may take several minutes

### Items Skipped Due to S3 Failures

If cleanup shows "items skipped due to S3 failures":

**Why this happens:**
The script validates that all S3 objects are deleted before removing STAC items. If any S3 objects remain or deletion fails, the STAC item is preserved to prevent orphaned metadata.

**Common causes:**
- S3 permission issues (need `s3:DeleteObject`)
- Network connectivity problems
- Objects locked or with retention policies
- Bucket access restrictions

**How to fix:**
1. Review specific error messages in the output
2. Check AWS credentials and permissions
3. Test bucket access: `aws s3 ls s3://bucket-name/`
4. Fix the underlying S3 issue
5. Re-run cleanup to process previously skipped items

### No S3 Data Found

If `--s3-stats` shows "No S3 data found":

1. **Use debug mode to diagnose:**
   ```bash
   uv run operator-tools/manage_collections.py info collection-id --s3-stats --debug
   ```

   This shows:
   - Exact S3 URLs extracted from each item
   - Which asset fields contain S3 URLs
   - Whether URLs are found in `alternate.s3.href` or main `href`

2. **Verify assets have S3 URLs:**
   ```bash
   # Check a sample item manually
   curl https://api.explorer.eopf.copernicus.eu/stac/collections/your-collection/items/item-id | jq '.assets'
   ```

   Look for S3 URLs in:
   - `assets[*].alternate.s3.href` (preferred)
   - `assets[*].href` starting with `s3://`

3. **Check AWS credentials:**
   ```bash
   echo $AWS_ACCESS_KEY_ID
   echo $AWS_SECRET_ACCESS_KEY
   aws s3 ls  # Test AWS CLI access
   ```

## Best Practices

1. **Debug with single items first** - Use `manage_item.py` to test and debug operations on individual items before using `manage_collections.py` on entire collections
2. **Always use `--dry-run` for S3 operations** - Preview deletions with object counts before executing
3. **Use `--debug` flag when troubleshooting** - Shows detailed S3 URL extraction and validation steps
4. **Test with single items first** - Use `submit_test_workflow_wh.py` or `manage_item.py` before bulk submissions
5. **Monitor processing** - Check pipeline logs/dashboards after submitting
6. **Use appropriate collections** - Use test/staging collections for validation
7. **Validate STAC URLs** - Ensure source URLs are accessible before submitting
8. **Check webhook service** - Ensure the webhook service is running before submitting items
9. **Check S3 statistics before cleanup** - Understand storage impact with `--s3-stats`
10. **Review validation warnings** - Pay attention to items skipped due to S3 failures
11. **Set AWS credentials** - Required for S3 features (cleanup and statistics)
12. **Trust the validation** - If items are skipped, fix S3 issues before retrying

## Support

For issues or questions about the data pipeline, contact the pipeline operations team.
