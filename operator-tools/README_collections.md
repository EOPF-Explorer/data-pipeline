# Collection Management Tool

A comprehensive CLI tool for managing STAC collections in the EOPF Explorer catalog using the Transaction API.

## Features

- **Clean Collections**: Remove all items from a collection
- **Clean with S3 Data**: Remove items AND delete associated S3 data (Zarr stores)
- **S3 Storage Statistics**: View storage usage and object counts
- **Create Collections**: Create new collections from JSON templates
- **Update Collections**: Update existing collection metadata
- **Batch Operations**: Process multiple collection templates at once
- **Collection Info**: View collection details and item counts

## Installation

The tool uses dependencies already included in the project. Ensure you have the environment set up:

```bash
# Install dependencies (if not already done)
uv sync
```

## AWS/S3 Credentials Setup

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

## Usage

### Basic Syntax

```bash
uv run operator-tools/manage_collections.py [OPTIONS] COMMAND [ARGS]
```

### Available Commands

#### 1. `clean` - Remove All Items from a Collection

Remove all items from a collection (useful for clearing test data or resetting a collection). **NEW**: Optionally delete associated S3 data with validation checks.

```bash
# Dry run (see what would be deleted without actually deleting)
uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging --dry-run

# Actually clean the collection (will prompt for confirmation)
uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging

# Skip confirmation prompt
uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging -y

# Clean collection AND delete S3 data (with validation)
uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging --clean-s3 --dry-run
uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging --clean-s3 -y

# With custom S3 endpoint
uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging --clean-s3 \
    --s3-endpoint https://s3.de.io.cloud.ovh.net
```

**Options:**
- `--dry-run`: Show what would be deleted without actually deleting (includes S3 object counts)
- `--yes, -y`: Skip confirmation prompt
- `--clean-s3`: **[NEW]** Delete all S3 data referenced by item assets with validation
- `--s3-endpoint`: S3 endpoint URL (optional, uses `AWS_ENDPOINT_URL` env var if not specified)

**⚠️ WARNING - S3 Data Deletion:**

When using `--clean-s3`:
- **ALL S3 data** referenced by item assets will be **permanently deleted**
- Works with **any S3 asset structure**: individual files, Zarr stores, directories
- Extracts S3 URLs from both `alternate.s3.href` and main `href` fields
- This action **CANNOT be undone**
- Always use `--dry-run` first to verify what will be deleted
- Requires AWS credentials (via environment variables or AWS CLI configuration)

**How it works:**
1. Fetches all items from the collection
2. For each item:
   - Extracts all S3 URLs from assets (`alternate.s3.href` or main `href`)
   - Deletes all S3 objects (handles individual files, directories, and Zarr stores)
   - **Validates deletion** - verifies all objects were removed
   - **Only if S3 cleanup succeeded** - deletes the STAC item
   - **If S3 cleanup failed** - skips STAC item deletion and shows warning
3. Reports final statistics

**Safety Features:**
- **Validation checks** - Verifies all S3 objects are deleted before removing STAC items
- **Conditional deletion** - STAC items are only removed if S3 cleanup succeeded
- **Preservation on failure** - Items with S3 cleanup failures are preserved
- **Confirmation prompt** before deletion (unless `--yes` is used)
- **Progress bar** showing deletion progress
- **Detailed reporting** of success/failure counts for both items and S3 objects
- **Dry-run mode** to preview deletions with object counts

**Dry-run Preview:**

When using `--clean-s3 --dry-run`, you'll see:

```
Would delete 43 items:
  - S2A_MSIL2A_20250831T103701...
  ...

S3 data that would be deleted:

  Item 1/43: S2A_MSIL2A_20250831T103701_N0511_R008_T31TFL_20250831T145420
    S3 objects to delete: 1,247
    Asset URLs (4):
      • s3://bucket/product.zarr/measurements/reflectance
      • s3://bucket/product.zarr/quality/atmosphere
      ...

  Estimated total S3 objects for all 43 items: ~53,621
```

**When S3 Cleanup Fails:**

If some items fail S3 deletion:

```
⚠️  Item S2A_MSIL2A_...: Failed to delete 3 S3 objects
⚠️  Item S2A_MSIL2A_...: Validation failed - 12 S3 objects still exist
⚠️  Skipping STAC item deletion for S2A_MSIL2A_... due to S3 cleanup failures

✅ Deleted 40 STAC items (3 skipped due to S3 failures)
✅ Deleted 50,000 S3 objects (3 failed)

⚠️  WARNING: 3 items were NOT deleted from STAC catalog because their
    S3 data could not be fully removed.
```

This ensures your STAC metadata is preserved if S3 cleanup encounters issues.

#### 2. `create` - Create or Update a Collection

Create a new collection or update an existing one from a JSON template file.

```bash
# Create a new collection
uv run operator-tools/manage_collections.py create stac/sentinel-2-l2a.json

# Update an existing collection
uv run operator-tools/manage_collections.py create stac/sentinel-2-l2a.json --update
```

**Options:**
- `--update`: Update existing collection instead of creating new

**Template Format:**
Templates must be valid STAC Collection JSON files with at minimum:
- `id`: Collection identifier
- `type`: Must be "Collection"
- Other standard STAC Collection fields (title, description, extent, etc.)

See `stac/sentinel-2-l2a.json` for an example.

#### 3. `batch-create` - Batch Create/Update Collections

Process multiple collection templates from a directory at once.

```bash
# Create all collections from templates in stac/ directory
uv run operator-tools/manage_collections.py batch-create stac/

# Update all collections
uv run operator-tools/manage_collections.py batch-create stac/ --update

# Use custom file pattern
uv run operator-tools/manage_collections.py batch-create stac/ --pattern "*-staging.json"
```

**Options:**
- `--update`: Update existing collections instead of creating new
- `--pattern`: File pattern to match (default: `*.json`)

**Features:**
- Processes all matching JSON files in directory
- Shows preview before proceeding
- Reports success/failure for each file
- Summary statistics at the end

#### 4. `delete` - Delete a Collection

Delete a collection from the STAC catalog. Some STAC servers require the collection to be empty before deletion.

```bash
# Delete a collection (will prompt for confirmation)
uv run operator-tools/manage_collections.py delete sentinel-2-l2a-staging

# Clean items first, then delete
uv run operator-tools/manage_collections.py delete sentinel-2-l2a-staging --clean-first

# Skip confirmation prompt
uv run operator-tools/manage_collections.py delete sentinel-2-l2a-staging --clean-first -y
```

**Options:**
- `--clean-first`: Remove all items from the collection before deleting it
- `--yes, -y`: Skip confirmation prompt

**Safety Features:**
- Confirmation prompt before deletion (unless `--yes` is used)
- Option to automatically clean items first
- Handles already-deleted collections gracefully

#### 5. `info` - Show Collection Information

Display detailed information about a collection, including item count. **NEW**: Optionally include comprehensive S3 storage statistics.

```bash
# Basic collection info
uv run operator-tools/manage_collections.py info sentinel-2-l2a-staging

# Include S3 storage statistics (samples first 5 items)
uv run operator-tools/manage_collections.py info sentinel-2-l2a-staging --s3-stats

# With debug output (shows detailed URL extraction)
uv run operator-tools/manage_collections.py info sentinel-2-l2a-staging --s3-stats --debug

# With custom S3 endpoint
uv run operator-tools/manage_collections.py info sentinel-2-l2a-staging --s3-stats \
    --s3-endpoint https://s3.de.io.cloud.ovh.net
```

**Options:**
- `--s3-stats`: **[NEW]** Include S3 storage statistics (object count, total size)
- `--debug`: **[NEW]** Show detailed debug information about S3 URL extraction
- `--s3-endpoint`: S3 endpoint URL (optional, uses `AWS_ENDPOINT_URL` env var if not specified)

**Output includes:**
- Collection ID and title
- Description
- License
- Item count
- Spatial and temporal extents
- **[NEW]** S3 storage statistics (when `--s3-stats` is used):
  - Sample S3 URLs from item assets
  - Object count and total size for sampled items
  - Estimated total storage for all items in collection
  - Works with **any S3 asset structure** (individual files, Zarr stores, directories)

**S3 Statistics Behavior:**
- Samples the first 5 items to avoid long wait times on large collections
- Extracts S3 URLs from both `alternate.s3.href` and main `href` fields
- Counts all S3 objects referenced by assets (handles prefixes and individual files)
- Shows actual count/size for sampled items
- Provides estimated total based on sample average
- Requires AWS credentials to access S3

**Example Output:**

```bash
uv run operator-tools/manage_collections.py info sentinel-2-l2a-staging --s3-stats
```

```
============================================================
Collection: sentinel-2-l2a-staging
Title: Sentinel-2 Level-2A [V1 staging]
...
Items: 43
────────────────────────────────────────────────────────────
S3 Storage Statistics:
Sampling 5 of 43 items...

  Sample S3 URLs:
    • s3://esa-zarr-sentinel-explorer-fra/.../product.zarr/measurements/reflectance
    • s3://esa-zarr-sentinel-explorer-fra/.../product.zarr/quality/atmosphere
    ...

  Sample statistics:
    Objects: 6,235
    Size: 11.7 GB

  Estimated total (all 43 items):
    Objects: ~53,621
    Size: ~100.5 GB
============================================================
```

**With Debug Output:**

```bash
uv run operator-tools/manage_collections.py info sentinel-2-l2a-staging --s3-stats --debug
```

Shows detailed per-item information:
```
  📄 Item: S2A_MSIL2A_20250831T103701_N0511_R008_T31TFL_20250831T145420
     Found 4 S3 URLs
       • s3://bucket/product.zarr/measurements/reflectance
       • s3://bucket/product.zarr/quality/atmosphere
       • s3://bucket/product.zarr/measurements/reflectance/r10m
       • s3://bucket/product.zarr/measurements/reflectance/r20m
     Objects: 1,247, Size: 2.34 GB (cumulative)
```

### Global Options

#### `--api-url`

Override the default STAC API URL:

```bash
uv run operator-tools/manage_collections.py --api-url https://custom.stac.api/stac info my-collection
```

**Default:** `https://api.explorer.eopf.copernicus.eu/stac`

## Common Workflows

### Create a New Collection

1. Create or edit a JSON template in the `stac/` directory
2. Run the create command:
   ```bash
   uv run operator-tools/manage_collections.py create stac/my-collection.json
   ```
3. Verify the collection was created:
   ```bash
   uv run operator-tools/manage_collections.py info my-collection
   ```

### Clean Up Test Data

1. Check what would be deleted:
   ```bash
   uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging --dry-run
   ```
2. If satisfied, proceed with deletion:
   ```bash
   uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging
   ```

### Clean Up Test Data with S3

When you need to completely remove test data including S3 storage:

1. **Check what you have:**
   ```bash
   uv run operator-tools/manage_collections.py info sentinel-2-l2a-staging --s3-stats
   ```

2. **Preview what will be deleted (with object counts):**
   ```bash
   uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging --clean-s3 --dry-run
   ```

   Review the output carefully:
   - Check estimated S3 object count (e.g., "~53,621 objects")
   - Verify sample S3 URLs look correct
   - Ensure you have the right collection

3. **Delete everything (with validation):**
   ```bash
   uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging --clean-s3 -y
   ```

   The script will:
   - Delete S3 objects for each item
   - Validate all objects were removed
   - Only delete STAC item if S3 cleanup succeeded
   - Skip items with S3 failures (with warnings)

4. **Verify deletion:**
   ```bash
   uv run operator-tools/manage_collections.py info sentinel-2-l2a-staging
   ```

   Should show 0 items and no S3 data.

**If some items were skipped:**

Check the warning messages to identify which items had S3 failures. Fix the S3 access issues (permissions, bucket access, etc.) and re-run the cleanup to process the skipped items.

### Update Collection Metadata

1. Edit the collection template in `stac/`
2. Update the collection:
   ```bash
   uv run operator-tools/manage_collections.py create stac/sentinel-2-l2a.json --update
   ```

### Bulk Collection Setup

When setting up multiple collections from templates:

```bash
# Review templates in stac/ directory
ls stac/*.json

# Create all collections at once
uv run operator-tools/manage_collections.py batch-create stac/
```

## Transaction API Endpoints

The tool uses the following STAC Transaction API endpoints:

- `GET /collections/{collection_id}/items` - List items (for cleaning and info)
- `DELETE /collections/{collection_id}/items/{item_id}` - Delete item
- `POST /collections` - Create collection
- `PUT /collections` - Update collection
- `DELETE /collections/{collection_id}` - Delete collection

## Error Handling

The tool includes comprehensive error handling:

- **404 Errors**: Gracefully handles missing items/collections
- **Validation**: Validates JSON templates before submission
- **Network Errors**: Reports connection issues clearly
- **Partial Failures**: In batch operations, continues processing remaining items even if some fail
- **S3 Access Errors**: Reports S3 permission and connectivity issues

## Best Practices

1. **Always use `--dry-run` first** when cleaning collections to verify what will be deleted
2. **Test with staging collections** before operating on production collections
3. **Keep collection templates in version control** (in the `stac/` directory)
4. **Verify collection info** after create/update operations
5. **Use batch operations** for consistency when managing multiple collections
6. **Use `--clean-s3` carefully** - S3 data deletion is irreversible, always preview with `--dry-run`
7. **Check S3 statistics** before cleaning to understand storage impact
8. **Set AWS credentials** in environment before using S3 features

## Troubleshooting

### Connection Refused / Network Errors

- Verify the API URL is correct
- Check network connectivity to the STAC API
- Ensure you have necessary permissions

### Collection Not Found

```bash
# List all collections to find the correct ID
uv run operator-tools/manage_collections.py --help
```

### Invalid JSON Template

- Validate JSON syntax: `python -m json.tool < stac/template.json`
- Ensure required fields (`id`, `type`) are present
- Check that `type` is set to "Collection"

### Permission Denied

- Verify you have write access to the STAC API
- Check authentication credentials (if required)

### S3 Access Errors

```bash
# Verify AWS credentials are set
echo $AWS_ACCESS_KEY_ID
echo $AWS_SECRET_ACCESS_KEY

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

### Items Skipped Due to S3 Failures

If you see warnings like "Skipping STAC item deletion due to S3 cleanup failures":

**Causes:**
- S3 permission issues (need `s3:DeleteObject` permission)
- Network connectivity problems
- S3 objects locked or with retention policies
- Bucket access restrictions

**Solutions:**
1. Check AWS credentials and permissions
2. Verify bucket access with AWS CLI: `aws s3 ls s3://bucket-name/`
3. Review specific error messages in the output
4. Fix the underlying issue
5. Re-run cleanup to process skipped items

**Why items are preserved:**
The script validates S3 deletion succeeded before removing STAC items. This prevents orphaned metadata where the STAC item exists but its data is gone.

### S3 Object Count Seems Wrong

If the S3 object count doesn't match expectations:

**Check with debug mode:**
```bash
uv run operator-tools/manage_collections.py info collection-id --s3-stats --debug
```

This shows:
- Exact S3 URLs being extracted from each item
- How many URLs found per item
- Whether assets use `alternate.s3.href` or main `href`

**Common causes:**
- Assets don't have S3 URLs (use web URLs instead)
- Assets use `alternate.s3.href` which isn't set
- Some assets are excluded (e.g., thumbnails are skipped)

### No S3 Data Found

If `--s3-stats` shows "No S3 data found":

1. **Verify assets have S3 URLs:**
   ```bash
   # Check a sample item manually
   curl https://api.explorer.eopf.copernicus.eu/stac/collections/your-collection/items/item-id
   ```

   Look for:
   - `assets[*].alternate.s3.href` (preferred)
   - `assets[*].href` starting with `s3://`

2. **Use debug mode to diagnose:**
   ```bash
   uv run operator-tools/manage_collections.py info collection-id --s3-stats --debug
   ```

3. **Check credentials:**
   ```bash
   echo $AWS_ACCESS_KEY_ID
   echo $AWS_SECRET_ACCESS_KEY
   aws s3 ls  # Test AWS CLI access
   ```

## Examples

### Complete Collection Lifecycle

```bash
# 1. Create a new collection from template
uv run operator-tools/manage_collections.py create stac/sentinel-2-l2a.json

# 2. Check collection info
uv run operator-tools/manage_collections.py info sentinel-2-l2a-staging

# 3. (After testing) Clean test items
uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging --dry-run
uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging -y

# 4. Update collection metadata
uv run operator-tools/manage_collections.py create stac/sentinel-2-l2a.json --update

# 5. (If needed) Delete collection
uv run operator-tools/manage_collections.py delete sentinel-2-l2a-staging --clean-first -y
```

### Development Workflow

```bash
# Set up all collections from templates
uv run operator-tools/manage_collections.py batch-create stac/

# Clean specific test collection
uv run operator-tools/manage_collections.py clean sentinel-2-l2a-dp-test -y

# Update all collections with latest templates
uv run operator-tools/manage_collections.py batch-create stac/ --update
```

### Complete Cleanup with S3 Data

```bash
# 1. Check what's in the collection (with S3 statistics)
uv run operator-tools/manage_collections.py info sentinel-2-l2a-staging --s3-stats

# 2. Preview full cleanup (items + S3 with object counts)
uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging --clean-s3 --dry-run

# Review the output:
# - Total items to delete
# - S3 objects per item (sampled)
# - Estimated total S3 objects
# - Sample S3 URLs

# 3. Perform cleanup (with automatic validation)
uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging --clean-s3 -y

# Expected output:
# Deleting items and S3 data  [####################################]  43/43
# ✅ Deleted 43 STAC items
# ✅ Deleted 53,621 S3 objects

# 4. Verify everything is deleted
uv run operator-tools/manage_collections.py info sentinel-2-l2a-staging

# Should show: Items: 0
```

### S3 Storage Management Workflow

```bash
# Monitor S3 storage across multiple collections
uv run operator-tools/manage_collections.py info sentinel-2-l2a-staging --s3-stats
uv run operator-tools/manage_collections.py info sentinel-2-l2a-dp-test --s3-stats

# Compare storage usage
# Collection A: ~100 GB across 43 items
# Collection B: ~25 GB across 12 items

# Clean up old test data with validation
uv run operator-tools/manage_collections.py clean sentinel-2-l2a-dp-test --clean-s3 --dry-run

# Review: Would delete ~25 GB (12 items)

uv run operator-tools/manage_collections.py clean sentinel-2-l2a-dp-test --clean-s3 -y

# Verify storage freed
uv run operator-tools/manage_collections.py info sentinel-2-l2a-dp-test
```

### Debug S3 URL Extraction

```bash
# If S3 stats aren't showing up, use debug mode
uv run operator-tools/manage_collections.py info sentinel-2-l2a-staging --s3-stats --debug

# Shows per-item details:
#   📄 Item: S2A_MSIL2A_20250831T103701...
#      Found 4 S3 URLs
#        • s3://bucket/product.zarr/measurements/reflectance
#        • s3://bucket/product.zarr/quality/atmosphere
#        ...
#      Objects: 1,247, Size: 2.34 GB
```

### Handle Partial Cleanup Failures

```bash
# Attempt cleanup
uv run operator-tools/manage_collections.py clean test-collection --clean-s3 -y

# If some items fail:
# ⚠️  Item S2A_...: Failed to delete 3 S3 objects
# ⚠️  Skipping STAC item deletion for S2A_... due to S3 cleanup failures
# ✅ Deleted 40 STAC items (3 skipped due to S3 failures)
# ⚠️  WARNING: 3 items were NOT deleted from STAC catalog...

# Fix the S3 access issue (permissions, connectivity, etc.)
# Then re-run to process skipped items
uv run operator-tools/manage_collections.py clean test-collection --clean-s3 -y

# This time should succeed:
# ✅ Deleted 3 STAC items
# ✅ Deleted 3,741 S3 objects
```

## Support

For issues or questions:
- Check the main [operator-tools README](README.md)
- Review STAC Transaction API documentation
- Contact the EOPF Explorer operations team

## Related Tools

- `submit_test_workflow_wh.py` - Submit STAC items for processing
- `submit_stac_items_notebook.ipynb` - Interactive batch item submission
