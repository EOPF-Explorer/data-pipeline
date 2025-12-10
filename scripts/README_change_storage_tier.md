# Storage Tier Management

## Overview

The `change_storage_tier.py` script allows you to change the storage tier (storage class) of S3 objects referenced in a STAC item. This is useful for optimizing storage costs by moving data to different storage tiers based on access patterns.

## Requirements

The script requires the following Python packages:
- `boto3` - AWS SDK for Python (S3 operations)
- `httpx` - HTTP client (fetching STAC items)
- `botocore` - AWS core functionality
- `uv` - Python package installer and runner

All dependencies are managed via `uv` and will be automatically installed when running the script.

## Environment Setup

### Credentials for OVH Cloud Storage

Configure your credentials to access OVH cloud storage using one of these methods:

```bash
# Option 1: Environment variables
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"

# Option 2: AWS CLI configuration
aws configure
```

### S3 Endpoint (Optional)

If using a custom S3-compatible service:

```bash
export AWS_ENDPOINT_URL="https://s3.de.io.cloud.ovh.net"
```

Or specify via command line:

```bash
uv run python scripts/change_storage_tier.py \
    --s3-endpoint https://s3.de.io.cloud.ovh.net \
    ...
```

### Define STAC Item ID

For easier command execution, define the STAC item ID as a variable:

```bash
ITEM_ID="S2A_MSIL2A_20251209T123131_N0511_R009_T26SPG_20251209T163109"
```

## Usage

### Dry Run

Test the script without making actual changes. Dry-run mode will:
- Query and display the current storage class of each object
- Show what changes would be made
- Display storage class distribution statistics
- Not modify any objects

```bash
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a-staging/items/$ITEM_ID \
    --storage-class STANDARD_IA \
    --dry-run
```

### Basic Usage

Run the script using the STAC item ID variable defined in the setup:

```bash
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a-staging/items/$ITEM_ID \
    --storage-class STANDARD_IA
```

### With Custom S3 Endpoint

```bash
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a-staging/items/$ITEM_ID \
    --storage-class STANDARD_IA \
    --s3-endpoint https://s3.de.io.cloud.ovh.net
```

### Filter Specific Subpaths

Only change storage class for specific parts of the Zarr store:

```bash
# Only process reflectance data
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a-staging/items/$ITEM_ID \
    --storage-class STANDARD_IA \
    --include-pattern "measurements/reflectance/*"

# Process multiple subdirectories
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a-staging/items/$ITEM_ID \
    --storage-class STANDARD_IA \
    --include-pattern "measurements/*" \
    --include-pattern "quality/*"

# Exclude metadata files
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a-staging/items/$ITEM_ID \
    --storage-class STANDARD_IA \
    --exclude-pattern "*.zattrs" \
    --exclude-pattern "*.zmetadata"

# Only process 60m resolution data
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a-staging/items/$ITEM_ID \
    --storage-class STANDARD_IA \
    --include-pattern "*/r60m/*"
```

## Available Storage Classes

- **STANDARD** - Standard storage tier (default, immediate access, higher cost)
- **STANDARD_IA** - Archive storage tier (lower cost, retrieval required before access)
- **EXPRESS_ONEZONE** - High-performance storage tier (single availability zone)

### OVH Cloud Storage Classes

**Important**: This script uses OVH Cloud Storage class naming directly to avoid confusion.

**Supported Storage Classes:**
- `STANDARD` - Standard storage (default)
- `STANDARD_IA` - Standard, Infrequent Access (archive storage, low-cost)
- `EXPRESS_ONEZONE` - High Performance (low-latency storage)

**Full AWS to OVH Storage Class Mapping:**

| AWS Storage Class | OVH Storage Class | CLI Value (this script) |
|-------------------|-------------------|------------------------|
| `EXPRESS_ONEZONE` | High Performance | `EXPRESS_ONEZONE` |
| `STANDARD` | Standard | `STANDARD` |
| `INTELLIGENT_TIERING` | Standard | `STANDARD` |
| `STANDARD_IA` | Standard, Infrequent Access | `STANDARD_IA` |
| `ONEZONE_IA` | Standard, Infrequent Access | `STANDARD_IA` |
| `GLACIER_IR` | Standard, Infrequent Access | `STANDARD_IA` |
| `GLACIER` | Standard, Infrequent Access | `STANDARD_IA` |
| `DEEP_ARCHIVE` | Cold Archive | N/A (not supported) |

**Note**: Multiple AWS storage classes map to the same OVH tier. This script uses OVH naming (`STANDARD_IA`) instead of AWS naming (`GLACIER`) to avoid confusion.

**Reference**: [OVH Cloud Storage S3 Location Documentation](https://help.ovhcloud.com/csm/en-public-cloud-storage-s3-location?id=kb_article_view&sysparm_article=KB0047384)

## How It Works

1. Fetches the STAC item from the provided URL
2. Extracts S3 URLs from the `alternate.s3.href` fields in each asset
3. Identifies the root Zarr store location
4. Lists all objects in the Zarr store recursively (includes storage class in response)
5. Optionally filters objects based on include/exclude patterns
6. **Optimization**: Skips objects already at target storage class (no API calls)
7. Changes the storage class only for objects that need it using the S3 API

### Performance Optimizations

The script has been optimized to minimize S3 API calls:

- **Storage class from list**: Retrieves storage class during initial listing (no extra `head_object` calls)
- **Smart filtering**: Only makes `copy_object` API calls for objects that actually need to change storage class
- **Progress tracking**: Shows how many objects need changes vs. already correct

**Example performance**: For a Zarr store with 1,058 objects where 260 need changes:
- Listing: ~1 second (unavoidable - must discover all objects)
- Processing: ~100 seconds for 260 changes (~400ms per object)
- **Objects already at target**: 0 API calls (instant)
- **Total time**: ~1m 42s instead of 8+ minutes without optimization

## Path Filtering

The script supports filtering which subpaths within the Zarr store should have their storage class changed:

- **Include patterns** (`--include-pattern`): Only process paths matching at least one include pattern
- **Exclude patterns** (`--exclude-pattern`): Skip paths matching any exclude pattern
- Patterns use Python's `fnmatch` syntax (similar to shell wildcards)
- Patterns are evaluated relative to the Zarr root directory
- Multiple patterns can be specified by repeating the flag
- Filters are applied after listing all objects, reducing cost and time

### Pattern Examples

- `measurements/*` - All files under measurements directory
- `measurements/reflectance/*` - All reflectance data
- `*/r60m/*` - All 60m resolution data across all groups
- `*.json` - All JSON files
- `*.zattrs` - All Zarr attribute files
- `quality/atmosphere/*` - Atmosphere quality data

### Filtering Logic

1. If include patterns are specified, only paths matching at least one pattern are selected
2. Exclude patterns are then applied to remove matching paths
3. The script logs the total number of objects found and how many were filtered out

## Integration in Workflow

This script can be integrated into your data pipeline workflow after the registration step:

```bash
# 1. Convert
uv run python scripts/convert_v1_s2.py \
    --source-url SOURCE_URL \
    --collection COLLECTION \
    --s3-output-bucket BUCKET \
    --s3-output-prefix PREFIX

# 2. Register
uv run python scripts/register_v1.py \
    --source-url SOURCE_URL \
    --collection COLLECTION \
    --stac-api-url STAC_API \
    --raster-api-url RASTER_API \
    --s3-endpoint S3_ENDPOINT \
    --s3-output-bucket BUCKET \
    --s3-output-prefix PREFIX

# 3. Change storage tier (optional)
ITEM_ID="your-item-id"
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a-staging/items/$ITEM_ID \
    --storage-class STANDARD_IA
```

## Error Handling

The script handles various error conditions:
- STAC item not found or inaccessible
- Missing S3 URLs in alternate assets
- S3 permission errors
- Network connectivity issues

The script returns:
- Exit code `0` - Success (all objects processed successfully)
- Exit code `1` - Failure (one or more objects failed to process)

## Logging

The script provides detailed logging at different levels:
- `INFO` - High-level progress and summary
- `DEBUG` - Detailed object-level operations
- `ERROR` - Errors and failures

Set the `LOG_LEVEL` environment variable to control verbosity:
```bash
LOG_LEVEL=DEBUG uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a-staging/items/$ITEM_ID \
    --storage-class STANDARD_IA
```

## Examples

### Setup

First, define the STAC item ID:

```bash
ITEM_ID="S2A_MSIL2A_20250831T103701_N0511_R008_T31TFL_20250831T145420"
```

### Check current storage class distribution

Use dry-run to see the current storage classes without making changes:

```bash
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a-staging/items/$ITEM_ID \
    --storage-class STANDARD_IA \
    --dry-run
```

Output example:
```
Processing 1500 objects...
  300 already have target storage class STANDARD_IA
  1200 need to be changed

============================================================
Summary for S2A_MSIL2A_20250831T103701_N0511_R008_T31TFL_20250831T145420:
  Total objects: 1500
  Skipped (filtered): 0
  Already correct storage class: 300
  Changed: 1200
  Succeeded: 1500
  Failed: 0

Initial storage class distribution (before changes):
  STANDARD_IA: 300 objects (20.0%)
  STANDARD: 1200 objects (80.0%)

Expected storage class distribution (after changes):
  STANDARD_IA: 1500 objects (100.0%)
```

**Note**: The script shows the expected distribution after successful changes. To verify changes were applied, run the same command again - you should see all objects already at the target storage class.

### Preview changes for specific data subset

Test what would happen when archiving only 60m resolution data:

```bash
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a-staging/items/$ITEM_ID \
    --storage-class STANDARD_IA \
    --include-pattern "*/r60m/*" \
    --dry-run
```

### Check storage distribution for reflectance data only

```bash
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a-staging/items/$ITEM_ID \
    --storage-class STANDARD_IA \
    --include-pattern "measurements/reflectance/*" \
    --dry-run
```

### Preview excluding metadata files

```bash
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a-staging/items/$ITEM_ID \
    --storage-class STANDARD_IA \
    --exclude-pattern "*.zattrs" \
    --exclude-pattern "*.zmetadata" \
    --dry-run
```

### Archive only reflectance data to STANDARD_IA

```bash
# First, preview the changes
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a-staging/items/$ITEM_ID \
    --storage-class STANDARD_IA \
    --include-pattern "measurements/reflectance/*" \
    --dry-run

# Then apply the changes
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a-staging/items/$ITEM_ID \
    --storage-class STANDARD_IA \
    --include-pattern "measurements/reflectance/*"
```

### Archive all measurement data except 10m resolution

```bash
# Preview first
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a-staging/items/$ITEM_ID \
    --storage-class STANDARD_IA \
    --include-pattern "measurements/*" \
    --exclude-pattern "*/r10m/*" \
    --dry-run

# Apply changes
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a-staging/items/$ITEM_ID \
    --storage-class STANDARD_IA \
    --include-pattern "measurements/*" \
    --exclude-pattern "*/r10m/*"
```

### Archive old data to STANDARD_IA

```bash
# Preview the changes
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a-staging/items/$ITEM_ID \
    --storage-class STANDARD_IA \
    --dry-run

# Apply the changes
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a-staging/items/$ITEM_ID \
    --storage-class STANDARD_IA
```

### Restore data from STANDARD_IA to STANDARD

```bash
# Preview the changes
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a-staging/items/$ITEM_ID \
    --storage-class STANDARD \
    --dry-run

# Apply the changes
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a-staging/items/$ITEM_ID \
    --storage-class STANDARD
```

### Use high-performance storage

```bash
# Preview the changes
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a-staging/items/$ITEM_ID \
    --storage-class EXPRESS_ONEZONE \
    --dry-run

# Apply the changes
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a-staging/items/$ITEM_ID \
    --storage-class EXPRESS_ONEZONE
```
