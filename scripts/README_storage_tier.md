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
ITEM_ID="S2B_MSIL2A_20250730T113319_N0511_R080_T29UQP_20250730T135754"
```

## Usage

### Basic Usage

Run the script using the STAC item ID variable defined in the setup:

```bash
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a/items/$ITEM_ID \
    --storage-class GLACIER
```

### Dry Run

Test the script without making actual changes. Dry-run mode will:
- Query and display the current storage class of each object
- Show what changes would be made
- Display storage class distribution statistics
- Not modify any objects

```bash
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a/items/$ITEM_ID \
    --storage-class GLACIER \
    --dry-run
```

### With Custom S3 Endpoint

```bash
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a/items/$ITEM_ID \
    --storage-class GLACIER \
    --s3-endpoint https://s3.de.io.cloud.ovh.net
```

### Filter Specific Subpaths

Only change storage class for specific parts of the Zarr store:

```bash
# Only process reflectance data
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a/items/$ITEM_ID \
    --storage-class GLACIER \
    --include-pattern "measurements/reflectance/*"

# Process multiple subdirectories
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a/items/$ITEM_ID \
    --storage-class GLACIER \
    --include-pattern "measurements/*" \
    --include-pattern "quality/*"

# Exclude metadata files
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a/items/$ITEM_ID \
    --storage-class GLACIER \
    --exclude-pattern "*.zattrs" \
    --exclude-pattern "*.zmetadata"

# Only process 60m resolution data
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a/items/$ITEM_ID \
    --storage-class GLACIER \
    --include-pattern "*/r60m/*"
```

## Available Storage Classes

- **STANDARD** - Standard storage tier (default, immediate access, higher cost)
- **GLACIER** - Archive storage tier (lower cost, retrieval required before access)
- **EXPRESS_ONEZONE** - High-performance storage tier (single availability zone)

## How It Works

1. Fetches the STAC item from the provided URL
2. Extracts S3 URLs from the `alternate.s3.href` fields in each asset
3. Identifies the root Zarr store location
4. Lists all objects in the Zarr store recursively
5. Optionally filters objects based on include/exclude patterns
6. Changes the storage class for each object using the S3 API

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
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a/items/$ITEM_ID \
    --storage-class GLACIER
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
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a/items/$ITEM_ID \
    --storage-class GLACIER
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
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a/items/$ITEM_ID \
    --storage-class GLACIER \
    --dry-run
```

Output example:
```
Summary for S2A_MSIL2A_20250831T103701_N0511_R008_T31TFL_20250831T145420:
  Total objects: 1500
  Skipped (filtered): 0
  Processed: 1500
  Succeeded: 1500
  Failed: 0

Current storage class distribution:
  GLACIER: 300 objects (20.0%)
  STANDARD: 1200 objects (80.0%)
  (DRY RUN)
```

### Preview changes for specific data subset

Test what would happen when archiving only 60m resolution data:

```bash
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a/items/$ITEM_ID \
    --storage-class GLACIER \
    --include-pattern "*/r60m/*" \
    --dry-run
```

### Check storage distribution for reflectance data only

```bash
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a/items/$ITEM_ID \
    --storage-class GLACIER \
    --include-pattern "measurements/reflectance/*" \
    --dry-run
```

### Preview excluding metadata files

```bash
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a/items/$ITEM_ID \
    --storage-class GLACIER \
    --exclude-pattern "*.zattrs" \
    --exclude-pattern "*.zmetadata" \
    --dry-run
```

### Archive only reflectance data to GLACIER

```bash
# First, preview the changes
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a/items/$ITEM_ID \
    --storage-class GLACIER \
    --include-pattern "measurements/reflectance/*" \
    --dry-run

# Then apply the changes
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a/items/$ITEM_ID \
    --storage-class GLACIER \
    --include-pattern "measurements/reflectance/*"
```

### Archive all measurement data except 10m resolution

```bash
# Preview first
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a/items/$ITEM_ID \
    --storage-class GLACIER \
    --include-pattern "measurements/*" \
    --exclude-pattern "*/r10m/*" \
    --dry-run

# Apply changes
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a/items/$ITEM_ID \
    --storage-class GLACIER \
    --include-pattern "measurements/*" \
    --exclude-pattern "*/r10m/*"
```

### Archive old data to GLACIER

```bash
# Preview the changes
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a/items/$ITEM_ID \
    --storage-class GLACIER \
    --dry-run

# Apply the changes
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a/items/$ITEM_ID \
    --storage-class GLACIER
```

### Restore data from GLACIER to STANDARD

```bash
# Preview the changes
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a/items/$ITEM_ID \
    --storage-class STANDARD \
    --dry-run

# Apply the changes
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a/items/$ITEM_ID \
    --storage-class STANDARD
```

### Use high-performance storage

```bash
# Preview the changes
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a/items/$ITEM_ID \
    --storage-class EXPRESS_ONEZONE \
    --dry-run

# Apply the changes
uv run python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a/items/$ITEM_ID \
    --storage-class EXPRESS_ONEZONE
```
