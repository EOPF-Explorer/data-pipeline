# Add Storage Tier Management Script

## Summary

Adds a new script to change S3 storage tiers for data items after STAC registration, enabling better control over storage costs by allowing data to be moved between storage classes (STANDARD, GLACIER, EXPRESS_ONEZONE).

## Changes

### New Files
- **`scripts/change_storage_tier.py`** - Main script to change storage class of S3 objects
- **`scripts/README_storage_tier.md`** - Comprehensive documentation with examples

### Key Features

- **Fetch STAC items** and extract S3 URLs from `alternate.s3.href` fields
- **Change storage class** for entire Zarr stores using S3 API
- **Path filtering** with include/exclude patterns for selective tier changes
- **Dry-run mode** for testing without making changes
- **Comprehensive logging** with progress tracking and statistics

### Path Filtering

Supports fnmatch patterns to selectively change storage tiers:

```bash
# Archive only reflectance data
--include-pattern "measurements/reflectance/*"

# Exclude metadata files
--exclude-pattern "*.zattrs" --exclude-pattern "*.zmetadata"

# Only process specific resolution
--include-pattern "*/r60m/*"
```

## Usage Example

```bash
# Archive data to GLACIER tier
python scripts/change_storage_tier.py \
    --stac-item-url https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a/items/ITEM_ID \
    --storage-class GLACIER \
    --include-pattern "measurements/reflectance/*"
```

## Workflow Integration

This script fits as an optional third step in the data pipeline:
1. **Convert** (convert_v1_s2.py) - Convert to GeoZarr
2. **Register** (register_v1.py) - Register in STAC
3. **Change Tier** (change_storage_tier.py) - Optimize storage costs ✨

## Testing

Tested with dry-run mode against Sentinel-2 STAC items to verify filtering and object selection logic.
