# Update STAC Storage Tier Metadata

Updates existing STAC items with current S3 storage tier metadata.

## Modes

**Update (default)**: Updates `storage:scheme.tier` for assets with existing `alternate.s3`

**Add Missing (`--add-missing`)**: Creates `alternate.s3` structure for legacy items without it

## Storage Tier Detection

- **Single file**: Returns tier directly (e.g., `"STANDARD_IA"`)
- **Uniform Zarr**: All files same tier (e.g., `"STANDARD_IA"` + distribution)
- **Mixed Zarr**: Different tiers detected (tier: `"MIXED"` + distribution breakdown)

Distribution shows file counts per tier, based on sample of up to 100 files.

### Example: Mixed Storage
```json
{
  "storage:scheme": {
    "platform": "OVHcloud",
    "region": "de",
    "requester_pays": false,
    "tier": "MIXED",
    "tier_distribution": {
      "STANDARD": 450,
      "STANDARD_IA": 608
    }
  }
}
```

## Notes

- Thumbnail assets automatically skipped
- Failed S3 queries remove existing `storage:scheme.tier` field
- Distribution metadata only for Zarr directories (stored in `storage:scheme.tier_distribution`)

## Setup

```bash
# Install dependencies
uv sync

# Set environment variables
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export S3_ENDPOINT="https://s3.de.io.cloud.ovh.net"
export STAC_API_URL="https://api.explorer.eopf.copernicus.eu/stac"
export ITEM_URL="${STAC_API_URL}/collections/sentinel-2-l2a/items/ITEM_ID"
```

## Usage

```bash
# Dry run (preview changes)
uv run python scripts/update_stac_storage_tier.py \
  --stac-item-url "$ITEM_URL" \
  --stac-api-url "$STAC_API_URL" \
  --s3-endpoint "$S3_ENDPOINT" \
  --dry-run

# Update existing alternate.s3
uv run python scripts/update_stac_storage_tier.py \
  --stac-item-url "$ITEM_URL" \
  --stac-api-url "$STAC_API_URL" \
  --s3-endpoint "$S3_ENDPOINT"

# Add missing alternate.s3 (legacy items)
uv run python scripts/update_stac_storage_tier.py \
  --stac-item-url "$ITEM_URL" \
  --stac-api-url "$STAC_API_URL" \
  --s3-endpoint "$S3_ENDPOINT" \
  --add-missing
```

## Output Examples

**Success:**
```
Processing: S2A_MSIL2A_20251008T100041_N0511_R122_T32TQM_20251008T122613
  Assets with alternate.s3: 15
  Assets with queryable storage tier: 15
  Assets updated: 15
  ✅ Updated item (HTTP 201)
```

**Mixed storage detected:**
```
Processing: S2A_MSIL2A_20251208T100431_N0511_R122_T32TQQ_20251208T121910
  reflectance: Mixed storage detected - {'STANDARD': 450, 'STANDARD_IA': 608}
  Assets updated: 1
  ✅ Updated item (HTTP 201)
```

**S3 query failures:**
```
  ⚠️  Failed to query storage tier from S3 for 4 asset(s)
      Check AWS credentials, S3 permissions, or if objects are Zarr directories
```

## Related Scripts

- `register_v1.py` - Initial STAC registration (includes storage tier)
- `change_storage_tier.py` - Change S3 storage classes
- `storage_tier_utils.py` - Shared utilities for storage tier operations
