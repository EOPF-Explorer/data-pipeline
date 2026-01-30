# Update STAC Storage Tier Metadata

Updates existing STAC items with current S3 storage metadata following the [storage extension v2.0](https://github.com/stac-extensions/storage) pattern: schemes at item properties, refs at asset alternate.

## Modes

**Update (default)**: Updates `storage:refs` and `objects_per_storage_class` for assets with existing `alternate.s3`, and ensures item `properties["storage:schemes"]` is set.

**Add Missing (`--add-missing`)**: Creates `alternate.s3` structure for legacy items without it (with `storage:refs` and optional `objects_per_storage_class`).

## Output structure

- **Item level** – `properties["storage:schemes"]`: defines schemes `standard`, `performance`, `glacier`, `mixed` (custom-s3, platform, bucket, region, storage_class).
- **Asset level** – each `alternate.s3` has:
  - `storage:refs`: array of scheme keys (e.g. `["performance"]`, `["mixed"]`) linking to `properties["storage:schemes"]`
  - `objects_per_storage_class`: optional object with object counts per storage class (e.g. `{"STANDARD": 4}`), for Zarr directories

## Storage tier detection

- **Single file**: One tier (e.g. `STANDARD_IA` → ref `glacier`)
- **Uniform Zarr**: All files same tier + `objects_per_storage_class` with counts
- **Mixed Zarr**: ref `mixed` + `objects_per_storage_class` breakdown

Distribution is computed with full pagination when updating (accurate for Zarr).

### Example: item properties and asset alternate
```json
"properties": {
  "storage:schemes": {
    "standard": { "type": "custom-s3", "platform": "https://s3.de.io.cloud.ovh.net/", "bucket": "esa-zarr-sentinel-explorer-fra", "region": "de", "storage_class": "STANDARD" },
    "performance": { "type": "custom-s3", "platform": "https://s3.de.io.cloud.ovh.net/", "bucket": "esa-zarr-sentinel-explorer-fra", "region": "de", "storage_class": "EXPRESS_ONEZONE" },
    "glacier": { "type": "custom-s3", "platform": "https://s3.de.io.cloud.ovh.net/", "bucket": "esa-zarr-sentinel-explorer-fra", "region": "de", "storage_class": "STANDARD_IA" },
    "mixed": { "type": "custom-s3", "platform": "https://s3.de.io.cloud.ovh.net/", "bucket": "esa-zarr-sentinel-explorer-fra", "region": "de", "storage_class": "MIXED" }
  }
},
"assets": {
  "reflectance": {
    "alternate": {
      "s3": {
        "href": "s3://bucket/data.zarr/...",
        "storage:refs": ["mixed"],
        "objects_per_storage_class": { "STANDARD": 450, "STANDARD_IA": 608 }
      }
    }
  }
}
```

## Notes

- Thumbnail assets are skipped
- Failed S3 queries set `storage:refs` to `["standard"]`

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
