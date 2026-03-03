# Pre-compute STAC Item Aggregations

## Overview

The `aggregate_items.py` script pre-computes daily and monthly item counts for a STAC collection and stores them as static JSON files on S3. This enables the EOPF Explorer timeline UI to quickly show when satellite data is available, without querying potentially hundreds of thousands of items at runtime.

The script also adds discoverable `rel: "pre-aggregation"` links to the STAC collection, so the Explorer UI can find the aggregation files automatically.

**Why pre-compute?** pgSTAC doesn't support the [STAC Aggregation Extension](https://github.com/stac-api-extensions/aggregation) ([pgstac#257](https://github.com/stac-utils/pgstac/issues/257)), and collections like `sentinel-2-l2a` have >500 items/day, making client-side counting unsustainable.

## Requirements

The script requires the following Python packages:
- `pystac-client` - STAC API client (item querying)
- `boto3` - AWS SDK for Python (S3 upload)
- `httpx` - HTTP client (collection update via Transaction API)
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
uv run python scripts/aggregate_items.py \
    --s3-endpoint https://s3.de.io.cloud.ovh.net \
    ...
```

## Usage

### CLI Arguments

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--collection` | Yes | — | Collection ID (e.g. `sentinel-2-l2a`) |
| `--stac-api-url` | Yes | — | STAC API base URL |
| `--s3-bucket` | Yes | — | S3 bucket for output files |
| `--s3-prefix` | No | `aggregations` | S3 key prefix |
| `--s3-endpoint` | No | `AWS_ENDPOINT_URL` env var | S3 endpoint URL |
| `--s3-gateway-url` | No | `https://s3.explorer.eopf.copernicus.eu` | Public HTTPS gateway for S3 |
| `--dry-run` | No | `false` | Generate JSON to stdout, skip upload and collection update |

### Dry Run

Preview the aggregation output without uploading to S3 or modifying the collection:

```bash
uv run python scripts/aggregate_items.py \
    --collection sentinel-2-l2a \
    --stac-api-url https://api.explorer.eopf.copernicus.eu/stac \
    --s3-bucket esa-zarr-sentinel-explorer-fra \
    --dry-run
```

Output example:
```
2026-03-03 10:00:01 - __main__ - INFO - Querying items for collection: sentinel-2-l2a
2026-03-03 10:00:03 - __main__ - INFO - Processed 1000 items so far...
2026-03-03 10:00:05 - __main__ - INFO - Processed 2000 items so far...
...
2026-03-03 10:01:15 - __main__ - INFO - Total items counted: 45230
2026-03-03 10:01:15 - __main__ - INFO - Daily buckets: 312
2026-03-03 10:01:15 - __main__ - INFO - Monthly buckets: 14
2026-03-03 10:01:15 - __main__ - INFO - Dry run - printing daily aggregation to stdout
{
  "type": "AggregationCollection",
  "aggregations": [
    {
      "key": "datetime_daily",
      "buckets": [
        {"key": "2024-11-15T00:00:00.000Z", "value": 127},
        {"key": "2024-11-16T00:00:00.000Z", "value": 543},
        ...
      ],
      "interval": "daily"
    }
  ]
}
```

### Full Run (Upload to S3 + Update Collection)

```bash
uv run python scripts/aggregate_items.py \
    --collection sentinel-2-l2a \
    --stac-api-url https://api.explorer.eopf.copernicus.eu/stac \
    --s3-bucket esa-zarr-sentinel-explorer-fra \
    --s3-prefix aggregations \
    --s3-endpoint https://s3.de.io.cloud.ovh.net
```

Output example:
```
2026-03-03 10:00:01 - __main__ - INFO - Querying items for collection: sentinel-2-l2a
...
2026-03-03 10:01:15 - __main__ - INFO - Total items counted: 45230
2026-03-03 10:01:15 - __main__ - INFO - Daily buckets: 312
2026-03-03 10:01:15 - __main__ - INFO - Monthly buckets: 14
2026-03-03 10:01:16 - __main__ - INFO - Uploaded s3://esa-zarr-sentinel-explorer-fra/aggregations/sentinel-2-l2a/daily.json (28.4 KB)
2026-03-03 10:01:16 - __main__ - INFO - Uploaded s3://esa-zarr-sentinel-explorer-fra/aggregations/sentinel-2-l2a/monthly.json (1.2 KB)
2026-03-03 10:01:17 - __main__ - INFO - Updated collection sentinel-2-l2a with pre-aggregation links
2026-03-03 10:01:17 - __main__ - INFO - Aggregation complete
```

## How It Works

1. **Query all items** using `pystac_client.Client.search()` with the `fields` extension (only fetches `datetime`), paginating with a page size of 1000 for efficiency
2. **Count by day** using `collections.Counter` keyed by `YYYY-MM-DD`
3. **Build daily JSON** — sorted chronologically, formatted as `AggregationCollection`
4. **Build monthly JSON** — derived from daily counts by summing per `YYYY-MM`
5. **Upload to S3** — `s3://{bucket}/{prefix}/{collection}/daily.json` and `monthly.json`
6. **Update collection links** — fetches collection via `GET`, removes existing `pre-aggregation` links, adds new ones, `PUT`s back

The script is **idempotent**: running it twice produces the same result (overwrites S3 files, replaces links).

## JSON Output Format

Both daily and monthly files follow the [STAC Aggregation Extension](https://github.com/stac-api-extensions/aggregation) bucket format:

```json
{
  "type": "AggregationCollection",
  "aggregations": [
    {
      "key": "datetime_daily",
      "buckets": [
        {"key": "2026-01-01T00:00:00.000Z", "value": 523},
        {"key": "2026-01-02T00:00:00.000Z", "value": 487}
      ],
      "interval": "daily"
    }
  ]
}
```

- `key`: ISO 8601 timestamp for the bucket start
- `value`: integer count of items in that bucket

## Collection Links

After a full run, the collection will have two `rel: "pre-aggregation"` links:

```json
{
  "rel": "pre-aggregation",
  "href": "https://s3.explorer.eopf.copernicus.eu/{bucket}/aggregations/{collection}/daily.json",
  "type": "application/json",
  "title": "Daily Item Aggregation",
  "aggregation:interval": "daily"
}
```

```json
{
  "rel": "pre-aggregation",
  "href": "https://s3.explorer.eopf.copernicus.eu/{bucket}/aggregations/{collection}/monthly.json",
  "type": "application/json",
  "title": "Monthly Item Aggregation",
  "aggregation:interval": "monthly"
}
```

**Note:** We use `rel: "pre-aggregation"` (not `rel: "aggregate"`) to avoid conflicting with the official STAC Aggregation Extension, which uses `rel: "aggregate"` for dynamic API endpoints. Our static files are semantically different.

## Verification

After running the script, verify the results:

```bash
# Check the JSON files are accessible
curl -s https://s3.explorer.eopf.copernicus.eu/esa-zarr-sentinel-explorer-fra/aggregations/sentinel-2-l2a/daily.json | python -m json.tool | head -10

# Check the collection has pre-aggregation links
curl -s https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a | python -m json.tool | grep -A4 pre-aggregation
```

## Error Handling

The script handles various error conditions:
- Empty collections (returns exit code 0, no files uploaded)
- Items without a `datetime` field (skipped with a warning)
- S3 upload failures
- STAC API connectivity issues

The script returns:
- Exit code `0` - Success
- Exit code `1` - Failure

## Logging

The script provides detailed logging at different levels:
- `INFO` - Progress, item counts, upload confirmations
- `DEBUG` - Individual item processing details
- `WARNING` - Skipped items (missing datetime)
- `ERROR` - Failures

Set the `LOG_LEVEL` environment variable to control verbosity:

```bash
LOG_LEVEL=DEBUG uv run python scripts/aggregate_items.py \
    --collection sentinel-2-l2a \
    --stac-api-url https://api.explorer.eopf.copernicus.eu/stac \
    --s3-bucket esa-zarr-sentinel-explorer-fra \
    --dry-run
```

## Integration in Workflow

This script can be run periodically (e.g. daily via cron or CI) to keep aggregations up to date:

```bash
# Update aggregations for sentinel-2-l2a
uv run python scripts/aggregate_items.py \
    --collection sentinel-2-l2a \
    --stac-api-url https://api.explorer.eopf.copernicus.eu/stac \
    --s3-bucket esa-zarr-sentinel-explorer-fra \
    --s3-prefix aggregations \
    --s3-endpoint https://s3.de.io.cloud.ovh.net

# Update aggregations for sentinel-1-grd
uv run python scripts/aggregate_items.py \
    --collection sentinel-1-grd \
    --stac-api-url https://api.explorer.eopf.copernicus.eu/stac \
    --s3-bucket esa-zarr-sentinel-explorer-fra \
    --s3-prefix aggregations \
    --s3-endpoint https://s3.de.io.cloud.ovh.net
```
