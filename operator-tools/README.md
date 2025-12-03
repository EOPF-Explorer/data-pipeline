# Operator Tools

This directory contains tools for operators to submit STAC items to the data pipeline for processing.

## Overview

The data pipeline processes STAC items from the EOPF STAC catalog. These tools allow operators to:

- Submit individual test items for debugging/validation
- Search for STAC items by area and time range
- Batch submit multiple items for processing

## Setup

### Environments

The data pipeline operates in two Kubernetes namespaces:

- **`devseed-staging`** - Testing and validation environment
- **`devseed`** - Production data pipeline

Examples below use `devseed-staging`. For production, replace with `devseed`.

### Port Forwarding

Before using these tools, you need to set up port forwarding to access the webhook service:

```bash
# Port forward from the webhook eventsource service (staging)
kubectl port-forward -n devseed-staging svc/eopf-explorer-webhook-eventsource-svc 12000:12000 &

# For production, use:
# kubectl port-forward -n devseed svc/eopf-explorer-webhook-eventsource-svc 12000:12000 &
```

This makes the webhook endpoint available at `http://localhost:12000/samples`.

## Available Tools

### 1. `manage_collections.py` - Collection Management Tool

**NEW**: Comprehensive tool for managing STAC collections using the Transaction API.

**Use cases:**
- Clean collections (remove all items)
- Create/update collections from templates
- Batch operations on multiple collections
- View collection information and statistics

**Prerequisites:**

- STAC API access to `https://api.explorer.eopf.copernicus.eu/stac`
- Write permissions for collection management operations

**Quick Start:**

```bash
# See all available commands
uv run operator-tools/manage_collections.py --help

# View collection info
uv run operator-tools/manage_collections.py info sentinel-2-l2a-staging

# Clean a collection (dry run first!)
uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging --dry-run
uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging

# Create/update collection from template
uv run operator-tools/manage_collections.py create stac/sentinel-2-l2a.json
uv run operator-tools/manage_collections.py create stac/sentinel-2-l2a.json --update

# Batch create collections
uv run operator-tools/manage_collections.py batch-create stac/
```

**Documentation:** See [README_collections.md](./README_collections.md) for detailed usage and examples.

### 2. `submit_test_workflow_wh.py` - HTTP Webhook Submission

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

### 3. `submit_stac_items_notebook.ipynb` - Interactive STAC Search & Submit

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

## Best Practices

1. **Test with single items first** - Use `submit_test_workflow_wh.py` before bulk submissions
2. **Monitor processing** - Check pipeline logs/dashboards after submitting
3. **Use appropriate collections** - Use test/staging collections for validation
4. **Validate STAC URLs** - Ensure source URLs are accessible before submitting
5. **Check webhook service** - Ensure the webhook service is running before submitting items

## Support

For issues or questions about the data pipeline, contact the pipeline operations team.
