# EOPF GeoZarr Data Pipeline

Automated Kubernetes pipeline for converting Sentinel Zarr datasets to cloud-optimized GeoZarr format with STAC catalog integration.

## Quick Start

```bash
export KUBECONFIG=.work/kubeconfig
kubectl create -f workflows/run-s1-test.yaml -n devseed-staging
kubectl get wf -n devseed-staging -w
```

📖 **First time?** See [GETTING_STARTED.md](GETTING_STARTED.md) for full setup
🎯 **Monitor:** [Argo UI](https://argo-workflows.hub-eopf-explorer.eox.at)

## What It Does

**Input:** STAC item URL → **Output:** Cloud-optimized GeoZarr + Interactive map (~15-20 min)

**Supports:** Sentinel-1 GRD, Sentinel-2 L2A
**Stack:** Argo Workflows • [eopf-geozarr](https://github.com/EOPF-Explorer/data-model) • Dask • RabbitMQ • Prometheus
**Resources:** 6Gi memory, burstable CPU per workflow

## Monitoring

```bash
# Health check
kubectl get wf -n devseed-staging --field-selector status.phase=Running

# Recent workflows (last hour)
kubectl get wf -n devseed-staging --sort-by=.metadata.creationTimestamp | tail -10
```

**Web UI:** [Argo Workflows](https://argo-workflows.hub-eopf-explorer.eox.at)

## Usage

### kubectl (Testing)
```bash
kubectl create -f workflows/run-s1-test.yaml -n devseed-staging
```

**Namespaces:** `devseed-staging` (testing) • `devseed` (production)

### Event-driven (Production)
Publish to RabbitMQ `geozarr` exchange:
```json
{"source_url": "https://stac.../items/...", "item_id": "...", "collection": "..."}
```

### Jupyter Notebooks
```bash
uv sync --extra notebooks
cp notebooks/.env.example notebooks/.env
uv run jupyter lab notebooks/
```

See [examples/](examples/) for more patterns.

## Configuration

```bash
# S3 credentials (OVH S3)
kubectl create secret generic geozarr-s3-credentials -n devseed \
  --from-literal=AWS_ACCESS_KEY_ID="..." \
  --from-literal=AWS_SECRET_ACCESS_KEY="..." \
  --from-literal=AWS_ENDPOINT_URL="https://s3.de.io.cloud.ovh.net"

# S3 output location
# Bucket: esa-zarr-sentinel-explorer-fra
# Prefix: tests-output (staging) or geozarr (production)

# Get RabbitMQ password
kubectl get secret rabbitmq-password -n core -o jsonpath='{.data.rabbitmq-password}' | base64 -d

# STAC API endpoints
# STAC API: https://api.explorer.eopf.copernicus.eu/stac
# Raster API: https://api.explorer.eopf.copernicus.eu/raster
```

## Troubleshooting

```bash
# Check workflow status
kubectl get wf -n devseed-staging --sort-by=.metadata.creationTimestamp | tail -5

# View logs
kubectl logs -n devseed-staging <pod-name> -c main -f

# Check resources
kubectl top nodes
```

**Common issues:**
- **Workflow not starting:** Check sensor logs: `kubectl logs -n devseed -l sensor-name=geozarr-sensor`
- **S3 errors:** Verify credentials secret exists
- **Pod pending:** Check node capacity with `kubectl top nodes`

**Performance:** S1 GRD (10GB): 15-20 min • S2 L2A (5GB): 8-12 min • Increase if >20GB dataset

See [GETTING_STARTED.md](GETTING_STARTED.md#troubleshooting) for more.

## Project Structure

```
workflows/          Argo WorkflowTemplates (YAML manifests)
scripts/            Production pipeline scripts (7 files, 904 lines)
  ├── utils.py                 Extract item IDs & Zarr asset URLs from STAC items (unified CLI)
  ├── get_conversion_params.py Sentinel-1/2 collection-specific settings (groups, chunks, tile sizes)
  ├── validate_geozarr.py      Validate Zarr structure, OGC TMS, CF conventions, spatial references
  ├── create_geozarr_item.py   Build STAC item from converted GeoZarr, copying source metadata
  ├── register_stac.py         Register/update items in STAC API via Transaction extension (upsert mode)
  ├── augment_stac_item.py     Add TiTiler viewer/xyz/tilejson links & projection metadata via pystac
  └── metrics.py               Expose Prometheus metrics (registration counts, preview timings)
tools/              Development & benchmarking (not in production)
  ├── benchmarking/  Performance testing (benchmark_geozarr.py, benchmark_tile_performance.py)
  └── testing/       Test utilities (publish_amqp.py for workflow trigger testing)
tests/              Pytest suite (93 tests, 85% coverage on scripts/)
notebooks/          Jupyter tutorials & examples (operator.ipynb, performance analysis)
```

## Development

```bash
# Setup
uv sync --all-extras
pre-commit install

# Test
pytest tests/ -v --cov=scripts

# Deploy
kubectl apply -f workflows/template.yaml -n devseed
```

**Documentation:** [CONTRIBUTING.md](CONTRIBUTING.md) • [GETTING_STARTED.md](GETTING_STARTED.md)

## License

Apache 2.0 - See [LICENSE](LICENSE) for details.
