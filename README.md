# EOPF GeoZarr Data Pipeline

Automated pipeline for converting Sentinel-2 Zarr datasets to cloud-optimized GeoZarr format with STAC catalog integration and interactive visualization.

## Quick Reference

```bash
# 1. Submit a workflow (simplest method)
uv run python examples/submit.py --stac-url "https://stac.core.eopf.eodc.eu/collections/sentinel-2-l2a/items/S2B_..."

# 2. Monitor progress
kubectl get wf -n devseed -w

# 3. View result
# Check logs for viewer URL: https://api.explorer.eopf.copernicus.eu/raster/viewer?url=...
```

💡 **Local testing:** Port-forward RabbitMQ first: `kubectl port-forward -n core svc/rabbitmq 5672:5672 &`

## Features

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Tests](https://github.com/EOPF-Explorer/data-pipeline/workflows/Tests/badge.svg)](https://github.com/EOPF-Explorer/data-pipeline/actions)

- **Multi-sensor support**: Sentinel-1 GRD and Sentinel-2 L2A
- STAC item registration with retry logic
- GeoZarr format conversion with cloud-optimized overviews
- Cloud-native workflows with Argo
- Interactive visualization with TiTiler

## What It Does

Transforms Sentinel satellite data into web-ready visualizations:

**Input:** STAC item URL → **Output:** Interactive web map (~5-10 min)

**Pipeline:** Convert (5 min) → Register (30 sec) → Augment (10 sec)

**Supported sensors:**
- **Sentinel-1** L1 GRD: SAR backscatter (VH/VV polarizations)
- **Sentinel-2** L2A: Multispectral reflectance (10m/20m/60m)

## Quick Start

📖 **New to the project?** See [GETTING_STARTED.md](GETTING_STARTED.md) for complete setup (15 min).

### Requirements

- **Kubernetes cluster** with [platform-deploy](https://github.com/EOPF-Explorer/platform-deploy) infrastructure
  - Argo Workflows (pipeline orchestration)
  - RabbitMQ (event-driven automation)
  - STAC API & TiTiler (catalog & visualization)
- **Python 3.11+** with `uv` package manager
- **S3 storage** credentials (outputs)
- **Kubeconfig** in `.work/kubeconfig`

Verify:
```bash
export KUBECONFIG=$(pwd)/.work/kubeconfig
kubectl get pods -n core -l app.kubernetes.io/name=argo-workflows
kubectl get pods -n core -l app.kubernetes.io/name=rabbitmq
```

### Run Your First Job

```bash
# 1. Install dependencies
uv sync --all-extras

# 2. Deploy workflows
kubectl apply -f workflows/ -n devseed

# 3. Port-forward RabbitMQ
kubectl port-forward -n core svc/rabbitmq 5672:5672 &

# 4. Submit a STAC item
export AMQP_PASSWORD=$(kubectl get secret rabbitmq-password -n core -o jsonpath='{.data.rabbitmq-password}' | base64 -d)
export AMQP_URL="amqp://user:${AMQP_PASSWORD}@localhost:5672/"

uv run python examples/submit.py \
  --stac-url "https://stac.core.eopf.eodc.eu/collections/sentinel-2-l2a/items/S2B_MSIL2A_20250518_T29RLL"

# 5. Monitor
kubectl get wf -n devseed -w
```

**Result:** Interactive map at `https://api.explorer.eopf.copernicus.eu/raster/viewer?url=...`

## How It Works

### Pipeline Stages

| Stage | Time | Function |
|-------|------|----------|
| **Convert** | 5 min | Zarr → GeoZarr with spatial indexing & cloud optimization |
| **Register** | 30 sec | Create/update STAC item with metadata & assets |
| **Augment** | 10 sec | Add visualization links (XYZ tiles, TileJSON, viewer) |

### Event-Driven Architecture

```
STAC URL → submit.py → RabbitMQ → AMQP Sensor → Argo Workflow
                                                      ↓
                                          Convert → Register → Augment
                                                      ↓
                                        STAC API + Interactive Map
```

**Automation:** New Sentinel-2 data publishes to RabbitMQ → Pipeline runs automatically

## Submitting Workflows

**Choose your approach:**

| Method | Best For | Documentation |
|--------|----------|---------------|
| 🎯 **CLI tool** | Quick testing, automation | [examples/README.md](examples/README.md) |
| 📓 **Jupyter notebook** | Learning, exploration | [notebooks/README.md](notebooks/README.md) |
| ⚡ **Event-driven** | Production (auto) | Already running! |
| 🔧 **Custom pika** | Custom integrations | [See Configuration](#configuration) |

**Quick example:**
```bash
uv run python examples/submit.py --stac-url "https://stac.core.eopf.eodc.eu/collections/sentinel-2-l2a/items/S2B_..."
```

**Monitor:**
```bash
kubectl get wf -n devseed -w  # Watch workflows
kubectl logs -n devseed -l sensor-name=geozarr-sensor --tail=50  # Sensor logs
```

### Related Projects

- **[data-model](https://github.com/EOPF-Explorer/data-model)** - `eopf-geozarr` conversion library (Python)
- **[platform-deploy](https://github.com/EOPF-Explorer/platform-deploy)** - K8s infrastructure (Flux, Argo, RabbitMQ, STAC, TiTiler)

## Configuration

### S3 Storage

```bash
kubectl create secret generic geozarr-s3-credentials -n devseed \
  --from-literal=AWS_ACCESS_KEY_ID="<your-key>" \
  --from-literal=AWS_SECRET_ACCESS_KEY="<your-secret>"
```

| Setting | Value |
|---------|-------|
| **Endpoint** | `https://s3.de.io.cloud.ovh.net` |
| **Bucket** | `esa-zarr-sentinel-explorer-fra` |
| **Region** | `de` |

### RabbitMQ

Get password:
```bash
kubectl get secret rabbitmq-password -n core -o jsonpath='{.data.rabbitmq-password}' | base64 -d
```

| Setting | Value |
|---------|-------|
| **URL** | `amqp://user:PASSWORD@rabbitmq.core.svc.cluster.local:5672/` |
| **Exchange** | `geozarr` |
| **Routing key** | `eopf.items.*` |

**Message format:**
```json
{
  "source_url": "https://stac.core.eopf.eodc.eu/collections/sentinel-2-l2a/items/...",
  "item_id": "S2B_MSIL2A_...",
  "collection": "sentinel-2-l2a"
}
```

## Web Interfaces

Access via [**EOxHub workspace**](https://workspace.devseed.hub-eopf-explorer.eox.at/) (single sign-on for all services):

| Service | Purpose | URL |
|---------|---------|-----|
| **Argo Workflows** | Monitor pipelines | [argo-workflows.hub-eopf-explorer.eox.at](https://argo-workflows.hub-eopf-explorer.eox.at) |
| **STAC Browser** | Browse catalog | [api.explorer.eopf.copernicus.eu/stac](https://api.explorer.eopf.copernicus.eu/stac) |
| **TiTiler Viewer** | View maps | [api.explorer.eopf.copernicus.eu/raster](https://api.explorer.eopf.copernicus.eu/raster) |
| **JupyterLab** | Operator tools | Via EOxHub workspace |

💡 **Tip:** Login to EOxHub first for seamless authentication across all services.

## Monitoring & Troubleshooting

### Workflow Status

```bash
# List all workflows
kubectl get wf -n devseed

# Watch real-time updates
kubectl get wf -n devseed -w

# Detailed status
kubectl describe wf <workflow-name> -n devseed
```

### Logs

```bash
# Workflow pod logs
kubectl logs <pod-name> -n devseed

# Sensor (message processing)
kubectl logs -n devseed -l sensor-name=geozarr-sensor --tail=50

# EventSource (RabbitMQ connection)
kubectl logs -n devseed -l eventsource-name=rabbitmq-geozarr --tail=50
```

### Common Issues

| Problem | Solution |
|---------|----------|
| **Workflow not starting** | Check sensor/eventsource logs for connection errors |
| **S3 access denied** | Verify secret `geozarr-s3-credentials` exists in `devseed` namespace |
| **RabbitMQ connection refused** | Port-forward required: `kubectl port-forward -n core svc/rabbitmq 5672:5672` |
| **Pod stuck in Pending** | Check node resources and pod limits |

## Development

### Setup

```bash
uv sync --all-extras
pre-commit install  # Optional: enable git hooks
```

### Testing

```bash
make test          # Run full test suite
make check         # Lint + typecheck + test
pytest tests/      # Run specific tests
pytest -v -k e2e   # End-to-end tests only
```

### Project Structure

```
├── docker/              # Container images
│   ├── Dockerfile           # Pipeline runtime
│   └── Dockerfile.test      # Test environment
├── scripts/             # Python pipeline scripts
│   ├── register_stac.py     # STAC catalog registration
│   ├── augment_stac_item.py # Add visualization links
│   └── get_zarr_url.py      # Extract Zarr URL from STAC
├── workflows/           # Argo workflow definitions
│   ├── template.yaml        # Main pipeline WorkflowTemplate
│   ├── eventsource.yaml     # RabbitMQ AMQP event source
│   ├── sensor.yaml          # Workflow trigger on messages
│   └── rbac.yaml            # Service account permissions
├── examples/            # Usage examples
│   └── submit.py            # Submit job via RabbitMQ
├── tests/               # Unit & integration tests
└── notebooks/           # Operator utilities
```

### Making Changes

1. **Edit workflow:** `workflows/template.yaml`
2. **Update scripts:** `scripts/*.py`
3. **Test locally:** `pytest tests/ -v`
4. **Build image:** `docker buildx build --platform linux/amd64 -t ghcr.io/eopf-explorer/data-pipeline:dev -f docker/Dockerfile . --push`
5. **Deploy:** `kubectl apply -f workflows/template.yaml -n devseed`
6. **Monitor:** `kubectl get wf -n devseed -w`

⚠️ **Important:** Always use `--platform linux/amd64` when building images for Kubernetes clusters.

See [CONTRIBUTING.md](CONTRIBUTING.md) for coding standards and development workflow.

## License

Apache 2.0 - See [LICENSE](LICENSE) for details.
