# EOPF GeoZarr Data Pipeline

**Kubernetes pipeline: Sentinel Zarr → Cloud-Optimized GeoZarr + STAC Registration**

Automated pipeline for converting Sentinel-1/2 Zarr datasets to cloud-optimized GeoZarr format with STAC catalog integration and interactive visualization.

---

## Quick Reference

```bash
# 1. Submit workflow (Sentinel-2 example)
kubectl create -n devseed-staging -f - <<'EOF'
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: geozarr-
spec:
  workflowTemplateRef:
    name: geozarr-pipeline
  arguments:
    parameters:
    - name: source_url
      value: "https://stac.core.eopf.eodc.eu/collections/sentinel-2-l2a/items/S2A_MSIL2A_20251022T094121_N0511_R036_T34TDT_20251022T114817"
    - name: register_collection
      value: "sentinel-2-l2a-dp-test"
EOF

# Or Sentinel-1:
# source_url: "https://stac.core.eopf.eodc.eu/collections/sentinel-1-l1-grd/items/S1A_IW_GRDH_1SDV_..."
# register_collection: "sentinel-1-l1-grd-dp-test"

# 2. Monitor progress
kubectl get wf -n devseed-staging --watch

# 3. View result in browser
# Check Argo UI: https://argo.core.eopf.eodc.eu/workflows/devseed-staging
# STAC Browser: https://api.explorer.eopf.copernicus.eu/stac
# TiTiler Viewer: https://api.explorer.eopf.copernicus.eu/raster
```

💡 **RabbitMQ submission:** Port-forward first: `kubectl port-forward -n devseed-staging svc/rabbitmq 5672:5672 &`

---

## What It Does

Transforms Sentinel-1/2 satellite data into web-ready visualizations:

**Input:** STAC item URL → **Output:** Interactive web map (~15-20 min)

**Pipeline:** Convert → Register

**Supported Missions:**
- Sentinel-2 L2A (Multi-spectral optical)
- Sentinel-1 GRD (SAR backscatter)


## Setup

**Prerequisites:**
- Kubernetes cluster with [platform-deploy](https://github.com/EOPF-Explorer/platform-deploy) (Argo Workflows, RabbitMQ, STAC API, TiTiler)
- Python 3.13+ with `uv`
- `kubectl` configured

**📖 Complete setup guide:** See [workflows/README.md](workflows/README.md) for:
- kubectl configuration (OVH Manager kubeconfig download)
- Required secrets (RabbitMQ, S3, STAC API)
- Workflow deployment (`kubectl apply -k`)

**Quick verification:**
```bash
kubectl get wf,sensor,eventsource -n devseed-staging
```

---

## Submit Workflow

### Method 1: kubectl (Testing - Bypasses Event System)

Direct workflow submission:

```bash
kubectl create -n devseed-staging -f - <<'EOF'
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: geozarr-
spec:
  workflowTemplateRef:
    name: geozarr-pipeline
  arguments:
    parameters:
    - name: source_url
      value: "https://stac.core.eopf.eodc.eu/collections/sentinel-2-l2a/items/S2A_MSIL2A_20251022T094121_N0511_R036_T34TDT_20251022T114817"
    - name: register_collection
      value: "sentinel-2-l2a-dp-test"
EOF

kubectl get wf -n devseed-staging --watch
```

**Monitor:** [Argo UI](https://argo.core.eopf.eodc.eu/workflows/devseed-staging)

### Method 2: RabbitMQ (Production - Event-Driven)

Triggers via EventSource → Sensor:

```bash
# Port-forward RabbitMQ
kubectl port-forward -n devseed-staging svc/rabbitmq 5672:5672 &

# Get password
export RABBITMQ_PASSWORD=$(kubectl get secret rabbitmq-password -n core -o jsonpath='{.data.rabbitmq-password}' | base64 -d)

# Submit workflow
python submit_test_workflow.py
```

---

## Web Interfaces

Access via **EOxHub workspace** (single sign-on): [workspace.devseed.hub-eopf-explorer.eox.at](https://workspace.devseed.hub-eopf-explorer.eox.at/)

| Service | Purpose | URL |
|---------|---------|-----|
| **Argo Workflows** | Monitor pipelines | [argo.core.eopf.eodc.eu](https://argo.core.eopf.eodc.eu/workflows/devseed-staging) |
| **STAC Browser** | Browse catalog | [api.explorer.eopf.copernicus.eu/stac](https://api.explorer.eopf.copernicus.eu/stac) |
| **TiTiler Viewer** | View maps | [api.explorer.eopf.copernicus.eu/raster](https://api.explorer.eopf.copernicus.eu/raster) |

💡 **Tip:** Login to EOxHub first for seamless authentication across all services.



---

## Pipeline

```
STAC item URL → Extract zarr → Convert (Dask) → S3 → Register STAC + TiTiler → Done (~15-20 min)
```

**Steps:**
1. **Convert** - Fetch STAC item, extract zarr URL, convert to GeoZarr, upload to S3
2. **Register** - Create STAC item with TiTiler preview links, register to catalog

**Stack:** Argo Workflows • [eopf-geozarr](https://github.com/EOPF-Explorer/data-model) • Dask • RabbitMQ • Kustomize

---

## Payload Format

### ✅ CORRECT
```yaml
# Sentinel-2
source_url: "https://stac.core.eopf.eodc.eu/collections/sentinel-2-l2a/items/S2A_MSIL2A_..."

# Sentinel-1
source_url: "https://stac.core.eopf.eodc.eu/collections/sentinel-1-l1-grd/items/S1A_IW_GRDH_..."
```

### ❌ WRONG
```yaml
source_url: "https://objectstore.eodc.eu/.../product.zarr"  # Direct zarr URLs not supported
```

**Why?** Pipeline extracts zarr URL from STAC item assets automatically.

**Find valid URLs:**
```bash
kubectl get wf -n devseed-staging --sort-by=.metadata.creationTimestamp \
  -o jsonpath='{range .items[?(@.status.phase=="Succeeded")]}{.spec.arguments.parameters[?(@.name=="source_url")].value}{"\n"}{end}' \
  | tail -n 5
```

---

## Structure

```
scripts/                      # Workflow steps
├── convert.py                # GeoZarr conversion (extract zarr URL, convert, upload)
├── register.py               # STAC registration orchestrator
├── register_stac.py          # STAC item creation with TiTiler links
├── create_geozarr_item.py    # Convert zarr → geozarr
├── augment_stac_item.py      # Add visualization links to STAC items
└── get_conversion_params.py  # Fetch collection config

workflows/                    # Kubernetes manifests (Kustomize)
├── base/                     # WorkflowTemplate, EventSource, Sensor, RBAC
└── overlays/                 # staging, production configs

docker/Dockerfile             # Pipeline image
tools/submit_burst.py         # RabbitMQ burst submission tool
```

Tests are planned for `tests/` directory (structure exists, test files to be added).

---

## Configuration

**📖 Full configuration:** See [workflows/README.md](workflows/README.md) for secrets setup and parameters.

**Quick reference:**
- S3: `s3.de.io.cloud.ovh.net` / `esa-zarr-sentinel-explorer-fra`
- Staging collection: `sentinel-2-l2a-dp-test`
- Production collection: `sentinel-2-l2a`

### Logging Level

By default, scripts log at **INFO** level. Third-party libraries (botocore, s3fs) are silenced to WARNING.

**Enable DEBUG logging:**
```yaml
# Add to workflow parameters or pod environment
env:
- name: LOG_LEVEL
  value: DEBUG
```

Or for local development:
```bash
export LOG_LEVEL=DEBUG
python scripts/convert.py --source-url ...
```

---

## Monitor

```bash
# Watch workflows
kubectl get wf -n devseed-staging --watch

# View logs
kubectl logs -n devseed-staging -l workflows.argoproj.io/workflow=<name> --tail=100

# Running workflows
kubectl get wf -n devseed-staging --field-selector status.phase=Running

# Sensor logs (RabbitMQ message processing)
kubectl logs -n devseed-staging -l sensor-name=geozarr-sensor --tail=50

# EventSource logs (RabbitMQ connection)
kubectl logs -n devseed-staging -l eventsource-name=rabbitmq-geozarr --tail=50
```


---

## Troubleshoot

| Problem | Solution |
|---------|----------|
| **"No group found in store"** | Using direct zarr URL instead of STAC item URL |
| **"Connection refused"** | RabbitMQ port-forward not active: `kubectl port-forward -n devseed-staging svc/rabbitmq 5672:5672` |
| **Workflow not starting** | Check sensor/eventsource logs for connection errors |
| **S3 access denied** | Verify secret `geozarr-s3-credentials` exists in `devseed-staging` namespace |
| **Workflow stuck** | Check logs: `kubectl logs -n devseed-staging -l workflows.argoproj.io/workflow=<name>` |



---

## Resources

**Pipeline Image:** `ghcr.io/eopf-explorer/data-pipeline:slim`

**Resource Limits:**
- CPU: 2 cores (convert), 500m (register)
- Memory: 8Gi (convert), 2Gi (register)
- Timeout: 3600s (convert), 600s (register)

**Related Projects:**
- [data-model](https://github.com/EOPF-Explorer/data-model) - `eopf-geozarr` conversion library
- [platform-deploy](https://github.com/EOPF-Explorer/platform-deploy) - Infrastructure (Argo, RabbitMQ, STAC, TiTiler)

**Documentation:**
- Workflow manifests: `workflows/README.md`
- Tests: `tests/` (pytest unit and integration tests)

**License:** MIT
