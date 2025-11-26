# EOPF GeoZarr Data Pipeline

**Kubernetes pipeline: Sentinel Zarr → Cloud-Optimized GeoZarr + STAC Registration**

Automated pipeline for converting Sentinel-1/2 Zarr datasets to cloud-optimized GeoZarr format with STAC catalog integration and interactive visualization.

---


## What It Does

Transforms Sentinel-1/2 satellite data into web-ready visualizations:

**Input:** STAC item URL → **Output:** Interactive web map (~15-20 min)

**Pipeline:** Convert → Register

**Supported Missions:**
- Sentinel-2 L2A (Multi-spectral optical)
- Sentinel-1 GRD (SAR backscatter)


## Setup

### Prerequisites:
- Kubernetes cluster with [platform-deploy](https://github.com/EOPF-Explorer/platform-deploy) (Argo Workflows, RabbitMQ, STAC API, TiTiler)
- Python 3.13+ with `uv`
- `GDAL` installed (on MacOS: `brew install gdal`)
- `kubectl` installed

### If needed, configure kubectl

Download kubeconfig from [OVH Manager → Kubernetes](https://www.ovh.com/manager/#/public-cloud/pci/projects/bcc5927763514f499be7dff5af781d57/kubernetes/f5f25708-bd15-45b9-864e-602a769a5fcf/service) (**Access and Security** tab).

```bash
mv ~/Downloads/kubeconfig.yml .work/kubeconfig
export KUBECONFIG=$(pwd)/.work/kubeconfig
kubectl get nodes  # Verify: should list several nodes
```

#### Quick verification:
```bash
kubectl get wf,sensor,eventsource -n devseed-staging
```

### Retrieve RABBITMQ_PASSWORD and store in .env file

```bash
# Check if RABBITMQ_PASSWORD already exists in .env
if [ -f .env ] && grep -q "^RABBITMQ_PASSWORD=" .env; then
  echo "RABBITMQ_PASSWORD already exists in .env"
else
  echo "RABBITMQ_PASSWORD=$(kubectl get secret rabbitmq-password -n core -o jsonpath='{.data.rabbitmq-password}' | base64 -d)" >> .env
  echo "✅ RABBITMQ_PASSWORD added to .env"
fi
```

### Add Harbor Registry credentials to .env file

Make sure you have an `HARBOR_USERNAME` and `HARBOR_PASSWORD` for OVH container registry added to the `.env` file.


### Setup port forwarding from local machine to RabbitMQ service
```bash
kubectl port-forward -n core svc/rabbitmq 5672:5672 &
```

### For development

**Make sure all dependencies are installed by running**
```bash
make setup
```

#### To test new code

- Authenticate with Harbor registry:
```bash
source .env
echo $HARBOR_PASSWORD | docker login w9mllyot.c1.de1.container-registry.ovh.net -u $HARBOR_USERNAME --password-stdin
```

- Build the new version of the code:


On macOS, the linux architecture needs to be specified when building the image with the flag `--platform linux/amd64` :
```bash
docker build -f docker/Dockerfile --network host -t w9mllyot.c1.de1.container-registry.ovh.net/eopf-sentinel-zarr-explorer/data-pipeline:v0 --platform linux/amd64 .
```

on linux:

```bash
docker build -f docker/Dockerfile --network host -t w9mllyot.c1.de1.container-registry.ovh.net/eopf-sentinel-zarr-explorer/data-pipeline:v0  .
```



- Push to container registry:
```bash
docker push w9mllyot.c1.de1.container-registry.ovh.net/eopf-sentinel-zarr-explorer/data-pipeline:v0
```

- Once the new image is pushed, run the example [Notebook](submit_stac_items_notebook.ipynb) and verify that worflows are running in [Argo Workflow server](https://workspace.devseed.hub-eopf-explorer.eox.at/argo-workflows-server)



---

## Submit Workflow

### Method 1: RabbitMQ (Production - Event-Driven)

Triggers via EventSource → Sensor:

**Submit workflow from python script**
```bash
python submit_test_workflow.py
```

or using the example [Notebook](submit_stac_items_notebook.ipynb)


### Method 2: kubectl (Testing - Bypasses Event System)

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

**Flow:** STAC item URL → Extract zarr → Convert to GeoZarr → Upload S3 → Register STAC item → Add visualization links

**Processing:**
1. **convert_v0.py** - Fetch STAC item, extract zarr URL, convert to cloud-optimized GeoZarr, upload to S3
2. **register.py** - Create STAC item with asset hrefs, add projection metadata and TiTiler links, register to catalog

**Runtime:** ~15-20 minutes per item

**Stack:**
- Orchestration: Argo Workflows, Kustomize
- Processing: eopf-geozarr, Dask, Python 3.13
- Storage: S3 (OVH)
- Catalog: pgSTAC, TiTiler
- Events: RabbitMQ

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

## Repository Structure

```
scripts/
├── convert_v0.py               # Zarr → GeoZarr conversion for V0  and S3 upload
└── register.py              # STAC item creation and catalog registration

workflows/                   # Kubernetes manifests
├── base/                    # WorkflowTemplate, EventSource, Sensor, RBAC
└── overlays/staging/        # Environment configuration
           /production/

docker/Dockerfile            # Container image
tests/unit/                  # Unit tests
     /integration/           # Integration tests
```

---

## Configuration

**📖 Full configuration:** See [workflows/README.md](workflows/README.md) for secrets setup and parameters.

**Quick reference:**
- S3: `s3.de.io.cloud.ovh.net` / `esa-zarr-sentinel-explorer-fra`
- Staging collection: `sentinel-2-l2a-dp-test`
- Production collection: `sentinel-2-l2a`
- **Enable debug logs:** `export LOG_LEVEL=DEBUG` (or add to workflow env)

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

**Container Image:** `w9mllyot.c1.de1.container-registry.ovh.net/eopf-sentinel-zarr-explorer/data-pipeline:latest`

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
