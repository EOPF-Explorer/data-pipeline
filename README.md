# EOPF Explorer Samples Data Pipeline

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

### Environments

The data pipeline is deployed in two Kubernetes namespaces:

- **`devseed-staging`** - Testing and validation environment
- **`devseed`** - Production data pipeline

This documentation uses `devseed-staging` in examples. For production, replace with `devseed`.

### Prerequisites

- Kubernetes cluster with [platform-deploy](https://github.com/EOPF-Explorer/platform-deploy) (Argo Workflows, STAC API, TiTiler)
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
kubectl get wf -n devseed-staging
```

### Add Harbor Registry credentials to .env file

Make sure you have an `HARBOR_USERNAME` and `HARBOR_PASSWORD` for OVH container registry added to the `.env` file.

### Setup port forwarding for webhook access

To submit workflows via the HTTP webhook endpoint, set up port forwarding:

```bash
kubectl port-forward -n devseed-staging svc/eopf-explorer-webhook-eventsource-svc 12000:12000 &
```

This makes the webhook endpoint available at `http://localhost:12000/samples`.

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

- Once the new image is pushed, run the example [Notebook](operator-tools/submit_stac_items_notebook.ipynb) and verify that workflows are running in [Argo Workflows](https://argo-workflows.hub-eopf-explorer.eox.at/workflows/devseed-staging)

---

## Submit Workflow

### Method 1: HTTP Webhook (Production - Event-Driven)

Submit STAC items via the HTTP webhook endpoint.

**Using the interactive notebook (recommended):**

```bash
cd operator-tools
jupyter notebook submit_stac_items_notebook.ipynb
```

**Using the Python script for single items:**

```bash
cd operator-tools
python submit_test_workflow_wh.py
```

See [operator-tools/README.md](operator-tools/README.md) for detailed usage instructions.

### Method 2: kubectl (Testing - Direct Workflow Submission)

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

**Monitor:** [Argo Workflows UI](https://argo-workflows.hub-eopf-explorer.eox.at/workflows/devseed-staging)

**View Results:**

- [STAC Browser](https://api.explorer.eopf.copernicus.eu/stac) - Browse catalog
- [TiTiler Viewer](https://api.explorer.eopf.copernicus.eu/raster) - View maps

💡 **Tip:** Login to [EOxHub workspace](https://workspace.devseed.hub-eopf-explorer.eox.at/) for seamless authentication.

---

## Pipeline

**Flow:** STAC item URL → Extract zarr → Convert to GeoZarr → Upload S3 → Register STAC item → Add visualization links

**Processing:**
1. **convert_v0.py** - Fetch STAC item, extract zarr URL, convert to cloud-optimized GeoZarr, upload to S3
2. **register.py** - Create STAC item with asset hrefs, add projection metadata and TiTiler links, register to catalog

**Runtime:** ~15-20 minutes per item

**Stack:**

- Processing: eopf-geozarr, Dask, Python 3.13
- Storage: S3 (OVH)
- Catalog: pgSTAC, TiTiler

**Infrastructure:** Deployment configuration and infrastructure details are maintained in [platform-deploy](https://github.com/EOPF-Explorer/platform-deploy/tree/main/workspaces/devseed-staging/data-pipeline)

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
├── convert_v0.py     # Zarr → GeoZarr conversion and S3 upload
└── register.py       # STAC item creation and catalog registration

operator-tools/       # Tools for submitting workflows
docker/Dockerfile     # Container image
tests/                # Unit and integration tests
```

**Deployment Configuration:** Kubernetes manifests and infrastructure are maintained in [platform-deploy](https://github.com/EOPF-Explorer/platform-deploy/tree/main/workspaces/devseed-staging/data-pipeline)

---

## Monitor

```bash
# Watch workflows
kubectl get wf -n devseed-staging --watch

# View workflow logs
kubectl logs -n devseed-staging -l workflows.argoproj.io/workflow=<name> --tail=100

# Running workflows only
kubectl get wf -n devseed-staging --field-selector status.phase=Running
```

**Web UI:** [Argo Workflows](https://argo-workflows.hub-eopf-explorer.eox.at/workflows/devseed-staging)


---

## Troubleshoot

| Problem | Solution |
|---------|----------|
| **"No group found in store"** | Using direct zarr URL instead of STAC item URL |
| **"Webhook not responding"** | Verify port-forward is active: `ps aux \| grep "port-forward.*12000"` |
| **Workflow not starting** | Check webhook submission returned success, verify port-forward |
| **S3 access denied** | Contact infrastructure team to verify S3 credentials |
| **Workflow stuck/failed** | Check workflow logs: `kubectl logs -n devseed-staging -l workflows.argoproj.io/workflow=<name>` |

For infrastructure issues, see platform-deploy troubleshooting: [staging](https://github.com/EOPF-Explorer/platform-deploy/tree/main/workspaces/devseed-staging/data-pipeline) | [production](https://github.com/EOPF-Explorer/platform-deploy/tree/main/workspaces/devseed/data-pipeline)



---

## Related Projects

- [data-model](https://github.com/EOPF-Explorer/data-model) - `eopf-geozarr` conversion library
- [platform-deploy](https://github.com/EOPF-Explorer/platform-deploy) - Infrastructure deployment and configuration

## Documentation

- **Operator Tools:** [operator-tools/README.md](operator-tools/README.md)
- **Tests:** `tests/` - pytest unit and integration tests
- **Deployment:** [platform-deploy/workspaces/devseed-staging/data-pipeline](https://github.com/EOPF-Explorer/platform-deploy/tree/main/workspaces/devseed-staging/data-pipeline)

## License

MIT
