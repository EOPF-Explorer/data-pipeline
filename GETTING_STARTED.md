# Getting Started

Setup guide for running GeoZarr conversions (15 minutes).

## Overview

Converts Sentinel-2 Zarr to cloud-optimized GeoZarr with web visualization.

**Input:** STAC item URL
**Output:** Interactive map at `https://api.explorer.eopf.copernicus.eu/raster/viewer?url=...`

## Prerequisites

**Required:**
- OVH Kubernetes cluster access (managed by platform-deploy)
- Python 3.11+ and kubectl on local machine

**Not required:**
- Docker, deep Kubernetes knowledge, Argo Workflows expertise

## Step 1: Configure kubectl

Download kubeconfig from [OVH Manager](https://www.ovh.com/manager/#/public-cloud/pci/projects/bcc5927763514f499be7dff5af781d57/kubernetes/f5f25708-bd15-45b9-864e-602a769a5fcf/service) (Access and security → kubeconfig).

```bash
mkdir -p .work
mv ~/Downloads/kubeconfig-*.yml .work/kubeconfig
export KUBECONFIG=$(pwd)/.work/kubeconfig
echo "export KUBECONFIG=$(pwd)/.work/kubeconfig" >> ~/.zshrc

kubectl get nodes  # Should list 3-5 nodes
```

## Step 2: Install Dependencies

```bash
# Using uv (recommended)
curl -LsSf https://astral.sh/uv/install.sh | sh
uv sync --all-extras

# Or using pip
pip install pika click requests
```

## Step 3: Deploy Infrastructure

```bash
kubectl apply -f workflows/rbac.yaml -n devseed
kubectl apply -f workflows/eventsource.yaml -n devseed
kubectl apply -f workflows/sensor.yaml -n devseed
kubectl apply -f workflows/template.yaml -n devseed

# Verify
./validate-setup.sh
```

Deploys: RBAC permissions, RabbitMQ event source, workflow trigger sensor, conversion template.

## Step 4: Submit Job

```bash
# Port-forward RabbitMQ and submit in one command
kubectl port-forward -n core svc/rabbitmq 5672:5672 &
sleep 2
export AMQP_URL="amqp://user:$(kubectl get secret rabbitmq-password -n core -o jsonpath='{.data.rabbitmq-password}' | base64 -d)@localhost:5672/"
uv run python examples/submit.py \
    --stac-url "https://stac.core.eopf.eodc.eu/collections/sentinel-2-l2a/items/S2B_MSIL2A_20250518T112119_N0511_R037_T29RLL_20250518T140519" \
    --collection "sentinel-2-l2a-dp-test" \
    --item-id "test-$(date +%s)"
```

## Step 5: Monitor Workflow

```bash
# Watch latest workflow (5-7 min conversion time)
sleep 10
kubectl get workflows -n devseed --sort-by=.metadata.creationTimestamp -o name | tail -1 | \
  xargs -I {} kubectl get {} -n devseed -w
```

**States:** Running (converting), Succeeded (done), Failed (check logs below)

## Step 6: View Result

```bash
# Use your item ID from Step 4 (e.g., test-1728315678)
ITEM_ID="YOUR_ITEM_ID"

# View in browser
open "https://api.explorer.eopf.copernicus.eu/raster/viewer?url=https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a-dp-test/items/${ITEM_ID}"

# Or get STAC metadata
curl "https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a-dp-test/items/${ITEM_ID}" | jq .
```

## Next Steps

**Batch processing:**
```bash
curl "https://stac.core.eopf.eodc.eu/collections/sentinel-2-l2a/items?limit=10" | \
  jq -r '.features[].id' | \
  xargs -I {} uv run python examples/submit.py --stac-url "https://stac.core.eopf.eodc.eu/collections/sentinel-2-l2a/items/{}" --collection "sentinel-2-l2a-dp-test"
```

**Jupyter notebook:** `jupyter lab notebooks/operator.ipynb` for interactive operations.

**Custom payloads:** Edit `workflows/payload.json` (groups, spatial_chunk, tile_width), then `--payload workflows/payload.json`.

## Troubleshooting

**Workflow failed:** Check logs:
```bash
WORKFLOW=$(kubectl get workflows -n devseed --sort-by=.metadata.creationTimestamp -o name | tail -1)
kubectl logs -n devseed -l workflows.argoproj.io/workflow=$(basename $WORKFLOW) --tail=100
```

**No workflow created:** Check sensor/eventsource:
```bash
kubectl logs -n devseed -l sensor-name=geozarr-sensor --tail=50
```

**Connection issues:** Ensure port-forward is running: `kubectl port-forward -n core svc/rabbitmq 5672:5672 &`

## Advanced

**Monitor all workflows:**
```bash
watch -n 2 'kubectl get workflows -n devseed --sort-by=.metadata.creationTimestamp | tail -20'
```

**Cleanup succeeded workflows (>7 days):**
```bash
kubectl delete workflows -n devseed --field-selector=status.phase=Succeeded \
  $(kubectl get workflows -n devseed -o json | jq -r '.items[] | select(.metadata.creationTimestamp | fromdateiso8601 < (now - 604800)) | .metadata.name')
```

## Architecture

```
submit.py → RabbitMQ → Sensor → Argo Workflow (convert → register → augment) → S3 + STAC
```

**Components:**
- STAC Item: Satellite metadata (JSON)
- GeoZarr: Cloud-optimized geospatial format
- AMQP: Message queue protocol
- Sensor: Event-driven workflow trigger

**Resources:**
- Docs: [README.md](README.md)
- Tools: [examples/README.md](examples/README.md)

## Web UIs

All bundled in EOxHub workspace: **https://workspace.devseed.hub-eopf-explorer.eox.at/**

**Login to EOxHub for authenticated access to:**
- Argo Workflows: Monitor pipeline execution
- STAC Browser: Catalog exploration

**Direct URLs (login through EOxHub first):**
- Argo UI: https://argo-workflows.hub-eopf-explorer.eox.at
- STAC API: https://api.explorer.eopf.copernicus.eu/stac
- Raster API: https://api.explorer.eopf.copernicus.eu/raster
