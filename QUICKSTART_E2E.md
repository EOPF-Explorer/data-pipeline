# Quick Start: End-to-End GeoZarr Pipeline

Complete a full GeoZarr conversion from STAC item to interactive web map in ~10 minutes.

## Prerequisites

- Kubernetes cluster with data-pipeline deployed
- kubectl configured with proper context
- Python 3.11+ with `pika` and `click` installed:
  ```bash
  pip install pika click
  # OR if using uv in the repo:
  cd data-pipeline && uv sync
  ```

## One-Command Test

```bash
# Port-forward RabbitMQ, publish message, and monitor
export RABBITMQ_PASSWORD=$(kubectl get secret rabbitmq-password -n core -o jsonpath='{.data.rabbitmq-password}' | base64 -d)
kubectl port-forward -n core svc/rabbitmq 5672:5672 >/dev/null 2>&1 &
sleep 2

# Submit job
ITEM_ID="quickstart-test-$(date +%s)"
python3 examples/submit.py \
  --stac-url "https://stac.core.eopf.eodc.eu/collections/sentinel-2-l2a/items/S2C_MSIL2A_20251007T125311_N0511_R138_T27WXN_20251007T141722" \
  --item-id "$ITEM_ID" \
  --collection "sentinel-2-l2a-dp-test" \
  --amqp-url "amqp://user:${RABBITMQ_PASSWORD}@localhost:5672/"

# Get workflow (wait 10s for sensor to trigger)
sleep 10
WORKFLOW=$(kubectl get workflows -n devseed --sort-by=.metadata.creationTimestamp -o name | tail -1 | cut -d'/' -f2)
echo "✅ Workflow: $WORKFLOW"
echo "🔗 Argo UI: https://argo-workflows.hub-eopf-explorer.eox.at/workflows/devseed/$WORKFLOW"

# Monitor (workflow takes ~5-10 minutes)
kubectl get workflow $WORKFLOW -n devseed -w
```

## Step-by-Step Guide

## Step-by-Step Guide

### 1. Verify Infrastructure

```bash
kubectl get eventsource rabbitmq-geozarr -n devseed
kubectl get sensor geozarr-sensor -n devseed
kubectl get workflowtemplate geozarr-pipeline -n devseed
```

All should exist without errors (AGE column shows they're deployed).

### 2. Publish AMQP Message

```bash
# Get RabbitMQ password
export RABBITMQ_PASSWORD=$(kubectl get secret rabbitmq-password -n core -o jsonpath='{.data.rabbitmq-password}' | base64 -d)

# Port-forward RabbitMQ
kubectl port-forward -n core svc/rabbitmq 5672:5672 &

# Submit job with unique ID
ITEM_ID="test-$(date +%s)"
python3 examples/submit.py \
  --stac-url "https://stac.core.eopf.eodc.eu/collections/sentinel-2-l2a/items/S2C_MSIL2A_20251007T125311_N0511_R138_T27WXN_20251007T141722" \
  --item-id "$ITEM_ID" \
  --collection "sentinel-2-l2a-dp-test" \
  --amqp-url "amqp://user:${RABBITMQ_PASSWORD}@localhost:5672/"

echo "Submitted with item_id: $ITEM_ID"
```

### 3. Find Workflow

Wait 10 seconds for sensor to trigger, then get workflow:

```bash
sleep 10
WORKFLOW=$(kubectl get workflows -n devseed --sort-by=.metadata.creationTimestamp -o name | tail -1 | cut -d'/' -f2)
echo "Workflow: $WORKFLOW"

# Verify it was created by sensor (should show operate-workflow-sa)
kubectl get workflow $WORKFLOW -n devseed -o jsonpath='{.metadata.labels.workflows\.argoproj\.io/creator}'
```

### 4. Monitor Execution

**Watch workflow status:**
```bash
kubectl get workflow $WORKFLOW -n devseed -w
```

**Check step progress:**
```bash
kubectl get workflow $WORKFLOW -n devseed -o jsonpath='{.status.nodes}' | \
  jq -r 'to_entries[] | "\(.value.displayName)\t\(.value.phase)"' | column -t
```

**View logs (once pods are running):**
```bash
# All steps
kubectl logs -n devseed -l workflows.argoproj.io/workflow=$WORKFLOW -f --prefix

# Convert step only
kubectl logs -n devseed -l workflows.argoproj.io/workflow=$WORKFLOW,workflows.argoproj.io/template=convert-geozarr -c main -f
```

### 5. Verify Results

**Wait for completion** (5-10 minutes):
```bash
kubectl wait --for=condition=Completed --timeout=15m workflow/$WORKFLOW -n devseed
```

**Check STAC registration:**
```bash
ITEM_ID=$(kubectl get workflow $WORKFLOW -n devseed -o jsonpath='{.spec.arguments.parameters[?(@.name=="item_id")].value}')

curl -s "https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a-dp-test/items/$ITEM_ID" | jq '{
  id: .id,
  assets: (.assets | length),
  viewer: [.links[] | select(.rel=="viewer") | .href][0]
}'
```

## Argo UI

View in browser:
```
https://argo-workflows.hub-eopf-explorer.eox.at/workflows/devseed/<WORKFLOW_NAME>
```

Workflows created via AMQP → Sensor are visible (sensor uses service account authentication).

See [docs/ARGO_UI_VISIBILITY.md](docs/ARGO_UI_VISIBILITY.md) for details.

## Workflow Steps

The pipeline executes three steps:

1. **convert-geozarr** - Convert Zarr to GeoZarr with tiling (~5 min)
2. **register-stac** - Register as STAC item (~30 sec)
3. **augment-stac** - Add viewer/XYZ/TileJSON links (~10 sec)

## Troubleshooting

**Workflow not created:**
```bash
# Check sensor logs
kubectl logs -n devseed -l sensor-name=geozarr-sensor --tail=50

# Check EventSource
kubectl logs -n devseed -l eventsource-name=rabbitmq-geozarr --tail=50
```

**Workflow failed:**
```bash
# Get error details
kubectl describe workflow $WORKFLOW -n devseed

# Check pod logs
kubectl logs -n devseed -l workflows.argoproj.io/workflow=$WORKFLOW --tail=200
```

**STAC item not found:**
- Verify workflow succeeded: `kubectl get workflow $WORKFLOW -n devseed`
- Check register step logs
- Confirm collection exists: `curl -s https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a-dp-test`

## Success Criteria

✅ Workflow Status: Succeeded
✅ All 3 steps completed
✅ STAC item has 20+ assets
✅ Viewer, XYZ, TileJSON links present
