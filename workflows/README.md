# Workflows

Event-driven Argo Workflows for Sentinel-2 GeoZarr conversion and STAC registration.

**Architecture**: RabbitMQ messages → Sensor → WorkflowTemplate (convert → register) → S3 + STAC API

---

## Quick Setup

### 1. Configure kubectl

Download kubeconfig from [OVH Manager → Kubernetes](https://www.ovh.com/manager/#/public-cloud/pci/projects/bcc5927763514f499be7dff5af781d57/kubernetes/f5f25708-bd15-45b9-864e-602a769a5fcf/service) (**Access and Security** tab).

```bash
mv ~/Downloads/kubeconfig-*.yml .work/kubeconfig
export KUBECONFIG=$(pwd)/.work/kubeconfig
kubectl get nodes  # Verify: should list 3-5 nodes
```

### 2. Create Required Secrets

The pipeline needs 3 secrets for: **event ingestion** (RabbitMQ), **output storage** (S3), and **STAC registration** (API auth).

**RabbitMQ credentials** (receives workflow trigger events):
```bash
# Get password from cluster-managed secret
RABBITMQ_PASS=$(kubectl get secret rabbitmq-password -n core -o jsonpath='{.data.rabbitmq-password}' | base64 -d)

kubectl create secret generic rabbitmq-credentials -n devseed-staging \
  --from-literal=username=user \
  --from-literal=password="$RABBITMQ_PASS"
```

**S3 credentials** (writes converted GeoZarr files):
```bash
# Get from OVH Manager → Users & Roles → OpenStack credentials
# https://www.ovh.com/manager/\#/public-cloud/pci/projects/bcc5927763514f499be7dff5af781d57/users

kubectl create secret generic geozarr-s3-credentials -n devseed-staging \
  --from-literal=AWS_ACCESS_KEY_ID=<your-ovh-access-key> \
  --from-literal=AWS_SECRET_ACCESS_KEY=<your-ovh-secret-key>
```

**STAC API token** (registers items, optional if API is public):
```bash
kubectl create secret generic stac-api-token -n devseed-staging \
  --from-literal=token=<bearer-token>
```

### 3. Deploy Workflows

```bash
kubectl apply -k workflows/overlays/staging     # Staging (devseed-staging)
kubectl apply -k workflows/overlays/production  # Production (devseed)
```

**Verify deployment:**
```bash
kubectl get workflowtemplate,sensor,eventsource,sa -n devseed-staging
# Expected: 1 WorkflowTemplate, 1 Sensor, 1 EventSource, 1 ServiceAccount
```

---

## Structure

```
workflows/
├── base/                      # Core resources (namespace-agnostic)
│   ├── workflowtemplate.yaml  # 2-step DAG: convert → register
│   ├── sensor.yaml            # RabbitMQ trigger
│   ├── eventsource.yaml       # RabbitMQ connection
│   ├── rbac.yaml              # Permissions
│   └── kustomization.yaml
└── overlays/
    ├── staging/               # devseed-staging namespace
    └── production/            # devseed namespace
```

---

## Monitoring

**Watch workflows:**
```bash
kubectl get wf -n devseed-staging --watch
```

**Example output:**
```
NAME            STATUS      AGE
geozarr-79jmg   Running     5m
geozarr-95rgx   Succeeded   9h
geozarr-jflnj   Failed      10h
```

---

## Configuration

### S3 Storage

- **Endpoint**: `https://s3.de.io.cloud.ovh.net` (OVH Frankfurt)
- **Bucket**: `esa-zarr-sentinel-explorer-fra`
- **Paths**: `tests-output/` (staging), `geozarr/` (production)

### Workflow Parameters

Key parameters (see [../README.md](../README.md) for full reference):

- `source_url`: STAC item URL or Zarr URL
- `register_collection`: Target STAC collection (default: `sentinel-2-l2a-dp-test`)
- `s3_output_bucket`: Output bucket
- `pipeline_image_version`: Docker image tag

**Override conversion parameters** (optional, for testing):

```bash
# Example: Test with different chunk size and disable sharding
argo submit workflows/base/workflowtemplate.yaml \
  --from workflowtemplate/geozarr-pipeline \
  -p source_url="https://api.example.com/stac/.../items/ITEM_ID" \
  -p override_spatial_chunk="2048" \
  -p override_enable_sharding="false"
```

Available overrides (empty = use collection defaults):
- `override_groups`: Comma-separated zarr groups (e.g., `/measurements/reflectance/r10m`)
- `override_spatial_chunk`: Chunk size (e.g., `2048`)
- `override_tile_width`: Tile width (e.g., `512`)
- `override_enable_sharding`: Enable sharding (`true`/`false`)

Defaults: S2 (1024/256/true), S1 (4096/512/false). See `scripts/get_conversion_params.py`.

### Resource Tuning

Edit `workflows/base/workflowtemplate.yaml`:

```yaml
resources:
  requests: { memory: 4Gi, cpu: '1' }
  limits:   { memory: 8Gi, cpu: '2' }  # Increase for larger datasets
```

---

## Troubleshooting

**Workflow not triggered:**
```bash
kubectl logs -n devseed-staging -l eventsource-name=rabbitmq  # Check RabbitMQ connection
kubectl get sensor -n devseed-staging geozarr-trigger -o yaml  # Check sensor status
```

**Workflow fails:**
```bash
kubectl logs -n devseed-staging <workflow-pod-name>  # View logs
kubectl get secret -n devseed-staging                 # Verify secrets exist
```

**Kustomize validation:**
```bash
kubectl kustomize workflows/overlays/staging  # Validate YAML
```

---

For complete documentation, see [../README.md](../README.md).
