# Workflows

Argo Workflows configuration using Kustomize for environment management.

## Purpose

Event-driven pipeline orchestration for Sentinel-2 GeoZarr conversion and STAC registration. RabbitMQ messages trigger workflows that run a 2-step DAG: **convert → register**.

## Structure

```
workflows/
├── base/                           # Core resources (namespace-agnostic)
│   ├── kustomization.yaml          # References all resources
│   ├── workflowtemplate.yaml       # 2-step pipeline DAG
│   ├── sensor.yaml                 # RabbitMQ → Workflow trigger
│   ├── eventsource.yaml            # RabbitMQ connection config
│   └── rbac.yaml                   # ServiceAccount + permissions
└── overlays/
    ├── staging/
    │   └── kustomization.yaml      # devseed-staging namespace patches
    └── production/
        └── kustomization.yaml      # devseed namespace patches
```

## Apply to Cluster

**Staging (devseed-staging):**
```bash
kubectl apply -k workflows/overlays/staging
```

**Production (devseed):**
```bash
kubectl apply -k workflows/overlays/production
```

**Verify deployment:**
```bash
# Check resources (expected output shows 1 of each)
kubectl get workflowtemplate,sensor,eventsource,sa -n devseed-staging

# Example output:
# NAME                                                AGE
# workflowtemplate.argoproj.io/geozarr-pipeline      5m
#
# NAME                                    AGE
# sensor.argoproj.io/geozarr-sensor      5m
#
# NAME                                          AGE
# eventsource.argoproj.io/rabbitmq-geozarr     5m
#
# NAME                                SECRETS   AGE
# serviceaccount/operate-workflow-sa   0         5m

# Watch for workflows (should show Running/Succeeded/Failed)
kubectl get wf -n devseed-staging --watch
```
Example outputs:
```
NAME            STATUS      AGE
geozarr-79jmg   Running     5m
geozarr-95rgx   Succeeded   9h
geozarr-hpcvf   Succeeded   10h
geozarr-jflnj   Failed      10h
```

## Required Secrets

The pipeline requires these Kubernetes secrets in the target namespace:

### 1. `rabbitmq-credentials`
RabbitMQ authentication for EventSource:

```bash
kubectl create secret generic rabbitmq-credentials \
  --from-literal=username=<rabbitmq-user> \
  --from-literal=password=<rabbitmq-password> \
  -n devseed-staging
```

### 2. `geozarr-s3-credentials`
S3 credentials for GeoZarr output:

```bash
kubectl create secret generic geozarr-s3-credentials \
  --from-literal=AWS_ACCESS_KEY_ID=<access-key> \
  --from-literal=AWS_SECRET_ACCESS_KEY=<secret-key> \
  -n devseed-staging
```

### 3. `stac-api-token` (optional)
Bearer token for STAC API authentication (if required):

```bash
kubectl create secret generic stac-api-token \
  --from-literal=token=<bearer-token> \
  -n devseed-staging
```

## WorkflowTemplate Parameters

See main [README.md](../README.md) for complete parameter reference.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `source_url` | - | STAC item URL or direct Zarr URL |
| `register_collection` | sentinel-2-l2a-dp-test | STAC collection ID |
| `stac_api_url` | https://api... | STAC API endpoint |
| `raster_api_url` | https://api... | TiTiler endpoint |
| `s3_output_bucket` | esa-zarr... | S3 output bucket |
| `pipeline_image_version` | fix-unit-tests | Docker image tag |

## Resource Configuration

To adjust CPU/memory limits, edit `workflows/base/workflowtemplate.yaml`:

```yaml
- name: convert-geozarr
  resources:
    requests:
      memory: 4Gi    # Increase for larger datasets
      cpu: '1'
    limits:
      memory: 8Gi
      cpu: '2'
```

## Troubleshooting

**Kustomize build fails:**
```bash
# Validate structure
kubectl kustomize workflows/overlays/staging

# Check for duplicate resources
find workflows -name "*.yaml" -not -path "*/base/*" -not -path "*/overlays/*"
```

**Workflow not triggered:**
- Check EventSource connection: `kubectl logs -n devseed-staging -l eventsource-name=rabbitmq`
- Check Sensor status: `kubectl get sensor -n devseed-staging geozarr-trigger -o yaml`
- Verify RabbitMQ port-forward or service access

**Workflow fails:**
- Check pod logs: `kubectl logs -n devseed-staging <workflow-pod-name>`
- Verify secrets exist: `kubectl get secret -n devseed-staging geozarr-s3-credentials stac-api-token`
- Check RBAC: `kubectl auth can-i create workflows --as=system:serviceaccount:devseed-staging:operate-workflow-sa`

For full pipeline documentation, see [../README.md](../README.md).
