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
# Check resources
kubectl get workflowtemplate,sensor,eventsource,sa -n devseed-staging

# Watch for workflows
kubectl get wf -n devseed-staging --watch
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
