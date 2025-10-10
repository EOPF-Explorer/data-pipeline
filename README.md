# EOPF GeoZarr Data Pipeline

Automated pipeline for converting Sentinel Zarr datasets to cloud-optimized GeoZarr format with STAC catalog integration and interactive visualization.

## Quick Start (30 seconds)

```bash
# 1. Submit workflow
export KUBECONFIG=.work/kubeconfig
kubectl create -f workflows/run-s1-test.yaml -n devseed-staging

# 2. Monitor
kubectl logs -n devseed-staging -l workflows.argoproj.io/workflow=<name> -c main -f
```

📖 **New here?** [GETTING_STARTED.md](GETTING_STARTED.md) • **Details:** [Full docs below](#submitting-workflows)

## What It Does

**Input:** STAC item URL → **Output:** Interactive web map in ~15-20 minutes

```
Convert (15 min) → Register (30 sec) → Augment (10 sec)
```

**Supports:** Sentinel-1 GRD (SAR) • Sentinel-2 L2A (optical)

**Prerequisites:** Kubernetes with [platform-deploy](https://github.com/EOPF-Explorer/platform-deploy) • Python 3.11+ • [GETTING_STARTED.md](GETTING_STARTED.md) for full setup

## Submitting Workflows

| Method | Best For | Setup | Status |
|--------|----------|-------|--------|
| 🎯 **kubectl** | Testing, CI/CD | None | ✅ Recommended |
| 📓 **Jupyter** | Learning, exploration | 2 min | ✅ Working |
| ⚡ **Event-driven** | Production (auto) | In-cluster | ✅ Running |
| 🐍 **Python CLI** | Scripting | Port-forward | ⚠️ Advanced |

<details>
<summary><b>kubectl</b> (recommended)</summary>

```bash
export KUBECONFIG=.work/kubeconfig
kubectl create -f workflows/run-s1-test.yaml -n devseed-staging -o name
kubectl logs -n devseed-staging -l workflows.argoproj.io/workflow=<wf-name> -c main -f
```
Edit `workflows/run-s1-test.yaml` with your STAC URL and collection.
</details>

<details>
<summary><b>Jupyter</b></summary>

```bash
uv sync --extra notebooks
cp notebooks/.env.example notebooks/.env
uv run jupyter lab notebooks/operator.ipynb
```
</details>

<details>
<summary><b>Event-driven</b> (production)</summary>

Publish to RabbitMQ `geozarr` exchange:
```json
{"source_url": "https://stac.../items/S1A_...", "item_id": "S1A_IW_GRDH_...", "collection": "sentinel-1-l1-grd-dp-test"}
```
</details>

<details>
<summary><b>Python CLI</b></summary>

```bash
kubectl port-forward -n core svc/rabbitmq 5672:5672
export AMQP_PASSWORD=$(kubectl get secret rabbitmq-password -n core -o jsonpath='{.data.rabbitmq-password}' | base64 -d)
uv run python examples/submit.py --stac-url "..." --collection sentinel-2-l2a
```
</details>

**Related:** [data-model](https://github.com/EOPF-Explorer/data-model) • [platform-deploy](https://github.com/EOPF-Explorer/platform-deploy) • [Testing report](docs/WORKFLOW_SUBMISSION_TESTING.md)

## Configuration

<details>
<summary><b>S3 & RabbitMQ</b></summary>

```bash
# S3 credentials
kubectl create secret generic geozarr-s3-credentials -n devseed \
  --from-literal=AWS_ACCESS_KEY_ID="<key>" \
  --from-literal=AWS_SECRET_ACCESS_KEY="<secret>"

# RabbitMQ password
kubectl get secret rabbitmq-password -n core -o jsonpath='{.data.rabbitmq-password}' | base64 -d
```

**Endpoints:** S3: `s3.de.io.cloud.ovh.net/esa-zarr-sentinel-explorer-fra` • RabbitMQ: `geozarr` exchange • [UIs](https://workspace.devseed.hub-eopf-explorer.eox.at/): [Argo](https://argo-workflows.hub-eopf-explorer.eox.at) • [STAC](https://api.explorer.eopf.copernicus.eu/stac) • [Viewer](https://api.explorer.eopf.copernicus.eu/raster)
</details>

## Troubleshooting

<details>
<summary><b>Logs & Issues</b></summary>

```bash
kubectl get wf -n devseed-staging -w
kubectl logs -n devseed-staging <pod-name> -c main -f
kubectl logs -n devseed -l sensor-name=geozarr-sensor --tail=50
```

**Common fixes:** Workflow not starting → check sensor logs • S3 denied → verify `geozarr-s3-credentials` secret • RabbitMQ refused → `kubectl port-forward -n core svc/rabbitmq 5672:5672` • Pod pending → check resources
</details>

## Development

```bash
uv sync --all-extras && pre-commit install
make test  # or: pytest tests/ -v -k e2e
```

**Deploy:** Edit `workflows/template.yaml` or `scripts/*.py` → `pytest tests/ -v` → `docker buildx build --platform linux/amd64 -t ghcr.io/eopf-explorer/data-pipeline:dev .` → `kubectl apply -f workflows/template.yaml -n devseed` • [CONTRIBUTING.md](CONTRIBUTING.md)

## License

Apache 2.0 - See [LICENSE](LICENSE) for details.
