# Operator Tools

Job submission and management tools. New users: start with [GETTING_STARTED.md](../GETTING_STARTED.md).

| Tool | Purpose | Use Case |
|------|---------|----------|
| `submit.py` | AMQP job submission | Production batch processing |
| `simple_register.py` | Direct STAC registration | Testing/development |
| `operator.ipynb` | Interactive notebook | Exploration & validation |

## submit.py

Submit jobs via RabbitMQ to trigger workflows.

**Basic:**
```bash
export RABBITMQ_PASSWORD=$(kubectl get secret rabbitmq-password -n core -o jsonpath='{.data.rabbitmq-password}' | base64 -d)
uv run python examples/submit.py \
    --stac-url "https://stac.core.eopf.eodc.eu/collections/sentinel-2-l2a/items/S2B_MSIL2A_20250518_T29RLL_20250518T140519" \
    --collection "sentinel-2-l2a-dp-test" \
    --amqp-url "amqp://user:${RABBITMQ_PASSWORD}@rabbitmq.core.svc.cluster.local:5672/"
```

**Custom ID:**
```bash
uv run python examples/submit.py --stac-url "..." --item-id "custom-$(date +%s)" --collection "sentinel-2-l2a-dp-test"
```

**Custom payload:**
```bash
uv run python examples/submit.py --stac-url "..." --payload workflows/payload.json
```

**Port-forward:**
```bash
kubectl port-forward -n core svc/rabbitmq 5672:5672 &
uv run python examples/submit.py --stac-url "..." --amqp-url "amqp://user:${RABBITMQ_PASSWORD}@localhost:5672/"
```

## simple_register.py

Direct STAC registration (no K8s required).

```bash
pip install httpx pystac
python examples/simple_register.py
```

## operator.ipynb

Interactive Jupyter notebook for pipeline operations.

```bash
pip install pika requests ipykernel ipywidgets ipyleaflet pystac-client
jupyter notebook examples/operator.ipynb
```

## Results

- **Argo UI:** https://argo-workflows.hub-eopf-explorer.eox.at
- **STAC API:** https://api.explorer.eopf.copernicus.eu/stac
- **Viewer:** https://api.explorer.eopf.copernicus.eu/raster/viewer?url=...
