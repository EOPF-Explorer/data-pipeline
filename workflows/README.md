# Workflows

Kustomize-based Argo Workflows for GeoZarr pipeline.

## Deploy

```bash
kubectl apply -k overlays/staging     # Deploy to staging
kubectl apply -k overlays/production  # Deploy to production
```

## Structure

- `base/` - Core templates (namespace-agnostic)
- `overlays/` - Environment configs (staging/production)
- `tests/` - Test workflows + payload examples

## Test

```bash
# Via AMQP
kubectl create configmap amqp-payload --from-file=body.json=tests/s1-minimal.json
kubectl apply -f tests/amqp-publish-once.yaml

# Direct
kubectl create -f tests/run-s1-test.yaml
```
