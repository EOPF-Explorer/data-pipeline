# Prometheus Metrics

## Metrics Collected

Pipeline scripts expose Prometheus metrics for observability. Metrics server runs on port 8000 in workflow pods.

### STAC Registration (`register_stac.py`)
```python
stac_registration_total{collection, operation, status}
# operation: create|update|skip|replace
# status: success|error
# Track failures, operation distribution

stac_http_request_duration_seconds{operation, endpoint}
# operation: get|put|post|delete
# endpoint: item|items
# STAC API latency, set SLOs
```

### Preview Generation (`augment_stac_item.py`)
```python
preview_generation_duration_seconds{collection}
# Augmentation performance by collection

preview_http_request_duration_seconds{operation, endpoint}
# operation: get|put
# STAC API response times during augmentation
```

## Key Queries

**Success Rate (SLO: >99%)**
```promql
sum(rate(stac_registration_total{status="success"}[5m])) / sum(rate(stac_registration_total[5m]))
```

**Errors by Collection**
```promql
sum(rate(stac_registration_total{status="error"}[5m])) by (collection)
```

**STAC API Latency P95 (SLO: <500ms)**
```promql
histogram_quantile(0.95, rate(stac_http_request_duration_seconds_bucket[5m])) by (operation)
```

**Preview Duration P95 (SLO: <10s)**
```promql
histogram_quantile(0.95, rate(preview_generation_duration_seconds_bucket[5m])) by (collection)
```

**Throughput (items/min)**
```promql
sum(rate(stac_registration_total[5m])) * 60
```

## Setup

Prometheus scrapes via PodMonitor (deployed in `platform-deploy/workspaces/devseed*/data-pipeline/`).

**Verify:**
```bash
kubectl port-forward -n core svc/prometheus-operated 9090:9090
# http://localhost:9090/targets → "geozarr-workflows"
```

## Grafana Dashboards

- **Overview**: Success rate, throughput, error rate by collection
- **Performance**: P95 latencies (STAC API, preview generation)
- **Capacity**: Peak load, processing rate trends

## Alerts

**High Failure Rate**
```yaml
expr: rate(stac_registration_total{status="error"}[5m]) / rate(stac_registration_total[5m]) > 0.1
for: 5m
# Check STAC API status, verify auth tokens
```

**Slow Preview Generation**
```yaml
expr: histogram_quantile(0.95, rate(preview_generation_duration_seconds_bucket[5m])) > 60
for: 10m
# Check TiTiler API or asset access
```

**STAC API Latency**
```yaml
expr: histogram_quantile(0.95, rate(stac_http_request_duration_seconds_bucket[5m])) > 1
for: 10m
# Database overload or network issues
```

## SLOs

- **Success Rate**: >99%
- **STAC API P95**: <500ms
- **Preview P95**: <10s
