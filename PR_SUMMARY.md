# Pull Request Summary

## What I am changing

This PR establishes the complete EOPF GeoZarr Data Pipeline repository - a Kubernetes-based, event-driven pipeline for converting Sentinel-1/2 Zarr datasets to cloud-optimized GeoZarr format with STAC catalog integration.

### Key Components Added:

1. **Core Processing Scripts** (`scripts/`)
   - `convert.py`: Zarr → GeoZarr conversion and S3 upload functionality
   - `register.py`: STAC item creation and catalog registration

2. **Kubernetes Workflows** (`legacy_workflows/`)
   - Base manifests: WorkflowTemplate, EventSource, Sensor, RBAC configurations
   - Environment overlays: staging and production configurations using Kustomize
   - RabbitMQ-based event-driven workflow triggering

3. **Container Infrastructure**
   - `docker/Dockerfile`: Container image definition for the pipeline
   - GitHub Actions workflows for CI/CD (build, test)

4. **Project Configuration**
   - `pyproject.toml`: Python 3.13+ project with dependencies including:
     - Data processing: xarray, zarr, s3fs, eopf-geozarr
     - STAC integration: pystac, pystac-client
     - Event handling: pika (RabbitMQ)
     - Development tools: ruff, mypy, pytest, pre-commit
   - `uv.lock`: Locked dependency versions (3,346+ lines)

5. **Development Tools & Documentation**
   - `Makefile`: Common development tasks
   - `.pre-commit-config.yaml`: Code quality hooks
   - `.env.example`: Environment configuration template
   - `README.md`: Comprehensive documentation (258 lines)
   - `submit_test_workflow.py`: Example workflow submission script
   - `submit_stac_items_notebook.ipynb`: Interactive notebook for workflow testing

6. **Supporting Files**
   - `.gitignore`: Standard Python/project exclusions
   - `LICENSE`: MIT license
   - `test_e2e_payload.json`: End-to-end test payload

## How I did it

### Repository Structure

```
data-pipeline/
├── scripts/                     # Core processing logic
│   ├── convert.py              # GeoZarr conversion
│   └── register.py             # STAC registration
├── legacy_workflows/           # Kubernetes manifests
│   ├── base/                   # Core workflow definitions
│   └── overlays/               # Environment-specific configs
├── docker/                     # Container build
├── .github/workflows/          # CI/CD automation
├── pyproject.toml             # Project dependencies & config
├── uv.lock                    # Locked dependencies
├── Makefile                   # Development automation
└── README.md                  # Comprehensive documentation
```

### Technical Architecture

- **Orchestration**: Argo Workflows on Kubernetes with Kustomize overlays
- **Event-Driven**: RabbitMQ triggers via EventSource → Sensor pattern
- **Processing**: Python 3.13 with eopf-geozarr, Dask, xarray
- **Storage**: S3-compatible object storage (OVH Cloud)
- **Catalog**: pgSTAC with TiTiler for visualization
- **Container**: Published to GitHub Container Registry

### Pipeline Flow

1. Submit STAC item URL via RabbitMQ or kubectl
2. EventSource receives message → Sensor triggers workflow
3. Convert step: Fetch STAC item → Extract zarr → Convert to GeoZarr → Upload to S3
4. Register step: Create STAC item with assets → Add TiTiler visualization links → Register to catalog

### Development Setup

- Package management via `uv` (Python package manager)
- Code quality: `ruff` (linting), `mypy` (type checking), `pre-commit` hooks
- Testing infrastructure: `pytest` with unit/integration test markers
- CI/CD: GitHub Actions for automated builds and tests

## How you can test it

### Prerequisites

1. **Infrastructure Access**:
   - Kubernetes cluster with platform-deploy (Argo Workflows, RabbitMQ, STAC API, TiTiler)
   - kubectl configured with cluster access
   - Python 3.13+ with `uv` installed
   - GDAL installed (MacOS: `brew install gdal`)

2. **Setup**:
   ```bash
   # Install dependencies
   make setup
   
   # Retrieve RabbitMQ password
   kubectl get secret rabbitmq-password -n core -o jsonpath='{.data.rabbitmq-password}' | base64 -d > .env
   
   # Port forward RabbitMQ
   kubectl port-forward -n core svc/rabbitmq 5672:5672 &
   ```

### Testing Methods

#### Method 1: Python Script (Event-Driven)
```bash
python submit_test_workflow.py
```

#### Method 2: Jupyter Notebook (Interactive)
```bash
jupyter notebook submit_stac_items_notebook.ipynb
```

#### Method 3: Direct kubectl (Bypass Events)
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
```

### Monitoring

1. **Argo Workflows UI**: https://argo.core.eopf.eodc.eu/workflows/devseed-staging
2. **STAC Browser**: https://api.explorer.eopf.copernicus.eu/stac
3. **TiTiler Viewer**: https://api.explorer.eopf.copernicus.eu/raster

```bash
# Watch workflows
kubectl get wf -n devseed-staging --watch

# View logs
kubectl logs -n devseed-staging -l workflows.argoproj.io/workflow=<name> --tail=100

# Check sensor status
kubectl logs -n devseed-staging -l sensor-name=geozarr-sensor --tail=50
```

### Expected Results

- **Duration**: ~15-20 minutes per item
- **Output**: Cloud-optimized GeoZarr stored in S3
- **Catalog**: STAC item registered with visualization links
- **Status**: Workflow shows "Succeeded" in Argo UI
- **Verification**: View processed data in TiTiler viewer

### Development Testing

```bash
# Run linting
make lint

# Run type checking
make typecheck

# Run tests (when implemented)
make test
```

## Additional Notes

### Supported Missions
- Sentinel-2 L2A (Multi-spectral optical imagery)
- Sentinel-1 GRD (SAR backscatter data)

### Resources
- **Container Image**: `ghcr.io/eopf-explorer/data-pipeline:latest`
- **Resource Limits**: 
  - Convert: 2 CPU cores, 8Gi memory, 3600s timeout
  - Register: 500m CPU, 2Gi memory, 600s timeout

### Related Projects
- [data-model](https://github.com/EOPF-Explorer/data-model): eopf-geozarr conversion library
- [platform-deploy](https://github.com/EOPF-Explorer/platform-deploy): Infrastructure deployment (Argo, RabbitMQ, STAC, TiTiler)
