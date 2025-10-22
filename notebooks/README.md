# GeoZarr Pipeline Notebook

Interactive Python notebook demonstrating pipeline submission and monitoring for the EOPF GeoZarr Pipeline.

## Purpose

The `01_quickstart.ipynb` notebook demonstrates:
- Port-forward setup to RabbitMQ
- Payload submission via AMQP
- Workflow monitoring with kubectl
- STAC item verification after registration

## Prerequisites

- Jupyter/JupyterLab installed
- `kubectl` configured with cluster access (devseed-staging or devseed namespace)
- AWS credentials for S3 access
- Copy `.env.example` to `.env` and configure

## Setup

```bash
# Install base dependencies (from repo root)
uv sync

# Install notebook visualization (matplotlib only missing dependency)
uv pip install matplotlib

# Configure S3 credentials (optional - notebook auto-detects from kubectl)
cp .env.example .env
# Edit .env if not using kubectl auto-detection
```

## Usage

**VSCode:** Open `01_quickstart.ipynb` → Select kernel **"Python 3.11.x ('.venv': venv)"**

**Jupyter Lab:**
```bash
uv run jupyter lab 01_quickstart.ipynb
```

## What It Covers

1. **Port-Forward Setup** - Connect to RabbitMQ in the cluster
2. **Payload Submission** - Publish AMQP message to trigger workflow
3. **Workflow Monitoring** - Watch Argo Workflow execution via kubectl
4. **STAC Verification** - Check registered STAC item with TiTiler previews

## Troubleshooting

**Import errors (matplotlib):**
```bash
uv pip install matplotlib
```

**S3 access denied:**
Notebook auto-detects credentials from kubectl. If that fails:
```bash
export AWS_ACCESS_KEY_ID='your-key'
export AWS_SECRET_ACCESS_KEY='your-secret'
```

For full pipeline documentation, see [../README.md](../README.md).

## Related Documentation

- [Main README](../README.md) - Pipeline overview and workflow submission
