# GeoZarr Pipeline Notebook

Interactive Python notebook demonstrating S3 access, GeoZarr visualization, and STAC metadata exploration.

## Purpose

The `01_quickstart.ipynb` notebook demonstrates:
- Cloud-optimized S3 access to GeoZarr datasets
- Embedded STAC metadata extraction
- RGB composite visualization
- Multi-resolution pyramid access

## Prerequisites

- Python 3.13+ with `uv` installed
- `kubectl` configured with cluster access (optional - for S3 credential auto-detection)
- AWS credentials for S3 access

## Setup

```bash
# From repo root, install all dependencies including notebook extras
uv sync --extra notebooks

# Configure S3 credentials (optional if using kubectl auto-detection)
cp .env.example .env
# Edit .env with your AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
```

## Usage

**VSCode:**
1. Open `01_quickstart.ipynb`
2. Select kernel: **"Python 3.13.x ('.venv': venv)"**
3. Run cells sequentially

**Jupyter Lab:**
```bash
cd notebooks/
uv run jupyter lab 01_quickstart.ipynb
```

**Command line:**
```bash
cd notebooks/
uv run jupyter notebook 01_quickstart.ipynb
```

## What It Covers

1. **S3 Credentials** - Auto-detection from kubectl or manual setup
2. **GeoZarr Loading** - Cloud-optimized access to multi-resolution pyramids
3. **STAC Metadata** - Embedded in Zarr attributes
4. **RGB Visualization** - True color composites with percentile stretching
5. **Geospatial Properties** - CRS, resolution, extent

## Troubleshooting

**Missing uv:**
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
# Or: brew install uv
```

**Missing notebook dependencies:**
```bash
uv sync --extra notebooks
```

**S3 access denied:**
Notebook auto-detects credentials from kubectl secret. If that fails:
```bash
export AWS_ACCESS_KEY_ID='your-key'
export AWS_SECRET_ACCESS_KEY='your-secret'
export AWS_ENDPOINT_URL='https://s3.de.io.cloud.ovh.net'
```

Or configure in `.env`:
```bash
cp .env.example .env
# Edit .env with credentials
```

**Kernel not found:**
Ensure venv is created and activated:
```bash
# From repo root
uv sync
source .venv/bin/activate  # or .venv/Scripts/activate on Windows
```

## Environment Variables

The notebook uses these (auto-detected from kubectl or `.env`):
- `AWS_ACCESS_KEY_ID` - S3 access key
- `AWS_SECRET_ACCESS_KEY` - S3 secret key
- `AWS_ENDPOINT_URL` - S3 endpoint (default: `https://s3.de.io.cloud.ovh.net`)
- `KUBECONFIG` - Path to kubeconfig (optional, for credential auto-detection)

## Related Documentation

- [Main README](../README.md) - Pipeline overview and workflow submission
- [STAC API](https://api.explorer.eopf.copernicus.eu/stac) - Browse collections and items
- [Raster Viewer](https://api.explorer.eopf.copernicus.eu/raster/viewer) - Interactive map tiles
