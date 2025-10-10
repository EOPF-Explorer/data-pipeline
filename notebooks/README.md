# GeoZarr Notebooks

Interactive tutorials demonstrating cloud-optimized GeoZarr data access, visualization, and performance analysis.

## Quick Start

**Setup** (from repository root):
```bash
uv sync --extra notebooks
```

**Run notebooks:**
- **VSCode:** Open notebook → Select kernel **"Python 3.11.x ('.venv': venv)"**
- **Jupyter Lab:** `uv run jupyter lab`

**S3 credentials** (auto-detected from Kubernetes or set manually):
```bash
export AWS_ACCESS_KEY_ID="your-key"
export AWS_SECRET_ACCESS_KEY="your-secret"
export AWS_ENDPOINT_URL="https://s3.gra.cloud.ovh.net"
```

See `.env.example` for configuration options.

## Notebooks

| Notebook | Learn About | Time |
|----------|-------------|------|
| **01_quickstart.ipynb** | Load S3 datasets, inspect STAC metadata, visualize RGB composites | 5 min |
| **02_pyramid_performance.ipynb** | Quantify pyramid value: 3-5× speedup, 33% storage overhead, ROI analysis | 15 min |
| **03_multi_resolution.ipynb** | Direct pyramid access (levels 0-3), 64× size reduction use cases | 10 min |
| **operator.ipynb** | Internal cluster utilities | - |

## Key Learnings

**01_quickstart.ipynb** - GeoZarr basics:
- Cloud-optimized Zarr format with embedded STAC metadata
- Multi-resolution pyramids (10980→1372 pixels, levels 0-3)
- Direct S3 access with lazy loading (no full download)
- RGB visualization with percentile stretch

**02_pyramid_performance.ipynb** - Performance validation:
- Measures tile serving latency with/without pyramids
- Quantifies 3-5× speedup at zoom levels 6-10
- Calculates 33% storage overhead (geometric series)
- Provides production deployment recommendations

**03_multi_resolution.ipynb** - Pyramid mechanics:
- Direct access to each pyramid level (0=native, 3=lowest)
- Size reduction: 4.7MB→72KB (64×) from level 0→3
- Use case guidance: full-resolution analysis vs fast preview
- Memory-efficient visualization at different scales

## Next Steps

- **Run the pipeline:** Convert your own Sentinel data ([GETTING_STARTED.md](../GETTING_STARTED.md))
- **Submit workflows:** Programmatic job submission ([examples/README.md](../examples/README.md))
- **Explore data:** STAC API at `https://api.explorer.eopf.copernicus.eu/stac`
- **Visualize online:** Raster viewer at `https://api.explorer.eopf.copernicus.eu/raster/viewer`

## Troubleshooting

### Kernel Not Found
If the Python kernel doesn't appear:
```bash
uv sync --extra notebooks
```

### Import Errors
Make sure you've installed notebook dependencies:
```bash
uv pip list | grep -E "(ipykernel|matplotlib|numpy)"
```

### S3 Access Denied
Check your AWS credentials are set:
```bash
env | grep AWS
```

Or use anonymous access for public datasets:
```python
ds = xr.open_zarr(s3_url, storage_options={'anon': True})
```

## Related Documentation

- [Main README](../README.md) - Pipeline overview
- [Getting Started](../GETTING_STARTED.md) - Complete setup guide
- [Examples](../examples/README.md) - CLI workflow submission
