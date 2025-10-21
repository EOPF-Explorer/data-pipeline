# Sentinel-1 GRD Pipeline

Quick guide to process Sentinel-1 Ground Range Detected (GRD) data through the GeoZarr pipeline.

## Quick Start

```bash
# Local conversion
python examples/s1_quickstart.py

# Or run the full workflow on cluster
kubectl apply -f workflows/examples/run-s1-test.yaml -n devseed-staging
```

## S1 vs S2 Differences

| Feature | Sentinel-2 L2A | Sentinel-1 GRD |
|---------|----------------|----------------|
| **Groups** | `/quality/l2a_quicklook/r10m` | `/measurements` |
| **Extra flags** | `--crs-groups /quality/...` | `--gcp-group /conditions/gcp` |
| **Chunk size** | 4096 | 2048 |
| **Polarizations** | RGB bands | VH, VV, HH, HV |
| **Preview query** | True color formula | Single-band grayscale |

## Collection Registry

S1 config in `scripts/get_conversion_params.py`:

```python
"sentinel-1-l1-grd": {
    "pattern": "sentinel-1-l1-grd*",
    "conversion": {
        "groups": "/measurements",
        "extra_flags": "--gcp-group /conditions/gcp",
        "spatial_chunk": 2048,
        "tile_width": 512,
    },
}
```

## Preview Generation

S1 items get grayscale preview with polarization detection:

```python
# Auto-detects VH/VV/HH/HV from assets
variables=/S01SIWGRD_..._VH/measurements:grd&bidx=1&rescale=0,219
```

See `scripts/augment_stac_item.py:_encode_s1_preview_query()` for implementation.

## Test Data

EODC STAC has S1 test items:
```bash
curl "https://stac.core.eopf.eodc.eu/collections/sentinel-1-l1-grd/items?limit=5"
```

## Workflow Parameters

```yaml
arguments:
  parameters:
    - name: source_url
      value: "https://stac.core.eopf.eodc.eu/collections/sentinel-1-l1-grd/items/S1C_..."
    - name: item_id
      value: "S1C_IW_GRDH_20251008_test"
    - name: register_collection
      value: "sentinel-1-l1-grd-dp-test"
```

## Known Issues

- GCP reprojection can fail for some S1 tiles (data-model issue)
- Memory requirements higher than S2 (recommend 16GB limit)
- TiTiler rendering needs polarization-specific rescaling

## Next Steps

- Add S1 benchmarks to compare with S2 performance
- Document optimal chunk sizes for different S1 modes (IW/EW/SM)
- Add S1-specific validation rules
