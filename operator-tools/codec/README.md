# codec — Zarr codec debug tools

Temporary scripts for validating the Zarr V3 codec pipeline (scale-offset, cast-value, sharding).
Delete once the scale-offset codec bug is root-caused.

## Scripts

### `run_local_conversion.py`
Converts a locally downloaded `.zarr` store to GeoZarr and writes output locally (no S3 needed).


```bash
# Fast codec probe (~seconds) — slices r10m to 512×512 px, writes codec_probe.zarr:

uv run operator-tools/codec/run_local_conversion.py \
    S2C_MSIL2A_20260427T101021_N0512_R022_T33UWT_20260427T151616.zarr \
    --quick

# Fast probe with a custom output directory:

uv run operator-tools/codec/run_local_conversion.py path/to/scene.zarr \
    --quick --output-dir /tmp

# Full local conversion — writes ./<stem>_converted.zarr:

uv run operator-tools/codec/run_local_conversion.py \
    S2C_MSIL2A_20260427T101021_N0512_R022_T33UWT_20260427T151616.zarr

# Full conversion with more workers:

uv run operator-tools/codec/run_local_conversion.py path/to/scene.zarr \
    --n-workers 4 --memory-limit 16Gi --output-dir /tmp
```

After `--quick`, verify the codec chain:
```bash

uv run operator-tools/codec/check_zarr_codecs.py codec_probe.zarr/b02
```

### `check_zarr_codecs.py`
Reads `zarr.json` from a local path or S3 URL and prints the dtype + codec chain.


```bash
# Local (probe output or full local conversion):
uv run operator-tools/codec/check_zarr_codecs.py \
    operator-tools/codec/codec_probe.zarr/b02

# S3:
uv run operator-tools/codec/check_zarr_codecs.py \
    s3://esa-zarr-sentinel-explorer-s2-l2a-staging/converted/sentinel-2-l2a-staging-codecs/<scene>.zarr/measurements/reflectance/r10m/b02 \
    --endpoint-url https://s3.de.io.cloud.ovh.net --profile eopfexplorer
```
