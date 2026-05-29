# Session Handoff — S1 GRD RTC End-to-End Pipeline

**Date:** 2026-05-29
**Branch:** `feat--s1_grd_phase5`
**Goal:** Get the complete S1 GRD RTC pipeline working end-to-end, including TiTiler rendering.

> ## ✅ RESOLVED 2026-05-29 (later session)
> **The TiTiler 500 is fixed and live-verified.** Root cause was NOT `tile_matrix_limits`
> (a red herring — deployed titiler-eopf **v0.5.0** is a GeoZarr-V0 reader that never reads it).
> Real cause: S1 stores carry only the geozarr `proj:code` attr and **no CF `grid_mapping`/`spatial_ref`
> coordinate**, so `rioxarray.rio.crs` is `None`; v0.5.0's `_validate_zarr` rejects every group →
> reader builds 0 groups → `zip(*[])` ("expected 4, got 0") / the `tile_matrix_set` access fails.
> **Fix:** `ingest_v1_s1_rtc.py::_patch_cf_grid_mapping` writes a CF `spatial_ref` coord + `grid_mapping`
> into every (y,x) sub-group (like S2). Proven live on a fresh **31TDH** store: `/info` + `/preview` +
> XYZ tiles all HTTP 200 with `crs=EPSG:32631`.
> **Also found (I-8):** TiTiler reads `s3://esa-zarr-sentinel-explorer-fra/tests-output/{collection}/{item}.zarr`,
> not the STAC asset href — the store must live there for TiTiler to find it.
> See `claude-docs/plans/subissue_4_end_to_end.md` (Tasks 4.5/4.6, issues I-7/I-8) for full detail.
> The section below is the original (pre-resolution) investigation, kept for history.

---

## Current State Summary

### What Works ✅
| Item | Status |
|------|--------|
| Script A (`run_s1tiling.py`) | Exit 0 — 14 GeoTIFFs cached to S3 |
| Script B (`run_ingest_register.py`) | Exit 0 — zarr uploaded, STAC registered HTTP 201 |
| STAC item registered | `s1-rtc-31TCH` in `sentinel-1-grd-rtc-staging` |
| Asset hrefs use HTTPS gateway | `https://s3.explorer.eopf.copernicus.eu/...` |
| S3 zarr store | All 6 resolution levels (r10m–r720m) present |
| `tile_matrix_limits` patch in ingest script | Added to `scripts/ingest_v1_s1_rtc.py` |
| `tile_matrix_limits` patched on S3 store | Applied directly to existing store |

### What Is Still Broken ❌
| Item | Status |
|------|--------|
| TiTiler `/info?assets=vh` | HTTP 500, detail: `"'tile_matrix_set'"` |
| TiTiler `/preview` | HTTP 500 |
| TiTiler tile rendering | Untested (blocked by above) |

---

## The Active Bug: TiTiler 500 Error

### Error progression (debugging trail)
1. **Before any patch:** `"not enough values to unpack (expected 4, got 0)"`
   → Root cause: `tile_matrix_limits` key was completely absent from S1 zarr `multiscales`

2. **After adding `tile_matrix_limits` to S3 zarr.json:** `"'tile_matrix_set'"`
   → A `KeyError: 'tile_matrix_set'` now occurs somewhere in TiTiler's server code

### What was patched on S3
Both files were updated:
- `s3://esa-zarr-sentinel-explorer-tests/s1-rtc-test/s1-grd-rtc-31TCH.zarr/descending/zarr.json`
- `s3://esa-zarr-sentinel-explorer-tests/s1-rtc-test/s1-grd-rtc-31TCH.zarr/zarr.json` (consolidated root)

The added `tile_matrix_limits` (all zeros because S1 uses single-chunk full-image arrays, `matrixWidth=1`):
```json
{
  "r10m": {"tileMatrix": "r10m", "minTileCol": 0, "maxTileCol": 0, "minTileRow": 0, "maxTileRow": 0},
  "r20m": {"tileMatrix": "r20m", "minTileCol": 0, "maxTileCol": 0, "minTileRow": 0, "maxTileRow": 0},
  "r60m": ..., "r120m": ..., "r360m": ..., "r720m": ...
}
```

### Current active error: `"'tile_matrix_set'"`
This is a Python `KeyError: 'tile_matrix_set'`. TiTiler server-side code is calling `something['tile_matrix_set']` where `something` doesn't have that key.

**Key observations so far:**
- S2 tile URL uses **only** `variables=`, NO `assets=` parameter
- S1 tile URL (our code) uses **both** `variables=` AND `assets=vh` — removing `assets=` didn't help
- Local pydantic parse of S1 multiscales (including `tile_matrix_limits`) succeeds fine
- S2 `reflectance/r10m/zarr.json` has far more attributes than S1 `descending/r10m/zarr.json`:
  - S2 r10m has: `spatial:bbox`, `spatial:dimensions`, `spatial:registration`, `grid_mapping`, `zarr_conventions`
  - S1 r10m has: only `spatial:shape`, `spatial:transform`, `proj:code`
- The `KeyError: 'tile_matrix_set'` could come from TiTiler trying to access `multiscales['tile_matrix_set']` on a **resolution sub-group** (like `r10m/zarr.json`) which has NO `multiscales` attribute at all

### Most likely remaining hypotheses (in order of likelihood)

**Hypothesis A (most likely):** TiTiler's server-side geozarr reader iterates over `tile_matrix_limits` entries, then for each resolution level opens the sub-group and calls something like `attrs['multiscales']['tile_matrix_set']` — but S1 resolution sub-groups (`r10m/`, `r20m/`, ...) have no `multiscales` attribute.

**Hypothesis B:** TiTiler's `eopf_geozarr` version differs from our `.venv` v1.8.1 and has a different model that accesses `tile_matrix_set` differently.

**Hypothesis C:** The `tile_matrix_limits` all-zero values (`maxTileCol=0, maxTileRow=0`) cause TiTiler to compute a zero-area bounding box. But this would be a ValueError/compute error, not `KeyError: 'tile_matrix_set'`.

> **Outcome (2026-05-29):** Hypothesis B was correct in spirit (version-specific behaviour), but the
> trigger was the missing CF CRS, not the multiscales model. See the RESOLVED banner at the top.

### Next debugging steps

1. **Try alternative variable paths for the TiTiler tile URL.** The current S1 variable path is `/descending:vh`. S2 uses `/measurements/reflectance:b04`. Try:
   - `/descending/r10m:vh` (explicit resolution level)
   - `/descending/r120m:vh` (coarser resolution)

2. **Check if TiTiler needs a `vh` STAC asset pointing to the root zarr store** (not the `descending` group). Currently `vh.href` = `...zarr/descending`. Try re-registering with `vh.href` = `...zarr/` (root) and variable `/descending:vh`.

3. **Check S1 resolution sub-group zarr.json files** — do they need `multiscales` or additional attributes? Compare with S2's `reflectance/r10m/zarr.json` which has `zarr_conventions`, `spatial:bbox`, `spatial:dimensions`, etc.

4. **Re-read `eopf_geozarr.data_api.geozarr.multiscales.geozarr.MultiscaleGroupAttrs`** — understand exactly when `_tms_multiscales` is populated and what TiTiler does when `tile_matrix_set` is MISSING vs present.

5. **Check TiTiler `eopf_geozarr` server version** — if the server uses an older version, the model may differ. Try checking the `/raster/` endpoint for version info. *(This is what resolved it — server is titiler-eopf v0.5.0.)*

---

## Key Infrastructure

| Resource | Value |
|----------|-------|
| S3 bucket | `esa-zarr-sentinel-explorer-tests` |
| S3 endpoint | `https://s3.de.io.cloud.ovh.net` |
| AWS profile | `eopfexplorer` |
| STAC API | `https://api.explorer.eopf.copernicus.eu/stac` |
| TiTiler raster API | `https://api.explorer.eopf.copernicus.eu/raster` |
| S3 HTTPS gateway | `https://s3.explorer.eopf.copernicus.eu/` |
| S1 zarr store S3 path | `s3://esa-zarr-sentinel-explorer-tests/s1-rtc-test/s1-grd-rtc-31TCH.zarr` |
| STAC collection | `sentinel-1-grd-rtc-staging` |
| STAC item ID | `s1-rtc-31TCH` |
| **TiTiler store path (actual, I-8)** | `s3://esa-zarr-sentinel-explorer-fra/tests-output/{collection}/{item_id}.zarr` |

---

## STAC Item State

The registered STAC item `s1-rtc-31TCH` has:
- `vh` asset: `https://s3.explorer.eopf.copernicus.eu/esa-zarr-sentinel-explorer-tests/s1-rtc-test/s1-grd-rtc-31TCH.zarr/descending`
- `vv` asset: same (different polarisation label)
- `zarr-store` link: `https://s3.explorer.eopf.copernicus.eu/esa-zarr-sentinel-explorer-tests/s1-rtc-test/s1-grd-rtc-31TCH.zarr`
- Current tile URL template (xyz link in STAC):
  `https://api.explorer.eopf.copernicus.eu/raster/collections/sentinel-1-grd-rtc-staging/items/s1-rtc-31TCH/tiles/WebMercatorQuad/{z}/{x}/{y}.png?variables=%2Fdescending%3Avh&bidx=1&rescale=0%2C219&assets=vh`
- Current tilejson URL:
  `https://api.explorer.eopf.copernicus.eu/raster/collections/sentinel-1-grd-rtc-staging/items/s1-rtc-31TCH/WebMercatorQuad/tilejson.json?variables=%2Fdescending%3Avh&bidx=1&rescale=0%2C219&assets=vh`

---

## Code Changes Made This Session

### `scripts/ingest_v1_s1_rtc.py`
Added `_patch_tile_matrix_limits()` function and call it as Step 6 after consolidation:

```python
import zarr  # added to imports

def _patch_tile_matrix_limits(store_path: str, orbit_direction: str) -> None:
    """Inject tile_matrix_limits into the orbit group's multiscales attribute.
    eopf_geozarr.conversion.s1_ingest.create_s1_store omits tile_matrix_limits
    from the multiscales metadata, but TiTiler requires it to compute tile bounds.
    S1 stores each resolution as a single full-image zarr chunk (matrixWidth=1,
    matrixHeight=1), so maxTileCol and maxTileRow are always 0.
    """
    store = zarr.open_group(store_path, mode="r+", zarr_format=3)
    orbit_group = store[orbit_direction]
    ms = dict(orbit_group.attrs).get("multiscales", {})
    if "tile_matrix_limits" in ms:
        return  # already present
    layout = ms.get("layout", [])
    tile_matrix_limits = {
        entry["asset"]: {
            "tileMatrix": entry["asset"],
            "minTileCol": 0,
            "maxTileCol": 0,
            "minTileRow": 0,
            "maxTileRow": 0,
        }
        for entry in layout
    }
    ms["tile_matrix_limits"] = tile_matrix_limits
    orbit_group.attrs["multiscales"] = ms

# In ingest_all():
    # Step 5 -- consolidate
    consolidate_s1_store(store_path, orbit_direction)
    # Step 6 -- patch tile_matrix_limits (omitted by eopf_geozarr.s1_ingest)
    _patch_tile_matrix_limits(store_path, orbit_direction)
    consolidate_s1_store(store_path, orbit_direction)
```

### Root cause: `eopf_geozarr.conversion.s1_ingest` library bug
The library file `.venv/lib/python3.14/site-packages/eopf_geozarr/conversion/s1_ingest.py` does NOT call `_create_tile_matrix_limits()` (which exists in `geozarr.py` for S2). This is the upstream library bug. The `_patch_tile_matrix_limits()` function in the pipeline is a workaround until the library is fixed.

> **Update (2026-05-29):** `tile_matrix_limits` turned out NOT to matter for the deployed
> titiler-eopf v0.5.0 (GeoZarr-V0 reader; never reads it). The real fix was
> `_patch_cf_grid_mapping` (CF `spatial_ref`). Both patches now run in Step 6.

---

## Quick Validation Commands

```bash
# Activate env
cd /Users/lhoupert/DevDS/EOPF/data-pipeline && source .venv/bin/activate

# Check S3 zarr has tile_matrix_limits
python3 -c "
import s3fs, json
fs = s3fs.S3FileSystem(profile='eopfexplorer', endpoint_url='https://s3.de.io.cloud.ovh.net')
with fs.open('esa-zarr-sentinel-explorer-tests/s1-rtc-test/s1-grd-rtc-31TCH.zarr/descending/zarr.json') as f:
    d = json.load(f)
ms = d['attributes']['multiscales']
print('keys:', list(ms.keys()))
print('tile_matrix_limits present:', 'tile_matrix_limits' in ms)
"

# Test TiTiler info endpoint
curl -s "https://api.explorer.eopf.copernicus.eu/raster/collections/sentinel-1-grd-rtc-staging/items/s1-rtc-31TCH/info?assets=vh" | python3 -c "import json,sys; print(json.dumps(json.load(sys.stdin),indent=2))"

# Test TiTiler preview (should return PNG if working)
curl -o /tmp/s1_preview.png "https://api.explorer.eopf.copernicus.eu/raster/collections/sentinel-1-grd-rtc-staging/items/s1-rtc-31TCH/preview?format=png&variables=%2Fdescending%3Avh&bidx=1&rescale=0%2C219&assets=vh"
file /tmp/s1_preview.png

# Compare S2 STAC item structure (S2 works, S1 doesn't — compare these)
curl -s "https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-2-l2a/items?limit=1" | python3 -c "
import json,sys; d=json.load(sys.stdin); item=d['features'][0]
print('S2 asset hrefs:')
for k,v in item['assets'].items(): print(f'  {k}: {v.get(\"href\",\"\")[:80]}')
"

# Check S1 STAC item
curl -s "https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-1-grd-rtc-staging/items/s1-rtc-31TCH" | python3 -c "
import json,sys; d=json.load(sys.stdin)
for k,v in d['assets'].items(): print(f'{k}: {v.get(\"href\",\"\")[:80]}')
for l in d.get('links',[]):
    if l['rel'] in ('xyz','tilejson','viewer'): print(l['rel'],':', l['href'][:120])
"
```

---

## S1 Zarr Store Structure

```
s1-grd-rtc-31TCH.zarr/
├── zarr.json                    ← consolidated root (66 KB)
└── descending/
    ├── zarr.json                ← orbit group attrs: zarr_conventions, multiscales, proj:, spatial:
    │                              multiscales keys: layout, resampling_method, tile_matrix_set, tile_matrix_limits ← PATCHED
    ├── r10m/                    ← shape: 3×10980×10980 (3 acquisitions)
    │   ├── zarr.json            ← attrs: spatial:shape, spatial:transform, proj:code ONLY (no multiscales!)
    │   ├── vv/, vh/, border_mask/
    │   ├── time/, absolute_orbit/, relative_orbit/, platform/
    ├── r20m/, r60m/, r120m/, r360m/, r720m/   ← overview levels
    └── conditions/
        ├── gamma_area_008/, gamma_area_037/, gamma_area_110/
```

**S1 r10m/zarr.json attrs:** `["spatial:shape", "spatial:transform", "proj:code"]`
**S2 r10m/zarr.json attrs:** `["grid_mapping", "spatial:dimensions", "spatial:registration", "spatial:bbox", "spatial:transform", "spatial:shape", "proj:code", "zarr_conventions"]`

This structural difference may be relevant to the ongoing TiTiler 500 error.
*(2026-05-29: it WAS the cause — S1 lacked the CF `spatial_ref` coord that gives rioxarray a CRS.)*

---

## S1 Zarr `tile_matrix_set` (native CRS 32631)

```json
{
  "id": "Native_CRS_32631",
  "crs": "http://www.opengis.net/def/crs/EPSG/0/32631",
  "tileMatrices": [
    {"id": "r10m",  "matrixWidth": 1, "matrixHeight": 1, "tileWidth": 10980, "tileHeight": 10980, "cellSize": 10.0},
    {"id": "r20m",  "matrixWidth": 1, "matrixHeight": 1, "tileWidth": 5490,  "tileHeight": 5490,  "cellSize": 20.0},
    {"id": "r60m",  "matrixWidth": 1, "matrixHeight": 1, "tileWidth": 1830,  "tileHeight": 1830,  "cellSize": 60.0},
    {"id": "r120m", "matrixWidth": 1, "matrixHeight": 1, "tileWidth": 915,   "tileHeight": 915,   "cellSize": 120.0},
    {"id": "r360m", "matrixWidth": 1, "matrixHeight": 1, "tileWidth": 305,   "tileHeight": 305,   "cellSize": 360.0},
    {"id": "r720m", "matrixWidth": 1, "matrixHeight": 1, "tileWidth": 153,   "tileHeight": 153,   "cellSize": ~718.0}
  ]
}
```

`spatial:bbox` (native CRS 32631): `[299999.9999974121, 4690199.99999915, 409799.9999974121, 4799999.99999915]`

---

## Useful File Locations

| File | Purpose |
|------|---------|
| `scripts/ingest_v1_s1_rtc.py` | ← MODIFIED this session (added `_patch_tile_matrix_limits`; later `_patch_cf_grid_mapping`) |
| `scripts/register_v1_s1_rtc.py` | Builds + registers STAC item (I-6 fix already in place) |
| `scripts/register_v1.py` | Shared register helpers: `add_visualization_links`, `warm_thumbnail_cache` |
| `scripts/run_ingest_register.py` | Main pipeline entry point (Script B) |
| `scripts/run_s1tiling.py` | S1Tiling runner (Script A) |
| `.venv/lib/python3.14/site-packages/eopf_geozarr/conversion/s1_ingest.py` | Library bug: missing `tile_matrix_limits` + CF `grid_mapping` generation |
| `.venv/lib/python3.14/site-packages/eopf_geozarr/data_api/geozarr/multiscales/geozarr.py` | `MultiscaleGroupAttrs` — parses multiscales metadata |
| `.venv/lib/python3.14/site-packages/eopf_geozarr/data_api/geozarr/multiscales/tms.py` | `TileMatrixSet`, `TileMatrixLimit`, `Multiscales` pydantic models |
| `claude-docs/plans/subissue_4_end_to_end.md` | Main plan doc (authoritative; Tasks 4.5/4.6, issues I-7/I-8) |
| `/tmp/patch_s1_tile_limits.py` | One-off S3 patch script used this session |

---

## Plan Document Status

`claude-docs/plans/subissue_4_end_to_end.md` is now updated with:
- I-7: Missing CF `grid_mapping` in S1 zarr (root cause found, pipeline fix applied, live-verified)
- I-8: TiTiler store-path convention mismatch (`fra/tests-output/{collection}/{item}.zarr`)

---

## Tests

Run before committing any changes:
```bash
cd /Users/lhoupert/DevDS/EOPF/data-pipeline
uv run pytest tests/ -x -q
```

Unit tests for the ingest script are in `tests/unit/test_ingest_v1_s1_rtc.py` — they cover both
`_patch_tile_matrix_limits` (mocked in orchestration tests) and `_patch_cf_grid_mapping`
(real-store tests asserting `rio.crs == EPSG:32631`).
