# Technical Priorities (Actionable)

**Focus:** Production-ready data-pipeline
**Date:** 2025-10-18

---

## Status: In Progress

### ✅ Completed

1. **Validation** (feat/validation - commit 1e630e0)
   - validate_geozarr.py validates STAC/TMS/CF specs
   - Dependencies added: pystac-client, morecantile, cf-xarray

2. **pystac-client example** (feat/stac-client - commit 9e33fca)
   - register_stac_new.py shows validation + pystac-client usage
   - 85 LOC vs 200+ manual HTTP

### 🚧 Next Up

3. **Refactor augment** (feat/stac-extensions - NOT STARTED)
   - Use ProjectionExtension for projection metadata
   - Use RasterExtension for band info
   - Target: 1200 LOC → ~200 LOC

4. **Integration tests** (test/validation-integration - NOT STARTED)
   - S1 GRD end-to-end with validation
   - S2 L2A end-to-end with validation

---

## Branch Strategy

**Rule:** One functional change per branch

```
feat/prometheus-metrics-integration (base)
  → feat/validation (DONE)
  → feat/stac-client (DONE)
  → feat/stac-extensions (NEXT)
  → test/validation-integration (AFTER)
```

**Per-branch checklist:**
- [ ] One file or one function changed
- [ ] Tests added/updated
- [ ] CHANGELOG.md updated
- [ ] Commit message: `feat/fix: what changed`

---

## The Real Issues

### Missing in data-pipeline

1. **No spec validation** → Added in feat/validation ✅
2. **Manual HTTP instead of pystac-client** → Example in feat/stac-client ✅
3. **1200-line augment script** → Needs refactor (feat/stac-extensions)
4. **No integration tests** → Needs addition (test/validation-integration)

### Missing in data-model (eopf-geozarr)

1. **1636-line monolith** (geozarr.py) → Needs decomposition
2. **Custom pyramid generation** → Could use GDAL
3. **Manual CF-conventions** → Could use cf-xarray more
4. **TODO comments** → Need resolution
5. **Warning suppression** → Need removal

**Note:** data-model issues are separate. Focus on data-pipeline first.

---

## Critical Fixes Needed

### 1. Add Spec Validation (BLOCKING)

**Problem:** Code claims "GeoZarr-spec 0.4 compliant" but never validates outputs.

**Fix:**
```python
# Add to data-pipeline/scripts/validate_geozarr.py

from morecantile import TileMatrixSet
from pystac import Item

def validate_geozarr_output(zarr_path, stac_item_path):
    """Validate GeoZarr + STAC outputs before declaring success."""

    # 1. Validate STAC item
    item = Item.from_file(stac_item_path)
    item.validate()  # Raises if non-compliant

    # 2. Validate TileMatrixSet
    zarr_attrs = zarr.open(zarr_path).attrs
    tms = TileMatrixSet.parse_obj(zarr_attrs["tile_matrix_set"])
    tms.validate()  # OGC TMS spec compliance

    # 3. Validate CF-conventions
    import cf_xarray as cfxr
    ds = xr.open_zarr(zarr_path)
    ds.cf.decode()  # Raises if non-CF-compliant
```

**Impact:** Catch spec violations before they hit production.

**Effort:** 1 day

---

### 2. Replace Manual STAC Transactions

**Problem:** `register_stac.py` does manual HTTP POST/PUT with no retry logic or validation.

**Current (200 lines):**
```python
response = httpx.post(
    f"{stac_api}/collections/{collection}/items",
    json=item_dict,
    headers={"Content-Type": "application/json"}
)
if response.status_code == 409:
    # Manual conflict handling...
```

**Fixed (20 lines):**
```python
from pystac_client import Client

client = Client.open(stac_api_url)
item = Item.from_dict(item_dict)
item.validate()  # Validate before sending
client.add_item(item, collection_id)  # Built-in retries + error handling
```

**Impact:**
- Automatic retry on transient failures
- STAC spec validation built-in
- Handles pagination/transactions properly

**Effort:** 3 days

---

### 3. Stop Reinventing Pyramid Generation

**Problem:** `geozarr.py:580-710` has 200+ lines recreating COG overview logic.

**Current approach:**
```python
# Custom /2 downsampling with manual block averaging
for level in range(1, num_levels):
    downsampled = downsample_2d_array(
        source_data,
        factor=2,
        nodata=nodata_value
    )
```

**Use GDAL instead:**
```python
from osgeo import gdal

ds = gdal.Open(input_path, gdal.GA_Update)
ds.BuildOverviews("AVERAGE", [2, 4, 8, 16])  # Done
```

**Why GDAL:**
- 10x faster (C++ + SIMD)
- Handles all edge cases (partial tiles, nodata, mixed dtypes)
- Used by entire geospatial industry
- Battle-tested for 20+ years

**Impact:** Replace 200 lines of fragile Python with 5 lines of GDAL.

**Effort:** 1 week (requires testing pyramid correctness)

---

### 4. Fix CF-Conventions Manual Metadata

**Problem:** `geozarr.py:150-240` manually constructs CF attributes instead of using cf-xarray.

**Current:**
```python
# 90 lines of manual attribute setting
ds[var_name].attrs["standard_name"] = "toa_bidirectional_reflectance"
ds[var_name].attrs["_ARRAY_DIMENSIONS"] = list(ds[var_name].dims)
spatial_ref_var = xr.DataArray(data=0, attrs={...})
```

**Use cf-xarray:**
```python
import cf_xarray as cfxr

ds = ds.cf.add_bounds("x")
ds = ds.cf.add_bounds("y")
ds = ds.rio.write_crs("EPSG:4326")  # Handles grid_mapping
ds.cf.decode()  # Validate CF compliance
```

**Impact:**
- Validates against CF standard
- Fewer bugs from typos
- cf-xarray is already in dependencies!

**Effort:** 3 days

---

### 5. Simplify S1 Reprojection (378 Lines → 30)

**Problem:** `sentinel1_reprojection.py` is a 378-line wrapper around rasterio.

**Current:**
```python
def reproject_sentinel1_with_gcps(ds, ds_gcp, target_crs):
    gcps = _create_gcps_from_dataset(ds_gcp)  # 50 lines
    transform, width, height = calculate_default_transform(...)  # 40 lines
    reprojected = _reproject_data_variable(...)  # 60 lines
    # ... 200 more lines
```

**Rasterio already does this:**
```python
from rasterio.warp import reproject, Resampling

src.gcps = (gcps, src.crs)
reproject(
    source=src.read(1),
    destination=dst_array,
    src_transform=src.transform,
    dst_crs="EPSG:4326",
    resampling=Resampling.bilinear
)
```

**Impact:**
- 12x code reduction
- Better tested (used ecosystem-wide)
- Fewer edge case bugs

**Effort:** 1 week (requires S1 regression testing)

---

## Known Issues in Production Code

### TODO Comments (Technical Debt)

**Found 8 TODOs in core conversion logic:**

```python
# geozarr.py:576
# TODO: check GCP bounds vs. raster data bounds?
# → No validation that GCPs cover raster extent

# geozarr.py:921
# TODO: use a better way to determine this than just checking for ds_gcp
# → Sentinel-1 detection is fragile

# geozarr.py:966
# TODO: refactor? grid mapping attributes and variables are handled
# → Acknowledged CF-conventions debt
```

**Impact:** These indicate incomplete design that needs fixing before scale.

---

### Warning Suppression (cli.py:24-29)

**Problem:** Blanket suppression hiding real issues:

```python
warnings.filterwarnings("ignore", message=".*", category=FutureWarning)
warnings.filterwarnings("ignore", message=".*", category=UserWarning)
warnings.filterwarnings("ignore", message=".*", category=RuntimeWarning)
```

**Why this is bad:**
- Zarr v3 breaking changes won't be visible
- xarray deprecations will go unnoticed
- Runtime issues get hidden

**Fix:** Remove blanket filters, address warnings individually.

---

### Zarr v3 Instability Risk

**Problem:**
```toml
# pyproject.toml
zarr>=3.1.1  # Experimental v3 API
```

Zarr v3 is alpha software (released 2024). Breaking changes expected.

**Evidence:** Warning suppression suggests compatibility issues already present.

**Fix:**
- Pin exact commit hash: `zarr @ git+https://github.com/zarr-developers/zarr-python@abc123`
- Add integration tests for each Zarr v3 update
- Monitor upstream releases weekly

---

## What data-pipeline Actually Needs

### Priority 1: Validation (1 Week)

**Add these checks to every workflow run:**

```python
# scripts/validate_geozarr.py (enhanced)

def validate_outputs(zarr_path, stac_item_path):
    """Validate before declaring success."""

    # 1. STAC spec compliance
    item = pystac.Item.from_file(stac_item_path)
    item.validate()

    # 2. TileMatrixSet OGC compliance
    import zarr
    from morecantile import TileMatrixSet

    zarr_attrs = zarr.open(zarr_path).attrs
    tms = TileMatrixSet.parse_obj(zarr_attrs["tile_matrix_set"])
    tms.validate()

    # 3. CF-conventions compliance
    import xarray as xr
    import cf_xarray as cfxr

    ds = xr.open_zarr(zarr_path)
    ds.cf.decode()  # Raises if non-compliant

    print("✓ All validations passed")
```

Call this at end of convert step in Argo workflow.

---

### Priority 2: Use pystac-client (3 Days)

**Replace `register_stac.py` manual HTTP:**

```python
# Current: 200 lines of httpx POST/PUT/DELETE
response = httpx.post(f"{stac_api}/collections/{collection}/items", ...)

# Fixed: 20 lines with proper library
from pystac_client import Client

client = Client.open(stac_api_url)
item = pystac.Item.from_dict(item_dict)
item.validate()  # Validate before sending!
client.add_item(item, collection_id)
```

**Benefits:**
- Built-in retry logic
- STAC spec validation
- Extension support
- Pagination handling

---

### Priority 3: Fix augment_stac_item.py (1 Week)

**Problem:** 1,200 lines of ad-hoc STAC manipulation.

**Fix:** Use STAC extension framework:

```python
from pystac.extensions.projection import ProjectionExtension
from pystac.extensions.raster import RasterExtension

# Instead of manual attribute setting
proj_ext = ProjectionExtension.ext(item, add_if_missing=True)
proj_ext.epsg = 4326
proj_ext.bbox = compute_bbox(item)

raster_ext = RasterExtension.ext(item.assets["data"], add_if_missing=True)
raster_ext.bands = [...]
```

Target: Reduce to ~200 lines using extensions properly.

---

### Priority 4: Add Integration Tests (1 Week)

**Current state:** No end-to-end tests for core flows.

**Required tests:**

```python
# tests/test_e2e_s1_grd.py
def test_s1_grd_to_geozarr():
    """Full S1 GRD conversion + validation."""
    input_zarr = "s3://eodc/S1A_IW_GRDH_..."
    output = convert_geozarr(input_zarr)

    # Validate outputs
    assert validate_geozarr(output)
    assert validate_stac_item(output + "/item.json")
    assert check_pyramid_correctness(output)

# tests/test_e2e_s2_l2a.py
def test_s2_l2a_to_geozarr():
    """Full S2 L2A conversion + validation."""
    # Similar structure
```

Add to CI, run on every PR.

---

## data-model Technical Priorities

### 1. Use GDAL for Pyramids (1 Week)

**Replace:** `geozarr.py:580-710` (200 lines custom downsampling)

**With:**
```python
from osgeo import gdal

ds = gdal.Open(input_path, gdal.GA_Update)
ds.BuildOverviews("AVERAGE", [2, 4, 8, 16])
```

**Why:**
- 10x faster (C++ vs Python)
- Handles all edge cases
- Battle-tested for 20 years

---

### 2. Use cf-xarray Properly (3 Days)

**Replace:** `geozarr.py:150-240` (manual CF attributes)

**With:**
```python
import cf_xarray as cfxr

ds = ds.cf.add_bounds("x")
ds = ds.cf.add_bounds("y")
ds = ds.rio.write_crs("EPSG:4326")  # grid_mapping handled
ds.cf.decode()  # Validate
```

cf-xarray is already in dependencies but barely used!

---

### 3. Simplify S1 Reprojection (1 Week)

**Replace:** `sentinel1_reprojection.py` (378 lines)

**With:** Direct rasterio usage (~30 lines)

```python
from rasterio.warp import reproject, Resampling

src.gcps = (gcps, src.crs)
reproject(
    source=src.read(1),
    destination=dst_array,
    dst_crs="EPSG:4326",
    resampling=Resampling.bilinear
)
```

Rasterio already does this—12x code reduction.

---

### 4. Fix Warning Suppression (1 Hour)

**Current:**
```python
warnings.filterwarnings("ignore", message=".*", category=FutureWarning)
```

**Fix:** Remove blanket suppression, address each warning:

```python
# Address specific warnings only
warnings.filterwarnings("ignore", message=".*crs_wkt.*", category=UserWarning)

# Let others through so we see real issues
```

---

### 5. Add Rechunker (2 Days)

**Replace:** Custom chunk alignment loop

**With:**
```python
from rechunker import rechunk

plan = rechunk(
    source,
    target_chunks={"x": 4096, "y": 4096},
    max_mem="2GB",
    target_store=output_path
)
plan.execute()
```

Memory-safe, optimized task graph, handles intermediate storage.

---

## Observability Gaps

### Missing: Distributed Tracing

**Problem:** Can't trace requests across convert → validate → register → augment steps.

**Fix:** Add OpenTelemetry:

```python
from opentelemetry import trace

tracer = trace.get_tracer(__name__)

with tracer.start_as_current_span("convert_geozarr") as span:
    span.set_attribute("input.size_gb", input_size)
    # Conversion logic
    with tracer.start_as_current_span("create_overviews"):
        # Pyramid generation
```

Export to Grafana Tempo, visualize with Jaeger UI.

---

### Missing: Structured Logging

**Problem:** Print statements everywhere, can't search/analyze.

**Fix:** Add structlog:

```python
import structlog

log = structlog.get_logger()

log.info(
    "conversion_started",
    input_path=input_path,
    spatial_chunk=4096,
    enable_sharding=False
)
```

Send to Grafana Loki for aggregation.

---

## Ecosystem Tools Missing

**Should be using but aren't:**

| Tool | Purpose | Impact |
|------|---------|--------|
| **pystac-client** | STAC transactions | Manual HTTP = fragile |
| **morecantile** | TMS validation | No OGC compliance check |
| **rechunker** | Zarr rechunking | Slower, memory-unsafe |
| **rio-cogeo** | COG validation | No pyramid validation |
| **stac-validator** | STAC spec check | Items might be invalid |
| **OpenTelemetry** | Distributed tracing | Can't debug prod issues |
| **structlog** | Structured logs | Can't analyze failures |

---

## Summary: What to Fix First

**Week 1: Validation**
1. Add STAC spec validation (pystac)
2. Add TileMatrixSet validation (morecantile)
3. Add CF-conventions validation (cf-xarray)

**Week 2-3: Library Integration**
1. Replace manual STAC HTTP with pystac-client
2. Use cf-xarray for CF attributes
3. Fix warning suppression issues

**Week 4-5: Testing**
1. Add S1 GRD end-to-end test
2. Add S2 L2A end-to-end test
3. Add pyramid correctness tests

**Beyond (If Time):**
1. GDAL pyramid generation (10x speedup)
2. Simplify S1 reprojection (12x code reduction)
3. Add distributed tracing (OpenTelemetry)

**Bottom line:** System works, but needs validation + established libraries to be production-grade.
