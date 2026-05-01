# Phase 5 — S1 GRD RTC STAC Registration & Argo Pipeline

## Objective

Automate the end-to-end S1 GRD RTC ingestion pipeline: from S1Tiling GeoTIFF output on S3, through Zarr store append, to a queryable STAC item in the EOPF Explorer catalog — orchestrated by two independent Argo Workflows targeting the `sentinel-1-grd-rtc-staging` collection.

**Target users**: EOPF Explorer data team running scheduled or manual ingestion of S1 GRD RTC tiles.

**Success metric**: A new S1Tiling acquisition over tile 31TCH, submitted to the ingest workflow, results in a viewable STAC item at `https://api.explorer.eopf.copernicus.eu/rstaging/collections/sentinel-1-grd-rtc-staging/items/s1-rtc-31TCH` within one workflow run.

---

## Scope

### In scope
- STAC item builder in `data-model` repo (`build_s1_rtc_stac_item()` + `generate-stac-s1` CLI)
- `scripts/ingest_v1_s1_rtc.py` in this repo (data-pipeline)
- `scripts/register_v1_s1_rtc.py` in this repo
- `stac/sentinel-1-grd-rtc-staging.json` collection definition in this repo
- Argo `WorkflowTemplate` for S1Tiling (`platform-deploy` repo, staging namespace)
- Argo `WorkflowTemplate` for S1 RTC ingest + register (`platform-deploy` repo, staging namespace)
- `CronWorkflow` for daily automated runs
- Webhook `Sensor` for manual submission (consistent with existing `submit_test_workflow_wh.py`)
- Concurrency configmap entry

### Out of scope
- Production deployment — promote from staging after end-to-end validation on tile 31TCH
- S1Tiling `.cfg` template management (tile/DEM selection) — operational concern, not code
- EODAG monkey-patch — assumed applied in the S1Tiling Docker image already
- `sentinel-1-grd-rtc` (production) collection creation

---

## Architecture

The two workflows are independent and loosely coupled via S3:

```
┌────────────────────────────────────────────────────────────────┐
│  Workflow 1: eopf-explorer-s1tiling  (platform-deploy)         │
│                                                                │
│  Trigger: CronWorkflow (daily) or manual Sensor webhook        │
│  Inputs:  tile_id, orbit_direction, date_start, date_end       │
│                                                                │
│  Step 1 [cnes/s1tiling:1.4.0]:                                 │
│    SAFE products → S1Tiling processing → GeoTIFFs              │
│    Output: s3://bucket/s1tiling-output/{tile}/{orbit}/{date}/  │
└───────────────────────────────┬────────────────────────────────┘
                                │ S3 GeoTIFF prefix
                                │ (manual handoff or chained trigger)
                                ▼
┌────────────────────────────────────────────────────────────────┐
│  Workflow 2: eopf-explorer-ingest-v1-s1rtc  (platform-deploy)  │
│                                                                │
│  Trigger: CronWorkflow (daily) or manual Sensor webhook        │
│  Inputs:  s3_geotiff_prefix, tile_id, orbit_direction,         │
│           collection, zarr store params, STAC API params       │
│                                                                │
│  Step 1 [data-pipeline]: ingest                                │
│    discover GeoTIFFs → append Zarr store → consolidate         │
│    Script: scripts/ingest_v1_s1_rtc.py                         │
│                                                                │
│  Step 2 [data-pipeline]: register-stac  (depends on ingest)    │
│    build STAC item from Zarr → augment → upsert                │
│    Script: scripts/register_v1_s1_rtc.py                       │
└────────────────────────────────────────────────────────────────┘
```

**Design decision**: `build_s1_rtc_stac_item()` lives in `data-model` (co-located with domain knowledge) and is imported directly into `register_v1_s1_rtc.py`. This follows the same pattern as `convert_v1_s2.py` importing from `eopf_geozarr.s2_optimization`. No subprocess calls.

**Zarr store path convention**: `s3://{s3_output_bucket}/{s3_output_prefix}/s1-grd-rtc-{tile_id}.zarr`

---

## Deliverables

### 1. STAC Item Builder — `data-model` repo, branch `s1-tiling`

**New file**: `src/eopf_geozarr/stac/s1_rtc.py`
**Modified file**: `src/eopf_geozarr/cli.py` — add `generate-stac-s1` subcommand
**New test file**: `tests/test_s1_stac.py`

#### Public API

```python
def build_s1_rtc_stac_item(
    zarr_store: str,    # S3 or local Zarr store path
    collection_id: str,
) -> pystac.Item:
    ...
```

#### Behaviour

Opens the Zarr store using consolidated metadata (single HTTP request, no full scan).
Derives `tile_id` from store basename (`s1-grd-rtc-{tile_id}.zarr`).

For each orbit direction present (`ascending`, `descending`):
- `proj:code` → `proj:epsg` (integer)
- `spatial:bbox` (UTM projected `[xmin, ymin, xmax, ymax]`) → WGS84 via pyproj → item bbox
- `{orbit}/r10m/time` coordinate → `start_datetime`, `end_datetime` (ISO 8601)
- `{orbit}/r10m/platform` coordinate → deduplicated list of platforms

Item fields:

| Field | Value |
|-------|-------|
| `id` | `s1-rtc-{tile_id}` |
| `datetime` | `null` (time-series item) |
| `start_datetime` | min across all orbit directions |
| `end_datetime` | max across all orbit directions |
| `bbox` | WGS84 union of orbit bboxes |
| `geometry` | GeoJSON Polygon from bbox |
| `properties.platform` | comma-joined unique platforms (e.g. `"sentinel-1a,sentinel-1c"`) |
| `properties.instruments` | `["c-sar"]` |
| `properties.constellation` | `"sentinel-1"` |
| `properties.sar:frequency_band` | `"C"` |
| `properties.sar:center_frequency` | `5.405` |
| `properties.sar:polarizations` | `["VV", "VH"]` |
| `properties.sar:instrument_mode` | `"IW"` |
| `properties.sar:product_type` | `"GRD"` |
| `properties.proj:epsg` | from store |
| `properties.title` | `"Sentinel-1 GRD RTC - Tile {tile_id}"` |

Assets:

| Key | `href` | `type` | `roles` |
|-----|--------|--------|---------|
| `zarr-store` | root store URL | `application/vnd.zarr` | `["data"]` |
| `vv` | `{store}/ascending/r10m/vv` (preferred) or descending | `application/vnd.zarr` | `["data"]` |
| `vh` | `{store}/ascending/r10m/vh` (preferred) or descending | `application/vnd.zarr` | `["data"]` |

STAC extensions declared: `sar`, `sat`, `projection`

#### CLI command

```bash
eopf-geozarr generate-stac-s1 \
  --store s3://bucket/s1-grd-rtc-31TCH.zarr \
  --collection sentinel-1-grd-rtc-staging \
  [--output item.json]   # writes to stdout if omitted
```

#### Tests (`tests/test_s1_stac.py`) — 8 tests minimum

| Test | What it checks |
|------|----------------|
| `test_build_item_roundtrip` | pystac schema validates, no missing fields |
| `test_temporal_extent` | `start_datetime`/`end_datetime` match fixture time coords |
| `test_bbox_wgs84` | bbox is in WGS84 (lng range −180..180, lat range −90..90) |
| `test_both_orbit_directions` | both ascending+descending → bbox is union |
| `test_ascending_only` | ascending present, no descending key → item still valid |
| `test_rejects_empty_store` | store with no orbit direction → `ValueError` |
| `test_asset_subpaths` | `vv`/`vh` hrefs contain `.zarr/ascending/r10m/` |
| `test_sar_extension_fields` | `sar:frequency_band`, `sar:polarizations`, etc. present |

---

### 2. Ingest Script — `data-pipeline` repo

**New file**: `scripts/ingest_v1_s1_rtc.py`

#### Interface

```bash
python scripts/ingest_v1_s1_rtc.py \
  --s3-geotiff-prefix  s3://bucket/s1tiling-output/31TCH/ascending/2026-04-01/ \
  --s3-zarr-store      s3://bucket/s1-rtc-staging/s1-grd-rtc-31TCH.zarr \
  --tile-id            31TCH \
  --orbit-direction    ascending
```

#### Behaviour

Imports directly from `eopf_geozarr.conversion.s1_ingest` (no subprocess):

1. `discover_s1tiling_acquisitions(s3_geotiff_prefix)` → list of acquisition groups
2. If list is empty: log warning and exit 2 (allows empty cron runs without failing the workflow)
3. For each acquisition: `ingest_s1tiling_acquisition(vv, vh, mask, zarr_store, orbit_direction)`
   — fail-fast on first error (exit 1)
4. `discover_s1tiling_conditions(s3_geotiff_prefix)` → condition groups (may be empty, non-fatal)
5. For each condition group: `ingest_s1tiling_conditions(store, orbit_dir, relative_orbit, ...)`
6. `consolidate_s1_store(zarr_store)`

#### Exit codes

| Code | Meaning |
|------|---------|
| 0 | Success — at least one acquisition ingested |
| 1 | Failure — ingest or consolidation error |
| 2 | No acquisitions found — skips cleanly (Argo retry policy: no retry on 2) |

---

### 3. STAC Registration Script — `data-pipeline` repo

**New file**: `scripts/register_v1_s1_rtc.py`

#### Interface

```bash
python scripts/register_v1_s1_rtc.py \
  --store            s3://bucket/s1-rtc-staging/s1-grd-rtc-31TCH.zarr \
  --collection       sentinel-1-grd-rtc-staging \
  --stac-api-url     https://api.explorer.eopf.copernicus.eu/stac \
  --raster-api-url   https://api.explorer.eopf.copernicus.eu/raster \
  --s3-endpoint      https://s3.de.io.cloud.ovh.net \
  --s3-output-bucket esa-zarr-sentinel-explorer-fra \
  --s3-output-prefix s1-rtc-staging
```

#### Behaviour

Reuses helper functions from `register_v1.py` (import, not copy):

1. `build_s1_rtc_stac_item(store, collection)` — from `eopf_geozarr.stac.s1_rtc`
2. `add_store_link(item, store)` — from `register_v1`
3. `add_alternate_s3_assets(item, s3_endpoint)` — from `register_v1`
4. `add_visualization_links(item, raster_api_url, collection)` — existing S1 branch in `register_v1` applies (requires `vh` asset with `.zarr/` in href)
5. `add_thumbnail_asset(item, raster_api_url, collection)` — existing S1 branch in `register_v1`
6. `warm_thumbnail_cache(item)`
7. `upsert_item(client, collection, item)` — from `register_v1`

Note: `consolidate_reflectance_assets()` and `fix_zarr_asset_media_types()` are S2-specific — skip.

---

### 4. STAC Collection Definition — `data-pipeline` repo

**New file**: `stac/sentinel-1-grd-rtc-staging.json`

Key fields (modelled on `stac/sentinel-2-l2a-staging.json`):

| Field | Value |
|-------|-------|
| `id` | `sentinel-1-grd-rtc-staging` |
| `title` | `Sentinel-1 GRD RTC GeoZarr V3 (staging)` |
| `extent.temporal` | `[["2014-04-03T00:00:00Z", null]]` (S1A launch, open end) |
| `extent.spatial.bbox` | `[[-180, -90, 180, 90]]` (global, narrow after staging) |

Collection must be created in the STAC API once before any item is registered — manual step using `operator-tools/manage_collections.py`. Not automated in the workflow.

---

### 5. Argo WorkflowTemplate: S1Tiling — `platform-deploy` repo

**New file**: `workspaces/devseed-staging/data-pipeline/eopf-explorer-s1tiling-template.yaml`

**Metadata**:
- `name`: `eopf-explorer-s1tiling`
- `namespace`: `devseed`
- `serviceAccountName`: `operate-workflow-sa`

**Parameters**:

| Name | Default | Description |
|------|---------|-------------|
| `tile_id` | — | MGRS tile (e.g. `31TCH`) |
| `orbit_direction` | `ascending` | `ascending` or `descending` |
| `date_start` | — | ISO date, acquisition window start |
| `date_end` | — | ISO date, acquisition window end |
| `s3_geotiff_bucket` | `esa-zarr-sentinel-explorer-fra` | Output bucket |
| `s3_geotiff_prefix` | `s1tiling-output` | Output S3 prefix |
| `s1tiling_image` | `registry.orfeo-toolbox.org/s1tiling/s1tiling:1.4.0` | S1Tiling image |
| `semaphore_key` | `v1-s1tiling-limit` | Concurrency key |

**DAG — single step** `run-s1tiling`:
- Image: `{{inputs.parameters.s1tiling_image}}`
- Runs S1Tiling CLI with config templated from workflow parameters
- Output GeoTIFFs path: `s3://{bucket}/{prefix}/{tile_id}/{orbit_direction}/{date_start}/`
- Resources: 4 CPU, 16Gi RAM
- `activeDeadlineSeconds: 7200` (2h)
- Retry: 3 attempts on failure, exponential backoff

**Outputs**: S3 GeoTIFF prefix string (for use as input to Workflow 2)

---

### 6. Argo WorkflowTemplate: S1 RTC Ingest + Register — `platform-deploy` repo

**New file**: `workspaces/devseed-staging/data-pipeline/eopf-explorer-ingest-v1-s1rtc-template.yaml`

**Metadata**:
- `name`: `eopf-explorer-ingest-v1-s1rtc`
- `namespace`: `devseed`
- `serviceAccountName`: `operate-workflow-sa`

**Parameters**:

| Name | Default | Description |
|------|---------|-------------|
| `s3_geotiff_prefix` | — | S3 path with S1Tiling GeoTIFFs |
| `tile_id` | — | MGRS tile (e.g. `31TCH`) |
| `orbit_direction` | `ascending` | `ascending` or `descending` |
| `collection` | `sentinel-1-grd-rtc-staging` | Target STAC collection |
| `s3_output_bucket` | `esa-zarr-sentinel-explorer-fra` | Zarr store bucket |
| `s3_output_prefix` | `s1-rtc-staging` | Zarr store S3 prefix |
| `s3_endpoint` | `https://s3.de.io.cloud.ovh.net` | S3 endpoint |
| `stac_api_url` | `https://api.explorer.eopf.copernicus.eu/stac` | STAC API |
| `raster_api_url` | `https://api.explorer.eopf.copernicus.eu/raster` | TiTiler API |
| `pipeline_image_version` | — | data-pipeline Docker image tag |
| `semaphore_key` | `v1-s1rtc-limit` | Concurrency key |

**DAG**:

1. `ingest` — runs `scripts/ingest_v1_s1_rtc.py`
   - Image: `data-pipeline:{pipeline_image_version}`
   - Resources: 2 CPU, 8Gi RAM
   - `activeDeadlineSeconds: 3600`
   - Retry policy: retry on exit codes 1, 137, 143 only — no retry on exit code 2
   - S3 credentials from `geozarr-s3-credentials` secret

2. `register-stac` (depends on: `ingest`) — runs `scripts/register_v1_s1_rtc.py`
   - Image: `data-pipeline:{pipeline_image_version}`
   - Resources: 1 CPU, 2Gi RAM
   - `activeDeadlineSeconds: 600`
   - Retry: 5 attempts, always

---

### 7. CronWorkflow — `platform-deploy` repo

**New file**: `workspaces/devseed-staging/data-pipeline/eopf-explorer-ingest-v1-s1rtc-cron.yaml`

- Schedule: `0 6 * * *` (daily at 06:00 UTC)
- Workflow ref: `eopf-explorer-ingest-v1-s1rtc`
- `concurrencyPolicy: Forbid`
- Initial tile: `31TCH`, orbit `ascending`
- `s3_geotiff_prefix`: points to S3 output of the previous night's S1Tiling run

---

### 8. Webhook Sensor — `platform-deploy` repo

**New file**: `workspaces/devseed-staging/data-pipeline/eopf-explorer-ingest-v1-s1rtc-sensor.yaml`

- `eventSourceName`: `eopf-explorer-webhook` (existing)
- `eventName`: `samples`
- Filter: `body.action == "^ingest-v1-s1rtc$"`
- Maps `body.s3_geotiff_prefix`, `body.tile_id`, `body.orbit_direction` to workflow params
- Concurrency via `eopf-workflow-concurrency.v1-s1rtc-limit`

**Manual trigger** (consistent with `submit_test_workflow_wh.py`):

```json
{
  "action": "ingest-v1-s1rtc",
  "s3_geotiff_prefix": "s3://esa-zarr-sentinel-explorer-fra/s1tiling-output/31TCH/ascending/2026-04-01/",
  "tile_id": "31TCH",
  "orbit_direction": "ascending"
}
```

Example: `operator-tools/submit_test_workflow_wh.py` POST body as above.

---

### 9. Concurrency configmap update — `platform-deploy` repo

**File modified**: `workspaces/devseed-staging/data-pipeline/eopf-workflow-concurrency-configmap.yaml`

```yaml
v1-s1rtc-limit: "3"     # ingest+register per tile — S3 writes are independent per tile
v1-s1tiling-limit: "2"  # S1Tiling runs are CPU/memory heavy
```

---

## Implementation Order

Dependencies flow left → right. Items in the same column can be parallelised.

```
[data-model]                [data-pipeline]             [platform-deploy]
stac/s1_rtc.py         →   ingest_v1_s1_rtc.py     →   ingest template YAML
generate-stac-s1 CLI   →   register_v1_s1_rtc.py   →   cron + sensor YAMLs
tests/test_s1_stac.py  →   stac collection JSON     →   configmap update
data-model new tag     →   pyproject.toml pin bump
                       →   CI tests pass
```

**Critical path**: data-model must be tagged before the data-pipeline Docker image can be built and the Argo template can reference a concrete `pipeline_image_version`. The data-model STAC builder (step 1) must be merged and tagged (`v0.10.0` or similar) first.

---

## Testing Strategy

| Test | Location | Method |
|------|----------|--------|
| `build_s1_rtc_stac_item` | `data-model/tests/test_s1_stac.py` | Synthetic Zarr fixture in `tmp_path` (8 tests) |
| `ingest_v1_s1_rtc.py` | `data-pipeline/tests/test_ingest_v1_s1_rtc.py` | Synthetic GeoTIFFs → local Zarr, check `xr.open_zarr` |
| `register_v1_s1_rtc.py` | `data-pipeline/tests/test_register_v1_s1_rtc.py` | Mock STAC API via `respx`, real item build |
| Collection JSON schema | `data-pipeline/tests/test_stac_collections.py` | `pystac.Collection.from_file()` + validate |
| Argo YAML lint | `platform-deploy` CI | `argo lint --offline` |

---

## Code Style

- `logging` (stdlib) for scripts (consistent with `register_v1.py`); `structlog` if extending data-model modules
- `argparse` CLI for all new scripts (consistent with `register_v1.py`, `convert_v1_s2.py`)
- Suppress noisy library loggers: boto3, s3fs, aiobotocore, urllib3, httpx
- Import `eopf_geozarr` functions directly — no subprocess calls to CLI
- Import helpers from `register_v1.py` — no copy-paste

---

## Acceptance Criteria

- [ ] `build_s1_rtc_stac_item()` produces a `pystac`-valid item with correct WGS84 bbox and ISO 8601 temporal extent for the 31TCH fixture store
- [ ] `ingest_v1_s1_rtc.py` runs without error on a synthetic GeoTIFF batch, producing a Zarr store readable by `xr.open_zarr()`
- [ ] `register_v1_s1_rtc.py` upserts to `sentinel-1-grd-rtc-staging` and the viewer link resolves (`/viewer` returns HTTP 200)
- [ ] All unit and integration tests pass in CI (`pytest` + pre-commit ruff/mypy)
- [ ] Argo YAML lints cleanly (`argo lint --offline`)
- [ ] End-to-end: Workflow 2 triggered manually for tile 31TCH produces a queryable item at the staging STAC API

---

## Open Questions

1. **S1Tiling `.cfg` template**: Which parameters to set for staging (CDSE credentials, DEM source, output path)? Must be resolved before Workflow 1 can be tested end-to-end. The implementation plan (`s1tiling_docker_instructions.md`) is the reference.

2. **data-model version tag**: `register_v1_s1_rtc.py` imports from `eopf_geozarr.stac.s1_rtc` which does not yet exist in `v0.9.0`. The data-pipeline `pyproject.toml` pin must be bumped to a new data-model release that includes the STAC builder.

3. **Multi-orbit STAC item**: Current spec builds one item per tile covering all orbit directions. If ascending/descending orbits need separate temporal extents or visualization configs, split into per-orbit items (`s1-rtc-31TCH-ascending`). Defer until staging validation reveals the viewer UX.

4. **Exit code 2 in Argo retry policy**: Argo `expression`-based retry skip on exit code 2 must be tested — verify `asInt(lastRetry.exitCode) == 2` evaluation works correctly in the cluster version.
