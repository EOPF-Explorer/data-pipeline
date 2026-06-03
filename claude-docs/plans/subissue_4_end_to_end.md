# Plan: Sub-issue 4 — End-to-end validation: run Script A → Script B for tile 31TCH (`claude-docs/specs/s1_grd_phase5_subissues.md`)

**Goal**: prove the full local pipeline integrates before writing any Argo YAML — Script A GeoTIFFs → Script B ingest → Zarr → STAC item queryable at staging API
**Constraint**: no new code unless forced by a blocker; execution + verification only

---

## Current state

| Resource | Status |
|----------|--------|
| `scripts/run_s1tiling.py` | **Done** — Docker run confirmed 2026-05-25; GeoTIFFs on S3 |
| `scripts/run_ingest_register.py` | **Done** — 9-arg orchestrator; 4 unit tests pass (2026-05-26) |
| `scripts/ingest_v1_s1_rtc.py` | **Done** — exits 0/1/2; 4 unit tests pass |
| `scripts/register_v1_s1_rtc.py` | **Done** — upserts mock item; 4 unit tests pass |
| S3 GeoTIFFs | **Present** — 6 `*GammaNaughtRTC.tif` + 3 `GAMMA_AREA*.tif` at `s3://esa-zarr-sentinel-explorer-tests/s1tiling-output/31TCH/descending/2025-02-01/` (confirmed 2026-05-25) |
| `stac/sentinel-1-grd-rtc-staging.json` | **Done** — live in staging API (HTTP 200, 2026-05-26) |
| `eopf-geozarr` pin (`9321df6`) | **Done** — `s1_ingest` + `build_s1_rtc_stac_item` importable |
| `uv run pytest` | **Green** — 325 passed (2026-05-26) |
| Sub-issue B gate | **Cleared** — all 3 checkboxes ticked (2026-05-26) |

---

## Dependency graph

```
[Sub-issue A — GeoTIFFs on S3]    ─┐
                                    ├─► Task 4.0 (pre-flight) ─► Task 4.1 (run Script B)
[Sub-issue B — run_ingest_register] ─┘           │
                                                  ├─► Task 4.2 (Zarr validation)
                                                  ├─► Task 4.3 (STAC API validation)
                                                  ├─► Task 4.4 (TiTiler validation)
                                                  └─► Task 4.5 (report issues / full chain re-run)
```

**Critical blocker surfaced during planning (OQ-1)**: `discover_s1tiling_acquisitions` uses
`pathlib.Path.glob()` — it does not support `s3://` URIs. Task 4.0 probes this before
Task 4.1 runs Script B. See Open questions → OQ-1 for the investigation and workaround.

---

## Tasks

### Task 4.0 — Pre-flight: verify S3 data + S3 env vars + ingest S3-path probe  ✅ DONE

**What**: confirm the GeoTIFFs from Sub-issue A are still on S3; set the env vars that
`ingest_v1_s1_rtc.py` needs to access S3; and probe whether `discover_s1tiling_acquisitions`
handles `s3://` URIs (OQ-1 investigation).

**Verify**:

```bash
# 1. Confirm S3 GeoTIFFs still present (≥ 2 GammaNaughtRTC + ≥ 1 GAMMA_AREA)
aws s3 ls s3://esa-zarr-sentinel-explorer-tests/s1tiling-output/31TCH/descending/2025-02-01/ \
  --endpoint-url https://s3.de.io.cloud.ovh.net --profile eopfexplorer

# 2. Export S3 credentials so sub-scripts inherit them
#    (ingest_v1_s1_rtc.py has no --profile flag; it relies on env vars)
export AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id --profile eopfexplorer)
export AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key --profile eopfexplorer)
export AWS_ENDPOINT_URL=https://s3.de.io.cloud.ovh.net

# 3. Probe OQ-1: does discover_s1tiling_acquisitions handle s3:// URIs?
uv run python - <<'PYEOF'
from eopf_geozarr.conversion.s1_ingest import discover_s1tiling_acquisitions
prefix = "s3://esa-zarr-sentinel-explorer-tests/s1tiling-output/31TCH/descending/2025-02-01/"
acqs = discover_s1tiling_acquisitions(prefix)
print(f"S3 probe: {len(acqs)} acquisitions found")
if not acqs:
    print("WARN: S3 URI returned 0 — local-path workaround needed (see OQ-1)")
PYEOF
```

**Acceptance criteria**:
- [x] `aws s3 ls` lists ≥ 9 files (6 GammaNaughtRTC + 3 GAMMA_AREA) — 15 files confirmed 2026-05-26
- [x] `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL` exported in shell
- [x] OQ-1 probe result recorded — **0 acquisitions; local-path workaround needed** (pathlib strips one slash from `s3://`, returns 0 matches; confirmed 2026-05-26)

---

### Task 4.1 — Run Script B (run_ingest_register.py) end-to-end  ✅ DONE

**What**: call `run_ingest_register.py` with the S3 prefix from Sub-issue A. The Zarr store
path is derived internally as `s3://esa-zarr-sentinel-explorer-tests/s1-rtc-test/s1-grd-rtc-31TCH.zarr`.

**If OQ-1 probe showed S3 URI works** (≥ 1 acquisition discovered):
```bash
uv run python scripts/run_ingest_register.py \
  --s3-geotiff-prefix s3://esa-zarr-sentinel-explorer-tests/s1tiling-output/31TCH/descending/2025-02-01/ \
  --tile-id 31TCH \
  --orbit-direction descending \
  --collection sentinel-1-grd-rtc-staging \
  --s3-output-bucket esa-zarr-sentinel-explorer-tests \
  --s3-output-prefix s1-rtc-test \
  --s3-endpoint https://s3.de.io.cloud.ovh.net \
  --stac-api-url https://api.explorer.eopf.copernicus.eu/stac \
  --raster-api-url https://api.explorer.eopf.copernicus.eu/raster
```

**If OQ-1 probe showed 0 acquisitions** (S3 URI unsupported by `pathlib.Path.glob()`):
Apply the local-path workaround — merge acquisitions + conditions into one directory so
`discover_s1tiling_acquisitions` and `discover_s1tiling_conditions` can find everything
under the same `$S1T_WORKDIR/s1tiling_merged/31TCH/descending/2025-02-01/` prefix.
```bash
export MERGED_PREFIX="$S1T_WORKDIR/s1tiling_merged/31TCH/descending/2025-02-01"
mkdir -p "$MERGED_PREFIX"
# Copy acquisitions (from data_out/31TCH/) and conditions (from data_gamma_area/) together
cp "$S1T_WORKDIR/data_out/31TCH/"*.tif  "$MERGED_PREFIX/"
cp "$S1T_WORKDIR/data_gamma_area/"*.tif "$MERGED_PREFIX/"
ls "$MERGED_PREFIX/"  # expect ≥ 9 files

uv run python scripts/run_ingest_register.py \
  --s3-geotiff-prefix "$MERGED_PREFIX/" \
  --tile-id 31TCH \
  --orbit-direction descending \
  --collection sentinel-1-grd-rtc-staging \
  --s3-output-bucket esa-zarr-sentinel-explorer-tests \
  --s3-output-prefix s1-rtc-test \
  --s3-endpoint https://s3.de.io.cloud.ovh.net \
  --stac-api-url https://api.explorer.eopf.copernicus.eu/stac \
  --raster-api-url https://api.explorer.eopf.copernicus.eu/raster
```

**Verify**: capture full stdout/stderr; confirm exit code 0.

```bash
echo "Exit code: $?"
```

**Acceptance criteria**:
- [x] Script exits 0 — confirmed 2026-05-26 (exit 0)
- [x] Log shows `discover_s1tiling_acquisitions` found ≥ 2 acquisitions (count=3, 2026-05-26)
- [x] Log shows `consolidate_s1_store` called — "Metadata consolidated" in log
- [x] Log shows `upsert_item` called — "✅ Registered s1-rtc-31TCH (HTTP 201)"

**Bugs fixed to unblock**:
- I-3: eopf_geozarr `Path(s3://)` → `s3:/` writes zarr locally; fix: use local temp dir + `aws s3 sync`
- I-4: `upsert_item` set `exists=True` when pystac-client `get_item()` returns `None` instead of raising; fix: check `fetched is not None`
- I-5: stale local zarr caused `BoundsCheckError` on re-run; fix: `shutil.rmtree` before ingest
- Each bug fixed with RED→GREEN TDD; 330 tests pass (2026-05-26)

---

### Task 4.2 — Zarr store validation  ✅ DONE

**What**: confirm the Zarr store at the derived path is readable and passes the `eopf-geozarr validate` check.

**Verify**:
```bash
ZARR_STORE="s3://esa-zarr-sentinel-explorer-tests/s1-rtc-test/s1-grd-rtc-31TCH.zarr"

# 1. Minimal xarray read (no GDAL/rasterio; pure zarr + xarray)
uv run python - <<'PYEOF'
import xarray as xr
store = "s3://esa-zarr-sentinel-explorer-tests/s1-rtc-test/s1-grd-rtc-31TCH.zarr"
import s3fs
fs = s3fs.S3FileSystem(
    endpoint_url="https://s3.de.io.cloud.ovh.net",
    key=__import__("os").environ["AWS_ACCESS_KEY_ID"],
    secret=__import__("os").environ["AWS_SECRET_ACCESS_KEY"],
)
import zarr
store_map = fs.get_mapper(store.replace("s3://",""))
root = zarr.open_consolidated(store_map, zarr_format=3)
print("Groups:", list(root.groups()))
# should show ('descending', <zarr.hierarchy.Group ...>)
# check time array present
for orbit, og in root.groups():
    times = og["r10m/time"][:]
    print(f"  {orbit}: {len(times)} timestamps")
PYEOF

# 2. eopf-geozarr validate
uv run eopf-geozarr validate "$ZARR_STORE"

# 3. generate-stac-s1 smoke check (reads store metadata; must not error)
uv run eopf-geozarr generate-stac-s1 \
  --store "$ZARR_STORE" \
  --collection sentinel-1-grd-rtc-staging \
  | python -c "import sys, json; item = json.load(sys.stdin); print('item id:', item['id'])"
```

**Acceptance criteria**:
- [x] `zarr.open_consolidated` reads root groups without error — Groups: ['descending'] (2026-05-26)
- [x] `descending` group present with ≥ 2 time values — 3 timestamps (2025-02-05, 02-10, 02-12)
- [x] `eopf-geozarr validate` exits 0 — "✅ Dataset appears to be GeoZarr compliant" (2026-05-26)
- [x] `eopf-geozarr generate-stac-s1` outputs `item id: s1-rtc-31TCH` (2026-05-26)

**OQ-2 resolved**: `eopf-geozarr validate` accepts S3 URIs directly — no local copy needed.

---

### Task 4.3 — STAC API validation  ✅ DONE

**What**: confirm item `s1-rtc-31TCH` is queryable at the staging STAC API.

**Verify**:
```bash
# Direct item lookup
curl -s "https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-1-grd-rtc-staging/items/s1-rtc-31TCH" \
  | python -c "
import sys, json
item = json.load(sys.stdin)
print('id:', item.get('id'))
print('collection:', item.get('collection'))
props = item.get('properties', {})
print('start_datetime:', props.get('start_datetime'))
print('end_datetime:', props.get('end_datetime'))
print('orbit_state:', props.get('sat:orbit_state'))
print('assets:', list(item.get('assets', {}).keys()))
print('links rel:', [l.get('rel') for l in item.get('links', [])])
"
```

**Acceptance criteria**:
- [x] HTTP 200 — valid JSON response received (2026-05-26)
- [x] `id` = `s1-rtc-31TCH`
- [x] `collection` = `sentinel-1-grd-rtc-staging`
- [x] `start_datetime` 2025-02-05 / `end_datetime` 2025-02-12 — both within 2025-02-01..14
- [x] `assets`: `['vh', 'vv', 'thumbnail', 'zarr-store']` — zarr-store, vv, vh all present
- [x] `links rel`: includes `viewer`

---

### Task 4.4 — TiTiler viewer validation  ✅ DONE (with caveat — I-6)

**What**: confirm the viewer and thumbnail links embedded in the item return HTTP 200. The
viewer URL pattern is `{raster_api_url}/collections/{collection_id}/items/{item_id}/viewer`.

**Verify**:
```bash
ITEM_URL="https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-1-grd-rtc-staging/items/s1-rtc-31TCH"

# Extract viewer and thumbnail URLs from the registered item
uv run python - <<'PYEOF'
import json, urllib.request

item = json.loads(urllib.request.urlopen(
    "https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-1-grd-rtc-staging/items/s1-rtc-31TCH"
).read())

viewer_links = [l for l in item.get("links", []) if l.get("rel") == "viewer"]
thumbnail = item.get("assets", {}).get("thumbnail", {}).get("href")
tilejson_links = [l for l in item.get("links", []) if l.get("rel") == "tilejson"]

print("viewer links:", viewer_links)
print("thumbnail href:", thumbnail)
print("tilejson links:", tilejson_links[:1])
PYEOF

# Check viewer returns HTTP 200
VIEWER_URL="https://api.explorer.eopf.copernicus.eu/raster/collections/sentinel-1-grd-rtc-staging/items/s1-rtc-31TCH/viewer"
curl -sI "$VIEWER_URL" | head -1
# expect: HTTP/... 200 OK

# Check thumbnail returns HTTP 200 (allow redirect 301/302)
THUMBNAIL_URL=$(curl -s "$ITEM_URL" | python -c "import sys,json; print(json.load(sys.stdin)['assets'].get('thumbnail',{}).get('href',''))")
[ -n "$THUMBNAIL_URL" ] && curl -sI "$THUMBNAIL_URL" | head -1 || echo "No thumbnail"
```

**Acceptance criteria**:
- [x] Viewer URL (`/viewer`) returns HTTP 200 — confirmed, returns valid HTML (2026-05-26)
- [x] Thumbnail asset href present in the STAC item — `https://api.explorer.eopf.copernicus.eu/raster/.../preview?...`
- [~] Thumbnail URL returns HTTP 200 — returns **500** (Issue I-6: TiTiler can't access OVH S3)
- [x] TiTiler tilejson link present for VH band visualization — link present in STAC item links

**Issue I-6 — corrected root cause** (2026-05-29): TiTiler accesses zarr via the HTTPS gateway
`s3.explorer.eopf.copernicus.eu`, not directly via S3 credentials. S2 STAC assets use
`https://s3.explorer.eopf.copernicus.eu/{bucket}/{path}` — TiTiler reads them through that proxy.
S1 assets were registered with `s3://` hrefs; `add_alternate_s3_assets` only processes `https://`
assets, so TiTiler received an `s3://` URI it couldn't resolve → 500.

**Two-part fix**:
1. `register_v1_s1_rtc.py`: convert `s3://` asset hrefs to `https://` via `s3_to_https()` immediately
   after `build_s1_rtc_stac_item` — **committed** (`b95bc43`). Item re-registered with `https://` hrefs.
2. `s3.explorer.eopf.copernicus.eu` gateway only proxies the production S2 bucket
   (`esa-zarr-sentinel-explorer-fra`). The test bucket (`esa-zarr-sentinel-explorer-tests`) and the
   production S1 buckets (`esa-zarr-sentinel-explorer-s1-l1grd-staging`,
   `esa-zarr-sentinel-explorer-s1-l1grd-prod`) are **not yet proxied** →
   **platform-deploy PR #205** opened: adds nginx-s3-gateway Deployment + Service + ingress routes for
   both S1 buckets. Once merged and credentials wired, TiTiler will serve tiles from S1 zarr stores.

**Status for Sub-issues 6/7**: fix (1) already in main branch. Fix (2) unblocks production use once
PR #205 merges. The test bucket 500 will persist until gateway is extended (expected; acceptable).

---

### Task 4.5 — Full chain re-run for DIFFERENT data: tile 31TDH  ✅ DONE (2026-05-29)

**What**: re-run the whole chain (Script A → Script B) on a **different MGRS tile** to prove
the pipeline generalises beyond 31TCH. Chose **31TDH** (east-adjacent: lon ~1.77–3.12°E,
lat ~42.36–43.35°N) — all 6 required Copernicus DEM tiles already staged, no DEM download.

**Run (2026-05-29)**:
- Script A: 31TDH cfg derived from `config/S1GRD_RTC.cfg` (only `roi_by_tiles`/`tiles` changed),
  passed via `--cfg /tmp/S1GRD_RTC_31TDH.cfg`. Exit 0. Downloaded 2 S1A descending scenes
  (orbit 110 @ 2025-02-05, orbit 037 @ 2025-02-12), produced 10 GeoTIFFs on S3 at
  `s1tiling-output/31TDH/descending/2025-02-01/`.
- Script B: ingested 2 acquisitions, consolidated, applied the **CF `grid_mapping` ×7 groups**
  patch (the I-7 fix), synced to S3, registered `s1-rtc-31TDH` (HTTP 201). Exit 0.
  (At the time it also injected `tile_matrix_limits` ×6 levels via `_patch_tile_matrix_limits`;
  that patch was **removed 2026-06-01** after Emmanuel confirmed `tile_matrix_set` — and hence its
  `tile_matrix_limits` companion — is not part of the S1 data model. See I-7 note.)

**Acceptance criteria**:
- [x] Script A exits 0 — 2 acquisitions, 10 GeoTIFFs on S3 (2026-05-29)
- [x] Script B exits 0 — store `s1-grd-rtc-31TDH.zarr` written + STAC item registered HTTP 201
- [x] Item `s1-rtc-31TDH` queryable at staging STAC API — start 2025-02-05 / end 2025-02-12, descending
- [x] `eopf-geozarr validate` passes on the fresh store — "✅ GeoZarr compliant"
- [x] **TiTiler renders (live, deployed server)** — once the fixed store was placed at TiTiler's
  expected path `s3://esa-zarr-sentinel-explorer-fra/tests-output/sentinel-1-grd-rtc-staging/s1-rtc-31TDH.zarr`
  (I-8 workaround): `/info` HTTP 200 (`crs=EPSG:32631`), `/preview` HTTP 200 (PNG 915×915),
  tilejson (z7–13, bounds 1.766,42.358,3.12,43.353), XYZ tile z9 HTTP 200 (256×256 PNG).
  **This is the end-to-end proof the I-7 CF `grid_mapping` fix resolves the `tile_matrix_set` 500.**

**Note on `run_s1tiling.py`** (Argo-relevant finding): the script does **not** template the cfg —
it copies `--cfg` verbatim and runs `S1Processor` on it; `--tile-id`/`--date-*`/`--orbit` only
build the S3 output path. So a per-tile/-date run needs a per-run cfg. Argo must template the cfg
(roi_by_tiles, tiles, first_date, last_date, orbit_direction) — logged for Sub-issue 7.

<details><summary>Original 31TCH back-to-back recipe (superseded)</summary>

**What**: re-run Script A to get a fresh S3 prefix, then pass it directly to Script B —
the formal proof of end-to-end integration. **Note**: Script A takes 30–60 min (Docker
+ CDSE download). Run this task only when Tasks 4.1–4.4 all pass and bandwidth allows.

```bash
# Script A (Workflow 1) — produces fresh GeoTIFFs
S3_PREFIX=$(uv run python scripts/run_s1tiling.py \
  --tile-id 31TCH --orbit-direction descending \
  --date-start 2025-02-01 --date-end 2025-02-14 \
  --s3-bucket esa-zarr-sentinel-explorer-tests --s3-prefix s1tiling-output \
  --s3-endpoint https://s3.de.io.cloud.ovh.net \
  --eodag-cfg "$S1T_WORKDIR/config/eodag.yml" \
  --dem-dir "$S1T_WORKDIR/DEM/COP_DEM_GLO30" \
  --data-dir "$S1T_WORKDIR" \
  --cfg config/S1GRD_RTC.cfg \
  | tail -1)  # last printed line is the S3 prefix

echo "Script A prefix: $S3_PREFIX"

# Script B (Workflow 2) — ingest + register
uv run python scripts/run_ingest_register.py \
  --s3-geotiff-prefix "$S3_PREFIX" \
  --tile-id 31TCH --orbit-direction descending \
  --collection sentinel-1-grd-rtc-staging \
  --s3-output-bucket esa-zarr-sentinel-explorer-tests \
  --s3-output-prefix s1-rtc-test \
  --s3-endpoint https://s3.de.io.cloud.ovh.net \
  --stac-api-url https://api.explorer.eopf.copernicus.eu/stac \
  --raster-api-url https://api.explorer.eopf.copernicus.eu/raster

echo "Pipeline exit: $?"
```

</details>

---

### Task 4.6 — Record issues and report to Emmanuel  NEXT

**What**: collect all issues encountered in Tasks 4.1–4.5 and report to Emmanuel. The spec
explicitly lists "Issues reported to Emmanuel" as an acceptance criterion.

**Issues identified** (all confirmed during Tasks 4.0–4.4, 2026-05-26):

| # | Issue | Observed in | Triage | Status |
|---|-------|-------------|--------|--------|
| I-1 | `discover_s1tiling_acquisitions` uses `pathlib.Path.glob()` — S3 URIs silently return 0 acquisitions (OQ-1) | Task 4.0 | (a) Fix in `eopf_geozarr` before Sub-issue 6 | **Fixed** — data-model PR #175 merged (`5a91b355`); `data-pipeline` pin updated; 330 tests pass |
| I-2 | `ingest_v1_s1_rtc.py` has no `--s3-endpoint` arg — endpoint must be in `AWS_ENDPOINT_URL` env var | Planning | (c) Accept as-is — document as Argo env var requirement | Documented |
| I-3 | eopf_geozarr `Path(s3://)` → `s3:/` normalizes to local write; `consolidate_s1_store` then uses real S3 (empty) → FileNotFoundError | Task 4.1 | (b) Fixed locally: `run_ingest_register.py` now writes to tempdir then `aws s3 sync` | Fixed + tested |
| I-4 | `upsert_item` in `register_v1.py` set `exists=True` when pystac-client `get_item()` returns `None` (instead of raising) → DELETE 404 | Task 4.1 | (b) Fixed locally: check `fetched is not None` | Fixed + tested |
| I-5 | Stale local zarr at tempdir caused `BoundsCheckError` on re-run (append tried at out-of-bounds index) | Task 4.1 | (b) Fixed locally: `shutil.rmtree` before ingest | Fixed + tested |
| I-6 | TiTiler 500 on all render endpoints — S1 assets registered with `s3://` hrefs; TiTiler uses HTTPS gateway `s3.explorer.eopf.copernicus.eu` and can't resolve bare S3 URIs; additionally S1 buckets were not proxied by gateway | Task 4.4 | (b) Two-part fix | **Fixed** — (1) `register_v1_s1_rtc.py` converts `s3://` → `https://` via `s3_to_https()` (commit `b95bc43`); (2) platform-deploy PR #205 **merged**: nginx-s3-gateway now proxies `esa-zarr-sentinel-explorer-s1-l1grd-staging` + `-prod`. Pending: credentials wired to new gateway deployments |
| **I-7** | **TiTiler 500 on render/info even after I-6 — `KeyError: 'tile_matrix_set'` (variables path) / `not enough values to unpack (expected 4, got 0)` (assets path).** Root cause (debugged 2026-05-29): the S1 GeoZarr stores written by `eopf_geozarr.conversion.s1_ingest.create_s1_store` carry only the GeoZarr `proj:code` attr — they have **no CF `grid_mapping` / `spatial_ref` coordinate**. rioxarray ignores `proj:code`, so `ds.rio.crs is None` for every resolution group. The deployed **titiler-eopf v0.5.0** (`/raster/api` → version 0.5.0) validates each multiscale group with `_validate_zarr`, which returns `False` when `rio.crs is None`. All S1 groups are rejected → `self.groups == []` → reader construction raises (`zip(*[])`). The all-zero `tile_matrix_limits` (I-handoff) was a **red herring**: v0.5.0 is a GeoZarr-V0 reader and never reads `tile_matrix_limits`. Confirmed by: (a) the working S2 store has a `spatial_ref` coord (`rio.crs=EPSG:32626`), S1 does not (`rio.crs=None`); (b) gateway now serves the test bucket (HTTP 200) so I-6's infra theory no longer applies; (c) local round-trip proves adding `spatial_ref` + `decode_coords="all"` recovers the CRS. | Task 4.5 (debug) | (a)+(b) Library bug; worked around in pipeline | **Fixed in pipeline + LIVE-VERIFIED** — `ingest_v1_s1_rtc.py::_patch_cf_grid_mapping` injects a CF `spatial_ref` coordinate + `grid_mapping` attr into every (y,x) sub-group after consolidation; 2 RED→GREEN unit tests assert `rio.crs.to_epsg()==32631`. End-to-end proof: 31TDH store with the fix → live TiTiler `/info` + `/preview` + XYZ tiles all HTTP 200 (2026-05-29). **Upstream**: report to data-model — `create_s1_store` should (a) write CF `grid_mapping` like the S2 converter does, and (b) **not** write `tile_matrix_set`. **Update 2026-06-01**: Emmanuel confirmed `tile_matrix_set` is not part of the S1 data model. The `tile_matrix_limits` companion the pipeline used to inject (`_patch_tile_matrix_limits`) was always a red herring for v0.5.0 — it is now **removed** from `ingest_v1_s1_rtc.py` (only `_patch_cf_grid_mapping` remains in Step 6). Removing `tile_matrix_set` itself is the data-model library's job, not the pipeline's. |
| **I-8** | **TiTiler reads from a server-side path convention, not the STAC asset href.** For collection `sentinel-1-grd-rtc-staging`, TiTiler opens `s3://esa-zarr-sentinel-explorer-fra/tests-output/{collection}/{item_id}.zarr` (note: **prod `fra` bucket**, `tests-output/` prefix, store named after the **item id** `s1-rtc-31TDH`, not the file `s1-grd-rtc-31TDH`). Our pipeline writes to `esa-zarr-sentinel-explorer-tests/s1-rtc-test/s1-grd-rtc-{tile}.zarr`. TiTiler ignores the registered `vh`/`zarr-store` hrefs entirely. Symptom: a tile that *has* a store at the fra path (31TCH, placed earlier) returns the I-7 `tile_matrix_set` error; one that doesn't (31TDH) returns `"No group found in store ... bucket=esa-zarr-sentinel-explorer-fra prefix=tests-output/sentinel-1-grd-rtc-staging/s1-rtc-31TDH.zarr"`. | Task 4.5 (debug) | (a) Platform config — needs Emmanuel | **Worked around for proof** — copied the fixed 31TDH store to the fra path; TiTiler then served it. **RESOLVED (decision 2026-06-02, code-confirmed):** S1 stores live in **dedicated per-environment buckets** (local→`esa-zarr-sentinel-explorer-tests`, staging→`esa-zarr-sentinel-explorer-s1-l1grd-staging`, prod→`esa-zarr-sentinel-explorer-s1-l1grd-prod`), key `{collection}/{item_id}.zarr`. Root cause is in titiler-eopf: `DatasetPathParams` (`titiler/eopf/dependencies.py`) reconstructs `{TITILER_EOPF_STORE_URL}/{collection}/{item}.zarr` from a **single per-deployment base** (`hr-titiler-eopf.yaml` prod→`-fra/tests-output/`; `hr-titiler-eopf-test.yaml` staging→`-s2-l2a-staging/converted/`) and ignores the STAC href — so dedicated S1 buckets can't be addressed. **Fix = Option A**: change `DatasetPathParams` to resolve the store root from the item's asset href (titiler-eopf code change; a sibling reader `EOPFSimpleSTACReader` in `stac.py` already reads `asset_info["href"]`+`alternate`). Needs `TITILER_EOPF_STAC_API_URL` set on the prod `/raster` HelmRelease (currently unset) + gateway/creds for the S1 buckets. Pipeline side: env-keyed bucket (caller passes `--s3-output-bucket` + `--s3-output-prefix {collection}`); store filename changed from `s1-grd-rtc-{tile}.zarr` to item id `s1-rtc-{tile}.zarr`. Local `-tests` runs are never read by TiTiler (validation only). Tracked: titiler-eopf issue (Option A) + platform-deploy env. |

**Verify**: issues file updated with outcomes; message sent to Emmanuel.

**Acceptance criteria**:
- [x] All issues from Tasks 4.1–4.4 recorded in this plan under Task 4.6
- [x] Issues triaged: I-1→(a), I-2→(c), I-3→(b), I-4→(b), I-5→(b), I-6→(a), I-7→(a)+(b), I-8→(a)
- [x] Emmanuel notified of blockers I-7 (CF grid_mapping — upstream) and I-8 (store-path convention — platform config)
  — **2026-06-01**: Emmanuel confirmed `tile_matrix_set` is **not** part of the S1 data model. Two upstream
  data-model asks for `create_s1_store`: (a) write CF `grid_mapping`/`spatial_ref` like the S2 converter,
  and (b) stop writing `tile_matrix_set`. Pipeline side: `_patch_tile_matrix_limits` removed (it injected the
  `tile_matrix_limits` TMS companion); `_patch_cf_grid_mapping` retained.

---

## Checkpoint — gate before Sub-issues 6 and 7

Sub-issues 6 (Argo ingest template) and 7 (Argo s1tiling template) must not start until:

| Check | Status |
|-------|--------|
| Task 4.1 — Script B exits 0; ≥ 2 acquisitions ingested | [x] 3 acquisitions, exit 0 (2026-05-26) |
| Task 4.2 — `eopf-geozarr validate` passes; Zarr readable | [x] Validates clean; 3 timestamps (2026-05-26) |
| Task 4.3 — STAC item `s1-rtc-31TCH` queryable at staging API | [x] HTTP 200, all fields correct (2026-05-26) |
| Task 4.4 — TiTiler viewer link returns HTTP 200 | [x] Viewer 200; **render 500 root-caused + fixed (I-7) and live-verified on 31TDH** (2026-05-29) |
| Task 4.5 — Full chain re-run on a different tile (31TDH) | [x] Script A + B exit 0; validate + STAC pass; TiTiler renders live (2026-05-29) |
| Task 4.6 — Issues reported to Emmanuel | [ ] Pending notification (I-7 upstream, I-8 platform config) |

**Argo (Sub-issue 6/7) prerequisites surfaced by Task 4.5**:
- I-7 fix (`_patch_cf_grid_mapping`) is in `ingest_v1_s1_rtc.py` → the Argo ingest step inherits it.
- I-8 **resolved (2026-06-02)**: S1 stores go to per-environment buckets, key `{collection}/{item_id}.zarr`
  (local→`esa-zarr-sentinel-explorer-tests`/`sentinel-1-grd-rtc-tests`, staging→`…-s1-l1grd-staging`/`sentinel-1-grd-rtc-staging`,
  prod→`…-s1-l1grd-prod`/`sentinel-1-grd-rtc`). Pipeline: env-keyed bucket + `--s3-output-prefix {collection}` +
  store filename `s1-rtc-{tile}.zarr`. **Blocker for staging/prod render:** TiTiler must resolve the store from the
  STAC asset href (titiler-eopf `path_dependency` change) — platform-deploy / Emmanuel. Local render is N/A.
- `run_s1tiling.py` does not template the cfg → Sub-issue 7 Argo template must parametrise
  `roi_by_tiles`/`tiles`/`first_date`/`last_date`/`orbit_direction`.

---

## Open questions

### OQ-1 — Does `discover_s1tiling_acquisitions` support S3 URIs?

**Owner**: investigate in Task 4.0 probe.

**Background**: `eopf_geozarr.conversion.s1_ingest.discover_s1tiling_acquisitions` uses
`Path(input_dir).glob("*.tif")`. `pathlib.Path` does not support `s3://` URIs — it silently
treats them as local relative paths and returns 0 matches.

If the probe returns 0 acquisitions, Script B called with an S3 prefix will exit 2
("no acquisitions found") instead of ingesting.

**Investigation steps** (run in Task 4.0):
```python
# Does pathlib.Path handle s3:// on this system?
from pathlib import Path
p = Path("s3://esa-zarr-sentinel-explorer-tests/s1tiling-output/31TCH/descending/2025-02-01/")
files = list(p.glob("*.tif"))
print(len(files))  # 0 = broken; > 0 = somehow works (unexpected)
```

**Expected outcomes**:
- **0 files → workaround**: use Task 4.1 local-path workaround (merge `data_out/31TCH/` +
  `data_gamma_area/` into a single temp directory, pass that local path as `--s3-geotiff-prefix`).
  Flag to Emmanuel that `discover_s1tiling_acquisitions` needs S3/fsspec support before Argo
  deployment (since Argo has no `$S1T_WORKDIR` to fall back on).
- **> 0 files → pass**: `s3fs` or similar is active; S3 URI works; no workaround needed.

**Impact on Sub-issue 6 (Argo)**: in Argo, there is no local GeoTIFF directory — the ingest
step MUST read from S3. If OQ-1 remains unresolved, `ingest_v1_s1_rtc.py` will always exit 2
in Argo and no ingestion will happen. This **must** be fixed before Sub-issue 6 starts.

### OQ-2 — Does `eopf-geozarr validate` accept S3 URIs? (resolved 2026-05-26)

**Answer**: Yes. `eopf-geozarr validate s3://bucket/path.zarr` uses fsspec/s3fs internally
and reads directly from S3. No local copy needed. Exits 0 with compliant output.

---

## Open questions — resolved

### OQ-1 — Does `discover_s1tiling_acquisitions` support S3 URIs? (resolved 2026-05-26; fixed 2026-05-29)

**Answer**: No (original). `pathlib.Path("s3://bucket/...")` silently collapses `s3://` to `s3:/`
and treats it as a relative local path, returning 0 `.tif` matches.

**Workaround used in Task 4.1**: merge local `data_out/31TCH/` + `data_gamma_area/` into a temp
directory and pass that local path as `--s3-geotiff-prefix`.

**Fix (2026-05-29)**: data-model PR #175 (`fix/s3-uri-discovery` → `feat/s1-rtc-stac-builder`)
adds `_list_tifs`, `_coerce_input_path`, `_input_path_exists`, `_rasterio_env` helpers to
`s1_ingest.py`; both `discover_*` and both `ingest_*` functions now accept `s3://` URIs via
`s3fs`. **Merged as `5a91b355`**. `data-pipeline` `pyproject.toml` pin updated. 330 tests pass.

**rasterio/GDAL OVH endpoint note**: `rasterio.open("s3://...")` on OVH S3 requires
`rasterio.Env(AWSSession(session, endpoint_url='s3.de.io.cloud.ovh.net'))` — hostname only,
no scheme. `AWS_ENDPOINT_URL` alone is ignored by GDAL. `AWSSession.get_credential_options()`
passes `AWS_S3_ENDPOINT` to GDAL which must be the bare hostname.

---

## Done definition

Sub-issue 4 is complete when:
- Script B runs without error against real S3 data (or local-path workaround) and ingests ≥ 2 acquisitions
- Zarr store at `s3://esa-zarr-sentinel-explorer-tests/s1-rtc-test/s1-grd-rtc-31TCH.zarr` passes `eopf-geozarr validate`
- STAC item `s1-rtc-31TCH` is queryable at `https://api.explorer.eopf.copernicus.eu/stac`
- TiTiler viewer link returns HTTP 200
- All issues encountered are recorded and reported to Emmanuel
- Sub-issue 6/7 gate fully checked
