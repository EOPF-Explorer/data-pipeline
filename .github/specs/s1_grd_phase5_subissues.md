# Phase 5 Sub-Issues — S1 GRD RTC Local Prototype → Argo Pipeline

Parent issue: https://github.com/EOPF-Explorer/data-pipeline/issues/185

---

## Architecture overview

The specs define two independent Argo workflows coupled via S3. The local prototype is
two Python scripts covering the same logical inputs as those workflows. Once the local
scripts work end-to-end, the Argo YAML is a mechanical translation.

Local-only args (`--eodag-cfg`, `--dem-dir`, `--data-dir`, `--cfg-template`) have no
Argo equivalent — those are handled by mounted secrets and ConfigMaps in the cluster.

```
LOCAL                                ARGO (future)
─────────────────────────────────    ──────────────────────────────────
scripts/run_s1tiling.py          →   WorkflowTemplate eopf-explorer-s1tiling
  --tile-id         31TCH              tile_id:         31TCH
  --orbit-direction descending         orbit_direction: descending
  --date-start      2025-02-01         date_start:      2025-02-01
  --date-end        2025-02-14         date_end:        2025-02-14
  --s3-bucket       <bucket>           s3_geotiff_bucket: <bucket>
  --s3-prefix       s1tiling-output    s3_geotiff_prefix: s1tiling-output
  [--eodag-cfg / --dem-dir / ...]      (← local-only; handled by secrets/ConfigMap in Argo)
        │                                      │
        │ S3 GeoTIFF prefix (handoff)           │ S3 GeoTIFF prefix (handoff)
        ▼                                      ▼
scripts/run_ingest_register.py   →   WorkflowTemplate eopf-explorer-ingest-v1-s1rtc
  --s3-geotiff-prefix <prefix>         s3_geotiff_prefix: <prefix>
  --tile-id           31TCH            tile_id:           31TCH
  --orbit-direction   descending       orbit_direction:   descending
  --collection        sentinel-1-…     collection:        sentinel-1-grd-rtc-staging
  --s3-output-bucket  <bucket>         s3_output_bucket:  <bucket>
  --s3-output-prefix  s1-rtc-test      s3_output_prefix:  s1-rtc-staging
  --stac-api-url      <url>            stac_api_url:      …
  --raster-api-url    <url>            raster_api_url:    …
```

Zarr store path is derived (never passed explicitly): `s3://{s3_output_bucket}/{s3_output_prefix}/s1-grd-rtc-{tile_id}.zarr`

`run_ingest_register.py` calls `ingest_v1_s1_rtc.py` then `register_v1_s1_rtc.py` in
sequence, respecting exit code 2 (empty → skip). No logic of its own.

`watch_cdse_and_process.py` sits above both: queries CDSE for new S1 GRD products and
calls `run_s1tiling.py` → `run_ingest_register.py` for each new one.

---

## Dependency map

```
[Prerequisites]  CDSE account + EODAG creds + DEM tiles + Docker pull + config template
      │
      ├─── Sub-issue A  run_s1tiling.py (local Workflow 1 sim)
      │          │
      │          └────────────────────────────────┐
      │                                           │
      ├─── Sub-issue 1  data-model STAC builder   │
      ├─── Sub-issue 2  ingest_v1_s1_rtc.py       │
      ├─── Sub-issue 5  collection JSON            │
      │         │                                 │
      │    Sub-issue 3  register_v1_s1_rtc.py     │
      │         │                                 │
      └─────────┴──────────────────────────────────┘
                              │
                        Sub-issue B
                        run_ingest_register.py
                        (local Workflow 2 sim)
                              │
                        Sub-issue 4
                        End-to-end validation
                        (run A → B for tile 31TCH)
                              │
               ┌──────────────┴──────────────┐
               │                             │
         Sub-issue 6                   Sub-issue 7a (DEM PVC) ──► Sub-issue 7
         Argo ingest template          [can start now]              Argo s1tiling template
               │                                                          │
               └──────────────────────────┬───────────────────────────────┘
                                    Sub-issue 8 (cron + sensor)
                                    Sub-issue 9 (configmap)

Sub-issue 10  watch_cdse_and_process.py  ← needs A + B; independent from Argo work
```

**Critical path**: Prerequisites → A and (1, 2, 5 start immediately in parallel) → 3 → B → 4 → 6 → 8

Sub-issues 1, 2, 5 are in different repos (data-model / data-pipeline) with no dependency
on the local Docker setup. They can — and should — be started from day one in parallel
with Sub-issue A.

---

## Prerequisites — environment setup (no code, do first)

**P1 — CDSE account + EODAG credentials**
1. Create account at https://dataspace.copernicus.eu
2. Fill `~/Downloads/eodag-empty.yml` (from Emmanuel) — **never commit with credentials**:
   ```yaml
   cop_dataspace:
     priority: 1
     auth:
       credentials:
         username: <your-email>
         password: <your-password>
   ```
3. Store as `~/.config/eodag/eodag.yml`
4. Smoke-test: confirm `eodag search` returns ≥ 1 S1 GRD product for tile 31TCH, Feb 2025 :
```
uvx eodag search -p cop_dataspace -c S1_SAR_GRD \
  -s 2025-02-01 -e 2025-02-28 \
  --box 0 42 2 43 \
  --limit 5
```

**P2 — DEM tiles for 31TCH swath**

S1Tiling needs SRTM 1-arcsec (30 m) HGT tiles covering the **full S1 IW swath** — not just
the MGRS tile. For 31TCH the swath extends to 41–44°N, 3°W–5°E (Phase 0 finding): 24 tiles.

**Task**: download the tiles below to `~/s1tiling/dem/SRTM_30_hgt/` (mounted in Docker at
`/MNT/SRTM_30_hgt`).

Tile list (SW corner of each 1°×1° cell):
```
N41W003 N41W002 N41W001 N41E000 N41E001 N41E002 N41E003 N41E004
N42W003 N42W002 N42W001 N42E000 N42E001 N42E002 N42E003 N42E004
N43W003 N43W002 N43W001 N43E000 N43E001 N43E002 N43E003 N43E004
```

Download source — NASA Earthdata (free account required, same as CDSE signup):
```bash
mkdir -p ~/s1tiling/dem/SRTM_30_hgt
cd ~/s1tiling/dem/SRTM_30_hgt

# One-liner using wget + netrc (set up ~/.netrc with Earthdata creds once):
#   machine urs.earthdata.nasa.gov login <user> password <password>
BASE=https://e4ftl01.cr.usgs.gov/MEASURES/SRTMGL1.003/2000.02.11
for tile in N41W003 N41W002 N41W001 N41E000 N41E001 N41E002 N41E003 N41E004 \
            N42W003 N42W002 N42W001 N42E000 N42E001 N42E002 N42E003 N42E004 \
            N43W003 N43W002 N43W001 N43E000 N43E001 N43E002 N43E003 N43E004; do
  wget -q --netrc "$BASE/${tile}.SRTMGL1.hgt.zip" -O "${tile}.SRTMGL1.hgt.zip" \
    && unzip -qo "${tile}.SRTMGL1.hgt.zip" && rm "${tile}.SRTMGL1.hgt.zip" || true
done
```

> Some tiles may be ocean-only and absent from the SRTM catalogue — `|| true` skips them.
> Ask Emmanuel if he has a pre-downloaded set; prefer that over re-downloading.

**Smoke-test** (expect ≥ 20 files):
```bash
ls ~/s1tiling/dem/SRTM_30_hgt/*.hgt | wc -l
```

**P3 — S1Tiling Docker image + EODAG patch file**

Two tasks: (a) pull the image, (b) create and commit the EODAG 4.0 patch file.

**(a) Pull image:**
```bash
docker pull registry.orfeo-toolbox.org/s1tiling/s1tiling:1.4.0
```

Check the EODAG version bundled in the image — this determines whether the patch is already
applied or still needed at runtime:
```bash
docker run --rm registry.orfeo-toolbox.org/s1tiling/s1tiling:1.4.0 \
  python -c "import eodag; print(eodag.__version__)"
# < 4.0.0 → patch still needed; ≥ 4.0.0 → already handled, confirm with Emmanuel
```

**(b) Create `analysis/s1tiling_eodag4_patch.py`:**

This file does **not yet exist** in the repo. Obtain the patch content from Emmanuel (it
monkey-patches the S1Tiling EODAG 3→4 breaking change). Once received:

```bash
mkdir -p analysis
# paste content from Emmanuel → analysis/s1tiling_eodag4_patch.py
git add analysis/s1tiling_eodag4_patch.py
git commit -m "chore: add S1Tiling EODAG 4.0 compatibility patch"
```

**Smoke-test** — verify the patch loads without error inside the image:
```bash
docker run --rm \
  -v "$(pwd)/analysis/s1tiling_eodag4_patch.py:/patches/eodag_patch.py:ro" \
  -e PYTHONSTARTUP=/patches/eodag_patch.py \
  registry.orfeo-toolbox.org/s1tiling/s1tiling:1.4.0 \
  python -c "print('patch OK')"
```

The patch is injected unconditionally by Script A at runtime — applying an already-applied
patch is a no-op. Remove the injection only after Emmanuel confirms it is merged upstream.

**P4 — S1Tiling config template committed to repo**

The file `config/S1GRD_RTC_template.cfg` does **not yet exist** in the repo.

**Task**: create the directory and file, then commit it on this branch.

```bash
mkdir -p config
```

Create `config/S1GRD_RTC_template.cfg` with this exact content (no secrets — credentials
live in the EODAG file):

```ini
[Paths]
output     = /data/data_out
gamma_area = /data/data_gamma_area
s1_images  = /data/data_raw
eof_dir    = /data/eof
tmp        = /tmp/s1tiling
dem_dir    = /MNT/SRTM_30_hgt
geoid_file = /opt/S1TilingEnv/lib/python3.10/site-packages/s1tiling/resources/Geoid/egm96.grd

[DataSource]
eodag_config           = /eo_config/eodag.yml
download               = True
nb_parallel_downloads  = 2
roi_by_tiles           = {tile_id}
platform_list          = S1A
polarisation           = VV-VH
orbit_direction        = {orbit_direction_s1t}
first_date             = {date_start}
last_date              = {date_end}

[Processing]
calibration                        = gamma_naught_rtc
remove_thermal_noise               = True
output_spatial_resolution          = 10.
tiles                              = {tile_id}
nb_parallel_processes              = 1
ram_per_process                    = 8192
nb_otb_threads                     = 4
disable_streaming.apply_gamma_area = True

[Mask]
generate_border_mask = True

[Quicklook]
generate = False
```

Note: `{orbit_direction_s1t}` is `DES` or `ASC` — Script A converts `descending` → `DES`,
`ascending` → `ASC` before filling the template.

**Smoke-test** — verify all four placeholders are present:
```bash
grep -E '\{tile_id\}|\{orbit_direction_s1t\}|\{date_start\}|\{date_end\}' \
  config/S1GRD_RTC_template.cfg | wc -l
# expect 4
```

Commit:
```bash
git add config/S1GRD_RTC_template.cfg
git commit -m "chore: add S1GRD RTC config template for run_s1tiling.py"
```

> If Emmanuel's template differs from the above, use his version — the placeholders above
> are the contract that Script A depends on.

**P5 — Test S3 bucket + awscli**

Test bucket: **`esa-zarr-sentinel-explorer-tests`** (separate from production `esa-zarr-sentinel-explorer-fra`).

**Task 1** — install awscli v2 if not present:
```bash
aws --version   # must show aws-cli/2.x; install via: brew install awscli
```

**Task 2** — obtain OVH S3 credentials (ask Emmanuel/team for the `esa-zarr-sentinel-explorer-tests` access key and secret). Then configure:
```bash
aws configure --profile ovh-tests
# AWS Access Key ID:     <ovh-access-key>
# AWS Secret Access Key: <ovh-secret-key>
# Default region name:   de           (or leave blank)
# Default output format: json
```

Or set as env vars for the session:
```bash
export AWS_ACCESS_KEY_ID=<ovh-access-key>
export AWS_SECRET_ACCESS_KEY=<ovh-secret-key>
```

**Smoke-test** — verify bucket is accessible before starting Sub-issue A:
```bash
aws s3 ls s3://esa-zarr-sentinel-explorer-tests/ \
  --endpoint-url https://s3.de.io.cloud.ovh.net [--profile ovh-tests]
# expect: list of prefixes or empty output (no "Access Denied")
```

---

## Sub-issue A — `scripts/run_s1tiling.py` (local Workflow 1 simulation)

**Repo**: `EOPF-Explorer/data-pipeline`

**What it is**: Thin script covering the same logical inputs as Argo Workflow 1
(`eopf-explorer-s1tiling`). Runs S1Tiling in Docker locally and uploads GeoTIFFs to S3.
No logic beyond config templating, one Docker call, and one S3 sync.

**Interface**:

```bash
python scripts/run_s1tiling.py \
  --tile-id          31TCH \
  --orbit-direction  descending \
  --date-start       2025-02-01 \
  --date-end         2025-02-14 \
  --s3-bucket        esa-zarr-sentinel-explorer-tests \
  --s3-prefix        s1tiling-output \
  --s3-endpoint      https://s3.de.io.cloud.ovh.net \
  --eodag-cfg        ~/.config/eodag/eodag.yml \
  --dem-dir          ~/s1tiling/dem/SRTM_30_hgt \
  --data-dir         ~/s1tiling/data \
  --cfg-template     config/S1GRD_RTC_template.cfg \
  [--dry-run]
```

**Output**: prints the S3 prefix where GeoTIFFs were written — passed as `--s3-geotiff-prefix` to `run_ingest_register.py`.

**Behaviour** (≤ 60 lines of logic):

```
1. Convert --orbit-direction: "descending" → "DES", "ascending" → "ASC"
2. Fill config template: {tile_id}, {orbit_direction_s1t} (DES/ASC), {date_start}, {date_end}
3. Write filled config to a temp file
4. docker run \
     -v {abs_data_dir}:/data \
     -v {abs_dem_dir}:/MNT/SRTM_30_hgt \
     -v {abs_eodag_cfg}:/eo_config/eodag.yml:ro \
     -v {abs_tmp_cfg}:/config/run.cfg:ro \
     -v {abs_patch}:/patches/eodag_patch.py:ro \
     -e PYTHONSTARTUP=/patches/eodag_patch.py \
     registry.orfeo-toolbox.org/s1tiling/s1tiling:1.4.0 \
     /config/run.cfg
   # IMPORTANT: docker options (-v, -e) must come BEFORE the image name.
   # All mount paths must be absolute — use os.path.abspath() to expand ~ and relative refs.
   # Patch injected unconditionally — safe whether or not it's already in the image (see P3)
5. On success: aws s3 sync {abs_data_dir}/data_out/{tile_id}/ \
                 s3://{bucket}/{prefix}/{tile_id}/{orbit}/{date_start}/ \
                 --endpoint-url {s3_endpoint} [--profile ovh-tests]
              aws s3 sync {abs_data_dir}/data_gamma_area/ \
                 s3://{bucket}/{prefix}/{tile_id}/{orbit}/{date_start}/ \
                 --endpoint-url {s3_endpoint} [--profile ovh-tests]
              # ↑ conditions synced INTO the same prefix as acquisitions so
              #   discover_s1tiling_conditions(prefix) finds them (see C1 fix)
              # Pass --profile if credentials are in ~/.aws/credentials under a named profile.
6. Print: s3://{bucket}/{prefix}/{tile_id}/{orbit}/{date_start}/
```

where `{orbit}` is the lowercase full word (`descending`/`ascending`) — consistent with
the Argo output path convention and `ingest_v1_s1_rtc.py`'s expected prefix format.

**Expected S3 output structure** (everything under the same date prefix):

```
s3://esa-zarr-sentinel-explorer-tests/s1tiling-output/31TCH/descending/2025-02-01/
  s1a_31TCH_vv_DES_037_20250205T062921_GammaNaughtRTC.tif
  s1a_31TCH_vh_DES_037_20250205T062921_GammaNaughtRTC.tif
  s1a_31TCH_BorderMask_DES_037_20250205T062921.tif
  GAMMA_AREA_s1a_31TCH_DES_008.tif      ← conditions co-located with acquisitions
```

**Acceptance criteria**:
- [ ] `--dry-run` prints Docker command and S3 sync commands without executing
- [ ] Docker run completes; GeoTIFFs and GAMMA_AREA tif present locally
- [ ] All files present under the same S3 prefix; GeoTIFFs readable with rasterio (10980×10980)
- [ ] Script exits non-zero if Docker fails

**Depends on**: Prerequisites P1–P5
**Blocks**: Sub-issue 4, Sub-issue 10

---

## Sub-issue B — `scripts/run_ingest_register.py` (local Workflow 2 simulation)

**Repo**: `EOPF-Explorer/data-pipeline`

**What it is**: Thin orchestrator covering the same logical inputs as Argo Workflow 2
(`eopf-explorer-ingest-v1-s1rtc`). Calls `ingest_v1_s1_rtc.py` then
`register_v1_s1_rtc.py` in sequence. No logic of its own — just wiring.

**Interface**:

```bash
python scripts/run_ingest_register.py \
  --s3-geotiff-prefix  s3://esa-zarr-sentinel-explorer-tests/s1tiling-output/31TCH/descending/2025-02-01/ \
  --tile-id            31TCH \
  --orbit-direction    descending \
  --collection         sentinel-1-grd-rtc-staging \
  --s3-output-bucket   esa-zarr-sentinel-explorer-tests \
  --s3-output-prefix   s1-rtc-test \
  --s3-endpoint        https://s3.de.io.cloud.ovh.net \
  --stac-api-url       https://api.explorer.eopf.copernicus.eu/stac \
  --raster-api-url     https://api.explorer.eopf.copernicus.eu/raster
```

Zarr store path is derived internally: `s3://{s3_output_bucket}/{s3_output_prefix}/s1-grd-rtc-{tile_id}.zarr`.
This matches the canonical convention in the specs and the Argo template — it is never
passed as an explicit argument.

**Behaviour** (≤ 40 lines of logic):

```
zarr_store = f"s3://{s3_output_bucket}/{s3_output_prefix}/s1-grd-rtc-{tile_id}.zarr"

Step 1 — ingest:
  result = subprocess.run(["python", "scripts/ingest_v1_s1_rtc.py",
                           "--s3-geotiff-prefix", s3_geotiff_prefix,
                           "--s3-zarr-store", zarr_store,
                           "--tile-id", tile_id,
                           "--orbit-direction", orbit_direction])
  # subprocess.run() returns CompletedProcess — check .returncode, not the object itself
  if result.returncode == 2:
      log "no acquisitions found — skipping register"
      # Exit 0 locally so the watcher continues to the next product.
      # In Argo, exit 2 propagates to the retry policy directly; the
      # behaviour is equivalent but the mechanism differs.
      sys.exit(0)
  if result.returncode != 0: sys.exit(result.returncode)

Step 2 — register-stac (only reached if step 1 exited 0):
  result = subprocess.run(["python", "scripts/register_v1_s1_rtc.py",
                           "--store", zarr_store,
                           "--collection", collection,
                           "--stac-api-url", stac_api_url,
                           "--raster-api-url", raster_api_url,
                           "--s3-endpoint", s3_endpoint,
                           "--s3-output-bucket", s3_output_bucket,
                           "--s3-output-prefix", s3_output_prefix])
  sys.exit(result.returncode)
```

**Acceptance criteria**:
- [ ] With real GeoTIFFs from Sub-issue A: step 1 ingests, step 2 registers, exits 0
- [ ] With an empty S3 prefix: step 1 exits 2, script logs "skipping" and exits 0
- [ ] If ingest fails (exit 1): register is not called, script exits 1
- [ ] Item `s1-rtc-31TCH` queryable at the staging STAC API after a successful run

**Depends on**: Sub-issues 2, 3, 5
**Blocks**: Sub-issue 4, Sub-issue 10

---

## Sub-issue 1 — [data-model] STAC item builder (`build_s1_rtc_stac_item`)

**Repo**: `EOPF-Explorer/data-model`, branch `s1-tiling`
(If the branch does not yet exist, create it from `main` as the first step.)

**New file**: `src/eopf_geozarr/stac/s1_rtc.py`

```python
def build_s1_rtc_stac_item(zarr_store: str, collection_id: str) -> pystac.Item:
    ...
```

- Opens Zarr via consolidated metadata (single request, no full scan)
- `tile_id` derived from store basename `s1-grd-rtc-{tile_id}.zarr`
- For each orbit direction (`ascending`, `descending`): UTM bbox → WGS84 via pyproj, `time` range, `platform`
- Item `id`: `s1-rtc-{tile_id}`; `datetime`: null; `start_datetime`/`end_datetime` = min/max across orbits
- Assets: `zarr-store`, `vv`, `vh` (ascending preferred, fallback descending)
- STAC extensions: `sar`, `sat`, `projection`

**Also**: add `generate-stac-s1` subcommand to `src/eopf_geozarr/cli.py`.

**Tests**: `tests/test_s1_stac.py`, 8 tests minimum (roundtrip, temporal, bbox, both orbits,
ascending-only, empty store → `ValueError`, asset subpaths, SAR extension fields).

**Acceptance criteria**:
- [ ] All 8 tests pass with synthetic Zarr fixture in `tmp_path`
- [ ] Pre-commit passes (ruff, mypy)
- [ ] New version tag published (e.g. `v0.10.0`)
- [ ] `pyproject.toml` in `data-pipeline` bumped to pin this tag (blocks Sub-issue 3 otherwise)

**Depends on**: nothing — start immediately
**Blocks**: Sub-issue 3, Sub-issue B

---

## Sub-issue 2 — [data-pipeline] `scripts/ingest_v1_s1_rtc.py`

**Repo**: `EOPF-Explorer/data-pipeline`

Discovers S1Tiling GeoTIFFs from an S3 prefix, appends to the per-tile Zarr, consolidates.
Imports from `eopf_geozarr.conversion.s1_ingest` — no subprocess calls.
Data-model Phases 2–3 (ingestion + conditions code) are already done in the data-model repo
but not yet released. The installed version is v0.9.0; `s1_ingest` is not present in it.

> **Unblocking step**: before `ingest_v1_s1_rtc.py` can run, confirm with Emmanuel which
> data-model tag includes Phase 2–3 code, bump `pyproject.toml` to pin it, and `pip install`
> the updated package. This is separate from the v0.10.0 STAC-builder bump in Sub-issue 1.

**Interface**:
```bash
python scripts/ingest_v1_s1_rtc.py \
  --s3-geotiff-prefix  s3://esa-zarr-sentinel-explorer-tests/s1tiling-output/31TCH/descending/2025-02-01/ \
  --s3-zarr-store      s3://esa-zarr-sentinel-explorer-tests/s1-rtc-test/s1-grd-rtc-31TCH.zarr \
  --tile-id            31TCH \
  --orbit-direction    descending
```

**Behaviour**:
1. `discover_s1tiling_acquisitions(prefix)` — if empty: log + **exit 2** (clean skip)
2. For each acquisition: `ingest_s1tiling_acquisition(...)` — fail-fast on error (exit 1)
3. `discover_s1tiling_conditions(prefix)` — same prefix; GAMMA_AREA files co-located (see Sub-issue A); may be empty, non-fatal
4. For each condition group: `ingest_s1tiling_conditions(...)`
5. `consolidate_s1_store(zarr_store)`

**Exit codes**: 0 = success, 1 = error, 2 = no acquisitions found (Argo: no retry on 2)

**Tests**: `tests/test_ingest_v1_s1_rtc.py` — synthetic GeoTIFFs + GAMMA_AREA → local Zarr → `xr.open_zarr()` roundtrip

**Acceptance criteria**:
- [ ] Ingests ≥ 2 synthetic acquisitions; store readable by xarray
- [ ] `discover_s1tiling_conditions` finds GAMMA_AREA files in the same prefix as acquisitions
- [ ] Correct exit codes for all three states
- [ ] CI passes

**Depends on**: nothing — start immediately
**Blocks**: Sub-issue B

---

## Sub-issue 3 — [data-pipeline] `scripts/register_v1_s1_rtc.py`

**Repo**: `EOPF-Explorer/data-pipeline`

Builds STAC item from Zarr, augments with visualization links, upserts to STAC API.
Reuses helpers from `scripts/register_v1.py` — import, not copy.

> **Import path**: `register_v1.py` lives in `scripts/` and is not installed as a package.
> Add `sys.path.insert(0, str(Path(__file__).parent))` at the top of `register_v1_s1_rtc.py`
> so `from register_v1 import upsert_item, add_visualization_links, ...` resolves correctly.

**Interface**:
```bash
python scripts/register_v1_s1_rtc.py \
  --store            s3://esa-zarr-sentinel-explorer-tests/s1-rtc-test/s1-grd-rtc-31TCH.zarr \
  --collection       sentinel-1-grd-rtc-staging \
  --stac-api-url     https://api.explorer.eopf.copernicus.eu/stac \
  --raster-api-url   https://api.explorer.eopf.copernicus.eu/raster \
  --s3-endpoint      https://s3.de.io.cloud.ovh.net \
  --s3-output-bucket esa-zarr-sentinel-explorer-tests \
  --s3-output-prefix s1-rtc-test
```

**Behaviour** (in order, all from existing `register_v1.py` helpers):
1. `build_s1_rtc_stac_item(store, collection)` — from `eopf_geozarr.stac.s1_rtc`
2. `add_store_link` → `add_alternate_s3_assets` → `add_visualization_links` → `add_thumbnail_asset`
3. `warm_thumbnail_cache`
4. `upsert_item`

Skip `consolidate_reflectance_assets` and `fix_zarr_asset_media_types` (S2-specific).

**Tests**: `tests/test_register_v1_s1_rtc.py` — mock STAC API via `respx`, real item build from synthetic Zarr fixture

**Acceptance criteria**:
- [ ] Upserts item without error; viewer link returns HTTP 200
- [ ] CI passes

**Depends on**: Sub-issue 1 (STAC builder tagged; `pyproject.toml` pin bumped as part of Sub-issue 1)
**Blocks**: Sub-issue B

---

## Sub-issue 4 — End-to-end validation: run Script A → Script B for tile 31TCH

**Repo**: `EOPF-Explorer/data-pipeline`

Run the two local workflow scripts back-to-back, passing Script A's printed S3 prefix
directly into Script B. Proof that all pieces integrate before any Argo YAML is written.

**Run sequence**:

```bash
# 1. Run local Workflow 1 (S1Tiling → GeoTIFFs on S3)
python scripts/run_s1tiling.py \
  --tile-id 31TCH --orbit-direction descending \
  --date-start 2025-02-01 --date-end 2025-02-14 \
  --s3-bucket esa-zarr-sentinel-explorer-tests --s3-prefix s1tiling-output \
  --s3-endpoint https://s3.de.io.cloud.ovh.net \
  --eodag-cfg ~/.config/eodag/eodag.yml \
  --dem-dir ~/s1tiling/dem/SRTM_30_hgt \
  --data-dir ~/s1tiling/data \
  --cfg-template config/S1GRD_RTC_template.cfg

# Script A prints: s3://esa-zarr-sentinel-explorer-tests/s1tiling-output/31TCH/descending/2025-02-01/

# 2. Run local Workflow 2 (ingest + register)
python scripts/run_ingest_register.py \
  --s3-geotiff-prefix s3://esa-zarr-sentinel-explorer-tests/s1tiling-output/31TCH/descending/2025-02-01/ \
  --tile-id 31TCH --orbit-direction descending \
  --collection sentinel-1-grd-rtc-staging \
  --s3-output-bucket esa-zarr-sentinel-explorer-tests --s3-output-prefix s1-rtc-test \
  --s3-endpoint https://s3.de.io.cloud.ovh.net \
  --stac-api-url https://api.explorer.eopf.copernicus.eu/stac \
  --raster-api-url https://api.explorer.eopf.copernicus.eu/raster

# 3. Verify
curl "https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-1-grd-rtc-staging/items/s1-rtc-31TCH"
```

**Acceptance criteria**:
- [ ] Script A produces GeoTIFFs + GAMMA_AREA under the same S3 prefix
- [ ] Script B ingests ≥ 2 acquisitions; Zarr readable by `xr.open_zarr()`
- [ ] `eopf-geozarr validate-s1` passes on the Zarr store
- [ ] Item `s1-rtc-31TCH` queryable at staging STAC API
- [ ] TiTiler viewer link for `vv` returns HTTP 200
- [ ] Issues reported to Emmanuel

**Depends on**: Sub-issues A, B, 5
**Blocks**: Sub-issues 6, 7

---

## Sub-issue 5 — [data-pipeline] STAC collection `sentinel-1-grd-rtc-staging.json`

**Repo**: `EOPF-Explorer/data-pipeline`
**New file**: `stac/sentinel-1-grd-rtc-staging.json` (model on `stac/sentinel-2-l2a-staging.json`)

Key fields: `id: sentinel-1-grd-rtc-staging`, temporal from `2014-04-03`, global bbox.

Create in staging API once:
```bash
python operator-tools/manage_collections.py create \
  --collection stac/sentinel-1-grd-rtc-staging.json \
  --stac-api-url https://api.explorer.eopf.copernicus.eu/stac
```

**Tests**: add `pystac.Collection.from_file()` + validate to `tests/test_stac_collections.py`.

**Acceptance criteria**: schema-validates; created in staging API.

**Depends on**: nothing — start immediately
**Blocks**: Sub-issue B (register needs collection to exist before first upsert)

---

## Sub-issue 10 — `scripts/watch_cdse_and_process.py` (automated trigger)

**Repo**: `EOPF-Explorer/data-pipeline`

Queries CDSE STAC API for new S1 GRD products and calls Script A → Script B for each
new one. Local equivalent of the Argo CronWorkflow.

**Interface**:
```bash
python scripts/watch_cdse_and_process.py \
  --tiles             31TCH \
  --orbit-direction   descending \
  --lookback-days     7 \
  --s3-bucket         esa-zarr-sentinel-explorer-tests \
  --s3-prefix         s1tiling-output \
  --s3-zarr-bucket    esa-zarr-sentinel-explorer-tests \
  --s3-zarr-prefix    s1-rtc-test \
  --s3-endpoint       https://s3.de.io.cloud.ovh.net \
  --collection        sentinel-1-grd-rtc-staging \
  --stac-api-url      https://api.explorer.eopf.copernicus.eu/stac \
  --raster-api-url    https://api.explorer.eopf.copernicus.eu/raster \
  [--dry-run]
```

**Behaviour**:
```
1. Query CDSE STAC API (catalogue.dataspace.copernicus.eu/stac)
   collection: SENTINEL-1-GRD
   bbox: tile WGS84 bbox  # 31TCH → [0.0, 42.0, 2.0, 43.0] (lon_min, lat_min, lon_max, lat_max)
                           # Derive from MGRS tile ID at runtime using mgrs or s2geometry lib,
                           # or hardcode a per-tile lookup dict for the initial scope (31TCH only)
   datetime: (now - lookback_days) / now

   ⚠️ VERIFY before implementing: confirm correct value of sat:orbit_state filter.
   EODAG 4.0 patch notes say orbit direction must be UPPERCASE ("DESCENDING") when
   passed through EODAG. Unclear if the raw CDSE STAC API also requires uppercase.
   Smoke-test both "descending" and "DESCENDING" against the live API before coding.

2. For each product:
   a. Skip if already in state file (data/.processed_products.json)
   b. Extract acquisition date from product datetime
   c. Run Script A (run_s1tiling.py) with --date-start/--date-end set to that date ± 1 day;
      capture the printed output prefix
   d. Run Script B (run_ingest_register.py) with Script A's output prefix and
      --s3-zarr-bucket / --s3-zarr-prefix for the Zarr output
      # NOTE: the watcher uses --s3-zarr-bucket/prefix internally but passes them to Script B
      # as --s3-output-bucket / --s3-output-prefix (Script B's actual arg names).
   e. On success: mark product in state file
   f. On failure: log error, continue to next product

3. Print summary: N found, M new, K processed, L failed
```

State file `data/.processed_products.json`:
```json
{"31TCH": {"descending": [{"product_id": "S1A_IW_GRDH_...", "date": "2025-02-05"}]}}
```

**Acceptance criteria**:
- [ ] `--dry-run` prints CDSE query results and planned runs without executing
- [ ] `sat:orbit_state` filter casing verified against live CDSE API before submission
- [ ] Idempotent: re-running with same `--lookback-days` skips already-processed products
- [ ] Processes at least one new product end-to-end

**Depends on**: Sub-issues A, B

---

## Sub-issue 6 — [platform-deploy] Argo WorkflowTemplate: `eopf-explorer-ingest-v1-s1rtc`

**Repo**: `EOPF-Explorer/platform-deploy`
**New file**: `workspaces/devseed-staging/data-pipeline/eopf-explorer-ingest-v1-s1rtc-template.yaml`

Argo translation of `run_ingest_register.py` (Sub-issue B). Written after Sub-issue 4 validates local scripts.

**Docker image**: both steps run the data-pipeline image (the same image used for existing
`convert_v1_s2.py` / `register_v1.py` jobs). `pipeline_image_version` selects the tag.
Look up the image name from an existing WorkflowTemplate in `platform-deploy` (e.g.
`eopf-explorer-convert-v1`).

**DAG** (2 steps, mirrors Sub-issue B):
1. `ingest` — `scripts/ingest_v1_s1_rtc.py`; 2 CPU, 8Gi; `activeDeadlineSeconds: 3600`
   - Retry on exit codes 1, 137, 143 — **no retry on exit code 2** (`asInt(lastRetry.exitCode) == 2`)
   - S3 credentials from `geozarr-s3-credentials` secret (keys: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. `register-stac` (depends-on `ingest`) — `scripts/register_v1_s1_rtc.py`; 1 CPU, 2Gi; retry 5×, always

**Parameters** (aligned with Script B, Zarr path derived — not passed explicitly):

| Parameter | Default |
|-----------|---------|
| `s3_geotiff_prefix` | — |
| `tile_id` | — |
| `orbit_direction` | `descending` |
| `collection` | `sentinel-1-grd-rtc-staging` |
| `s3_output_bucket` | `esa-zarr-sentinel-explorer-fra` |
| `s3_output_prefix` | `s1-rtc-staging` |
| `s3_endpoint` | `https://s3.de.io.cloud.ovh.net` |
| `stac_api_url` | `https://api.explorer.eopf.copernicus.eu/stac` |
| `raster_api_url` | `https://api.explorer.eopf.copernicus.eu/raster` |
| `pipeline_image_version` | — |
| `semaphore_key` | `v1-s1rtc-limit` |

**Acceptance criteria**: `argo lint --offline` passes; manually triggered run for 31TCH completes; exit-2 no-retry verified in cluster.

**Depends on**: Sub-issue 4

---

## Sub-issue 7 — [platform-deploy] Argo WorkflowTemplate: `eopf-explorer-s1tiling`

**Repo**: `EOPF-Explorer/platform-deploy`
**New file**: `workspaces/devseed-staging/data-pipeline/eopf-explorer-s1tiling-template.yaml`

Argo translation of `run_s1tiling.py` (Sub-issue A). Written after Sub-issue 4 validates local scripts.

**Single step** `run-s1tiling`: image `registry.orfeo-toolbox.org/s1tiling/s1tiling:1.4.0`; 4 CPU, 16Gi; `activeDeadlineSeconds: 7200`; retry 3×, exponential backoff.

**Parameters** (aligned with Script A logical inputs):

| Parameter | Default |
|-----------|---------|
| `tile_id` | — |
| `orbit_direction` | `descending` |
| `date_start` | — |
| `date_end` | — |
| `s3_geotiff_bucket` | `esa-zarr-sentinel-explorer-fra` |
| `s3_geotiff_prefix` | `s1tiling-output` |
| `s1tiling_image` | `registry.orfeo-toolbox.org/s1tiling/s1tiling:1.4.0` |
| `semaphore_key` | `v1-s1tiling-limit` |

**Config injection**: The `{tile_id}`-style Python placeholders in `S1GRD_RTC_template.cfg`
cannot be filled by Argo's `{{inputs.parameters.*}}` substitution directly (different syntax).
Recommended approach:
1. Commit `config/S1GRD_RTC_template.cfg` to `platform-deploy` as a ConfigMap, mounted
   read-only at `/config/template.cfg`.
2. Add an `init-container` (or `initContainers` in the pod spec) that runs a tiny Python
   one-liner to render the template and write it to an `emptyDir` volume:
   ```
   python -c "
   import sys, pathlib
   t = pathlib.Path('/config/template.cfg').read_text()
   out = t.format(
     tile_id=sys.argv[1],
     orbit_direction_s1t=('DES' if sys.argv[2]=='descending' else 'ASC'),
     date_start=sys.argv[3], date_end=sys.argv[4])
   pathlib.Path('/rendered/run.cfg').write_text(out)
   " {{inputs.parameters.tile_id}} {{inputs.parameters.orbit_direction}} \
     {{inputs.parameters.date_start}} {{inputs.parameters.date_end}}
   ```
3. The main S1Tiling container mounts the same `emptyDir` at `/config/run.cfg`.

**DEM access in Argo — recommended approach: PersistentVolumeClaim (ReadOnlyMany)**

S1Tiling expects the DEM at a fixed path (`/MNT/SRTM_30_hgt`). A PVC with `ReadOnlyMany`
access mode mounts directly there without an init container or per-run S3 download.
Multiple S1Tiling pods can read simultaneously with no contention.

One-time setup (before Sub-issue 7):
1. Create a PVC in the `devseed` namespace (e.g. `s1tiling-dem-pvc`, `ReadOnlyMany`, 5 Gi)
2. Populate it with SRTM 30m HGT tiles for the tiles of interest (same set as P2)
3. Mount in the Argo pod spec:
   ```yaml
   volumes:
     - name: dem
       persistentVolumeClaim:
         claimName: s1tiling-dem-pvc
         readOnly: true
   containers:
     - volumeMounts:
         - name: dem
           mountPath: /MNT/SRTM_30_hgt
           readOnly: true
   ```

**EODAG patch in Argo**: same inject-unconditionally approach as Sub-issue A.
Mount `analysis/s1tiling_eodag4_patch.py` as a ConfigMap and set
`PYTHONSTARTUP=/patches/eodag_patch.py` in the container env.

**Acceptance criteria**: `argo lint --offline` passes; run for 31TCH produces GeoTIFFs at `s3://{bucket}/s1tiling-output/31TCH/descending/{date_start}/`.

**Depends on**: Sub-issue 4 + DEM access strategy confirmed + EODAG patch question resolved

---

## Sub-issue 7a — [platform-deploy] DEM PVC: create, populate, verify

**Repo**: `EOPF-Explorer/platform-deploy`

One-time setup that must be done before Sub-issue 7 can be implemented. The S1Tiling pod
expects DEM tiles at `/MNT/SRTM_30_hgt`; a `ReadOnlyMany` PVC is the recommended mount
strategy (no per-run download, multiple pods can read concurrently).

**Steps**:
1. Create PVC `s1tiling-dem-pvc` in the `devseed` namespace (`ReadOnlyMany`, 5 Gi):
   ```yaml
   apiVersion: v1
   kind: PersistentVolumeClaim
   metadata:
     name: s1tiling-dem-pvc
     namespace: devseed
   spec:
     accessModes: [ReadOnlyMany]
     resources:
       requests:
         storage: 5Gi
   ```
2. Populate from the same SRTM 30m HGT tiles used locally (P2 — same ~20 tiles for the
   31TCH swath: 41–44°N, 3°W–5°E). Copy via a one-off pod or `kubectl cp`.
3. Commit the PVC manifest to `workspaces/devseed-staging/data-pipeline/`.

**Acceptance criteria**:
- [ ] PVC manifest committed and applied in `devseed` namespace; `kubectl get pvc s1tiling-dem-pvc -n devseed` shows `Bound`
- [ ] Test pod mounts the PVC at `/MNT/SRTM_30_hgt` and lists ≥ 20 `.hgt` files covering the 31TCH swath
- [ ] Coordinate with Emmanuel to confirm tile set is complete before populating

**Depends on**: P2 (DEM tiles downloaded locally)
**Blocks**: Sub-issue 7

---

## Sub-issue 8 — [platform-deploy] CronWorkflow + Webhook Sensor

**CronWorkflow** (`0 6 * * *`, `concurrencyPolicy: Forbid`): triggers the **full pipeline** —
Workflow 1 (`eopf-explorer-s1tiling`) followed by Workflow 2 (`eopf-explorer-ingest-v1-s1rtc`)
— for tile 31TCH / descending. It runs daily; most days will find no new data (S1 cadence is
6 days) and S1Tiling will produce no output. This is intentional: the cost is low and
`concurrencyPolicy: Forbid` ensures no concurrent runs stack up.

**Sensor** (Webhook): filter `body.action == "^ingest-v1-s1rtc$"`, maps POST body fields to
**Workflow 2 only** (used for manual re-ingest when GeoTIFFs already exist on S3).

Manual trigger body:
```json
{
  "action": "ingest-v1-s1rtc",
  "s3_geotiff_prefix": "s3://esa-zarr-sentinel-explorer-fra/s1tiling-output/31TCH/descending/2025-02-05/",
  "tile_id": "31TCH",
  "orbit_direction": "descending"
}
```

**Acceptance criteria**:
- [ ] `argo lint --offline` passes for both the CronWorkflow and Sensor manifests
- [ ] CronWorkflow manually triggered in cluster completes end-to-end for tile 31TCH
- [ ] Webhook POST with the manual trigger body above fires `eopf-explorer-ingest-v1-s1rtc` and the run completes
- [ ] `concurrencyPolicy: Forbid` verified: a second manual trigger while first is running does not spawn a second run

**Depends on**: Sub-issue 6

---

## Sub-issue 9 — [platform-deploy] Concurrency configmap

Add to `eopf-workflow-concurrency-configmap.yaml`:
```yaml
v1-s1rtc-limit: "3"     # ingest+register — S3 writes are independent per tile
v1-s1tiling-limit: "2"  # CPU/memory heavy
```

Per-tile ZARR write isolation requirement (2026-04-23 meeting): only one ingest workflow
should write to a given tile's Zarr store at a time.

The Argo semaphore `v1-s1rtc-limit: "3"` is a **global** limit — it caps total concurrent
ingest workflows to 3, but does not prevent two workflows from writing to the same tile
simultaneously. True per-tile isolation requires the `semaphore_key` parameter to be
tile-specific (e.g. `v1-s1rtc-31TCH`), with each tile having its own ConfigMap key set
to `"1"`. Coordinate with Emmanuel to confirm whether:
(a) global limit of 1 (`"1"`) is acceptable for the initial scope (single tile), or
(b) per-tile keys are needed now (add one key per active tile to the ConfigMap).
Until confirmed, document the chosen approach here before implementing Sub-issue 9.

**Acceptance criteria**:
- [ ] ConfigMap committed and applied; `kubectl get configmap eopf-workflow-concurrency-configmap -n devseed -o yaml` shows both new keys
- [ ] Run 4 concurrent `eopf-explorer-ingest-v1-s1rtc` workflows: only 3 run in parallel (4th queues)
- [ ] Run 3 concurrent `eopf-explorer-s1tiling` workflows: only 2 run in parallel (3rd queues)

**Depends on**: Sub-issues 6, 7

---

## Summary

| # | Deliverable | Mirrors | Can start | Blocks |
|---|-------------|---------|-----------|--------|
| P1–P5 | CDSE account, DEM, Docker, config template, test bucket + awscli | — | now | A |
| **A** | `scripts/run_s1tiling.py` | Argo Workflow 1 | after P1–P5 | 4, 10 |
| **B** | `scripts/run_ingest_register.py` | Argo Workflow 2 | after 2, 3 | 4, 10 |
| 1 | data-model STAC builder + pyproject.toml pin bump | — | **now** | 3, B |
| 2 | `scripts/ingest_v1_s1_rtc.py` | Argo step 1 of Wf2 | **now** | B |
| 3 | `scripts/register_v1_s1_rtc.py` | Argo step 2 of Wf2 | after 1 | B |
| 5 | STAC collection JSON | — | **now** | B |
| **4** | **End-to-end validation (run A → B for 31TCH)** | Full pipeline | after A, B, 5 | 6, 7 |
| 10 | `scripts/watch_cdse_and_process.py` | Argo CronWorkflow | after A, B | — |
| 6 | Argo template: ingest+register | Script B | after 4 | 8, 9 |
| 7a | DEM PVC: create, populate, verify | — | **now** (parallel with local work) | 7 |
| 7 | Argo template: s1tiling | Script A | after 4 + 7a + EODAG patch resolved | 8 |
| 8 | CronWorkflow + Sensor | — | after 6 | — |
| 9 | Concurrency configmap | — | after 6, 7 | — |

**Critical path**: Prerequisites → A and (1, 2, 5 start immediately in parallel) → 3 → B → 4 → 6 → 8
