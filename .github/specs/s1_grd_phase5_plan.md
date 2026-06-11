# S1 GRD Phase 5 — Implementation Plan

Parent issue: https://github.com/EOPF-Explorer/data-pipeline/issues/185
Spec: `.github/specs/s1_grd_phase5_subissues.md`

---

## Principle

Local prototype first, Argo second. Sub-issues 6–9 (Argo) are not started until Sub-issue 4
(end-to-end local validation) passes. Every task below has a concrete smoke-test or acceptance
check so you know when it's done.

---

## Phase 0 — Environment (P1–P5) — do before any code

These are manual setup steps, not code. Do them in order; each one unblocks the next phase.

### 0.1 — CDSE account + EODAG credentials (P1)

**Do:**
1. Create account at https://dataspace.copernicus.eu
2. Fill `~/.config/eodag/eodag.yml` from Emmanuel's template (never commit)

**Verify:**
```bash
uvx eodag search -p cop_dataspace -c S1_SAR_GRD \
  -s 2025-02-01 -e 2025-02-28 --box 0 42 2 43 --limit 5
# expect ≥ 1 result
```

---

### 0.2 — DEM tiles (P2)

**Do:** download 24 SRTM 1-arcsec tiles to `~/s1tiling/dem/SRTM_30_hgt/`

Tiles (41–44°N, 3°W–5°E):
```
N41W003 N41W002 N41W001 N41E000 N41E001 N41E002 N41E003 N41E004
N42W003 N42W002 N42W001 N42E000 N42E001 N42E002 N42E003 N42E004
N43W003 N43W002 N43W001 N43E000 N43E001 N43E002 N43E003 N43E004
```

> Ask Emmanuel first — he may have the set already downloaded.

**Verify:**
```bash
ls ~/s1tiling/dem/SRTM_30_hgt/*.hgt | wc -l   # expect ≥ 20
```

---

### 0.3 — Docker image + EODAG patch file (P3)

**Do:**
```bash
docker pull registry.orfeo-toolbox.org/s1tiling/s1tiling:1.4.0
docker run --rm registry.orfeo-toolbox.org/s1tiling/s1tiling:1.4.0 \
  python -c "import eodag; print(eodag.__version__)"
# record version; report to Emmanuel
```

Then obtain `analysis/s1tiling_eodag4_patch.py` from Emmanuel and commit it:
```bash
mkdir -p analysis
# paste content → analysis/s1tiling_eodag4_patch.py
git add analysis/s1tiling_eodag4_patch.py
git commit -m "chore: add S1Tiling EODAG 4.0 compatibility patch"
```

**Verify:**
```bash
docker run --rm \
  -v "$(pwd)/analysis/s1tiling_eodag4_patch.py:/patches/eodag_patch.py:ro" \
  -e PYTHONSTARTUP=/patches/eodag_patch.py \
  registry.orfeo-toolbox.org/s1tiling/s1tiling:1.4.0 \
  python -c "print('patch OK')"
```

---

### 0.4 — Config template committed (P4)

**Do:** create `config/S1GRD_RTC_template.cfg` (content in spec) and commit:
```bash
mkdir -p config
# write file from spec
git add config/S1GRD_RTC_template.cfg
git commit -m "chore: add S1GRD RTC config template"
```

**Verify:**
```bash
grep -cE '\{tile_id\}|\{orbit_direction_s1t\}|\{date_start\}|\{date_end\}' \
  config/S1GRD_RTC_template.cfg   # expect 4
```

---

### 0.5 — S3 credentials + bucket access (P5)

**Do:**
```bash
brew install awscli   # if not present
aws configure --profile ovh-tests   # get key/secret from Emmanuel
```

**Verify:**
```bash
aws s3 ls s3://esa-zarr-sentinel-explorer-tests/ \
  --endpoint-url https://s3.de.io.cloud.ovh.net --profile ovh-tests
# expect: listing or empty — no "Access Denied"
```

### 0.6 — Unblock data-model ingestion functions (prerequisite for Task 2.1)

**Do:** Ask Emmanuel which data-model tag contains Phase 2–3 (`s1_ingest` module). Bump
`pyproject.toml` to pin it and install:
```bash
pip install "eopf-geozarr @ git+https://github.com/EOPF-Explorer/data-model@<tag>"
python -c "from eopf_geozarr.conversion import s1_ingest; print('ok')"
```

> Current installed version is v0.9.0 — `s1_ingest` is absent from it.

---

## Phase 1 — Parallel groundwork (start after Phase 0)

Tasks 1.1, 1.2, and 1.3 have no dependency on each other. Start all three in parallel.

---

### Task 1.1 — [data-model] STAC item builder (Sub-issue 1)

**Repo:** `EOPF-Explorer/data-model`, branch `s1-tiling`
**New file:** `src/eopf_geozarr/stac/s1_rtc.py`

```python
def build_s1_rtc_stac_item(zarr_store: str, collection_id: str) -> pystac.Item
```

Key behaviour:
- Opens Zarr via `zarr.open_consolidated()` — single request, no full scan
- `tile_id` from store basename `s1-grd-rtc-{tile_id}.zarr`
- Temporal range: `start_datetime`/`end_datetime` = min/max across orbits; `datetime`: null
- Assets: `zarr-store`, `vv`, `vh` (ascending preferred, fallback descending)
- STAC extensions: `sar`, `sat`, `projection`

Also add `generate-stac-s1` CLI subcommand.

**Tests** (`tests/test_s1_stac.py`, ≥ 8):
roundtrip, temporal range, bbox WGS84, both orbits, ascending-only, empty store → ValueError,
asset subpaths, SAR extension fields.

**Acceptance:**
- [ ] All ≥ 8 tests pass with synthetic Zarr fixture in `tmp_path`
- [ ] `pre-commit` passes (ruff, mypy)
- [ ] New version tag published (e.g. `v0.10.0`)
- [ ] `pyproject.toml` in `data-pipeline` bumped to pin this tag

**Blocks:** Task 2.2

---

### Task 1.2 — [data-pipeline] Ingest script (Sub-issue 2)

**Repo:** `EOPF-Explorer/data-pipeline`
**New file:** `scripts/ingest_v1_s1_rtc.py`

Calls `eopf_geozarr.conversion.s1_ingest` functions in order:
1. `discover_s1tiling_acquisitions(prefix)` → exit 2 if empty
2. `ingest_s1tiling_acquisition(...)` per acquisition → exit 1 on error
3. `discover_s1tiling_conditions(prefix)` → GAMMA_AREA files (non-fatal if empty)
4. `ingest_s1tiling_conditions(...)` per group
5. `consolidate_s1_store(zarr_store)`

Exit codes: 0 = success, 1 = error, 2 = no acquisitions.

> Requires data-model tag with Phase 2–3 code (see Task 0.6).

**Tests** (`tests/test_ingest_v1_s1_rtc.py`):
synthetic GeoTIFFs + GAMMA_AREA → local Zarr → `xr.open_zarr()` roundtrip; all three exit codes.

**Acceptance:**
- [ ] ≥ 2 synthetic acquisitions ingested; Zarr readable by xarray
- [ ] GAMMA_AREA files discovered from same prefix
- [ ] Correct exit codes for all three states
- [ ] CI passes

**Blocks:** Task 2.3

---

### Task 1.3 — [data-pipeline] STAC collection JSON (Sub-issue 5)

**Repo:** `EOPF-Explorer/data-pipeline`
**New file:** `stac/sentinel-1-grd-rtc-staging.json`

Model on `stac/sentinel-2-l2a-staging.json`. Key fields:
- `id`: `sentinel-1-grd-rtc-staging`
- `temporal`: start `2014-04-03` (S1A launch), no end
- `bbox`: `[-180, -90, 180, 90]`
- `platform`: `sentinel-1a`, `sentinel-1b`; `instruments`: `["c-sar"]`

Add `pystac.Collection.from_file()` + `validate()` to `tests/test_stac_collections.py`.

Create in staging API once:
```bash
python operator-tools/manage_collections.py create \
  --collection stac/sentinel-1-grd-rtc-staging.json \
  --stac-api-url https://api.explorer.eopf.copernicus.eu/stac
```

**Acceptance:**
- [ ] `pystac` validation passes
- [ ] Collection exists in staging API (`curl .../stac/collections/sentinel-1-grd-rtc-staging`)
- [ ] CI passes

**Blocks:** Task 2.3 (register needs the collection before first upsert)

---

### Checkpoint 1

All of the following must be true before starting Phase 2:
- [ ] Task 1.1: data-model tag published; `pyproject.toml` pinned
- [ ] Task 1.2: ingest script passes its tests
- [ ] Task 1.3: collection exists in staging API
- [ ] Task 0.6: `s1_ingest` importable from installed package

---

## Phase 2 — Scripts that wire the pieces together

Tasks 2.1 and 2.2 are independent; Task 2.3 depends on both.

---

### Task 2.1 — [data-pipeline] `run_s1tiling.py` (Sub-issue A)

**Repo:** `EOPF-Explorer/data-pipeline`
**New file:** `scripts/run_s1tiling.py`

Thin script (≤ 60 lines of logic): template → Docker → S3 sync → print prefix.

Key implementation notes:
- Use `os.path.abspath()` on all paths before passing to `docker run` — Docker requires
  absolute bind-mount paths.
- Docker options (`-v`, `-e`) must come **before** the image name.
- Pass `--profile ovh-tests` (or rely on env vars) to both `aws s3 sync` calls.
- The patch file is always mounted; applying an already-applied patch is a no-op.

**Acceptance:**
- [ ] `--dry-run` prints Docker + S3 commands without executing
- [ ] Docker run completes; GeoTIFFs + GAMMA_AREA present locally
- [ ] Files present under correct S3 prefix; GeoTIFFs readable with `rasterio` (10980×10980)
- [ ] Script exits non-zero if Docker fails

**Depends on:** Phase 0 (P1–P5)

---

### Task 2.2 — [data-pipeline] `register_v1_s1_rtc.py` (Sub-issue 3)

**Repo:** `EOPF-Explorer/data-pipeline`
**New file:** `scripts/register_v1_s1_rtc.py`

Reuses helpers from `scripts/register_v1.py` — import, not copy. Add at the top:
```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from register_v1 import (
    upsert_item, add_store_link, add_alternate_s3_assets,
    add_visualization_links, add_thumbnail_asset, warm_thumbnail_cache,
)
```

Call order:
1. `build_s1_rtc_stac_item(store, collection)` — from `eopf_geozarr.stac.s1_rtc`
2. `add_store_link` → `add_alternate_s3_assets` → `add_visualization_links` → `add_thumbnail_asset`
3. `warm_thumbnail_cache`
4. `upsert_item`

Skip `consolidate_reflectance_assets` and `fix_zarr_asset_media_types` (S2-specific).

**Tests** (`tests/test_register_v1_s1_rtc.py`): mock STAC API via `respx`; real item build
from synthetic Zarr fixture.

**Acceptance:**
- [ ] Upserts item without error; viewer link returns HTTP 200
- [ ] CI passes

**Depends on:** Task 1.1 (STAC builder)

---

### Task 2.3 — [data-pipeline] `run_ingest_register.py` (Sub-issue B)

**Repo:** `EOPF-Explorer/data-pipeline`
**New file:** `scripts/run_ingest_register.py`

Orchestrator (≤ 40 lines): calls `ingest_v1_s1_rtc.py` then `register_v1_s1_rtc.py`
in sequence using `subprocess.run()`. Check `.returncode`, not the `CompletedProcess` object:

```python
result = subprocess.run(["python", "scripts/ingest_v1_s1_rtc.py", ...])
if result.returncode == 2:
    print("no acquisitions — skipping register")
    sys.exit(0)
if result.returncode != 0:
    sys.exit(result.returncode)
result = subprocess.run(["python", "scripts/register_v1_s1_rtc.py", ...])
sys.exit(result.returncode)
```

Zarr path derived internally:
`s3://{s3_output_bucket}/{s3_output_prefix}/s1-grd-rtc-{tile_id}.zarr`

**Acceptance:**
- [ ] Real GeoTIFFs from Task 2.1: step 1 ingests, step 2 registers, exits 0
- [ ] Empty S3 prefix: exits 0 with "skipping" log
- [ ] Ingest failure: register not called, exits 1
- [ ] Item `s1-rtc-31TCH` queryable at staging STAC API

**Depends on:** Tasks 1.2, 2.2, 1.3

---

### Checkpoint 2

Both scripts are independently tested. Before Phase 3:
- [ ] Task 2.1: `run_s1tiling.py` acceptance criteria pass (dry-run + real run)
- [ ] Task 2.3: `run_ingest_register.py` acceptance criteria pass (with mock data)

---

## Phase 3 — End-to-end local validation (Sub-issue 4)

Run Script A → Script B back-to-back for tile 31TCH with real data.

```bash
# Step 1: S1Tiling → GeoTIFFs on S3 (~30–90 min depending on downloads)
python scripts/run_s1tiling.py \
  --tile-id 31TCH --orbit-direction descending \
  --date-start 2025-02-01 --date-end 2025-02-14 \
  --s3-bucket esa-zarr-sentinel-explorer-tests --s3-prefix s1tiling-output \
  --s3-endpoint https://s3.de.io.cloud.ovh.net \
  --eodag-cfg ~/.config/eodag/eodag.yml \
  --dem-dir ~/s1tiling/dem/SRTM_30_hgt \
  --data-dir ~/s1tiling/data \
  --cfg-template config/S1GRD_RTC_template.cfg

# Step 2: ingest + register (pass Script A's printed prefix)
python scripts/run_ingest_register.py \
  --s3-geotiff-prefix s3://esa-zarr-sentinel-explorer-tests/s1tiling-output/31TCH/descending/2025-02-01/ \
  --tile-id 31TCH --orbit-direction descending \
  --collection sentinel-1-grd-rtc-staging \
  --s3-output-bucket esa-zarr-sentinel-explorer-tests --s3-output-prefix s1-rtc-test \
  --s3-endpoint https://s3.de.io.cloud.ovh.net \
  --stac-api-url https://api.explorer.eopf.copernicus.eu/stac \
  --raster-api-url https://api.explorer.eopf.copernicus.eu/raster

# Step 3: verify
curl "https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-1-grd-rtc-staging/items/s1-rtc-31TCH"
eopf-geozarr validate-s1 s3://esa-zarr-sentinel-explorer-tests/s1-rtc-test/s1-grd-rtc-31TCH.zarr
```

**Acceptance:**
- [ ] GeoTIFFs + GAMMA_AREA present under same S3 prefix
- [ ] ≥ 2 acquisitions ingested; Zarr readable by `xr.open_zarr()`
- [ ] `eopf-geozarr validate-s1` passes
- [ ] Item `s1-rtc-31TCH` queryable at staging STAC API
- [ ] TiTiler viewer link for `vv` returns HTTP 200
- [ ] All issues logged and reported to Emmanuel

### Checkpoint 3 — Gate for Argo work

**Do not start Phase 4 until this checkpoint passes.**

- [ ] End-to-end run completed without manual intervention
- [ ] Issues found documented in GitHub issue #185
- [ ] Emmanuel reviewed the output

---

## Phase 4 — Argo translation (Sub-issues 6, 7a, 7, 8, 9)

Start only after Checkpoint 3. Tasks 4.1 and 4.2 are independent.

---

### Task 4.1 — [platform-deploy] DEM PVC (Sub-issue 7a)

**Repo:** `EOPF-Explorer/platform-deploy`

Create PVC `s1tiling-dem-pvc` (ReadOnlyMany, 5 Gi) in `devseed` namespace; populate with the
same 24 SRTM tiles from Phase 0. Commit the manifest.

> Confirm with Emmanuel that the cluster's storage class supports `ReadOnlyMany`.

**Acceptance:**
- [ ] `kubectl get pvc s1tiling-dem-pvc -n devseed` → `Bound`
- [ ] Test pod lists ≥ 20 `.hgt` files at `/MNT/SRTM_30_hgt`

**Blocks:** Task 4.3

---

### Task 4.2 — [platform-deploy] Argo ingest template (Sub-issue 6)

**Repo:** `EOPF-Explorer/platform-deploy`
**New file:** `workspaces/devseed-staging/data-pipeline/eopf-explorer-ingest-v1-s1rtc-template.yaml`

DAG mirroring `run_ingest_register.py`:
- `ingest` step: data-pipeline image, 2 CPU / 8Gi, no retry on exit 2
- `register-stac` step: 1 CPU / 2Gi, retry 5×

Image: use same image name as existing `eopf-explorer-convert-v1` template.
S3 credentials: from `geozarr-s3-credentials` secret (keys: `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`).

**Resolve before implementing:** confirm semaphore design with Emmanuel:
- Option A: global limit `v1-s1rtc-limit: "1"` (single tile scope, acceptable for now)
- Option B: per-tile keys `v1-s1rtc-31TCH: "1"` (tile-safe at any scale)

**Acceptance:**
- [ ] `argo lint --offline` passes
- [ ] Manually triggered run for 31TCH completes in cluster
- [ ] Exit-2 no-retry verified: trigger with empty prefix, confirm workflow does not retry

**Depends on:** Checkpoint 3

---

### Task 4.3 — [platform-deploy] Argo s1tiling template (Sub-issue 7)

**Repo:** `EOPF-Explorer/platform-deploy`
**New file:** `workspaces/devseed-staging/data-pipeline/eopf-explorer-s1tiling-template.yaml`

Single step `run-s1tiling`: S1Tiling image, 4 CPU / 16Gi, retry 3× exponential backoff.

Config rendering via init container:
1. ConfigMap mounts template at `/config/template.cfg` (read-only)
2. Init container renders it to `emptyDir` at `/rendered/run.cfg` using Python one-liner
   (see spec for exact command)
3. Main container mounts `emptyDir` at `/config/run.cfg`

DEM: mount `s1tiling-dem-pvc` at `/MNT/SRTM_30_hgt`.
EODAG patch: ConfigMap mount + `PYTHONSTARTUP` env var.

**Acceptance:**
- [ ] `argo lint --offline` passes
- [ ] Run for 31TCH produces GeoTIFFs at expected S3 prefix

**Depends on:** Checkpoint 3, Task 4.1

---

### Task 4.4 — [platform-deploy] Concurrency ConfigMap (Sub-issue 9)

Add keys to `eopf-workflow-concurrency-configmap.yaml`:
```yaml
v1-s1rtc-limit: "3"      # or per-tile keys — see Task 4.2 decision
v1-s1tiling-limit: "2"
```

**Acceptance:**
- [ ] ConfigMap applied; both keys visible in `kubectl get configmap ... -o yaml`
- [ ] Concurrency limits enforced (4th ingest queues; 3rd s1tiling queues)

**Depends on:** Tasks 4.2, 4.3

---

### Task 4.5 — [platform-deploy] CronWorkflow + Webhook Sensor (Sub-issue 8)

**CronWorkflow** (`0 6 * * *`, `concurrencyPolicy: Forbid`): triggers the full pipeline
(Workflow 1 → Workflow 2) for 31TCH / descending. Runs daily; most days no new data.

**Sensor**: Webhook filter `body.action == "^ingest-v1-s1rtc$"` → triggers Workflow 2 only
(for manual re-ingest when GeoTIFFs are already on S3).

**Acceptance:**
- [ ] `argo lint --offline` passes for CronWorkflow and Sensor manifests
- [ ] CronWorkflow manually triggered → end-to-end run completes
- [ ] Webhook POST fires Workflow 2; run completes
- [ ] `concurrencyPolicy: Forbid` verified (second trigger does not spawn parallel run)

**Depends on:** Tasks 4.2, 4.3

---

### Checkpoint 4 — Argo complete

- [ ] All four Argo manifests lint-clean and manually verified in cluster
- [ ] Concurrency limits tested
- [ ] Pipeline runs daily without intervention

---

## Phase 5 — Automated local watcher (Sub-issue 10, independent)

Can start after Checkpoint 2 (both local scripts work). Independent of Argo work.

### Task 5.1 — `watch_cdse_and_process.py`

**New file:** `scripts/watch_cdse_and_process.py`

**Before implementing:** smoke-test the CDSE STAC API filter casing:
```bash
# test both casings against the live API:
curl "https://catalogue.dataspace.copernicus.eu/stac/collections/SENTINEL-1-GRD/items\
?bbox=0,42,2,43&datetime=2025-02-01T00:00:00Z/2025-02-28T00:00:00Z\
&filter=sat:orbit_state='descending'"
curl "... &filter=sat:orbit_state='DESCENDING'"
# use whichever returns results
```

WGS84 bbox for 31TCH: `[0.0, 42.0, 2.0, 43.0]`.
Hardcode for initial scope; add a tile→bbox lookup dict when more tiles are needed.

Arg name mapping: `--s3-zarr-bucket` / `--s3-zarr-prefix` (watcher interface) map to
`--s3-output-bucket` / `--s3-output-prefix` (Script B interface).

Add `data/` to `.gitignore` (state file `data/.processed_products.json` must not be committed).

**Acceptance:**
- [ ] `--dry-run` prints CDSE results and planned calls without executing
- [ ] `sat:orbit_state` casing verified against live API before submission
- [ ] Idempotent: re-running skips already-processed products
- [ ] Processes ≥ 1 new product end-to-end

**Depends on:** Checkpoint 2

---

## Summary table

| Task | Deliverable | Can start | Blocks |
|------|-------------|-----------|--------|
| 0.1–0.5 | Env setup (P1–P5) | now | everything |
| 0.6 | data-model version unblock | now | 1.2 |
| **1.1** | data-model STAC builder | after 0.x | 2.2 |
| **1.2** | `ingest_v1_s1_rtc.py` | after 0.6 | 2.3 |
| **1.3** | STAC collection JSON | now | 2.3 |
| **2.1** | `run_s1tiling.py` | after 0.1–0.5 | Phase 3 |
| **2.2** | `register_v1_s1_rtc.py` | after 1.1 | 2.3 |
| **2.3** | `run_ingest_register.py` | after 1.2, 2.2, 1.3 | Phase 3 |
| **3** | End-to-end validation | after 2.1, 2.3 | Phase 4 |
| **4.1** | DEM PVC | after Checkpoint 3 | 4.3 |
| **4.2** | Argo ingest template | after Checkpoint 3 | 4.4, 4.5 |
| **4.3** | Argo s1tiling template | after Checkpoint 3 + 4.1 | 4.4, 4.5 |
| **4.4** | Concurrency ConfigMap | after 4.2, 4.3 | — |
| **4.5** | CronWorkflow + Sensor | after 4.2, 4.3 | — |
| **5.1** | `watch_cdse_and_process.py` | after Checkpoint 2 | — |

**Critical path:** 0.x → 1.1 + 1.2 + 1.3 (parallel) → 2.2 → 2.3 → Phase 3 → Phase 4
