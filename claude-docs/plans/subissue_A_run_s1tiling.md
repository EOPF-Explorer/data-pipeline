# Plan: Sub-issue A — `scripts/run_s1tiling.py`

**Spec**: `claude-docs/specs/s1_grd_phase5_subissues.md` → Sub-issue A
**Goal**: thin local Workflow 1 simulation — one Docker call, one S3 sync, one printed prefix
**Constraint**: ≤ 60 lines of logic; no abstractions beyond what is needed

---

## Current state (2026-05-01)

| Resource | Status |
|----------|--------|
| `scripts/run_s1tiling.py` | **Written** — 84 lines total, ~50 lines logic |
| P1 EODAG creds at `$S1T_WORKDIR/config/eodag.yml` | Done — **Sentinel Hub service account** (`sh-*` / `client_credentials` grant); token smoke-test passes |
| P2 DEM tiles (29), `DEM_Union.gpkg`, `egm2008.grd` | Done |
| P3 Docker image `1.4.0-ubuntu-otb9.1.1` pulled | Done |
| P3 `analysis/s1tiling_eodag4_patch.py` in repo | Done |
| P4 `config/S1GRD_RTC.cfg` in repo + workdir | Done |
| P5 awscli + `eopfexplorer` profile | Done |
| P6 `eopf_geozarr.s1_ingest` | **Pending** — blocks Sub-issue 2 only, not Sub-issue A |

All Sub-issue A prerequisites are met.

---

## Dependency graph

```
P1 (eodag creds at $S1T_WORKDIR/config/eodag.yml)  ─┐
P2 (DEM tiles + GPKG + geoid)                        ├─► run_s1tiling.py ─► S3 prefix ─► Sub-issue B
P3 (Docker image + patch)                            │
P4 (S1GRD_RTC.cfg)                                  ─┘
P5 (awscli + eopfexplorer profile)                   ──► S3 sync step
```

---

## Tasks

### Task 1 — dry-run correctness  ✅ DONE

**What**: run with `--dry-run` and inspect every printed command.

**Verify**:
```bash
cd /Users/lhoupert/DevDS/EOPF/data-pipeline
uv run python scripts/run_s1tiling.py \
  --tile-id 31TCH --orbit-direction descending \
  --date-start 2025-02-01 --date-end 2025-02-14 \
  --s3-bucket esa-zarr-sentinel-explorer-tests --s3-prefix s1tiling-output \
  --s3-endpoint https://s3.de.io.cloud.ovh.net \
  --eodag-cfg "$S1T_WORKDIR/config/eodag.yml" \
  --dem-dir "$S1T_WORKDIR/DEM/COP_DEM_GLO30" \
  --data-dir "$S1T_WORKDIR" \
  --cfg config/S1GRD_RTC.cfg \
  --dry-run
```

**Acceptance criteria**:
- [x] `--entrypoint bash` appears before the image name in the docker command
- [x] All six `-v` mounts present and paths are absolute (no `~`, no relative segments)
- [x] `-v .../analysis:/patch:ro` points to the real `analysis/` dir
- [x] `-c 'python3 /patch/s1tiling_eodag4_patch.py && S1Processor /data/config/S1GRD_RTC.cfg'` is the command
- [x] Two `aws s3 sync` commands printed — one for `data_out/31TCH/`, one for `data_gamma_area/`
- [x] Both syncs target `s3://.../s1tiling-output/31TCH/descending/2025-02-01/`
- [x] Last line printed is the S3 prefix (no extra output)
- [x] Script exits 0

---

### Task 2 — Docker run (local, no S3)  🟡 NEXT — pre-flight cleared, ~30–60 min

**What**: run without `--dry-run`; S1Tiling downloads one S1 GRD acquisition, orthorectifies it,
writes GeoTIFFs to `$S1T_WORKDIR/data_out/31TCH/` and GAMMA_AREA to `$S1T_WORKDIR/data_gamma_area/`.

**Before running**: confirm `$S1T_WORKDIR/config/S1GRD_RTC.cfg` has the right date range:
```bash
grep 'first_date\|last_date\|roi_by_tiles' "$S1T_WORKDIR/config/S1GRD_RTC.cfg"
# expect: roi_by_tiles : 31TCH, first_date : 2025-02-01, last_date : 2025-02-14
```

**Run** (add `--platform linux/amd64` is already in the script):
```bash
uv run python scripts/run_s1tiling.py \
  --tile-id 31TCH --orbit-direction descending \
  --date-start 2025-02-01 --date-end 2025-02-14 \
  --s3-bucket esa-zarr-sentinel-explorer-tests --s3-prefix s1tiling-output \
  --s3-endpoint https://s3.de.io.cloud.ovh.net \
  --eodag-cfg "$S1T_WORKDIR/config/eodag.yml" \
  --dem-dir "$S1T_WORKDIR/DEM/COP_DEM_GLO30" \
  --data-dir "$S1T_WORKDIR" \
  --cfg config/S1GRD_RTC.cfg
```

**Acceptance criteria**:
- [ ] Docker container exits 0
- [ ] `ls $S1T_WORKDIR/data_out/31TCH/*.tif` shows ≥ 2 GeoTIFF files (VV + VH ± BorderMask)
- [ ] `ls $S1T_WORKDIR/data_gamma_area/GAMMA_AREA*.tif` shows ≥ 1 file
- [ ] Script exits non-zero if Docker fails (test: kill container mid-run)

**Known risk**: `N41E004` / `N42E004` absent from `DEM_Union.gpkg` — S1Tiling may log a
warning for those cells. Non-fatal; confirm with Emmanuel if it affects output quality.

---

### Task 3 — S3 sync and output verification  ⚠️ requires Task 2 complete

**What**: confirm files land in S3 and are rasterio-readable.

**Verify**:
```bash
# List the output prefix
aws s3 ls s3://esa-zarr-sentinel-explorer-tests/s1tiling-output/31TCH/descending/2025-02-01/ \
  --endpoint-url https://s3.de.io.cloud.ovh.net --profile eopfexplorer

# Spot-check one GeoTIFF is readable and has correct shape
python3 -c "
import rasterio, sys
from pathlib import Path
tifs = list(Path('$S1T_WORKDIR/data_out/31TCH').glob('*GammaNaughtRTC.tif'))
if not tifs: sys.exit('no GammaNaughtRTC tifs found')
with rasterio.open(tifs[0]) as ds:
    print(tifs[0].name, ds.width, ds.height)
    assert ds.width == ds.height == 10980, f'unexpected shape {ds.width}x{ds.height}'
print('OK')
"
```

**Acceptance criteria**:
- [ ] ≥ 2 `*GammaNaughtRTC.tif` files in the S3 prefix
- [ ] ≥ 1 `GAMMA_AREA*.tif` file in the **same** S3 prefix (not a separate prefix)
- [ ] Each GeoTIFF is 10980 × 10980 pixels
- [ ] Script printed `s3://esa-zarr-sentinel-explorer-tests/s1tiling-output/31TCH/descending/2025-02-01/` as last line

---

### Task 4 — failure-mode check  ✅ DONE

**What**: verify the script exits non-zero when Docker fails, so Sub-issue B / Argo can detect errors.

```bash
# Quick test: pass a bad image name
uv run python scripts/run_s1tiling.py \
  --tile-id 31TCH --orbit-direction descending \
  --date-start 2025-02-01 --date-end 2025-02-14 \
  --s3-bucket x --s3-prefix x --s3-endpoint x \
  --eodag-cfg "$S1T_WORKDIR/config/eodag.yml" \
  --dem-dir "$S1T_WORKDIR/DEM/COP_DEM_GLO30" \
  --data-dir "$S1T_WORKDIR" \
  --cfg config/S1GRD_RTC.cfg
echo "exit: $?"   # must be non-zero
```

**Acceptance criteria**:
- [ ] Exit code non-zero when Docker fails
- [ ] S3 sync is not attempted after a Docker failure

---

## Open questions — resolved (2026-05-05)

1. **`N41E004` / `N42E004` GPKG gap** ✅ — These cells do not exist in Copernicus DEM GLO-30.
   N41–42°N, 4–5°E is the Gulf of Lion (open Mediterranean); the dataset simply has no tiles
   over open water. The GPKG gap is correct, not a download failure. 31TCH spans −0.64°E to
   0.53°E (entirely west of E001) so it never touches these cells. S1Tiling may log a warning
   when processing the eastern IW swath edge, but the orthorectified 31TCH output is unaffected.
   **Safe to proceed.**

2. **EODAG orbit direction casing** ✅ — Handled by the patch (`analysis/s1tiling_eodag4_patch.py`
   lines 59–64). It replaces `'DES': 'descending'` with `'DES': 'DESCENDING'`, so
   `orbit_direction : DES` in the config correctly maps to `DESCENDING` for `cop_dataspace`.
   The mapping is explicit and documented in the patch's docstring (issue #4). **No action needed.**

3. **RAM/CPU** ✅ — Safe. Docker is allocated 16 GB; `ram_per_process: 8192` + ~1–2 GB overhead
   ≈ 10 GB peak — well within the 16 GB limit. Single process (`nb_parallel_processes: 1`).
   **Safe to proceed with Task 2.**

---

## Done definition

Sub-issue A is complete when Tasks 1–4 all pass and the printed S3 prefix can be passed
directly to `run_ingest_register.py --s3-geotiff-prefix` (Sub-issue 4 end-to-end test).
