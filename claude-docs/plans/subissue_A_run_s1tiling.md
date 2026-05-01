# Plan: Sub-issue A — `scripts/run_s1tiling.py`

**Spec**: `claude-docs/specs/s1_grd_phase5_subissues.md` → Sub-issue A
**Goal**: thin local Workflow 1 simulation — one Docker call, one S3 sync, one printed prefix
**Constraint**: ≤ 60 lines of logic; no abstractions beyond what is needed

---

## Current state (2026-05-01)

| Resource | Status |
|----------|--------|
| `scripts/run_s1tiling.py` | **Written** — 84 lines total, ~50 lines logic |
| P1 EODAG creds at `$S1T_WORKDIR/config/eodag.yml` | Done |
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

### Task 1 — dry-run correctness  ✅ ready to run

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
- [ ] `--entrypoint bash` appears before the image name in the docker command
- [ ] All six `-v` mounts present and paths are absolute (no `~`, no relative segments)
- [ ] `-v .../analysis:/patch:ro` points to the real `analysis/` dir
- [ ] `-c 'python3 /patch/s1tiling_eodag4_patch.py && S1Processor /data/config/S1GRD_RTC.cfg'` is the command
- [ ] Two `aws s3 sync` commands printed — one for `data_out/31TCH/`, one for `data_gamma_area/`
- [ ] Both syncs target `s3://.../s1tiling-output/31TCH/descending/2025-02-01/`
- [ ] Last line printed is the S3 prefix (no extra output)
- [ ] Script exits 0

---

### Task 2 — Docker run (local, no S3)  ⚠️ ~30–60 min, downloads S1 data

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

### Task 4 — failure-mode check

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

## Open questions (confirm with Emmanuel before Task 2)

1. **`N41E004` / `N42E004` GPKG gap**: do these cells fall inside the 31TCH swath? If so,
   will S1Tiling skip them silently or error?
2. **EODAG orbit direction casing**: the patch notes say `DESCENDING` (uppercase) is required
   for `cop_dataspace` via EODAG. The config uses `orbit_direction : DES`. Confirm the patch
   handles this mapping correctly before the first real run.
3. **RAM/CPU**: S1Tiling config has `nb_parallel_processes : 1`, `ram_per_process : 8192`.
   Confirm this is safe on the dev machine before the long run.

---

## Done definition

Sub-issue A is complete when Tasks 1–4 all pass and the printed S3 prefix can be passed
directly to `run_ingest_register.py --s3-geotiff-prefix` (Sub-issue 4 end-to-end test).
