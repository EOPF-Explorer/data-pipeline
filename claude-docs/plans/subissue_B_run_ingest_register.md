# Plan: Sub-issue B — `scripts/run_ingest_register.py` (`claude-docs/specs/s1_grd_phase5_subissues.md`)

**Goal**: thin local Workflow 2 simulation — derive Zarr path, call ingest, handle exit 2, call register
**Constraint**: ≤ 40 lines of logic; no logic of its own — pure wiring; no new dependencies

---

## Current state

| Resource | Status |
|----------|--------|
| `scripts/ingest_v1_s1_rtc.py` | **Done** — exits 0/1/2 correctly; 4 unit tests pass |
| `scripts/register_v1_s1_rtc.py` | **Done** — upserts mock item; 4 unit tests pass |
| `stac/sentinel-1-grd-rtc-staging.json` | **Done** — live in staging API (HTTP 200, 2026-05-26) |
| `eopf-geozarr` pin (`9321df6`) | **Done** — `s1_ingest` + `build_s1_rtc_stac_item` available |
| `uv run pytest` | **Green** — 280 passed (2026-05-26) |
| `scripts/run_ingest_register.py` | **Done** — 9-arg orchestrator; 4 unit tests pass |
| `tests/unit/test_run_ingest_register.py` | **Done** — 4 tests pass (2026-05-26) |

Gate (from `subissues_1_2_5_3_ingest_register.md`): all checkboxes checked — Sub-issue B ready.

---

## Dependency graph

```
[Sub-issue 2 — ingest_v1_s1_rtc.py]    ─┐
                                          ├─► run_ingest_register.py ─► Sub-issue 4 (end-to-end A→B)
[Sub-issue 3 — register_v1_s1_rtc.py]  ─┤
                                          │
[Sub-issue 5 — collection JSON + live]  ─┘
```

All dependencies satisfied — both tasks can start now.

---

## Tasks

### Task B.1 — `scripts/run_ingest_register.py`  ✅ DONE

**What**: thin subprocess orchestrator; ≤ 40 lines of logic; no novel logic.

**Interface**:
```bash
uv run python scripts/run_ingest_register.py \
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

**Zarr store** derived internally — never passed as an explicit arg:
```python
zarr_store = f"s3://{args.s3_output_bucket}/{args.s3_output_prefix}/s1-grd-rtc-{args.tile_id}.zarr"
```

**Behaviour** (exact from spec):
```python
# Step 1 — ingest
result = subprocess.run([
    "uv", "run", "python", "scripts/ingest_v1_s1_rtc.py",
    "--s3-geotiff-prefix", s3_geotiff_prefix,
    "--s3-zarr-store",     zarr_store,
    "--tile-id",           tile_id,
    "--orbit-direction",   orbit_direction,
])
if result.returncode == 2:
    log("no acquisitions found — skipping register")
    sys.exit(0)
if result.returncode != 0:
    sys.exit(result.returncode)

# Step 2 — register (only if step 1 exited 0)
result = subprocess.run([
    "uv", "run", "python", "scripts/register_v1_s1_rtc.py",
    "--store",            zarr_store,
    "--collection",       collection,
    "--stac-api-url",     stac_api_url,
    "--raster-api-url",   raster_api_url,
    "--s3-endpoint",      s3_endpoint,
    "--s3-output-bucket", s3_output_bucket,
    "--s3-output-prefix", s3_output_prefix,
])
sys.exit(result.returncode)
```

**Verify**:
```bash
uv run python scripts/run_ingest_register.py --help
# expect: all 9 args listed, no import error
```

**Acceptance criteria**:
- [x] `--help` prints all 9 arguments without error
- [x] `zarr_store` never appears as a CLI argument (derived, not user-supplied)
- [x] Script is ≤ 40 lines of logic (blank lines and imports excluded)

---

### Task B.2 — tests: `tests/unit/test_run_ingest_register.py`  ✅ DONE

**What**: 4 unit tests via `pytest-mock`; patch `subprocess.run` at the module boundary.
No real subprocess calls, no S3 access, no STAC API calls.

**Test cases**:
| Test | Ingest returncode | Register returncode | Expected script exit |
|------|:-----------------:|:-------------------:|:--------------------:|
| `test_exits_0_on_success` | 0 | 0 | 0 |
| `test_exits_0_skips_register_on_empty_prefix` | 2 | (not called) | 0 |
| `test_exits_1_on_ingest_error` | 1 | (not called) | 1 |
| `test_zarr_store_derived_correctly` | 0 | 0 | verify `--store` arg = `s3://{bucket}/{prefix}/s1-grd-rtc-{tile_id}.zarr` |

**Patch target**: `run_ingest_register.subprocess.run` (patch where used, not at source).

**Verify**:
```bash
uv run pytest tests/unit/test_run_ingest_register.py -v
uv run pytest   # full suite must still be green
```

**Acceptance criteria**:
- [x] All 4 tests pass
- [x] `uv run pytest` green overall (325 passed, 2026-05-26)
- [x] No new packages added to `pyproject.toml`

---

## Checkpoint — gate before Sub-issue 4

Sub-issue 4 (end-to-end A→B run) must not start until:

| Check | Status |
|-------|--------|
| Task B.1 — `--help` lists all 9 args | [x] |
| Task B.2 — 4 tests pass | [x] |
| `uv run pytest` green | [x] (325 passed, 2026-05-26) |

---

## Open questions

None — spec is fully specified, all dependencies verified.

---

## Done definition

Sub-issue B is complete when:
- `scripts/run_ingest_register.py` orchestrates ingest → register with correct exit-code semantics
- Exit code 2 from ingest → register skipped, script exits 0
- Exit code 1 from ingest → register skipped, script exits 1
- Exit code 0 from ingest → register called, script exits register's returncode
- 4 unit tests pass; `uv run pytest` green
- Sub-issue 4 can proceed: `run_s1tiling.py` output prefix fed directly into `run_ingest_register.py`
