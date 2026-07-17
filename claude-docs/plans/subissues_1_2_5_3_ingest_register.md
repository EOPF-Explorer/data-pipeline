# Plan: Sub-issues 1, 2, 5, 3 — ingest + register pipeline

**Spec**: `claude-docs/specs/s1_grd_phase5_subissues.md` → Sub-issues 1, 2, 5, 3
**Goal**: implement the four deliverables that unblock `run_ingest_register.py` (Sub-issue B)
**Constraint**: no logic beyond what each sub-issue specifies; reuse `register_v1.py` helpers in Sub-issue 3

---

## Current state

| Resource | Status |
|----------|--------|
| Sub-issue A (`run_s1tiling.py`) | **Done** — GeoTIFFs + GAMMA_AREA in S3 |
| `eopf-geozarr` pin (`9321df6`) — `s1_ingest` functions | **Available** — all 5 functions present (2026-05-26) |
| `eopf-geozarr` pin (`9321df6`) — `eopf_geozarr.stac` | **Available** — `build_s1_rtc_stac_item` imports cleanly (2026-05-26) |
| data-model branch `feat/s1-rtc-stac-builder` | **Created** from `origin/s1-tiling` (2026-05-26) |
| data-model `pyproject.toml` — `pystac>=1.8.0` | **Added** (uncommitted, data-model branch) |
| data-model `src/eopf_geozarr/stac/__init__.py` | **Created** (uncommitted, data-model branch) |
| data-model `src/eopf_geozarr/stac/s1_rtc.py` | **Done** — commit `9321df6`, PR #173 open |
| data-model `tests/test_s1_stac.py` | **Done** — 8 tests pass |
| `stac/sentinel-1-grd-rtc-staging.json` | **Done** — created 2026-05-26; 1 test passes |
| `scripts/ingest_v1_s1_rtc.py` | **Done** — 5-step pipeline, exit 0/1/2; 4 tests pass |
| `scripts/register_v1_s1_rtc.py` | **Done** — 4-step register; 4 tests pass (stub-patched for unavailable pin) |
| `tests/test_stac_collections.py` | **Done** — 1 test passes |
| `tests/unit/test_ingest_v1_s1_rtc.py` | **Done** — 4 tests pass |
| `tests/unit/test_register_v1_s1_rtc.py` | **Done** — 4 tests pass |

---

## Dependency graph

```
[Sub-issue 1 — data-model: build_s1_rtc_stac_item]  ──► [Sub-issue 3 — register_v1_s1_rtc.py] ──┐
                                                                                                   │
[Sub-issue 2 — ingest_v1_s1_rtc.py]  (s1_ingest already available — start now)  ─────────────────┤──► Sub-issue B
                                                                                                   │
[Sub-issue 5 — sentinel-1-grd-rtc-staging.json]  (no deps — start now)  ──────────────────────────┘
```

**What can start immediately** (data-pipeline repo):
- Sub-issue 5 (Tasks 5.1, 5.2)
- Sub-issue 2 (Tasks 2.1, 2.2) — `s1_ingest` confirmed present at current pin

**What needs data-model work first** (separate repo, branch `feat/s1-rtc-stac-builder` → PR to `s1-tiling`):
- Sub-issue 1 (Task 1.1 **IN PROGRESS**) → then Task 1.2 (pin bump after PR merges) → then Sub-issue 3 (Tasks 3.1, 3.2)

---

## Tasks

### Task 5.1 — STAC collection JSON `sentinel-1-grd-rtc-staging.json`  ✅ DONE

**Repo**: `EOPF-Explorer/data-pipeline`
**What**: create `stac/sentinel-1-grd-rtc-staging.json` modelled on `stac/sentinel-2-l2a-staging.json`.

Key fields:
- `id`: `sentinel-1-grd-rtc-staging`
- `title`: `Sentinel-1 GRD RTC [V1 staging]`
- `stac_version`: `1.1.0`
- `extent.spatial.bbox`: `[[-180, -90, 180, 90]]`
- `extent.temporal.interval`: `[["2014-04-03T00:00:00Z", null]]` (S1A launch)
- `license`: `proprietary`
- `stac_extensions`: `sar`, `sat`, `projection`
- `item_assets`: `zarr-store`, `vv`, `vh` (type `application/vnd.zarr; version=3`)
- Omit S2-specific fields (reflectance bands, gsd 60, sci:doi, etc.)

**Verify**:
```bash
uv run python -c "
import pystac
c = pystac.Collection.from_file('stac/sentinel-1-grd-rtc-staging.json')
c.validate()
print('OK:', c.id)
"
```

**Acceptance criteria**:
- [x] `pystac.Collection.from_file()` + `validate()` passes with no error
- [x] `id` is `sentinel-1-grd-rtc-staging`
- [x] Temporal interval starts `2014-04-03T00:00:00Z`
- [x] `item_assets` has keys `zarr-store`, `vv`, `vh`

---

### Task 5.2 — test: `tests/test_stac_collections.py`  ✅ DONE

**What**: create `tests/test_stac_collections.py` with a test that loads and validates the
new collection JSON. Model style on `tests/unit/test_register_v1.py`.

```python
def test_s1_rtc_staging_collection_valid():
    c = pystac.Collection.from_file("stac/sentinel-1-grd-rtc-staging.json")
    c.validate()
    assert c.id == "sentinel-1-grd-rtc-staging"
```

**Verify**:
```bash
uv run pytest tests/test_stac_collections.py -v
uv run pytest  # full suite must still pass
```

**Acceptance criteria**:
- [x] `test_s1_rtc_staging_collection_valid` passes
- [x] `uv run pytest` green overall

---

### Task 5.3 — create collection in staging API  ✅ DONE

**What**: one-time manual operation after Task 5.1 lands.

```bash
uv run python operator-tools/manage_collections.py create \
  --collection stac/sentinel-1-grd-rtc-staging.json \
  --stac-api-url https://api.explorer.eopf.copernicus.eu/stac
```

**Acceptance criteria**:
- [x] Command exits 0
- [x] `curl https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-1-grd-rtc-staging` returns HTTP 200 (verified 2026-05-26)

---

### Task 2.1 — `scripts/ingest_v1_s1_rtc.py`  ✅ DONE

**Repo**: `EOPF-Explorer/data-pipeline`
**What**: thin 5-step wiring script; no logic of its own.

Interface:
```bash
uv run python scripts/ingest_v1_s1_rtc.py \
  --s3-geotiff-prefix  s3://esa-zarr-sentinel-explorer-tests/s1tiling-output/31TCH/descending/2025-02-01/ \
  --s3-zarr-store      s3://esa-zarr-sentinel-explorer-tests/s1-rtc-test/s1-grd-rtc-31TCH.zarr \
  --tile-id            31TCH \
  --orbit-direction    descending
```

Behaviour (exact sequence from spec):
1. `discover_s1tiling_acquisitions(prefix)` → if empty: log + **exit 2**
2. For each acquisition: `ingest_s1tiling_acquisition(...)` → exit 1 on any error
3. `discover_s1tiling_conditions(prefix)` → same S3 prefix; non-fatal if empty
4. For each condition group: `ingest_s1tiling_conditions(...)`
5. `consolidate_s1_store(zarr_store)`

All five functions imported from `eopf_geozarr.conversion.s1_ingest`.
Exit codes: 0 = success, 1 = error, 2 = no acquisitions found.

**Verify**:
```bash
uv run pytest tests/unit/test_ingest_v1_s1_rtc.py -v
```

**Acceptance criteria**:
- [x] Exit code 2 on empty prefix; exit code 1 on ingest error; exit code 0 on success
- [x] `discover_s1tiling_conditions` called with same prefix as acquisitions
- [x] `uv run pytest` green

---

### Task 2.2 — test: `tests/unit/test_ingest_v1_s1_rtc.py`  ✅ DONE

**What**: unit tests via `pytest-mock` — patch `s1_ingest` functions at import boundary;
no real S3, no real Zarr writes for exit-code tests.

Test cases:
- `test_exits_2_on_empty_prefix` — `discover_s1tiling_acquisitions` returns `[]` → `sys.exit(2)`
- `test_exits_1_on_ingest_error` — `ingest_s1tiling_acquisition` raises → `sys.exit(1)`, `consolidate_s1_store` not called
- `test_exits_0_on_success` — 2 mock acquisitions → all 5 steps called in order → `sys.exit(0)`
- `test_conditions_non_fatal_if_empty` — `discover_s1tiling_conditions` returns `[]` → still reaches `consolidate_s1_store`

Patch target: `ingest_v1_s1_rtc.discover_s1tiling_acquisitions` etc. (patch at the module
where the names are used, not at the source module).

**Verify**:
```bash
uv run pytest tests/unit/test_ingest_v1_s1_rtc.py -v --cov=scripts/ingest_v1_s1_rtc
```

**Acceptance criteria**:
- [x] All 4 tests pass
- [ ] Coverage for `ingest_v1_s1_rtc.py` ≥ 90% (currently 65% — uncovered: exception paths in conditions + main)

---

### Task 1.1 — [data-model] `src/eopf_geozarr/stac/s1_rtc.py`  ✅ DONE

**Repo**: `EOPF-Explorer/data-model`, branch `feat/s1-rtc-stac-builder` (PR target: `s1-tiling`)
**Already done**: `pystac>=1.8.0` added to `pyproject.toml`; `src/eopf_geozarr/stac/__init__.py` created.
**What**: implement `build_s1_rtc_stac_item(zarr_store: str, collection_id: str) -> pystac.Item`.

**Verified Zarr store layout** (from `s1_ingest.py` inspection):
- Root group → orbit direction groups (`ascending`, `descending`)
- Orbit group attrs: `proj:code` (e.g. `"EPSG:32631"`), `spatial:bbox` (`[xmin, ymin, xmax, ymax]` in UTM)
- `r10m/time`: `int64` array, nanoseconds since Unix epoch
- `r10m/platform`: `"<U4"` array, values like `"S1A"` (uppercase, from GeoTIFF `FLYING_UNIT_CODE` tag)
- All other arrays (`vv`, `vh`, `border_mask`, overviews) are not needed by the STAC builder

**Implementation contract**:
- `zarr.open_consolidated(zarr_store, zarr_format=3)` — single metadata request
- `tile_id` = `Path(zarr_store).name.removeprefix("s1-grd-rtc-").removesuffix(".zarr")`
- Iterate `("ascending", "descending")` — skip if group absent
- UTM → WGS84: `pyproj.Transformer.from_crs(proj_code, "EPSG:4326", always_xy=True)`, transform all 4 corners
- WGS84 bbox = union across all present orbit bboxes
- `start_datetime`/`end_datetime` = `datetime.fromtimestamp(ns / 1e9, tz=utc)` of min/max `r10m/time`
- `ValueError` if no orbit group has any time values
- Preferred orbit for assets = `"ascending"` if present, else `"descending"`
- Assets: `zarr-store` (store URI), `vv` and `vh` at `{store}/{orbit}/r10m/{pol}`
- `stac_extensions`: SAR `v1.0.0`, SAT `v1.0.0`, projection `v2.0.0` schema URIs
- SAR properties: `sar:instrument_mode="IW"`, `sar:frequency_band="C"`, `sar:center_frequency=5.405`, `sar:polarizations=["VV","VH"]`, `sar:product_type="GRD"`
- SAT properties: `sat:orbit_state=preferred_orbit`
- Projection properties: `proj:code` from preferred orbit attrs

**Also**: add `generate-stac-s1` subcommand to `src/eopf_geozarr/cli.py`, following the `add_s1_ingestion_commands` pattern.

**Test fixture strategy** (minimal — no rasterio, no full ingest pipeline):
```python
def _make_s1_store(tmp_path, orbits):
    """orbits: dict of orbit_dir -> list of (time_ns, platform) tuples"""
    store_path = tmp_path / "s1-grd-rtc-31TCH.zarr"
    root = zarr.open_group(str(store_path), mode="w", zarr_format=3)
    for orbit_dir, acquisitions in orbits.items():
        og = root.create_group(orbit_dir)
        og.attrs.update({"proj:code": "EPSG:32631",
                         "spatial:bbox": [500000.0, 4890240.0, 609760.0, 5000040.0]})
        r10m = og.create_group("r10m")
        times = [t for t, _ in acquisitions]
        platforms = [p for _, p in acquisitions]
        r10m.create_array("time", data=np.array(times, dtype="int64"), chunks=(512,))
        r10m.create_array("platform", data=np.array(platforms, dtype="<U4"), chunks=(512,))
    zarr.consolidate_metadata(str(store_path), zarr_format=3)
    return store_path
```

**Verify** (in data-model repo):
```bash
uv run pytest tests/test_s1_stac.py -v
uv run pre-commit run --all-files
```

**Acceptance criteria**:
- [x] 8 tests pass: roundtrip, temporal range, bbox WGS84, both orbits, ascending-only, empty store → `ValueError`, asset subpaths, SAR extension fields
- [x] `ruff` + `mypy` clean on new files
- [x] `from eopf_geozarr.stac.s1_rtc import build_s1_rtc_stac_item` imports cleanly
- [x] PR #173 open: `feat/s1-rtc-stac-builder` → `s1-tiling`

---

### Task 1.2 — [data-model] PR merged → bump `pyproject.toml` pin in data-pipeline  ✅ DONE

**Trigger**: `feat/s1-rtc-stac-builder` PR merged into `s1-tiling` in data-model.
**What**: update the commit-hash pin in `data-pipeline/pyproject.toml` to the merge commit SHA.
The `s1_ingest` functions are already present — only the STAC builder module is new.

```bash
# In data-pipeline repo:
# Edit pyproject.toml: replace @1e891e5... with @<merge-commit-sha>
uv sync
uv run python -c "from eopf_geozarr.stac.s1_rtc import build_s1_rtc_stac_item; print('ok')"
uv run python -c "from eopf_geozarr.conversion.s1_ingest import discover_s1tiling_acquisitions; print('ok')"
uv run pytest
```

**Acceptance criteria**:
- [x] `pyproject.toml` pin updated to `9321df6` (PR #173 commit)
- [x] `from eopf_geozarr.stac.s1_rtc import build_s1_rtc_stac_item` imports cleanly
- [x] `from eopf_geozarr.conversion.s1_ingest import discover_s1tiling_acquisitions` still imports (regression)
- [x] `uv run pytest` passes (280 passed 2026-05-26)

---

### Task 3.1 — `scripts/register_v1_s1_rtc.py`  ✅ DONE (stub-imported; activate after Task 1.2)

**Repo**: `EOPF-Explorer/data-pipeline`
**What**: thin orchestrator; imports helpers from `register_v1.py`; no novel logic.

Interface:
```bash
uv run python scripts/register_v1_s1_rtc.py \
  --store            s3://esa-zarr-sentinel-explorer-tests/s1-rtc-test/s1-grd-rtc-31TCH.zarr \
  --collection       sentinel-1-grd-rtc-staging \
  --stac-api-url     https://api.explorer.eopf.copernicus.eu/stac \
  --raster-api-url   https://api.explorer.eopf.copernicus.eu/raster \
  --s3-endpoint      https://s3.de.io.cloud.ovh.net \
  --s3-output-bucket esa-zarr-sentinel-explorer-tests \
  --s3-output-prefix s1-rtc-test
```

`sys.path` preamble (top of file — needed because `scripts/` is not a package):
```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
```

Behaviour (in order — helpers imported from `register_v1`):
1. `build_s1_rtc_stac_item(store, collection)` — from `eopf_geozarr.stac.s1_rtc`
2. `add_store_link` → `add_alternate_s3_assets` → `add_visualization_links` → `add_thumbnail_asset`
3. `warm_thumbnail_cache`
4. `upsert_item`

Do **not** call `consolidate_reflectance_assets` or `fix_zarr_asset_media_types` (S2-specific).

**Verify**:
```bash
uv run pytest tests/unit/test_register_v1_s1_rtc.py -v
```

**Acceptance criteria**:
- [x] `upsert_item` called once with item whose `id` is `s1-rtc-31TCH`
- [x] `add_visualization_links` called (verified via mock assert)
- [x] S2-specific helpers (`consolidate_reflectance_assets`, `fix_zarr_asset_media_types`) not called
- [x] `uv run pytest` green

---

### Task 3.2 — test: `tests/unit/test_register_v1_s1_rtc.py`  ✅ DONE

**What**: unit tests using `pytest-mock` only — patch at the internal boundary, no new test deps.
Build item from a synthetic Zarr fixture in `tmp_path`; patch the four helper functions and
`build_s1_rtc_stac_item` so the test never hits S3 or the STAC API.

**Patching strategy** (AI engineering principle — own each layer's boundary):
- Patch `register_v1_s1_rtc.upsert_item` — script imports it by name, so patch where used
- Patch `register_v1_s1_rtc.build_s1_rtc_stac_item` — returns a pre-built `pystac.Item` fixture
- Patch `register_v1_s1_rtc.warm_thumbnail_cache` — no-op
- Other helpers (`add_store_link`, etc.) can run against the fixture item — they have no I/O

Test cases:
- `test_upserts_item_with_correct_id` — `upsert_item` called once; `call_args` item id = `s1-rtc-31TCH`
- `test_visualization_links_called` — `add_visualization_links` mock called once
- `test_s2_helpers_not_called` — assert `consolidate_reflectance_assets` is never imported or called
- `test_exits_nonzero_on_bad_store` — patch `build_s1_rtc_stac_item` to raise `ValueError` → script exits non-zero

**Verify**:
```bash
uv run pytest tests/unit/test_register_v1_s1_rtc.py -v --cov=scripts/register_v1_s1_rtc
```

**Acceptance criteria**:
- [x] All 4 tests pass
- [x] No new packages added to `pyproject.toml`

---

## Checkpoint — gate before Sub-issue B

Sub-issue B (`run_ingest_register.py`) must not be started until **all** are true:

| Check | Status |
|-------|--------|
| Task 5.1 — JSON schema-validates | [x] |
| Task 5.3 — collection live in staging API | [x] (HTTP 200 verified 2026-05-26) |
| Task 1.1 — `build_s1_rtc_stac_item` passes 8 tests in data-model | [x] |
| Task 1.2 — `pyproject.toml` pin includes STAC builder | [x] (commit `9321df6`, 2026-05-26) |
| Task 2.1 — `ingest_v1_s1_rtc.py` exits correctly (2/1/0) | [x] |
| Task 3.1 — `register_v1_s1_rtc.py` upserts mock item | [x] |
| `uv run pytest` green overall | [x] (280 passed 2026-05-26) |

---

## Open questions — resolved

1. **P6: `s1_ingest` at current pin?** ✅ — confirmed 2026-05-26. All 5 functions present at
   commit `1e891e5`. Sub-issue 2 can start immediately — no pin bump needed before it.

2. **data-model `s1-tiling` branch exists?** ✅ — confirmed by user 2026-05-26. Start Task 1.1
   directly on that branch.

3. **`respx` vs `pytest-mock` for Task 3.2?** ✅ — use `pytest-mock` exclusively. Patch
   `upsert_item` and `build_s1_rtc_stac_item` at the script's import boundary; no new dep needed.
   Each layer tests its own contract; HTTP-level mocking belongs in `register_v1.py`'s own tests.

---

## Done definition

Sub-issues 1, 2, 5, 3 are collectively complete when:
- `stac/sentinel-1-grd-rtc-staging.json` schema-validates and is live in the staging API
- `build_s1_rtc_stac_item` importable at the pinned data-model commit; 8 tests pass
- `scripts/ingest_v1_s1_rtc.py` exits 0/1/2 correctly; 4 unit tests pass; coverage ≥ 90%
- `scripts/register_v1_s1_rtc.py` upserts mock item; 4 unit tests pass; no new deps
- `uv run pytest` green in CI
- Checkpoint gate fully checked → Sub-issue B ready to write
