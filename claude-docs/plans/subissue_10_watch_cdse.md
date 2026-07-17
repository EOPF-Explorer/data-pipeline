# Plan: Sub-issue 10 — `scripts/watch_cdse_and_process.py` (`claude-docs/specs/s1_grd_phase5_subissues.md` §Sub-issue 10)

**Goal**: a local watcher that queries CDSE for new S1 GRD products over a tile and drives Script A → Script B for each unseen one (local stand-in for the future Argo CronWorkflow).
**Constraint**: pure orchestration — no data logic; calls A and B as subprocesses; one new dependency (`mgrs`); idempotent via a state file.

---

## Current state

| Resource | Status |
|----------|--------|
| `scripts/run_s1tiling.py` (Script A) | **Done** — prints `s3://{bucket}/{prefix}/{tile}/{orbit}/{date_start}/` as last line |
| `scripts/run_ingest_register.py` (Script B) | **Done** — Zarr path derived from `--collection`; **no `--s3-output-prefix`** |
| `pystac_client` (0.9.0) | Available — reused for the CDSE query (no new STAC dep) |
| `mgrs` | **Done** — pinned 1.5.4; mypy override added |
| `scripts/watch_cdse_and_process.py` | **Done** (Tasks 1–5) — `tile_bbox`/`query_cdse`/state/`process_product`/`run_watch` |
| `tests/unit/test_watch_cdse_and_process.py` | **Done** — 29 tests; full suite 378 passed |
| Live `--dry-run` | **Verified** — lists real S1 GRD products for 31TCH + planned A/B runs |

### Decisions taken before planning (confirmed with user, 2026-06-05)

1. **Watcher CLI aligns to the real Script B**, not the spec. Script B dropped `--s3-output-prefix`; the Zarr path is `s3://{s3_output_bucket}/{collection}/s1-grd-rtc-{tile_id}.zarr`. Watcher therefore has **no `--s3-zarr-prefix`**; it keeps `--s3-zarr-bucket` (→ Script B `--s3-output-bucket`) and passes `--collection`.
2. **Tile → WGS84 bbox via the `mgrs` dependency**, not a hardcoded dict.

---

## Dependency graph

```
mgrs dep + tile_bbox()  ─┐
                          ├─► query_cdse() ─┐
state-file helpers      ─┘                  ├─► process_product() ─► main()/--dry-run ─► Task 6 live verify
                                            │      (Script A → Script B)
   (Scripts A + B already done) ────────────┘
```

Build bottom-up: foundation (dep + bbox) → query → state → orchestration → wiring → live check.

---

## Tasks

Tests in `tests/unit/`, patch `subprocess.run`/`pystac_client` at the module boundary, import via
`sys.path.insert` — mirroring `tests/unit/test_run_ingest_register.py`.

### Task 1 — Foundation: `mgrs` dep + `tile_bbox()` + CLI skeleton  ✅ DONE (`423f096`)
**What**: Add `mgrs` to `pyproject.toml` + `uv lock`. Create `scripts/watch_cdse_and_process.py`
with the full argparse interface (`--tiles`, `--orbit-direction`, `--lookback-days`, `--s3-bucket`,
`--s3-prefix`, `--s3-zarr-bucket`, `--s3-endpoint`, `--collection`, `--stac-api-url`,
`--raster-api-url`, `--dry-run`; **no `--s3-zarr-prefix`**) and
`tile_bbox(tile_id) -> list[float]` (WGS84 bbox = min/max over the 4 corners of the MGRS 100 km
square via `m.toLatLon("{tile}0000000000")` etc.; corners are non-axis-aligned in lat/lon, so all
four are needed).
> **Validated (2026-06-05, mgrs 1.5.4)**: `m.toLatLon` on the four corners of `31TCH` yields bbox
> `[0.533, 42.427, 1.784, 43.346]`. This is the genuine MGRS-square bbox and is **intentionally
> tighter/offset** vs the spec's hand-rounded `[0.0, 42.0, 2.0, 43.0]` — do not "correct" it to the
> spec number. `mgrs` also needs `packaging` at import (undeclared by mgrs; already in the project env).
**Verify**: `uv run python -c "import mgrs"`; `uv run python scripts/watch_cdse_and_process.py --help`.
**Acceptance criteria**:
- [ ] `mgrs` imports under `uv` (prebuilt wheel; `packaging` already present)
- [ ] `tile_bbox("31TCH")` == `[0.533, 42.427, 1.784, 43.346]` (±0.05°), unit-tested
- [ ] `--help` lists every arg above and **no `--s3-zarr-prefix`**
- [ ] Adversarial: malformed/unknown tile id raises a clear error (not a silent empty bbox)

### Task 2 — `query_cdse()` against the CDSE STAC API  ✅ DONE (`8e4339b`)
**What**: `query_cdse(stac_url, bbox, orbit_direction, lookback_days) -> list[dict]` via
`Client.open`, `collections=["SENTINEL-1-GRD"]`, `bbox`, `datetime=(now-lookback)/now`, filtering on
`sat:orbit_state`. Returns `[{"product_id", "date": "YYYY-MM-DD"}, ...]`.
**Verify**: `uv run pytest tests/unit/test_watch_cdse_and_process.py -k query -v`.
**Acceptance criteria**:
- [ ] Parsed product list from a mocked search; empty → `[]`
- [ ] Orbit filter applied; **filter isolated as one swappable block** (casing + mechanism pinned in Task 6)
- [ ] `date` extracted as `YYYY-MM-DD` from item datetime
- [ ] Adversarial: item with missing/`null` datetime skipped or clear error, not a crash

### Task 3 — State file (idempotency)  ✅ DONE (`f72b44c`)
**What**: `load_processed`/`is_processed`/`mark_processed`/`save_processed` over a plain dict
`{"31TCH": {"descending": [{"product_id", "date"}]}}` at `data/.processed_products.json`; add it to
`.gitignore`. Keep minimal — no class/schema layer.
**Verify**: `uv run pytest tests/unit/test_watch_cdse_and_process.py -k state -v`.
**Acceptance criteria**:
- [ ] Round-trips JSON; missing file → empty state
- [ ] `is_processed` true only after `mark_processed` for same tile+orbit+product_id
- [ ] `.gitignore` excludes the state file
- [ ] Adversarial: malformed/empty state file → treated as empty, not a crash

### Task 4 — `process_product()` orchestration (Script A → Script B)  ✅ DONE (`6211b02`)
> Added optional Script-A local args (`--eodag-cfg`/`--dem-dir`/`--data-dir`/`--cfg`) to the watcher
> CLI, defaulting to the `$S1T_WORKDIR` layout — a gap the spec's watcher interface omitted.
**What**: `date_start = date−1d`, `date_end = date+1d`. Run Script A with those dates, **streaming
its output (not captured)**. **Reconstruct** the prefix as
`s3://{s3_bucket}/{s3_prefix}/{tile}/{orbit}/{date_start}/` (watcher owns all inputs). Run Script B
with that prefix, `--collection`, `--s3-output-bucket` (= `--s3-zarr-bucket`) — **no
`--s3-output-prefix`**. Return success bool; failures logged, caller continues.
> Trade-off: reconstruction duplicates Script A's path formula (two files). Accepted for the
> prototype; CI test guards drift. Rejected stdout-capture (suppresses live docker logs, fragile).
**Verify**: `uv run pytest tests/unit/test_watch_cdse_and_process.py -k process -v`.
**Acceptance criteria**:
- [ ] Reconstructed prefix == Script A's formula for the same inputs (asserted in test → drift fails CI)
- [ ] Script A gets `date_start/date_end` = date ∓1d; output not captured
- [ ] Script B gets `--collection` + `--s3-output-bucket`, and **never** `--s3-output-prefix`
- [ ] Script A failure → B not called, returns False; B failure → returns False

### Task 5 — `main()` wiring, `--dry-run`, summary  ✅ DONE (`99a4c3a`)
> Fixed a wiring bug found via the dry-run smoke: `run_watch` now queries the CDSE *source* catalogue
> (`CDSE_STAC_URL`), not the EOPF target STAC (`--stac-api-url`, which is for Script B registration).
**What**: per tile: `tile_bbox` → `query_cdse` → skip via state → `process_product` →
`mark_processed`+`save_processed` on success. `--dry-run` prints planned runs, no subprocess, no
state write. Summary: `N found, M new, K processed, L failed`.
**Verify**: `uv run pytest tests/unit/test_watch_cdse_and_process.py -v`; `--dry-run` smoke run.
**Acceptance criteria**:
- [ ] `--dry-run` prints planned runs, invokes no subprocess, writes no state
- [ ] Already-processed products skipped (state consulted first)
- [ ] Summary line with the four counts

### Checkpoint — after Tasks 1–5 (offline gate)  ✅ PASSED (2026-06-05)
- [x] `uv run pytest` green overall — 378 passed, 1 skipped
- [x] `ruff` + `mypy` (pre-commit) pass
- [x] `--dry-run` runs end-to-end — lists real S1 GRD products for 31TCH + planned A/B runs

### Task 6 — Live verification + `sat:orbit_state` casing  🟢 all criteria met (pending report to Emmanuel)
**Query side resolved (2026-06-05, verified live)**: collection id is lowercase `sentinel-1-grd`
(`SENTINEL-1-GRD` → 0); `sat:orbit_state` is lowercase; the `query` extension filters correctly
(descending→6 / ascending→8, no cross-contamination). All pinned in code.

> **Live-run findings (2026-06-08)** — the first end-to-end attempt surfaced two real blockers the
> offline/mocked tests could not catch (they mock `subprocess.run`, so they assert Script A is
> *called* with the dates, never that S1Processor *consumes* them):
> 1. **`run_s1tiling.py` ignored the date window (BUG, fixed).** It copied `S1GRD_RTC.cfg` verbatim
>    and only used `--date-start` for the S3 *output prefix*; S1Processor ran the cfg's static
>    `first_date 2025-02-01 / last_date 2025-02-14`, reprocessing the old Feb-2025 test data. Fixed by
>    `_render_cfg()` which patches `roi_by_tiles`/`tiles`/`orbit_direction`/`first_date`/`last_date`
>    into a per-run cfg — mirrors the merged Argo `eopf-explorer-s1tiling` template's `sed` step.
>    Covered by 4 new tests in `tests/unit/test_run_s1tiling.py`.
> 2. **Pipeline is S1A-only.** Both the local cfg and the Argo `cfg-base` pin `platform_list : S1A`
>    and neither patches it, so S1C/S1D scenes the watcher discovers won't process. Live test target
>    must be an S1A product (chosen: `S1A…BB4B`, 2026-06-05, window 06-04→06-06). Making platform
>    selectable is out of scope here (deferred — see [[project_s1_watcher_s3_targets]]).

**Live target/config (2026-06-08)**: tests bucket `esa-zarr-sentinel-explorer-tests` on OVH
(`s3.de.io.cloud.ovh.net`, AWS profile `eopfexplorer`), collection `sentinel-1-grd-rtc-staging`,
explorer STAC/raster APIs. State seeded so all but the S1A target are pre-marked processed.
**Verify**: two consecutive real runs; second reports `0 new`.
**Acceptance criteria**:
- [x] Correct `sat:orbit_state` casing + filter mechanism verified live and pinned in code
- [x] `run_s1tiling.py` renders the requested date window into the cfg (verified live: s1tiling ran
      `first_date 2026-06-04 / last_date 2026-06-06`); fix + 4 unit tests landed
- [x] ≥ 1 new product processed A→B end-to-end — S1A 2026-06-05 (`…BB4B`) → `s1-rtc-31TCH` registered
      (HTTP 201), queryable in `sentinel-1-grd-rtc-staging`, `datetime 2026-06-05T06:09:07Z`
      (2026-06-08, after the output-contamination fix — see `fix_s1tiling_output_contamination.md`)
- [x] Idempotent re-run reports `0 new`, runs nothing — `Summary: 7 found, 0 new, 0 processed, 0 failed`
- [ ] Outcome reported to Emmanuel

---

## Risks & mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| ~~`mgrs` C-extension may not build~~ | Low (retired) | Validated 2026-06-05: mgrs 1.5.4 installs from a prebuilt wheel in ~3 ms under `uv`; needs `packaging` (already present) |
| ~~CDSE query returns 0 silently — wrong casing/mechanism~~ | Resolved | Root cause was the collection id (`SENTINEL-1-GRD`→0); fixed to `sentinel-1-grd`. Casing/mechanism verified live |
| Prefix-formula coupling (watcher reconstructs Script A's path) | Med | Task 4 test asserts reconstruction == Script A's formula → drift fails CI |
| CDSE endpoint/collection (`SENTINEL-1-GRD`, `catalogue.dataspace.copernicus.eu/stac`) unverified | Low | Confirmed live in Task 6; URL/collection isolated as constants |

---

## Open questions

None open. All resolved below.

### Resolved
- **OQ-1 — CDSE casing + filter mechanism (2026-06-05, live)**: collection `sentinel-1-grd` (lowercase), `sat:orbit_state` lowercase, `query` extension filters correctly. Pinned in code.
- **OQ — Script B interface (2026-06-05)**: align watcher to real Script B (drop `--s3-zarr-prefix`; pass `--collection` + `--s3-zarr-bucket`). Impact: spec block stale (see below).
- **OQ — tile→bbox (2026-06-05)**: use `mgrs` dependency (validated installable; bbox `[0.533, 42.427, 1.784, 43.346]`).
- **OQ — Script A local args (2026-06-05)**: add `--eodag-cfg`/`--dem-dir`/`--data-dir`/`--cfg` as optional watcher flags defaulting to `$S1T_WORKDIR`.

---

## Out of scope (flagged, not bundled)

The sub-issue 10 interface block in `claude-docs/specs/s1_grd_phase5_subissues.md` is stale (shows
`--s3-zarr-prefix` / Script B `--s3-output-prefix`). Fix as a *separate* one-line spec edit if the
user wants it — not part of the script commit.

---

## Done definition

`watch_cdse_and_process.py` queries CDSE, dedupes via the state file, and drives A→B for each new
31TCH product; `--dry-run` plans without executing; unit suite + `--dry-run` green; one product
processed end-to-end live with idempotent re-run; outcome reported to Emmanuel.
