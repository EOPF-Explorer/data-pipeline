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
| `mgrs` | **Not yet added** — Task 1 adds it; validated installable (1.5.4, prebuilt wheel) + bbox derivation works |
| `scripts/watch_cdse_and_process.py` | **Not started** |
| `tests/unit/test_watch_cdse_and_process.py` | **Not started** |

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

### Task 1 — Foundation: `mgrs` dep + `tile_bbox()` + CLI skeleton  <NEXT>
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

### Task 2 — `query_cdse()` against the CDSE STAC API
**What**: `query_cdse(stac_url, bbox, orbit_direction, lookback_days) -> list[dict]` via
`Client.open`, `collections=["SENTINEL-1-GRD"]`, `bbox`, `datetime=(now-lookback)/now`, filtering on
`sat:orbit_state`. Returns `[{"product_id", "date": "YYYY-MM-DD"}, ...]`.
**Verify**: `uv run pytest tests/unit/test_watch_cdse_and_process.py -k query -v`.
**Acceptance criteria**:
- [ ] Parsed product list from a mocked search; empty → `[]`
- [ ] Orbit filter applied; **filter isolated as one swappable block** (casing + mechanism pinned in Task 6)
- [ ] `date` extracted as `YYYY-MM-DD` from item datetime
- [ ] Adversarial: item with missing/`null` datetime skipped or clear error, not a crash

### Task 3 — State file (idempotency)
**What**: `load_processed`/`is_processed`/`mark_processed`/`save_processed` over a plain dict
`{"31TCH": {"descending": [{"product_id", "date"}]}}` at `data/.processed_products.json`; add it to
`.gitignore`. Keep minimal — no class/schema layer.
**Verify**: `uv run pytest tests/unit/test_watch_cdse_and_process.py -k state -v`.
**Acceptance criteria**:
- [ ] Round-trips JSON; missing file → empty state
- [ ] `is_processed` true only after `mark_processed` for same tile+orbit+product_id
- [ ] `.gitignore` excludes the state file
- [ ] Adversarial: malformed/empty state file → treated as empty, not a crash

### Task 4 — `process_product()` orchestration (Script A → Script B)
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

### Task 5 — `main()` wiring, `--dry-run`, summary
**What**: per tile: `tile_bbox` → `query_cdse` → skip via state → `process_product` →
`mark_processed`+`save_processed` on success. `--dry-run` prints planned runs, no subprocess, no
state write. Summary: `N found, M new, K processed, L failed`.
**Verify**: `uv run pytest tests/unit/test_watch_cdse_and_process.py -v`; `--dry-run` smoke run.
**Acceptance criteria**:
- [ ] `--dry-run` prints planned runs, invokes no subprocess, writes no state
- [ ] Already-processed products skipped (state consulted first)
- [ ] Summary line with the four counts

### Checkpoint — after Tasks 1–5 (offline gate)
- [ ] `uv run pytest` green overall
- [ ] `ruff` + `mypy` (pre-commit) pass
- [ ] `--dry-run` runs end-to-end with stub state

### Task 6 — Live verification + `sat:orbit_state` casing (resolves OQ-1)
**What**: Smoke-test `"descending"` vs `"DESCENDING"` **and** the filter mechanism (cql2 vs `query`
ext vs client-side) against the live CDSE STAC API; pin what works. Run the watcher for 31TCH
end-to-end, then re-run for idempotency. Needs CDSE creds + S3 + Docker (Sub-issue 4 environment).
**Verify**: two consecutive real runs; second reports `0 new`.
**Acceptance criteria**:
- [ ] Correct `sat:orbit_state` casing + filter mechanism verified live and pinned in code
- [ ] ≥ 1 new product processed A→B end-to-end (item queryable in staging STAC)
- [ ] Idempotent re-run reports `0 new`, runs nothing
- [ ] Outcome reported to Emmanuel

---

## Risks & mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| ~~`mgrs` C-extension may not build~~ | Low (retired) | Validated 2026-06-05: mgrs 1.5.4 installs from a prebuilt wheel in ~3 ms under `uv`; needs `packaging` (already present) |
| CDSE query returns 0 silently — wrong casing **or** filter mechanism | Med | Task 2 isolates the filter; Task 6 smoke-tests both before pinning |
| Prefix-formula coupling (watcher reconstructs Script A's path) | Med | Task 4 test asserts reconstruction == Script A's formula → drift fails CI |
| CDSE endpoint/collection (`SENTINEL-1-GRD`, `catalogue.dataspace.copernicus.eu/stac`) unverified | Low | Confirmed live in Task 6; URL/collection isolated as constants |

---

## Open questions

1. **OQ-1 `sat:orbit_state` casing + filter mechanism** — owner: this work; resolved in Task 6.

### Resolved
- **OQ — Script B interface (2026-06-05)**: align watcher to real Script B (drop `--s3-zarr-prefix`; pass `--collection` + `--s3-zarr-bucket`). Impact: spec block stale (see below).
- **OQ — tile→bbox (2026-06-05)**: use `mgrs` dependency. Impact: one new dep, C-extension build risk.

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
