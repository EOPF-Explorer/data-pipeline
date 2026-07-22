# Plan: Fix stale-output contamination in `run_s1tiling.py` (blocks sub-issue 10 Task 6)

**Goal**: each local Script A run uploads **only the current window's** GeoTIFFs to its date-keyed S3
prefix, so Script B ingests exactly the discovered acquisition (not leftovers from earlier runs).

**Constraint**: surgical fix to `scripts/run_s1tiling.py` (local-only orchestration) + unit tests;
no change to the merged Argo template, to Script B, or to the watcher. Small and bounded (~30–40
lines incl. path-safety); any destructive op must be path-guarded and `--dry-run`-safe.

---

## Problem (observed 2026-06-08, live Task-6 run #2)

`run_s1tiling.py` runs S1Processor into the **reused** `$S1T_WORKDIR/data_out/{tile}/` and
`data_gamma_area/`, then `aws s3 sync`s those **whole directories** to
`s3://…/{tile}/{orbit}/{date_start}/`. Those dirs **accumulate across runs** (s1tiling never cleans
them), so the sync pushes stale acquisitions into the new prefix.

**Evidence**:
- `data_out/31TCH/` held 4 acquisition dates: `20250205`, `20250210`, `20250212` (stale Feb-2025
  from earlier sub-issue A/B tests) + `20260605` (the scene this run actually processed).
- Script B then logged: `Discovered acquisitions count=4 input_dir=s3://…/31TCH/descending/2026-06-04/`
  — it ingested all four into one Zarr and would have registered a messy multi-date item into the
  **live** explorer STAC catalog. The run was stopped before register.

**Why the Argo path is immune**: each Argo workflow gets a **fresh volume**, so `data_out` starts
empty every run. The local prototype reuses `$S1T_WORKDIR`, so it must clean explicitly.

**Why tests missed it**: `tests/unit/test_run_s1tiling.py` mocks/`--dry-run`s and never populates
`data_out`, so accumulation across real runs was never exercised.

---

## Design notes (alternatives weighed)

- **Chosen: clean-before-run + purge-the-run's-prefix-before-sync** (clean-by-default, `--keep-output`
  opt-out). Cleans the *local* output dirs and the *destination* S3 prefix so each run is a fresh slate
  on both sides. Minimal and stays within Script A's existing structure.
- **Rejected — `aws s3 sync --delete`**: Script A does **two** syncs into the **same** prefix
  (`data_out/{tile}/` then `data_gamma_area/`; `run_s1tiling.py:97-101`). `--delete` on the second sync
  would delete the GeoTIFFs the first just uploaded (they aren't in `data_gamma_area`). So a one-shot
  prefix purge *before* both syncs is the correct lever, not `--delete`.
- **Rejected — fresh temp output dir per run (`mkdtemp`)**: this is the truest mirror of Argo's fresh
  volume and would avoid `rmtree`-ing a user dir entirely. Rejected because s1tiling's cfg hardcodes
  `/data/data_out` + `/data/data_gamma_area` and the docker mount is `data_dir`; remapping output to a
  temp dir is more invasive than the surgical clean-before-run, for no extra correctness given the
  path-safety guard below.

> **Local-only**: both deletions matter only because the prototype reuses `$S1T_WORKDIR`. The Argo
> path gets a fresh volume per workflow and is unaffected — do not port this cleanup into the template.

---

## Current state

| Resource | Status |
|----------|--------|
| `run_s1tiling.py` cfg rendering (`_render_cfg`) | ✅ fixed 2026-06-08 (date window honored) |
| Path-safe delete helper (`_safe_clean` + `_validate_tile_id`) | ✅ Task 1a (15 tests) |
| Local output cleanup before run (`data_out/{tile}/` + `data_gamma_area/`) | ✅ Task 1b (`--keep-output`) |
| Destination S3 prefix purge before sync | ✅ Task 1b (`aws s3 rm`; replaces `--delete`) |
| Cleanup unit tests | ✅ Task 2 (suite 341 passed) |
| Live Task-6 re-run | 🟡 ready — code fixed; live run pending (Task 3) |
| `s3://…/31TCH/descending/2026-06-04/` (run #2 partial upload) | ⚠️ stale objects — self-purged by the Task-3 re-run |

---

## Dependency graph

```
Task 1a path-safe delete helper ──► Task 1b clean local + purge S3 prefix ──► Task 2 unit tests ──► Task 3 clean Task-6 re-run
```

---

## Tasks

### Task 1a — Path-safe deletion helper  <status: DONE>
**What**: A single guarded-delete helper that every destructive op in this fix routes through, so an
empty / `..` / absolute `tile_id` (argparse does not constrain it) or a misconfigured `--data-dir`
cannot escape — or over-broaden within — the workdir. Two layers:
1. **Validate `tile_id` upstream** (before building any target): non-empty and tile-shaped
   (`^[0-9]{2}[A-Z]{3}$`). This stops an empty id from collapsing `data_out/{tile_id}` to the whole
   `data_out/` (which would pass the escape-guard yet wipe every tile). Mirrors the watcher's MGRS ids.
2. **Guard the delete path**: resolve the target with `Path.resolve()` and **assert it is strictly
   under `data_dir.resolve()`** (never `== data_dir`) before deleting; refuse otherwise.

A missing target is a no-op (first run). Honor `--dry-run` (print intent, delete nothing).

```python
def _safe_clean(target: Path, data_dir: Path, dry_run: bool) -> None:
    data_root = data_dir.resolve()
    resolved = target.resolve()
    if data_root not in resolved.parents:        # strictly inside, never == data_dir
        raise ValueError(f"refusing to delete outside data_dir: {resolved}")
    if dry_run:
        print(f"[dry-run] would clean {resolved}")
        return
    if resolved.exists():
        shutil.rmtree(resolved)
```

**Verify**: `uv run pytest tests/unit/test_run_s1tiling.py -k safe_clean -v`.
**Acceptance criteria**:
- [x] Empty / non-tile-shaped `tile_id` is rejected **upstream** (`_validate_tile_id`; 8 malformed
      cases incl. `""`, `..`, `/abs`, `31tch`) — never reaches a delete
- [x] Deletes a path strictly inside `data_dir`; **refuses** `==`/outside (`..`, `/abs`, sibling dir)
      — raises `ValueError`, no deletion (adversarial tests pass)
- [x] Missing target → no-op (first run); `--dry-run` → prints intent, deletes nothing

> **Evidence (2026-06-08)**: `_validate_tile_id` + `_safe_clean` added to `run_s1tiling.py`; 15 new
> tests (`-k "safe_clean or validate_tile"`) pass; full `test_run_s1tiling.py` 30 passed; ruff+mypy clean.

### Task 1b — Clean local outputs + purge the run's S3 prefix  <status: DONE>
**What**: In `run_s1tiling.py`, wire `_safe_clean` into the run. Default = clean; `--keep-output`
opt-out skips **both** deletions below (manual-inspection mode).
1. **Before `docker run`** — `_safe_clean` `{data_dir}/data_out/{tile_id}/` and
   `{data_dir}/data_gamma_area/` so only the current window's products exist when the sync runs.
   (`data_gamma_area` is global, not tile-scoped — fine because the watcher invokes Script A
   sequentially per product; note it, don't widen scope.)
2. **Before the two `aws s3 sync`s** — purge the run's own destination prefix
   `s3://{bucket}/{prefix}/{tile}/{orbit}/{date_start}/` with one `aws s3 rm --recursive` so the sync
   is authoritative and a re-run of the same window self-heals (see Design notes: why not `--delete`).
   `--dry-run`-safe; reuse the existing `--profile eopfexplorer` + `--endpoint-url` flags. Guard against
   an empty `bucket`/`prefix`/`tile`/`date_start` (never issue an `rm` on a bucket root).
3. **`data_raw`** is left intact by default (date-filtered, not a contamination source); opt-in
   `--prune-raw` routes `{data_dir}/data_raw/` through `_safe_clean` to bound disk (re-downloads on a
   same-window re-run — accepted).

**Verify**:
```bash
uv run pytest tests/unit/test_run_s1tiling.py -q
# manual: seed data_out/31TCH with a dummy stale tif; --dry-run prints both clean + prefix-purge
# intents and deletes nothing; a real run leaves only current-window files before the sync.
```

**Acceptance criteria**:
- [x] Before `docker run`, `data_out/{tile}/` and `data_gamma_area/` are cleaned (test:
      `test_main_cleans_stale_output_by_default`)
- [x] Destination S3 prefix is purged before sync (dry-run emits the `aws s3 rm --recursive` block;
      live self-heal verified in Task 3)
- [x] S3 purge refuses to run if `bucket`/`prefix`/`tile`/`date_start` is empty
      (`test_main_refuses_s3_purge_with_empty_bucket`; tile also caught upstream by `_validate_tile_id`)
- [x] `--dry-run` performs **no** local or S3 deletion (prints both intents only)
- [x] `--keep-output` preserves local outputs **and** skips the S3 prefix purge
- [x] `data_raw` left untouched by default; `--prune-raw` clears it (via `_safe_clean`) when passed

> **Evidence (2026-06-08)**: `main()` validates `tile_id`, cleans `data_out/{tile}` +
> `data_gamma_area` (recreating the roots), optionally prunes `data_raw`, and purges the S3 prefix
> before the two syncs; `--keep-output`/`--prune-raw` flags added. Dry-run eyeballed; full unit suite
> **341 passed**; ruff+mypy clean.

### Task 2 — Unit tests for cleanup + path-safety  <status: DONE>
**What**: Extend `tests/unit/test_run_s1tiling.py`, mirroring the existing `_render_cfg` test style.
Cover both the helper (Task 1a) and the wiring (Task 1b):
- **Happy path**: plant a stale file under `data_out/{tile}/`; real run removes it, `--dry-run` and
  `--keep-output` leave it.
- **Adversarial (the case that justifies 1a)**: `tile_id` = `""`, `".."`, `"/abs"` must raise / refuse
  and delete nothing — assert no path at or above `data_dir` is ever removed.
- **Edge**: missing `data_out`/`data_gamma_area` (first run) → no error.
- **Prune**: `--prune-raw` clears `data_raw`; default leaves it.
- The S3 prefix purge is exercised via `--dry-run` (asserts the `aws s3 rm` intent is printed with the
  correct prefix; no live S3 call in unit tests — live behavior is covered by Task 3).

**Verify**: `uv run pytest tests/unit/test_run_s1tiling.py -q` (all green, incl. new cases).

**Acceptance criteria**:
- [x] Happy path: real run removes planted output; `--keep-output` preserves it (via stubbed `_run`)
- [x] Adversarial `tile_id` (8 cases) refuses upstream; `_safe_clean` refuses `==`/outside `data_dir`
- [x] Missing dirs → no error; `--prune-raw` clears `data_raw`, default preserves it
- [x] `--dry-run` prints the S3 `rm` intent with the correct prefix, issues no live call
- [x] Full unit suite still green (341 passed, no regressions)

> **Evidence (2026-06-08)**: 22 new tests in `tests/unit/test_run_s1tiling.py` (helper + wiring +
> dry-run intents); updated `test_dry_run_two_s3_sync_commands` for the new `rm` block.

### Task 3 — Clean Task-6 re-run  <status: DONE>
**What**: With the fix in place, the re-run **self-purges** the contaminated
`s3://esa-zarr-sentinel-explorer-tests/s1tiling-output/31TCH/descending/2026-06-04/` prefix (run #2's
partial sync) as part of Task 1b's pre-sync purge — no manual `aws s3 rm` needed. Re-seed watcher state
to leave only the S1A `…BB4B` (2026-06-05) product new, run the watcher, and confirm Script B discovers
**count=1** acquisition. (If verifying the purge independently, list the prefix before/after.)

**Verify**:
```bash
# optional pre-check: aws s3 ls .../31TCH/descending/2026-06-04/ (shows run #2's 4-date contamination)
# re-seed state (all but BB4B processed) then run the watcher (AWS_PROFILE=eopfexplorer)
grep "Discovered acquisitions" <run log>   # expect count=1
```

**Acceptance criteria**:
- [x] Re-run's pre-sync purge clears the stale `2026-06-04/` objects — prefix went from 4 acquisition
      dates (`20250205/10/12` + `20260605`) to just `20260605t060907`; no manual deletion step
- [x] Script B logs `Discovered acquisitions count=1` (was `count=4`)
- [x] Clean item registered + queryable: `s1-rtc-31TCH` (HTTP 201), `datetime 2026-06-05T06:09:07Z`,
      assets `vh/vv/thumbnail/zarr-store`
- [x] Idempotent re-run reports `Summary: 7 found, 0 new, 0 processed, 0 failed` — no docker launched

> **Evidence (2026-06-08)**: live re-run log `/tmp/s1watch/run3.log`; `Summary: 7 found, 1 new,
> 1 processed, 0 failed`; STAC item verified via the explorer API. Closes sub-issue 10 Task 6.

---

## Open questions

None open.

### Resolved
- *(2026-06-08)* **Cleanup default = clean-by-default** with a `--keep-output` opt-out (mirrors Argo
  fresh-volume semantics; right for unattended watcher runs). Impact: Task 1 default behavior.
- *(2026-06-08)* **`data_raw` = leave intact by default, add opt-in `--prune-raw`** to bound disk.
  Impact: Task 1 gains a `--prune-raw` flag (re-downloads on same-window re-runs, accepted).

---

## Done definition

`run_s1tiling.py` produces a clean per-run output (no stale acquisitions in the synced prefix),
covered by a unit test; the contaminated `2026-06-04/` prefix is purged; the Task-6 live re-run shows
Script B discovering exactly one acquisition and registers a single clean 31TCH 2026-06-05 item, with
an idempotent re-run reporting `0 new`.
