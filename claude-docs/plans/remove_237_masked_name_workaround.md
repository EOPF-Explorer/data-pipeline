# Plan: Remove the #237 masked-name workaround (F1)

> Convention note: project plans live here (`claude-docs/plans/`), not `tasks/plan.md` (which is a
> pointer for sub-issue 10). Task checklist is embedded below rather than overwriting `tasks/todo.md`.

## Goal
Delete data-pipeline's `_normalize_masked_stamps` workaround now that the durable upstream fix
(data-model #184, pinned via #240 at rev `8c8ee81`) makes `discover_s1tiling_acquisitions` resolve
the masked `…txxxxxx` multi-frame timestamp itself — and verify multi-frame ingest still works.

## Background
- **Workaround** (`scripts/ingest_v1_s1_rtc.py`): `_normalize_masked_stamps(prefix)` is Step 0 of
  `ingest_all`. It globs `*txxxxxx*GammaNaughtRTC.tif`, reads `ACQUISITION_DATETIME`, and **renames
  the files on disk** (data + `_BorderMask`) so the old filename regex could parse them. No-op on `s3://`.
- **Durable fix** (data-model #184): the filename regex accepts a masked time, and `discover` resolves
  the real `acq_stamp` from the `ACQUISITION_DATETIME` tag in-memory (`_acq_stamp_from_geotiff`).
  Works for both local and `s3://` (via `_list_tifs`).

## Why removal is safe (correctness argument)
- The cube's `time` is written from the GeoTIFF `ACQUISITION_DATETIME` tag (`np.datetime64(tag)`), and
  `acq_stamp` now comes from discover's resolved value — **neither depends on the filename time**. The
  on-disk rename is redundant; masked filenames are fine for reading.
- **Bonus:** removal + #184's s3fs-aware discover means masked `s3://` inputs now work too — the
  workaround silently no-op'd on `s3://`.

## Dependency graph / ordering
1. **#240 (pin bump → `8c8ee81`) must merge into `feat--s1_grd_phase6` first.** Removing the workaround
   before the pinned data-model resolves masked stamps would break multi-frame ingest.
2. Then the removal is a single vertical slice in `scripts/ingest_v1_s1_rtc.py` + its tests.

## Tasks

### T1 — Gate: pinned data-model resolves masked stamps  ⟦CP-A⟧
- Precondition: #240 merged (or branch synced to the `8c8ee81` pin).
- Verify: `discover_s1tiling_acquisitions` on a masked local fixture returns one acquisition with
  `acq_stamp` resolved from the tag (upstream `test_resolves_masked_multiframe_stamp_from_tag` covers
  this; confirm it runs against the pinned rev).
- Done when: import + discover smoke passes on the pinned data-model.

### T2 — Remove the workaround
- Delete `_normalize_masked_stamps`; remove the Step 0 call in `ingest_all`; drop the now-orphan
  `extract_geotiff_metadata` import (used only by the workaround in this file); renumber step comments.
- Verify: `ruff` + `mypy` clean; no remaining `_normalize_masked_stamps` / `extract_geotiff_metadata`
  references in `ingest_v1_s1_rtc.py`.

### T3 — Tests: remove workaround tests, add regression guard  ⟦CP-B⟧
- Remove the 3 `test_normalize_masked_stamps_*` tests in `tests/unit/test_ingest_v1_s1_rtc.py`.
- Add a regression test: a masked-stamp local fixture flows through `ingest_all` (or at least
  `discover_s1tiling_acquisitions`) and yields a complete acquisition with the resolved `acq_stamp` —
  proving the path works **without** the rename.
- Verify: full `uv run pytest tests/unit` green.

### T4 — End-to-end verification (real multi-frame tile)  ⟦CP-C⟧
- On a multi-frame tile (e.g. 32TLR), run the ingest path against real GeoTIFFs and confirm `discover`
  count > 0 and the cube gains the expected `time` slice(s). Needs creds/data — run manually / in the
  e2e harness, not CI.

### T5 — PR
- Open PR → `feat--s1_grd_phase6`, referencing #184 (fix), #240 (pin), #237 (workaround retired); note
  the `s3://` masked-input improvement.

## Checkpoints
- **CP-A** (after T1, before T2): do not remove until #240 is merged and masked-stamp discover is
  confirmed on the pinned version.
- **CP-B** (after T3): unit suite green → safe to open the PR.
- **CP-C** (after T4): real multi-frame ingest confirmed before the pipeline relies on the removal.

## Risks
- **Ordering** — removing before #240 merges breaks masked ingest (mitigated by CP-A).
- **Behavior parity** — anything downstream re-parsing the filename for time would break with masked
  names; argued safe (time comes from the tag), T3 guards it.
- **s3:// path** — previously untested for masked stamps; T4 ideally exercises a real prefix.

## Checklist
- [ ] T1 — confirm pinned data-model resolves masked stamps (gate on #240)  ⟦CP-A⟧
- [ ] T2 — remove `_normalize_masked_stamps` + Step 0 call + orphan import
- [ ] T3 — drop 3 workaround tests, add regression guard; unit suite green  ⟦CP-B⟧
- [ ] T4 — real multi-frame ingest verified (32TLR)  ⟦CP-C⟧
- [ ] T5 — PR → `feat--s1_grd_phase6` (refs #184 / #240 / #237)
