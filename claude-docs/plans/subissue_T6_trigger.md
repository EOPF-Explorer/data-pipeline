# Plan: T6 — CDSE data-driven trigger (in-repo entrypoint) (`claude-docs/specs/s1_grd_phase6_productionization.md` · plan Task 6)

**Goal**: ship the **in-repo trigger entrypoint** for Phase-6 Task 6 — query CDSE for a tile+window,
keep only **{S1A, S1C}** (skip S1D, logged), drop acquisitions whose **per-acquisition STAC item
already exists**, and **emit the remaining new products as JSON** for the CronWorkflow to fan out
child Workflows over (decision B). Pure query→filter→dedup→emit; **no subprocess, no S3, no submit**.
**Constraint**: reuse `tile_bbox` + the CDSE query path and `acquisition_id`; leave
`watch_cdse_and_process.py` (the local stand-in) untouched; new code only; TDD at each boundary;
staging-only.

> **Repo split (mirrors T1–T5).** The *in-repo* deliverable is `scripts/trigger.py` +
> `tests/unit/test_trigger.py`. The **CronWorkflow manifest (6 h), the `withParam` child-Workflow
> fan-out (decision B), and retiring the suspended sub-issue-8 cron** ship in **platform-deploy** and
> are verified on-cluster — tracked here as the "(cluster — pending)" tail of Task 6's acceptance,
> not built in this repo.

---

## Current state

| Resource | Status |
|----------|--------|
| `tile_bbox`, `query_cdse` (`watch_cdse_and_process.py`) | ✅ live (sub-issue 10); reused read-only |
| `acquisition_id(tile, when)` → `s1-rtc-{tile}-{YYYYMMDDtHHMMSS}` (`register_per_acquisition.py`) | ✅ shipped (#229); the dedup key |
| `sentinel-1-grd-rtc-acquisitions` per-acq items | ✅ live (31TCH ×2); the dedup authority |
| cube time-present skip (`new_acquisitions`/`store_times_ns`, `ingest_v1_s1_rtc.py`) | ✅ shipped (#231) — the **downstream** backstop (T4), runs in ingest, **not** in the trigger |
| `scripts/trigger.py` (this task) | ❌ to build |
| CronWorkflow 6 h + child-Workflow fan-out + retire blind cron | ⏳ platform-deploy / cluster (out of in-repo scope) |

---

## Architecture & key decisions

- **The trigger is a pure function: query → platform-allowlist → STAC item-exists dedup → emit JSON.**
  It does **not** run s1tiling/ingest (that's the child Workflow the CronWorkflow submits via
  `workflowTemplateRef`, decision B) and it does **not** read S3. This keeps it decoupled, fast, and
  fully unit-testable by mocking the STAC client — the same boundary `query_cdse` is already tested at.
- **Dedup split across components (faithful to spec §3 + P2).** The *trigger's* arm is the
  **per-acquisition STAC item-exists** check against `sentinel-1-grd-rtc-acquisitions`. The
  **cube-time-present** arm is **already shipped in T4** and runs *downstream in ingest*
  (`new_acquisitions`) — it is the backstop for the STAC-indexing-latency race, so the trigger does
  **not** open the cube. (P2: "the trigger skips a product whose per-acquisition STAC item already
  exists; the ingest also skips a `time` already present in the cube.")
- **Platform filter = product-id prefix allowlist** `{S1A, S1C}` (`product_id.split("_")[0]`). S1D (and
  anything else) is dropped *before* emit with a `log.info` line → no child Workflow. Allowlist (not an
  S1D denylist) so a future S1B/S1E can't leak through.
- **Dedup key precision (the one real risk).** `acquisition_id` needs the acquisition instant **to the
  second**; `query_cdse` keeps only the date, so the trigger keeps the full `item.datetime`. The
  registered item id is built from the **cube** time (the s1tiling `ACQUISITION_DATETIME` tag), while
  the trigger derives the id from the **CDSE** `datetime`. If those differ by ≥1 s the id-exists check
  never matches and the trigger re-submits every 6 h. **Mitigation is layered:** (1) a one-time
  ground-truth on cluster — compare the two live items (`s1-rtc-31TCH-20260605t060907`,
  `…-20260607t055248`) against their CDSE product `datetime`s; (2) even on a miss, the **T4 cube-time
  backstop** prevents a duplicate `time` slice and `register_per_acquisition` re-emits the *canonical*
  id, so the worst case is a redundant s1tiling run, **never** cube corruption or duplicate items;
  (3) if parity fails on cluster, switch the existence check to a bounded **datetime-window search**
  (S1 revisit ≫ tolerance) — a localized change behind the same `item_exists` seam. Tracked as OQ-T6-1.

### Module reuse (no edits to existing files)
`trigger.py` imports `tile_bbox` + the CDSE constants from `watch_cdse_and_process`, and
`acquisition_id` + `DEFAULT_ACQ_COLLECTION` from `register_per_acquisition`. `query_cdse` is *not*
edited (its date-only contract + tests stay intact); the trigger has its own `query_products` that
runs the same `Client.search(...)` but keeps `datetime` + `platform`. ~6 lines of search construction
are duplicated deliberately rather than widening `query_cdse`'s tested contract.

---

## Dependency graph

```
T1–T5 (shipped) ── acquisition_id + acquisitions collection (dedup key+authority)
        │           tile_bbox + CDSE query path (reused)
        ▼
T6.1 platform parse + allowlist ─┐
T6.2 query_products (+datetime)  ─┤
T6.3 item_exists dedup           ─┼─► T6.4 select_new_products + main() emit JSON
                                  ┘        │
                                           ▼  CP-T6 (pytest test_trigger.py green)
                          ── platform-deploy: CronWorkflow 6 h + withParam fan-out + retire blind cron
                             + cluster verify (only-new submit; re-run→0; S1D skipped; datetime parity)
```

---

## Tasks

> **Build complete (2026-06-10)**: `scripts/trigger_cdse.py` + `tests/unit/test_trigger_cdse.py`
> (24 tests) shipped; T6.1–T6.4 ✅; CP-T6 met (full suite **500 passed**, ruff clean, watcher
> untouched). Platform-deploy CronWorkflow + cluster verify remain.

### Task T6.1 — Platform parse + {S1A,S1C} allowlist  <status: ✅ DONE>
**What**: `platform_of(product_id) -> str` (`product_id.split("_", 1)[0].upper()`); module constant
`ENABLED_PLATFORMS = {"S1A", "S1C"}`; `is_enabled_platform(platform) -> bool`. No I/O.
**Verify**: `uv run pytest tests/unit/test_trigger.py -k platform`
**Acceptance**:
- [ ] `platform_of("S1A_IW_GRDH_…")=="S1A"`, `…("S1D_…")=="S1D"`, `…("S1C_…")=="S1C"`
- [ ] `is_enabled_platform` true for S1A/S1C, false for S1D (and S1B/garbage)
- [ ] Malformed id (no `_`) → returns the upper-cased token, `is_enabled_platform` false (no crash)
**Files**: `scripts/trigger.py`, `tests/unit/test_trigger.py` · **Scope**: XS

### Task T6.2 — `query_products` (CDSE query keeping datetime+platform)  <status: ✅ DONE>
**What**: `query_products(stac_url, bbox, orbit_direction, lookback_days) -> list[dict]` returning
`{"product_id", "platform", "datetime"(ISO, to seconds), "date"}`. Same `Client.search` filter as
`query_cdse` (collection `sentinel-1-grd`, bbox, `sat:orbit_state`, lookback). Items without a
datetime are skipped (logged), mirroring `query_cdse`.
**Verify**: `uv run pytest tests/unit/test_trigger.py -k query`
**Acceptance**:
- [ ] Returns one record per item with full `datetime` (not just date) + parsed `platform`
- [ ] Search scoped to `sentinel-1-grd`, the bbox, and `{"sat:orbit_state": {"eq": orbit}}` (asserted)
- [ ] Item with no datetime is skipped, not crashed on (adversarial)
**Files**: `scripts/trigger.py`, `tests/unit/test_trigger.py` · **Dep**: none · **Scope**: S

### Task T6.3 — Per-acquisition STAC item-exists dedup  <status: ✅ DONE>
**What**: `expected_item_id(tile, when)` = `acquisition_id(tile, when)` (reused);
`item_exists(stac_api_url, acq_collection, item_id) -> bool` via
`Client.open(...).search(collections=[acq_collection], ids=[item_id])` → any item returned ⇒ True.
**Verify**: `uv run pytest tests/unit/test_trigger.py -k exists`
**Acceptance**:
- [ ] `expected_item_id("31TCH", 2026-06-07T05:52:48Z) == "s1-rtc-31TCH-20260607t055248"`
- [ ] `item_exists` True when search yields an item, False when it yields none (mocked client)
- [ ] Search is scoped to the acquisitions collection + the exact `ids=[item_id]` (asserted)
**Files**: `scripts/trigger.py`, `tests/unit/test_trigger.py` · **Dep**: T6.1 · **Scope**: S

### Task T6.4 — `select_new_products` + `main()` JSON emit  <status: ✅ DONE>
**What**: `select_new_products(args) -> list[dict]`: per tile → `query_products` → drop
`not is_enabled_platform` (log `"skip %s: platform %s not enabled"`) → drop `item_exists(...)` (log
`"skip %s: item %s already registered"`) → keep `{tile, orbit, product_id, datetime, date, platform}`.
`main()` parses args (`--tiles`, `--orbit-direction`, `--lookback-days`, `--acq-collection`
[default `DEFAULT_ACQ_COLLECTION`], `--stac-api-url`, optional `--output`), writes the JSON **array**
to `--output` or stdout (the CronWorkflow's `withParam` source). Logging on stderr so stdout is clean
JSON.
**Verify**: `uv run pytest tests/unit/test_trigger.py -k select`
**Acceptance**:
- [ ] Emits only products with no existing per-acq item (dedup), mocked `query_products`+`item_exists`
- [ ] Empty CDSE result → `[]`; all-already-registered → `[]` (idempotent re-run)
- [ ] An S1D product is dropped (logged), absent from the emitted array
- [ ] Multiple tiles iterate; each record carries its tile+orbit; stdout is valid JSON only
**Files**: `scripts/trigger.py`, `tests/unit/test_trigger.py` · **Dep**: T6.1–T6.3 · **Scope**: S

### Checkpoint CP-T6 (after T6.1–T6.4)
- [ ] `uv run pytest tests/unit/test_trigger.py` green; full `uv run pytest` green (no regressions)
- [ ] `scripts/trigger.py` is query→filter→dedup→emit only (no subprocess/S3/submit) — re-read diff
- [ ] `watch_cdse_and_process.py` + its tests unchanged
- [ ] Update plan Task 6 in-repo acceptance bullets with evidence; **human review before platform-deploy**

---

## Out of in-repo scope (platform-deploy / cluster — the Task 6 "pending" tail)
- CronWorkflow (6 h) that runs `trigger.py` then `withParam`-fans-out one child Workflow per emitted
  product (decision B, `workflowTemplateRef`).
- Retire/replace the suspended sub-issue-8 blind cron (one trigger of record).
- **Cluster verify**: data-bearing window → only-new submissions; no-data → 0; re-run → 0; S1D skipped
  (logged), no child Workflow; **CDSE↔cube `datetime` parity ground-truth** (OQ-T6-1).

## Open questions
| OQ | Question | Blocks | Owner |
|----|----------|--------|-------|
| T6-1 | Does CDSE product `datetime` equal the cube `ACQUISITION_DATETIME` to the second? If not, switch `item_exists` to a bounded datetime-window search. | cluster idempotency | me (cluster verify) |

## Risks & mitigations
| Risk | Impact | Mitigation |
|------|--------|------------|
| CDSE `datetime` ≠ cube id second → trigger re-submits forever | Med | T4 cube-time backstop (no dup slice/item; worst case = redundant s1tiling run); cluster parity check; window-search fallback behind `item_exists` seam (OQ-T6-1) |
| STAC indexing latency → brief re-submit window | Low | downstream T4 cube-time-present skip is the backstop (by design) |
| Pulling `watch_cdse_and_process` transitively imports `run_ingest_register` | Low | same import the live watcher already carries; only `tile_bbox`+constants used |

## Done definition (in-repo)
`scripts/trigger.py` emits a JSON array of **new** {S1A,S1C} products for a tile+window — S1D skipped
(logged), already-registered acquisitions deduped via per-acq item-exists, idempotent re-run → `[]` —
covered by `tests/unit/test_trigger.py` (all green, no regressions). CronWorkflow wiring + cluster
verify are tracked as the platform-deploy tail of Phase-6 Task 6.
