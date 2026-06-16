# Plan: per-acquisition items render from the shared cube via `sel=time={index}` (NO duplication)

**Goal**: Per-acquisition STAC items render **now** on the deployed TiTiler with **zero data duplication** — by pointing their render links at the **cube's** TiTiler endpoint with `sel=time={index}` (+ the composite render), instead of writing a store per acquisition. Remove the per-acquisition-store bridge from PR #267.

**Constraint**: No per-acquisition stores anywhere. Reuse `register_v1._select_render` / `_render_to_query`. Touch `register_per_acquisition.py` + `run_ingest_register.py`; remove the bridge files.

## Context — the key finding that changes everything

PR #267 wrote a single-acquisition GeoZarr per acquisition (≈2× storage) because we believed the deployed TiTiler couldn't render a per-acquisition item from the shared cube. **That belief was wrong.** Verified this session on `s1-rtc-30TXM` (a 2-slice cube):

- TiTiler-eopf **supports `sel` by integer index**: `sel=time=0` and `sel=time=1` → **200** and render **distinct** slices (mean 106 vs 166, abs-diff 75); out-of-range `sel=time=2/99` → 500; the `nearest::<datetime>` *method syntax* → 500. tilejson + preview both 200 with `sel=time={index}`.

So a per-acquisition item can render **its slice of the shared cube** by linking to the **cube's** render endpoint with `sel=time={index}` — TiTiler reconstructs the cube path (which exists) and `isel`s the slice. **No store, no copy, renders today, and now verifiable.** This supersedes both the per-acquisition-store bridge (duplication, rejected) and the "defer rendering" fallback (which was premised on the wrong `sel` belief).

**Index robustness**: the index is positional (`isel`-like — confirmed: `sel=time=0/1` work, out-of-range 500s, and the time *labels* are ns-epoch ints so `0` can't be a label). The cube only ever **appends** (T4: new slice at the end; backfill also appends at the end), so an item's index is stable; and `register_per_acquisition` re-registers **all** items every ingest run, re-deriving indices from the current cube — self-healing even if the cube is ever rebuilt. **Requirement**: compute the index from the cube's **physical** time order (NOT sorted — `read_times_ns` sorts today, which would silently mis-map).

**Forward-risk + upgrade path**: baking `sel=time={index}` couples to TiTiler's *current* integer-`isel` semantics. If a future titiler reinterprets `sel` (label-based / `nearest::{datetime}`), `sel=time=0` could change meaning — mitigated by the re-bake-every-run above, but the cleaner long-term form is **`sel=time=nearest::{datetime}`** (reorder-proof, label-based). Today that syntax 500s, so we use the index; **switch to the datetime form when the deployed titiler supports it** (track as a follow-up).

**Independent of #246**: the acquisition links point at the *cube* endpoint, so they render via whatever serves the cube — the #246 path workaround now, or titiler#108 href-resolution later. This approach is **not** itself a #246 bridge and survives the #246 revert untouched (it only needs the cube to render).

## Current state (PR #267)

| Piece | Action |
|---|---|
| `write_per_acquisition_stores.py` + test; `ingest_v1_s1_rtc.write_single_acquisition_store` + tests; `run_ingest_register` Step 3 | **delete / revert** — already done in the working tree (uncommitted): files restored to baseline + bridge removed |
| `register_per_acquisition` render links | **rework**: point at the **cube** endpoint (`{cube_collection}/items/s1-rtc-{tile}`) with the composite render + `sel=time={physical index}`. (Baseline used the acq endpoint + `variables=/orbit:vh` + `sel=time=nearest::…`, which 500s.) |
| `register_per_acquisition._reorient_item_to_orbit` | **keep** (orbit metadata + asset orbit group) — added in working tree |
| `-tests` validation store `s1-rtc-31TCH-20260605t060907.zarr` | **delete** (no longer needed; this approach writes no store) |
| data-pipeline #246 per-acq-store extension | **revert** that section; note the no-dup `sel=index` approach |

## Tasks

### Task A — Remove the per-acquisition store machinery (no duplication)  <status: DONE in working tree>
Bridge files deleted; `ingest_v1_s1_rtc` / `run_ingest_register` / their tests restored to the 3-subprocess baseline. **Verify**: `grep -rn "write_per_acquisition\|write_single_acquisition_store" scripts tests` → none; `uv run pytest tests/unit/test_run_ingest_register.py test_ingest_v1_s1_rtc.py` green.

### Task B — `register_per_acquisition`: render links → cube endpoint + `sel=time={index}`  <status: NEXT>
**What**:
- Add `--cube-collection` arg (the collection whose TiTiler endpoint hosts the cube store, e.g. `sentinel-1-grd-rtc-staging`). The render links target `{raster}/collections/{cube_collection}/items/s1-rtc-{tile_id}/…`.
- Read the orbit group's `time` axis in **physical order** (new/changed helper; `read_times_ns` currently sorts — keep a physical-order read for indexing). For each `(index, t_ns)` emit one item.
- Build the render query from the item's **reoriented `renders.rgb`** by calling `register_v1._render_to_query(item["properties"]["renders"]["rgb"], include_tilesize=…)` **directly on the dict** (don't use `_select_render`, which is pystac-`Item`-based — `register_per_acquisition` is dict-based). Composite expression for the run orbit + `rescale 0,0.1` + `bidx`; `tilesize` for tiles/tilejson, not preview. Then append `&sel=time={index}`. Build `tilejson` + `xyz` + `thumbnail` links/asset against the cube endpoint.
- Keep `_reorient_item_to_orbit` (sat:orbit_state / renders orbit / vv,vh asset group → run orbit). Items stay **references** (asset href → cube; **no store**).
**Verify**: unit — a per-acq item's links point at `{cube_collection}/items/s1-rtc-{tile}`, carry the composite expression (run orbit) + `rescale=0,0.1` + `sel=time={index}` (index = physical position), and **no per-acquisition store path** appears anywhere.
**Acceptance**:
- [ ] Links target the cube endpoint + `sel=time={physical index}` + composite render
- [ ] No per-acquisition store path in any item; asset href → cube
- [ ] Orbit reconciliation retained; unit tests updated/green

### Task C — `run_ingest_register`: pass `--cube-collection`  <status: blocked on B>
**What**: the per-acq register step passes `--cube-collection {collection}` (the cube collection) alongside the existing `--collection {acquisitions_collection}`. 3-subprocess flow unchanged.
**Verify**: `test_run_ingest_register` asserts the per-acq step receives both the acquisitions collection and `--cube-collection`.

### Task D — Live validation on `-tests` (renders now)  <status: blocked on C>
**What**: Register the `31TCH` acquisition item(s) into `…-acquisitions-tests` with cube-endpoint+`sel=index` links (cube collection `sentinel-1-grd-rtc-tests`, item `s1-rtc-31TCH`). The 31TCH cube has 1 slice → `sel=time=0`.
**Verify**:
- The item's own `tilejson` + `preview` (+ an `xyz` tile at a **realistic zoom** over the tile, not `0/0/0`) → **200**, rendering the composite; confirm **no** `…-acquisitions-tests/*.zarr` store exists.
- **Index↔datetime mapping** (the core assumption): on a 2-slice cube (`30TXM` staging), for the item whose physical index is `i`, confirm its rendered slice equals the cube's `sel=time={i}` render **and** that index `i` corresponds to that item's `datetime` (read the cube's physical `time[i]` and check it matches the item id's stamp). Not just "distinct."
**Acceptance**:
- [ ] `-tests` acquisition item renders (tilejson + preview + real-zoom xyz 200) from the cube, no store
- [ ] Physical index `i` ↔ item datetime ↔ rendered slice verified on a multi-slice tile

### Task E — Cleanup + tracking  <status: blocked on D>
**What**: Delete the `-tests` per-acquisition **store** I created earlier (`…/tests-output/sentinel-1-grd-rtc-acquisitions-tests/s1-rtc-31TCH-20260605t060907.zarr`, OVH `de` endpoint) — this approach writes none. Revert the "Extension: per-acquisition render stores" section in data-pipeline **#246** and replace with a one-liner: per-acq items render from the shared cube via `sel=time={index}` (no dup, no titiler change needed). Update **PR #267** title/description to the `sel=index` approach. Record the `sel`-by-index finding in memory.
**Verify**: store path → 404; #246 + PR #267 updated; memory note written.
**Acceptance**:
- [ ] `-tests` store removed; #246 + PR description corrected; finding recorded

## Verification (end-to-end)
1. `uv run pytest tests/unit` + `uv run ruff check` green.
2. Run `register_per_acquisition` against the `31TCH` `-tests` cube → item renders (tilejson + preview 200) with **no** per-acq store present.
3. On a 2-slice cube (`30TXM`), confirm `sel=time=0` vs `=1` via the acquisition items' links render **distinct** correct slices.

## Done definition
Per-acquisition items render on the deployed TiTiler from the **single shared cube** via cube-endpoint links + `sel=time={physical index}` + composite render — **zero data duplication, no per-acquisition stores, no TiTiler change**. The store bridge is removed; orbit metadata is reconciled; `-tests` store + #246 + PR #267 cleaned up; the `sel`-by-index finding is recorded.

## Lesson recorded (memory)
Deployed **titiler-eopf supports `sel=time={integer index}`** (isel-like) but **not** the `nearest::<datetime>` syntax, and it **reconstructs the store path from `{collection}/{item_id}`, ignoring the asset href**. ⇒ render a per-acquisition slice from the shared multi-time cube by linking to the **cube** item's endpoint + `sel=time={physical index}`; **never** create per-acquisition stores (no dual storage). Earlier "titiler doesn't support sel / rendering must be deferred" was wrong.
