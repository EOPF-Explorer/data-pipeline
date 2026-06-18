# SPEC — data-pipeline follow-up: adopt the new data-model S1 RTC STAC builder

Companion to data-model PR #196 (issue [#195](https://github.com/EOPF-Explorer/data-model/issues/195)).
That PR moved all *pure* S1 RTC STAC **construction** into the `eopf_geozarr.stac.s1_rtc` library and
reworked the asset model. This follow-up updates **data-pipeline** to consume it, removes the now-
duplicated/obsolete construction code, keeps registration (decoration + upsert) working with the new
asset model, and migrates the existing staging items.

## 1. Objective

- data-pipeline registration consumes the data-model library as the single source of STAC item shape.
- No construction logic remains duplicated in `scripts/` (it now lives in data-model).
- Registered items carry the **new asset model + metadata** *and* the full deployment decoration
  (render/tile/thumbnail links, `store` link, S3 `alternate`/`storage`) so Explorer previews and
  "Additional Resources" keep working.
- All existing items in `sentinel-1-grd-rtc-staging` (cube) and
  `sentinel-1-grd-rtc-acquisitions-staging` (per-acquisition) are migrated to the new model.

Users: the EOPF Explorer ingest/registration pipeline (Argo cron) and operators running manual
re-registration.

## 2. Scope of changes (by file)

### `pyproject.toml`
- Bump the pin (line 37) `eopf-geozarr @ git+…/data-model.git@<SHA>` to the merge commit of PR #196 on
  `feat/s1-rtc-stac-builder` (currently `@16c5f14…`). NB: supersedes the `s1-rtc-pin-hold` hold once #195
  lands.

### `scripts/register_per_acquisition.py`
- **Remove** the locally-defined construction now in the library: `per_acquisition_items`,
  `read_times_ns`, and `apply_s1_rtc_rescale` (the builder now emits the corrected `rescale 0.0,0.2`).
- `acquisition_id` moves to the library; **re-export** it here (`from eopf_geozarr.stac.s1_rtc import
  acquisition_id`) so `trigger_cdse.py:30` (which imports `acquisition_id` *and* `DEFAULT_ACQ_COLLECTION`
  from this module — the latter stays here) is unaffected.
- `_reorient_item_to_orbit` is **no longer used by per-acquisition construction** (the library sets
  `sat:orbit_state`/render per orbit). It is still needed by the **cube preview pin** — see
  `register_v1_s1_rtc.py` below: relocate a simplified, properties-only version there and drop the
  cross-import at `register_v1_s1_rtc.py:30`. Do **not** simply delete it.
- **Use** `from eopf_geozarr.stac.s1_rtc import build_s1_rtc_per_acquisition_items`.
- New flow: `items = build_s1_rtc_per_acquisition_items(store, orbit=…, collection_id=…)` → for each
  `pystac.Item`, derive `sel_time` from `item.datetime`, then **decorate** (this stays in the pipeline):
  `store` link, `add_alternate_s3_assets`, render `tilejson`/`xyz`/`thumbnail` + `via` links pointing at
  the **cube** endpoint (`render_tilejson/xyz/thumbnail` helpers — keep these), then `upsert`.
- Keep: `_sel_time`, the render-link URL helpers, incremental dedup (`existing_item_ids`),
  `--reregister-all`, `_upsert_items`, the CLI.
- `acquisition_id` is imported by `trigger_cdse.py` — re-export it from this module (`from … import
  acquisition_id`) so `trigger_cdse` needs no change, or update that import.

### `scripts/register_v1_s1_rtc.py`
- **Remove** the local `Slice`, `pick_slice`, `slice_coverages`, and `_open_cube_root`; **import the
  public** `slice_coverages`/`pick_slice` (and `Slice` if needed) from `eopf_geozarr.stac.s1_rtc`. Do
  **not** import the library's private `_open_root` — `slice_coverages` opens the store internally, so
  `_open_cube_root` just deletes.
- Update the import at `:30` (it currently pulls `_reorient_item_to_orbit, apply_s1_rtc_rescale` from
  `register_per_acquisition`): drop `apply_s1_rtc_rescale` and its call (`:162`); **own** a simplified
  `_reorient_item_to_orbit` here.
- `_reorient_item_to_orbit` (cube preview pinning): the asset-href rewrite is **obsolete** (no `vv`/`vh`
  keys; both orbits are first-class assets). Reduce it to **properties only** — set `sat:orbit_state`
  and the `renders.rgb` expression to the chosen preview slice's orbit. (The new builder omits
  `sat:orbit_state` on dual-orbit cubes, so the preview pin re-adds it for the rendered slice; add
  `SAT_EXT` if not present.)
- Keep the augmentation chain: `add_store_link`, `add_alternate_s3_assets`, `add_visualization_links`,
  `add_thumbnail_asset`, `warm_thumbnail_cache`, `upsert_item`.

### `scripts/register_v1.py`
- `add_visualization_links` / `add_thumbnail_asset`: both are **render-first**, so the new S1 RTC items
  (which always carry `renders.rgb`) never reach the `item.assets.get("vh")` fallback (~lines 269, 361)
  — confirmed by `TestVisualizationFromRenders`. **Leave the fallback in place**: it is the
  mission-default for *render-less* / legacy S1 (e.g. `register_v0`) and is covered by
  `test_falls_back_to_mission_default_without_renders`; deleting it would break that test and drop the
  legacy path. (Earlier draft said to delete it — corrected: it is render-superseded for RTC, not dead.)
- `add_alternate_s3_assets`: generic over assets → automatically covers the new keys
  (`gamma0-rtc-backscatter-{asc,desc}`, `border-mask-{asc,desc}`). Verify alternates land on all data
  assets; confirm the `thumbnail` skip still holds.
- `add_store_link`: unchanged.

### `scripts/trigger_cdse.py`
- Only `acquisition_id` import is affected — see re-export above. No logic change. (CDSE per-product
  provenance — `derived_from`, `view:incidence_angle`, orbit numbers — is a *separate* future task, not
  this PR.)

## 3. Migration (one-shot)

A driver that re-registers every existing item in both staging collections via the updated paths
(idempotent DELETE-then-POST):
- **Cube**: for each store under `sentinel-1-grd-rtc-staging`, run the `register_v1_s1_rtc.register(...)`
  path (build → pin preview → decorate → upsert).
- **Per-acquisition**: for each tile present, run `register_per_acquisition` per orbit with
  `--reregister-all` (rebuilds every slice's item with the new model).
- Discover the work list by listing each collection's items (cube ids → stores via the `zarr-store`
  asset / `store` link; tiles+orbits for per-acq).
- **Idempotent & resumable**; log per-item status; **staging only**.
- **Verify before bulk, and have a rollback:** record per-collection item counts up front; run the
  single-tile smoke (below) and inspect a sample before the full run; rollback = re-run the prior
  pipeline at the previous pinned data-model SHA (the DELETE-then-POST upsert is idempotent, so a
  re-run cleanly restores the old items).

Acceptance: a sampled migrated cube + per-acq item shows the new asset model, datacube/identity/proj
metadata, **and** working `tilejson`/`xyz`/`thumbnail` + `store`/`via` + S3 `alternate` (i.e. previews
and "Additional Resources" intact).

## 4. Commands

```
# unit tests + lint/type (data-pipeline conventions)
uv run pytest tests/unit -q
uv run ruff check scripts tests && uv run mypy scripts

# single-tile smoke (verify decoration end-to-end before mass migration)
uv run python scripts/register_v1_s1_rtc.py --store <s3-cube> --collection sentinel-1-grd-rtc-staging \
  --stac-api-url <url> --raster-api-url <url> --s3-endpoint <url>
uv run python scripts/register_per_acquisition.py --store <cube> --tile-id 30TWN \
  --orbit-direction descending --collection sentinel-1-grd-rtc-acquisitions-staging \
  --cube-collection sentinel-1-grd-rtc-staging --stac-api-url <url> --raster-api-url <url> --reregister-all

# full migration (after smoke passes)
uv run python scripts/<migrate_s1_rtc>.py --stac-api-url <url> --raster-api-url <url> --s3-endpoint <url>
```

## 5. Tests

- **Move out (delete here):** `tests/unit/test_pick_slice.py`, `tests/unit/test_slice_coverage.py`, and
  the construction subset of `tests/unit/test_register_per_acquisition.py` (`per_acquisition_items`/
  `acquisition_id`/reorient) — these now live in data-model `tests/test_s1_stac_per_acquisition.py`.
- **Split, don't delete, `test_register_per_acquisition.py`:** drop the construction tests
  (`per_acquisition_items`/`acquisition_id`/reorient — moved to data-model), **keep** the link/`via`/
  alternate tests, but **rebuild their base fixture** — the hand-built `_base_item()` carries the old
  `vv`/`vh` assets + `rescale 0.0,0.1`; regenerate it from `build_s1_rtc_stac_item` (new asset model,
  rescale 0.2) so the tests exercise reality.
- **Keep + update:** the registration/link tests — assert the render-first `tilejson`/`xyz`/`thumbnail`
  point at the cube endpoint with `sel=time`, that `add_alternate_s3_assets` covers the new asset keys
  (`gamma0-rtc-backscatter-*`, `border-mask-*`), and that the cube preview pin sets orbit-only metadata.
- `test_register_v1_s1_rtc.py`: update for library `slice_coverages`/`pick_slice` + property-only
  reorient. `test_run_ingest_register.py`: keep.
- Add a small migration unit test (work-list discovery + idempotent upsert against a mocked API).

## 6. Boundaries

**Always**
- Preserve the full registration decoration so previews + "Additional Resources" keep working.
- Idempotent upserts (DELETE-then-POST); resumable migration; log every item.
- Staging only unless explicitly authorized for prod.

**Ask first**
- Running the collection-wide migration against staging (bulk overwrite) — and never prod without sign-off.
- Bumping the pin to a non-merged / moving ref.

**Never**
- Re-introduce construction logic in `scripts/` — it lives in data-model now (the pipeline only decorates + registers).
- Strip render/preview/store links from live items.
- Reference `vv`/`vh` asset keys (replaced by `gamma0-rtc-backscatter-*` + named `bands`).

## Out of scope (separate future work)
- Ingest-time provenance: band `statistics` (#157), `processing:software`/DEM lineage written into store
  attrs; CDSE per-product fields (`derived_from`, `view:incidence_angle`, orbit numbers) injected at register.
