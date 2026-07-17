# PLAN — Align the S1 RTC staging collection metadata with the migrated item content

## Context

The S1 RTC **items** in both staging collections were migrated to the new asset model (data-model #196 /
data-pipeline #279): `gamma0-rtc-backscatter-{asc,desc}` + `border-mask-{asc,desc}` with named `bands`,
the datacube/timestamps extensions, identity/projection metadata, normalized `platform`, etc. But the two
**collections'** own metadata is stale — it still describes the *old* model and carries placeholder
values. This plan reviews and aligns the collection metadata (`item_assets`, `summaries`, extensions,
extent, renders, descriptions) with what the items now actually contain, and version-controls the
definitions (currently they exist only in the live API).

Scope: **collection metadata only** (items are already migrated). Staging only. Two collections:
- `sentinel-1-grd-rtc-staging` (per-tile cubes)
- `sentinel-1-grd-rtc-acquisitions-staging` (per-acquisition slices)

## Findings — current collection vs actual item content

| Field | Current (both collections) | Actual item content → target |
|---|---|---|
| `item_assets` | `vv`, `vh`, `zarr-store` | `gamma0-rtc-backscatter-asc/-desc`, `border-mask-asc/-desc`, `zarr-store`, `thumbnail` (with `bands`, `data_type`, `nodata`, `unit`, `gsd`, `roles`) |
| `summaries.platform` | `["Sentinel-1A","Sentinel-1B"]` | **cube**: omit (cube items carry no `platform`); **acq**: `["sentinel-1a","sentinel-1c"]` (lowercase, S1C — S1B is decommissioned) |
| `summaries.processing:level` | present | items carry no `processing:level` → **drop** (or defer to the ingest-provenance follow-up) |
| `stac_extensions` (collection) | sar v1.0.0, sat v1.0.0, projection v2.0.0 | add the ones the items use: **timestamps v1.1.0, render v1.0.0, alternate-assets v1.2.0, storage v2.0.0, item-assets**; **datacube v2.2.0 on the cube collection only** |
| `extent.spatial.bbox` | `[[-180,-90,180,90]]` | **derived from live items** each run — currently `[[-3.00, 41.40, 4.37, 44.25]]`; re-running the generator after new ingests keeps it aligned (avoids a stale hand-set value) |
| `extent.temporal` | `[["2014-04-03", null]]` | start = earliest item (`2025-02-05T06:01:10Z`, derived); keep end `null` (ongoing) |
| `renders` | none | add collection-level `renders.rgb` (the render extension is **collection-oriented**, so this is also where it validates — see #196). Note: it's orbit-generic and **informational** for clients; titiler still renders from the per-item `renders` |
| cube vs acq differentiation | identical metadata | cube: datacube ext + dual-orbit `item_assets` + no `platform` summary; acq: `platform` summary + single-`datetime` semantics, no datacube |
| `title` / `description` | OK (V1 staging) | minor: ensure cube vs per-acquisition wording is accurate; mention the γ⁰/RTC + datacube nature |

`summaries` already correct: `constellation` (`sentinel-1`), `instruments` (`c-sar`), `gsd` (`[10]`),
`sar:*`, `sat:orbit_state` (`[ascending, descending]`).

## Where the metadata lives

The collections are created/updated via `operator-tools/manage_collections.py create --template-path
<json> [--update]` (PUT/POST to the STAC Transaction API). **No template JSON is committed** — the live
definitions are the only source. So this work also **introduces version-controlled templates**.

## Plan (ordered, each step verifiable)

- [ ] **T1 — Snapshot current collections** (rollback safety). `GET /collections/{id}` for both → save to
  `operator-tools/backups/{id}-<date>.json`. *Verify:* both files saved, valid JSON.
- [ ] **T2 — Generator (not hand-written JSON).** Add `operator-tools/build_s1_rtc_collections.py` that,
  for each collection, **loads the live collection (the T1 snapshot) and patches only the stale fields**,
  preserving the good ones (title/description/keywords/providers/license/links). It:
  - rebuilds `item_assets` for the new model — one entry per `gamma0-rtc-backscatter-{asc,desc}` +
    `border-mask-{asc,desc}` + `zarr-store` + `thumbnail`, with `bands`/`data_type`/`nodata`/`unit`/`gsd`/
    `roles` **derived from `eopf_geozarr.stac.s1_rtc` constants** (GAMMA0_DTYPE/NODATA/UNIT, GSD, the asset
    titles + `_gamma0_bands()`), so it can't drift from the builder;
  - fixes `summaries` (`platform` → acq `["sentinel-1a","sentinel-1c"]`, cube: drop; remove
    `processing:level`); updates `stac_extensions`; adds collection-level `renders.rgb`; adds the datacube
    ext on the **cube** collection only;
  - **derives `extent` (spatial bbox + temporal start) from the live items each run** (not a hand-set
    value) so a re-run after new ingests keeps the extent aligned.
  Writes `collections/{id}.json` (committed artifact). *Verify:* `pystac.Collection.from_dict().validate()`
  passes for both; a unit test asserts `item_assets`/`platform`/extensions match the library + expected fixes.
- [ ] **T3 — Cross-check templates against live items.** A small check script: each `summaries` value set
  ⊇ the distinct values across a sample of items; every item asset key ∈ template `item_assets`; collection
  `stac_extensions` ⊇ the union of item extensions (minus item-only ones). *Verify:* check passes for both collections.
- [ ] **T4 — Apply to staging (idempotent).** `manage_collections.py create --update --template-path …`
  for both. *Verify:* re-`GET` both collections; `item_assets`/`summaries`/`extent`/`renders`/extensions
  match the templates; the STAC Browser pages render the new asset list + a working collection preview.
- [ ] **T5 — Wire templates into the repo workflow.** Document the apply command in
  `operator-tools/README.md`; if collection creation is part of an Argo/bootstrap step, point it at the
  committed templates so staging/prod stay reproducible. *Verify:* docs updated; a dry re-apply is a no-op.

## Out of scope / follow-ups
- **prod** collections (`…-prod`) — apply the same templates after staging is verified (separate change).
- `processing:level` / DEM lineage / `derived_from` summaries — gated on the ingest-time provenance
  follow-up that writes those onto items first.
- Auto-maintaining `extent.spatial`/`temporal` as new tiles ingest (a periodic `manage_collections`
  extent-refresh) — nice-to-have, not required here.

## Boundaries
- **Always:** snapshot before PUT; keep changes idempotent; validate templates before applying; staging first.
- **Never:** modify items (already migrated); touch prod without sign-off; let collection `item_assets`
  drift from the builder's asset model again (templates derive from the library constants).

## Verification (end-to-end)
1. `pystac` validates both collection templates.
2. T3 cross-check: collection metadata is a faithful superset of item content.
3. After apply: STAC Browser shows the new asset model, correct platforms (`sentinel-1a/c`), a tightened
   bbox, and a collection-level render preview — for both collections.
