# Sentinel-3 OLCI L1 EFR pipeline — runbook

Status as of 2026-07-28: **E2E re-validated in devseed-staging on the gridded converter**
(rc2). Crons remain **suspended**. Visualization is **verified working against a live
store** but is **NOT yet enabled** — the deployed image is still rc2 and carries no
`S3_VIZ_ENABLED`, so registered items currently have no viz links. Enabling it is
**pending a platform-deploy PR** (branch `feat/s3olci-enable-viz`, sets `S3_VIZ_ENABLED=1`
and pins rc4). The code default stays off. Staging only.

Collection: `sentinel-3-olci-l1-efr-staging` on `https://api.explorer.eopf.copernicus.eu/stac`
(⚠️ staging isolation is by the `-staging` collection-id suffix — the API **host is shared
with prod**, and the `-fra` bucket is shared too; collection scoping is the only isolation).

## Image / branch model (read this first)

This branch (`feat/s3-olci-pipeline`) builds a **dedicated S3-only RC image**:
`data-pipeline:v1.15.0-s3olci-rc4`. It is **not mergeable to main as-is**:

- The eopf-geozarr pin `547981de` (head of data-model OLCI PR #212, re-pinned
  2026-07-28 from `5ea5662` to pick up the gridded converter) provides
  `s3_olci_optimization.olci_converter` but **drops the `eopf_geozarr.stac` package**
  (S1-RTC support). No data-model ref has both (re-checked at the new head).
  The SHA lives on the contributor fork but resolves from the EOPF-Explorer URL via
  `refs/pull/212/head`, which GitHub keeps on the base repo while the PR is open.
- Consequence: the S1-RTC test surface cannot run on this branch **by design**. A
  guard in `tests/unit/conftest.py` excludes those modules from collection (they
  would abort the whole suite at import) and skips two S1-only cases with an
  explicit reason — the exclusion is loud and documented, not hidden, and
  auto-reactivates once `eopf_geozarr.stac` returns. S1 keeps running on main's
  image.
- Eventual path (user-driven): merge S1 + S3 data-model into main, then re-pin.
- data-pipeline PR #370 is **DRAFT / do-not-merge** for this reason; the image is built
  from the tag, not the PR.

## Components

data-pipeline (this repo, tag `v1.15.0-s3olci-rc4`):

| Piece | What |
|---|---|
| `scripts/query_stac.py` | `discover --max-items N` cap (N ≥ 1 enforced) |
| `scripts/convert_v1_s3.py` | OLCI conversion entry point — `convert_olci_optimized`, `--min-dimension` stops overview generation |
| `scripts/register_v1.py` | Sentinel-3 branches; `remap_olci_measurement_paths` repoints band assets at the `r0` base level; viz/thumbnail links gated by `S3_VIZ_ENABLED` (code default **off**, set to `1` in platform-deploy); tilejson link carries explicit zoom bounds |
| `scripts/s3_item_cleanup.py` | shared S3-deletion helpers used by the cleanup cron |
| `stac/sentinel-3-olci-l1-efr-staging.json` | collection template — 21 Oa radiance bands, **not a datacube**. Rationale was "swath, no native CRS"; the gridded converter removes that reason, so revisit once the rc4 E2E confirms the layout |

platform-deploy (merged via #340, Flux-reconciled into `devseed-staging`,
all under `workspaces/devseed-staging/data-pipeline/`):

| Manifest | What | State |
|---|---|---|
| `eopf-workflow-concurrency-configmap.yaml` | s3olci semaphore keys | live |
| `eopf-explorer-convert-v1-s3-prestage-template.yaml` | prestage → convert → register WorkflowTemplate | live |
| `eopf-explorer-conversion-v1-s3-sensor.yaml` | webhook sensor (`action: convert-v1-s3`) | live |
| `templates/eopf-sync-data-processor-s3olci-template.yaml` | discovery sync template | live |
| `cronwf/eopf-explorer-cronwf-s3olci.yaml` | discovery cron `eopf-s3olci-data-processor`, `0 */2 * * *` | **suspend: true** |
| `cronwf/eopf-explorer-cronwf-s3olci-cleanup.yaml` | cleanup cron `eopf-explorer-s3olci-cleanup`, `30 * * * *` (offset from S2 at :00) | **suspend: true, dry_run: true** |

## E2E evidence (2026-07-21)

Webhook POST on a real S3A item → workflow `eopf-samples-convert-v1-s3-qx2v2`
SUCCEEDED, all 7 steps, ~34 min. Verified: `staged==true` on prestage
(anti-passthrough held), item GET 200, assets under
`…-fra/s3-olci-staging/sentinel-3-olci-l1-efr-staging/` (prefix as of this run;
moved to `tests-output/` by platform-deploy #342), https gateway hrefs resolve
(`zarr.json` 200), `alternate.s3` present, `expires` stamped (+183 d), no viz links.

## Converter-side blockers (all resolved upstream)

These four gated visualization off until 2026-07-28. Kept for context on why the store
looks the way it does; see the verification section below for current state.

1. **Store location — RESOLVED, was ours not titiler's.** The deployed titiler
   reconstructs store paths as `{TITILER_EOPF_STORE_URL}/{collection}/{item_id}.zarr`
   under `tests-output/` and **ignores STAC asset hrefs** (known since the S1 work;
   href-based resolution is titiler-eopf#108, a nice-to-have). Writing OLCI under
   `s3-olci-staging/` is what made `/info` 500 with `"No group found in store …
   prefix='tests-output/'"`. Fixed by platform-deploy #342 (output prefix →
   `tests-output`); the pre-flip scene needs one webhook re-run.
2. **DataTree alignment — FIXED upstream, verified live 2026-07-28.** `/info` used to
   fail with `group '/measurements/r2' is not aligned with its parents`: the old
   converter wrote base arrays + coords directly in `measurements/` with `r2/r4/r8`
   nested beneath, so children shared dim names at different sizes and inherited the
   parent's 2-D coords. The gridded converter (data-model #212, pinned at
   `547981de`) gives every level its own leaf group — `measurements/r0`, `r2`, … with
   no arrays in the parent — matching the S2 shape.
3. **CRS in the store — FIXED upstream, verified live 2026-07-28.** Each level now
   carries a `spatial_ref` variable with `crs_wkt`, a `grid_mapping` attribute on
   every band, and group-level `proj:code` / `spatial:transform` / `spatial:bbox`.
4. **Swath tiling — ANSWERED by gridding.** The converter now reprojects the
   curvilinear swath onto a regular grid (default `EPSG:4326`, `--target-crs` to
   override) with 1-D `x`/`y` coords and an affine transform, so a standard
   affine-only reader can tile it. The per-pixel `latitude`/`longitude` arrays are
   gone from `measurements/*` as a result.

## Visualization — VERIFIED WORKING 2026-07-28 (rc2 E2E)

All four items above were confirmed against a real store, workflow
`eopf-explorer-convert-v1-s3-prestage-kvm8b` (Succeeded 7/7) on scene
`S3B_OL_1_EFR____20260728T073033_…`:

- `/info` → **200**, the `not aligned with its parents` error is gone; 21 bands,
  EPSG:4326, 4717×6201, `grid_mapping=spatial_ref`.
- `preview` and XYZ tiles → **200**, and visually confirmed as correctly georeferenced
  natural-colour imagery (Cyprus, Levant coast, Euphrates), not a blank 200.
- Item hrefs and their s3 alternates all land on `measurements/r0/…`.

Two gotchas worth keeping:

- **The variable path that works is `/measurements/r0:<band>`.** titiler's own `/info`
  response advertises the variables as `/measurements:<band>`, but that form returns 500
  on render. Do not "correct" `_S3_OLCI_VIZ_QUERY` to match the `/info` keys.
- **`tilejson.json` needs explicit `minzoom`+`maxzoom`.** titiler cannot derive them for
  this store and 500s without both (minzoom alone still 500s). `register_v1` appends
  `_S3_OLCI_TILEJSON_ZOOM` to the tilejson link only — they describe the tile matrix, not
  the render, and the same query is shared with `/preview` and `/tiles`.

Still open: the unconditional `viewer` link points at the bare `/viewer` endpoint, which
builds its own tilejson URL from the UI fields only and cannot be passed zoom bounds — so
it 500s and the map renders blank for OLCI. **This is our endpoint choice, not a titiler
limitation:** `/WebMercatorQuad/map.html` forwards `minzoom`/`maxzoom` through to its
tilejson fetch and renders correctly with the same query (verified 2026-07-28). Fixing it
means making that unconditional link mission-aware. The link predates this work and is not
gated by `S3_VIZ_ENABLED`; STAC Browser's image comes from the thumbnail asset, which does
render.

## Operations

**Trigger a conversion** (webhook; port-forward per `operator-tools/README.md`):
POST with `action: convert-v1-s3` and the source item URL. Expected outcome = the
E2E evidence above.

**Collection writes need an OIDC bearer.** Write enforcement is live on `/stac`
(unauthenticated → 401). `operator-tools/manage_collections.py` sends no auth header
and will 401 — instead mint a client-credentials token from the `stac-auth-oidc`
secret in `devseed-staging` (Keycloak `hub-eopf-explorer.eox.at`) and use curl.

**Resume the discovery cron (D2 — ask-first).** Set `suspend: false` via a
platform-deploy PR (Flux tracks main; direct kubectl edits drift/revert). Supervise
the first window, check dedup against already-registered items, re-suspend if in doubt.

**Cleanup cron (D3 — ask-first).** Ships `dry_run: true`. The `-fra` bucket is
prod-shared and has **no versioning — deletes are permanent**. Before flipping
`dry_run` to false: run a manual dry-run (`argo submit --from cronwf/…-cleanup
-p dry_run=true -p max_items_per_run=100`), verify every candidate is confined to
`tests-output/sentinel-3-olci-l1-efr-staging/` (pre-#342 items may still reference
`s3-olci-staging/…`), then get explicit sign-off.

**Known-absent secret:** `eodc-s3-credentials` does not exist in `devseed-staging`.
The prestage template mounts it `optional: true`, so pods start; the OLCI source is
EODC's public STAC (mirrors S2). Re-check on the first cron-driven prestage run.

## Dev environment quirks

- Run tests with `uv run pytest` (repo convention; `.venv/bin/python -m pytest` is
  an equivalent fallback if `uv run` resolves a broken system interpreter locally).
  Expected on this branch: full suite green, with the S1-RTC surface excluded /
  skipped by the conftest guard (see "Image / branch model" above).
- `make typecheck` shows ~74 pre-existing errors (unpinned venv mypy); the CI gate
  is pre-commit mypy 1.11.2, which passes.
