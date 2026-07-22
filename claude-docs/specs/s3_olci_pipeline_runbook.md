# Sentinel-3 OLCI L1 EFR pipeline — runbook

Status as of 2026-07-21: **E2E validated in devseed-staging** (Checkpoint C green).
Crons deployed **suspended**; visualization **gated off**. Staging only.

Collection: `sentinel-3-olci-l1-efr-staging` on `https://api.explorer.eopf.copernicus.eu/stac`
(⚠️ staging isolation is by the `-staging` collection-id suffix — the API **host is shared
with prod**, and the `-fra` bucket is shared too; collection scoping is the only isolation).

## Image / branch model (read this first)

This branch (`feat/s3-olci-pipeline`) builds a **dedicated S3-only RC image**:
`data-pipeline:v1.15.0-s3olci-rc1`. It is **not mergeable to main as-is**:

- The eopf-geozarr pin `5ea5662` (data-model OLCI PR #212) provides
  `s3_olci_optimization.olci_converter` but **drops the `eopf_geozarr.stac` package**
  (S1-RTC support). No data-model ref has both (checked all branches 2026-07-21).
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

data-pipeline (this repo, tag `v1.15.0-s3olci-rc1`):

| Piece | What |
|---|---|
| `scripts/query_stac.py` | `discover --max-items N` cap (N ≥ 1 enforced) |
| `scripts/convert_v1_s3.py` | OLCI conversion entry point — `convert_olci_optimized`, `--min-dimension` stops overview generation |
| `scripts/register_v1.py` | Sentinel-3 branches; viz/thumbnail links gated by `S3_VIZ_ENABLED` (default **off**) |
| `scripts/s3_item_cleanup.py` | shared S3-deletion helpers used by the cleanup cron |
| `stac/sentinel-3-olci-l1-efr-staging.json` | collection template — 21 Oa radiance bands, deliberately **not a datacube** (swath, no native CRS) |

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

## Why visualization is OFF

`S3_VIZ_ENABLED` defaults off. Three layers, in dependency order (updated 2026-07-22):

1. **Store location — RESOLVED, was ours not titiler's.** The deployed titiler
   reconstructs store paths as `{TITILER_EOPF_STORE_URL}/{collection}/{item_id}.zarr`
   under `tests-output/` and **ignores STAC asset hrefs** (known since the S1 work;
   href-based resolution is titiler-eopf#108, a nice-to-have). Writing OLCI under
   `s3-olci-staging/` is what made `/info` 500 with `"No group found in store …
   prefix='tests-output/'"`. Fixed by platform-deploy #342 (output prefix →
   `tests-output`); the pre-flip scene needs one webhook re-run.
2. **DataTree alignment (converter gap, data-model) — current first blocker,
   verified live 2026-07-22 after the prefix fix.** `/info` now opens the store and
   fails with `group '/measurements/r2' is not aligned with its parents`: the
   converter writes the base arrays + coords directly in `measurements/` with
   `r2/r4/r8` nested beneath, so children share dim names (`rows`/`columns`) at
   different sizes and inherit the parent's 2-D coords — xarray DataTree rejects
   that. S2 stores avoid it by keeping every resolution in its own leaf group with
   no arrays in the parent.
3. **No CRS in the store (converter gap, data-model).** The output has CF swath
   geolocation (2-D lat/lon + `coordinates` attrs at every level) but **no
   `grid_mapping` attribute and no CRS variable** — `ds.rio.crs` is `None`, the exact
   S1 failure mode fixed by data-model #176/#201. Will hit once alignment is fixed.
   Reported on data-model PR #212.
4. **Swath tiling (open design question).** Even with a datum declared, the grid is
   curvilinear (no affine transform); current GeoZarr readers don't tile that.
   Either titiler gains geolocation-array reprojection or the converter emits a
   gridded variant.

Flip the gate only after a titiler reader-open smoke test passes against a real
OLCI store.

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
