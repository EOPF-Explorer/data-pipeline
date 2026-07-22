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
- Consequence: 10 S1-RTC tests fail on this branch **by design**. Do not skip them to
  fake green. S1 keeps running on main's image.
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
`…-fra/s3-olci-staging/sentinel-3-olci-l1-efr-staging/`, https gateway hrefs resolve
(`zarr.json` 200), `alternate.s3` present, `expires` stamped (+183 d), no viz links.

## Why visualization is OFF

`S3_VIZ_ENABLED` defaults off. titiler `/info` on the converted store returns
HTTP 500 `"No group found in store … prefix='tests-output/'"` — titiler resolves the
store root under `tests-output/` while the data lives under `s3-olci-staging/`
(store-root resolution mismatch, titiler-eopf#108). This fails before the
swath/projection question is even reached. Flip the gate only after a titiler
reader-open smoke test passes against a real OLCI store.

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
`s3-olci-staging/sentinel-3-olci-l1-efr-staging/`, then get explicit sign-off.

**Known-absent secret:** `eodc-s3-credentials` does not exist in `devseed-staging`.
The prestage template mounts it `optional: true`, so pods start; the OLCI source is
EODC's public STAC (mirrors S2). Re-check on the first cron-driven prestage run.

## Dev environment quirks

- Run tests with `.venv/bin/python -m pytest` (not `uv run pytest` — broken x86
  system Python on this machine). Expected on this branch: full suite green except
  the 10 by-design S1-RTC failures; a conftest guard keeps collection alive.
- `make typecheck` shows ~74 pre-existing errors (unpinned venv mypy); the CI gate
  is pre-commit mypy 1.11.2, which passes.
