# Plan: Sub-issue 7 + 7a — Argo s1tiling on staging (`claude-docs/specs/s1_grd_phase5_subissues.md`)

**Goal**: run the S1Tiling → ingest → register chain in-cluster, writing to the staging
collection (`sentinel-1-grd-rtc-staging`) and bucket (`esa-zarr-sentinel-explorer-s1-l1grd-staging`).
**Constraint**: GitOps via flux + SealedSecrets; no raw creds in git. Model on `eopf-explorer-convert-v1-s2-template.yaml`.

Decided 2026-06-03: DEM reaches the pod via a **csi-rclone S3 mount** (not a Cinder populate-job);
the **CDSE/EODAG secret is created by user/Emmanuel** (referenced by name here).

---

## Current state (verified 2026-06-03)

| Resource | Status |
|----------|--------|
| Data-pipeline image w/ S1 scripts | ✅ `sha-651dbb2` built+pushed (PR #186); registry `w9mllyot.c1.de1.container-registry.ovh.net/eopf-sentinel-zarr-explorer/data-pipeline` |
| `ingest_v1_s1_rtc.py` container-native | ✅ `run_ingest()`+`_upload_store_to_s3()` (s3fs) added; 340 tests pass; **uncommitted** |
| Staging collection | ✅ live (HTTP 200), 0 items |
| OTB image | `registry.orfeo-toolbox.org/s1-tiling/s1tiling:1.4.0-ubuntu-otb9.1.1` (eodag 4.0.0) |
| Local DEM data to upload | ✅ 31 COG tiles (980 MB) + `DEM_Union.gpkg` (18 MB) + `egm2008.grd` (4 MB) in `$S1T_WORKDIR` |
| csi-rclone storageclass | ✅ present; static PV → secret (`volumeAttributes.secretName/secretNamespace`); secret keys `configData`/`remote`/`remotePath`; existing model `devseed/workspace-bucket` (ROX) |
| Secret mgmt | **SealedSecrets** (`*-sealed.yaml` via flux); raw secrets must be `kubeseal`-ed |
| CDSE/EODAG secret in `devseed-staging` | ❌ **MISSING — hard blocker** (only `geozarr-s3-credentials`, `datapipeline-secrets`, `argo-artifact-s3-credentials`) |
| DEM PVC | ❌ not created |
| s1tiling WorkflowTemplate | ❌ not written (greenfield; zero S1 refs in platform-deploy) |

---

## Dependency graph

```
7a.1 upload DEM → S3 prefix      ─┐
7a.2 DEM rclone SealedSecret      ├─► 7a.4 DEM PV+PVC ─┐
7a.3 (Emmanuel) confirm rclone     │                    │
     secret-name convention      ─┘                    │
                                                        ├─► 7.x s1tiling WorkflowTemplate ─► trigger 31TDH
CDSE/EODAG SealedSecret (Emmanuel) ─────────────────────┘            │
config/S1GRD_RTC.cfg → ConfigMap (cfg templating) ──────────────────┘
                                                                     │
                                          (then) Sub-issue 6 ingest+register template ─► staging item
```

---

## Tasks

### Task 7a.1 — Upload DEM data to an S3 prefix  ready
**What**: one-time push of local DEM data so csi-rclone can mount it.
**Verify** (user runs; needs OVH creds w/ write on target bucket):
```bash
S1T="$S1T_WORKDIR"; DEST=s3://esa-zarr-sentinel-explorer-tests/dem
aws s3 cp "$S1T/DEM/COP_DEM_GLO30" "$DEST/COP_DEM_GLO30" --recursive --exclude '*' --include '*.tif' --endpoint-url https://s3.de.io.cloud.ovh.net --profile eopfexplorer
aws s3 cp "$S1T/DEM/dem_db/DEM_Union.gpkg" "$DEST/dem_db/DEM_Union.gpkg" --endpoint-url https://s3.de.io.cloud.ovh.net --profile eopfexplorer
aws s3 cp "$S1T/geoid/egm2008.grd" "$DEST/geoid/egm2008.grd" --endpoint-url https://s3.de.io.cloud.ovh.net --profile eopfexplorer
```
**Acceptance**: `aws s3 ls $DEST/COP_DEM_GLO30/ | wc -l` ≥ 31; gpkg + grd present.
> Open: target bucket — tests bucket (laptop has write) vs the S1 staging bucket (laptop is AccessDenied; would need in-cluster job). Default: tests bucket; DEM is env-agnostic reference data.

### Task 7a.2 — DEM rclone SealedSecret  blocked-on-7a.3
**What**: SealedSecret `s1-dem-rclone` (devseed-staging) with keys `configData` (`[s1dem]` type=s3, endpoint OVH, region de, access_key_id/secret_access_key), `remote=s1dem`, `remotePath=/esa-zarr-sentinel-explorer-tests/dem`.
**Verify**: `kubeseal` produces `s1-dem-rclone-sealed.yaml`; `kubectl apply --dry-run=client` ok.
**Acceptance**: sealed manifest committed; no raw creds in git.

### Task 7a.3 — (Emmanuel) confirm csi-rclone provisioning convention  OPEN
**What**: existing PVC `workspace-bucket` pairs with same-named secret `workspace-bucket`. Confirm whether csi-rclone dynamic provisioning keys the secret off the PVC name, or a static PV must be declared with `volumeAttributes.secretName`. Shapes 7a.4.

### Task 7a.4 — DEM PV + PVC  blocked-on-7a.2/7a.3
**What**: ROX PVC `s1-dem` (+ static PV if required) in devseed-staging, storageclass csi-rclone.
**Verify**: PVC `Bound`; a debug pod mounting it sees `COP_DEM_GLO30/*.tif`, `dem_db/DEM_Union.gpkg`, `geoid/egm2008.grd`.

### Task CDSE — (Emmanuel) EODAG SealedSecret  OPEN / HARD BLOCKER
**What**: SealedSecret (proposed name `cdse-eodag-credentials`, devseed-staging) holding `eodag.yml` (cop_dataspace username/password) to mount at `/eo_config/eodag.yml`. S1Tiling cannot download scenes without it.

### Task 7.1 — cfg as ConfigMap + templating  ready
**What**: `run_s1tiling.py` finding — S1Tiling does NOT template the cfg; it runs `S1Processor` on the file verbatim. The Argo step must produce a per-run cfg with `roi_by_tiles`/`tiles`/`first_date`/`last_date`/`orbit_direction` substituted from the base `config/S1GRD_RTC.cfg`. Approach: base cfg in a ConfigMap; an init step `sed`-substitutes into an emptyDir before `S1Processor` runs.
**Acceptance**: rendered cfg has the per-run tile/date/orbit; DEM/geoid paths point at the mount (`/MNT/COP_DEM_GLO30`, `/MNT/dem_db/DEM_Union.gpkg`, `/MNT/geoid/egm2008.grd`); `eodag_config: /eo_config/eodag.yml`.

### Task 7.2 — s1tiling WorkflowTemplate  blocked-on-7a + CDSE
**What**: `workspaces/devseed-staging/data-pipeline/eopf-explorer-s1tiling-template.yaml`. Single OTB-image step (4 CPU / 16Gi, `activeDeadlineSeconds: 7200`, retry 3× exp backoff, nodepool `pipeline`). Mounts: DEM PVC (ro) → `/MNT`; CDSE secret → `/eo_config/eodag.yml`; eodag4 patch (ConfigMap) → `/patch`; rendered cfg (emptyDir) → `/data/config`; working dirs (emptyDir) for `data_raw`/`tmp`. Command: `bash -c 'python3 /patch/s1tiling_eodag4_patch.py && S1Processor /data/config/S1GRD_RTC.cfg'`.
**Params** (Script A inputs): `tile_id`, `orbit_direction=descending`, `date_start`, `date_end`, `s3_geotiff_bucket`, `s3_geotiff_prefix=s1tiling-output`, `s3_endpoint`, `pipeline_image_version`.
**Acceptance**: `argo lint --offline` passes; manual run for 31TDH completes; GeoTIFFs land at `s3://{bucket}/s1tiling-output/31TDH/descending/{date}/`.
> **OPEN 7.2a — output→S3 path**: OTB image has no aws CLI. Decided design: S1Tiling writes `data_out`/`data_gamma_area` to a **second RWX csi-rclone mount** pointing at the staging output prefix (so no aws/s3fs needed in OTB image); intermediate `data_raw`/`tmp` stay on emptyDir. Confirm rclone-mount write perf is acceptable for S1Tiling I/O, else fall back to a 2-step DAG (OTB → RWX PVC → data-pipeline image syncs via s3fs).

### Task 6 — ingest+register WorkflowTemplate (after 7)  ready-pending-commit
**What**: `eopf-explorer-ingest-v1-s1rtc-template.yaml` — 2 steps using the data-pipeline image:
`ingest` (`scripts/ingest_v1_s1_rtc.py`, no retry on exit 2) → `register-stac` (`scripts/register_v1_s1_rtc.py`).
Store derived `s3://{s3_output_bucket}/{collection}/s1-grd-rtc-{tile}.zarr`. Add semaphore key `v1-s1rtc-limit` to `eopf-workflow-concurrency` configmap. Defaults: `collection=sentinel-1-grd-rtc-staging`, `s3_output_bucket=esa-zarr-sentinel-explorer-s1-l1grd-staging`. Unblocked now (ingest is container-native).

---

## Open questions
1. **CDSE SealedSecret** — owner Emmanuel. Name/keys (`cdse-eodag-credentials` w/ `eodag.yml`?). HARD BLOCKER for 7.2.
2. **csi-rclone secret convention** (7a.3) — owner Emmanuel. PVC-name-keyed vs static-PV.
3. **DEM upload target bucket** (7a.1) — tests bucket (laptop writable) vs S1 staging (in-cluster job).
4. **rclone-mount write perf for S1Tiling output** (7.2a) — direct mount vs 2-step sync.
5. **flux apply path** — confirm flux Kustomization picks up new files under `workspaces/devseed-staging/data-pipeline/` (no local `kustomization.yaml` found).

## Done definition
S1Tiling WorkflowTemplate runs in-cluster for 31TDH → GeoTIFFs on staging bucket → ingest+register
template produces `s1-rtc-31TDH` in `sentinel-1-grd-rtc-staging` with a store in the S1 staging bucket.
