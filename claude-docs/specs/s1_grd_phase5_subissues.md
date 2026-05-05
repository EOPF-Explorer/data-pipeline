# Phase 5 Sub-Issues — S1 GRD RTC Local Prototype → Argo Pipeline

Parent issue: https://github.com/EOPF-Explorer/data-pipeline/issues/185

---

## Architecture overview

The specs define two independent Argo workflows coupled via S3. The local prototype is
two Python scripts covering the same logical inputs as those workflows. Once the local
scripts work end-to-end, the Argo YAML is a mechanical translation.

Local-only args (`--eodag-cfg`, `--dem-dir`, `--data-dir`, `--cfg`) have no
Argo equivalent — those are handled by mounted secrets and ConfigMaps in the cluster.

```
LOCAL                                ARGO (future)
─────────────────────────────────    ──────────────────────────────────
scripts/run_s1tiling.py          →   WorkflowTemplate eopf-explorer-s1tiling
  --tile-id         31TCH              tile_id:         31TCH
  --orbit-direction descending         orbit_direction: descending
  --date-start      2025-02-01         date_start:      2025-02-01
  --date-end        2025-02-14         date_end:        2025-02-14
  --s3-bucket       <bucket>           s3_geotiff_bucket: <bucket>
  --s3-prefix       s1tiling-output    s3_geotiff_prefix: s1tiling-output
  [--eodag-cfg / --dem-dir / ...]      (← local-only; handled by secrets/ConfigMap in Argo)
        │                                      │
        │ S3 GeoTIFF prefix (handoff)           │ S3 GeoTIFF prefix (handoff)
        ▼                                      ▼
scripts/run_ingest_register.py   →   WorkflowTemplate eopf-explorer-ingest-v1-s1rtc
  --s3-geotiff-prefix <prefix>         s3_geotiff_prefix: <prefix>
  --tile-id           31TCH            tile_id:           31TCH
  --orbit-direction   descending       orbit_direction:   descending
  --collection        sentinel-1-…     collection:        sentinel-1-grd-rtc-staging
  --s3-output-bucket  <bucket>         s3_output_bucket:  <bucket>
  --s3-output-prefix  s1-rtc-test      s3_output_prefix:  s1-rtc-staging
  --stac-api-url      <url>            stac_api_url:      …
  --raster-api-url    <url>            raster_api_url:    …
```

Zarr store path is derived (never passed explicitly): `s3://{s3_output_bucket}/{s3_output_prefix}/s1-grd-rtc-{tile_id}.zarr`

`run_ingest_register.py` calls `ingest_v1_s1_rtc.py` then `register_v1_s1_rtc.py` in
sequence, respecting exit code 2 (empty → skip). No logic of its own.

`watch_cdse_and_process.py` sits above both: queries CDSE for new S1 GRD products and
calls `run_s1tiling.py` → `run_ingest_register.py` for each new one.

---

## Dependency map

```
[Prerequisites]  CDSE account + EODAG creds + DEM tiles + Docker pull + config + data-model unblock
      │
      ├─── Sub-issue A  run_s1tiling.py (local Workflow 1 sim)
      │          │
      │          └────────────────────────────────┐
      │                                           │
      ├─── Sub-issue 1  data-model STAC builder   │
      ├─── Sub-issue 2  ingest_v1_s1_rtc.py       │
      ├─── Sub-issue 5  collection JSON            │
      │         │                                 │
      │    Sub-issue 3  register_v1_s1_rtc.py     │
      │         │                                 │
      └─────────┴──────────────────────────────────┘
                              │
                        Sub-issue B
                        run_ingest_register.py
                        (local Workflow 2 sim)
                              │
                        Sub-issue 4
                        End-to-end validation
                        (run A → B for tile 31TCH)
                              │
               ┌──────────────┴──────────────┐
               │                             │
         Sub-issue 6                   Sub-issue 7a (DEM PVC) ──► Sub-issue 7
         Argo ingest template          [can start now]              Argo s1tiling template
               │                                                          │
               └──────────────────────────┬───────────────────────────────┘
                                    Sub-issue 8 (cron + sensor)
                                    Sub-issue 9 (configmap)

Sub-issue 10  watch_cdse_and_process.py  ← needs A + B; independent from Argo work
```

**Critical path**: Prerequisites → A and (1, 2, 5 start immediately in parallel) → 3 → B → 4 → 6 → 8

Sub-issues 1, 2, 5 are in different repos (data-model / data-pipeline) with no dependency
on the local Docker setup. They can — and should — be started from day one in parallel
with Sub-issue A.

---

## Prerequisites — environment setup (no code, do first)

**P1 — CDSE account + EODAG credentials**

Use a regular **human CDSE account** (email + password); this is the only supported credential
type now that the keycloak patch has been removed.

1. Create account at https://dataspace.copernicus.eu
2. Fill `~/Downloads/eodag-empty.yml` (from Emmanuel) — **never commit with credentials**:
   ```yaml
   cop_dataspace:
     priority: 1
     auth:
       credentials:
         username: <your-email>
         password: <your-CDSE-password>
   ```
3. Store as `~/.config/eodag/eodag.yml` **and** copy/symlink to `$S1T_WORKDIR/config/eodag.yml`:
   ```bash
   mkdir -p "$S1T_WORKDIR/config"
   ln -sf ~/.config/eodag/eodag.yml "$S1T_WORKDIR/config/eodag.yml"
   ```
4. Smoke-test credentials — two checks:

   **4a.** Confirm `eodag search` returns ≥ 1 S1 GRD product:
```bash
uvx eodag search -p cop_dataspace -c S1_SAR_GRD \
  -s 2025-02-01 -e 2025-02-28 \
  --box 0 42 2 43 \
  --limit 5
```

   **4b.** Confirm a raw token can be obtained (catches YAML parsing issues):
   ```bash
   uv run python3 -c '
   import json, os, urllib.error, urllib.parse, urllib.request, yaml
   path = os.path.expandvars("$S1T_WORKDIR/config/eodag.yml")
   c = yaml.safe_load(open(path))["cop_dataspace"]["auth"]["credentials"]
   data = {"grant_type": "password", "client_id": "cdse-public",
           "username": c["username"], "password": c["password"]}
   req = urllib.request.Request(
       "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
       data=urllib.parse.urlencode(data).encode(), method="POST",
   )
   try:
       r = json.loads(urllib.request.urlopen(req).read())
       print("OK — token received" if "access_token" in r else r)
   except urllib.error.HTTPError as e:
       print(e.code, e.reason); print(e.read().decode(errors="replace"))
   '
   ```
   Expected output: `OK — token received`

**P2 — Workdir layout + DEM tiles for 31TCH swath**

**Do:** match the docker instructions layout so bind mounts line up:

```bash
export S1T_WORKDIR="${S1T_WORKDIR:-$HOME/s1tiling}"
mkdir -p "$S1T_WORKDIR"/{data_out,data_raw,data_gamma_area,tmp,eof,config}
mkdir -p "$S1T_WORKDIR"/DEM/COP_DEM_GLO30
mkdir -p "$S1T_WORKDIR"/DEM/dem_db
mkdir -p "$S1T_WORKDIR"/geoid
# Persist across shell sessions (run once):
echo 'export S1T_WORKDIR="$HOME/s1tiling"' >> ~/.zshrc
```

S1Tiling needs Copernicus DEM GLO-30 COG GeoTIFF tiles covering the **full S1 IW swath** —
not just the MGRS tile. For 31TCH the swath extends to 41–44°N, 3°W–5°E (Phase 0 finding):
~24 tiles. Copernicus DEM is open access — no authentication required.

**Task**: download ~24 Copernicus DEM COG tiles covering the 31TCH swath (41–44°N, 3°W–5°E)
to `$S1T_WORKDIR/DEM/COP_DEM_GLO30/` (mounted in Docker at `/MNT/COP_DEM_GLO30`).

```bash
# Download ~24 Copernicus DEM COG tiles covering the 31TCH swath (41–44°N, 3°W–5°E)
# Uses eodag with the earth_search provider (Element84 / AWS) — no authentication required
mkdir -p "$S1T_WORKDIR/DEM/COP_DEM_GLO30"
cd "$S1T_WORKDIR/DEM/COP_DEM_GLO30"

# Step 1: search and save results (earth_search uses public AWS S3, no credentials needed)

uvx eodag@4.1.0 search -p earth_search -c COP_DEM_GLO30_DGED \
  --box -3 41 5 44 \
  --all \
  --storage dem_search

# Creates dem_search.geojson in current directory

# Step 2: download tiles via HTTPS
# (earth_search assets have s3:// hrefs — eodag download requires boto3 anonymous S3 access,
#  which is not set up by default. Convert to HTTPS instead; the bucket is public.)
# (still in $S1T_WORKDIR/DEM/COP_DEM_GLO30)
python3 <<'PYEOF'
import json, os, time, urllib.request, urllib.error
features = json.load(open("dem_search.geojson"))["features"]
for f in features:
    href = f["assets"]["tif"]["href"]
    url = href.replace("s3://copernicus-dem-30m/", "https://copernicus-dem-30m.s3.amazonaws.com/")
    name = os.path.basename(url)
    if os.path.exists(name) and os.path.getsize(name) > 0:
        print(f"Skipping {name} (already exists)")
        continue
    for attempt in range(1, 4):
        try:
            print(f"Downloading {name} (attempt {attempt}) ...")
            urllib.request.urlretrieve(url, name)
            break
        except (urllib.error.URLError, OSError) as e:
            print(f"  WARN attempt {attempt} failed: {e}")
            if attempt < 3:
                time.sleep(2 ** attempt)
            else:
                print(f"  SKIP {name} after 3 failures")
PYEOF

# Step 3: clean up and rename to match eotile's Product10 column convention
#   Copernicus_DSM_COG_10_N41_00_W003_00_DEM.tif → Copernicus_DSM_10_N41_00_W003_00.tif
# First: remove any 0-byte .tif files left by failed wget downloads (wget creates empty
# files on 404 before the || catches the error)
find . -name "*.tif" -empty -delete
# Then: rename valid COG_ files (skip if already renamed)
# (setopt nullglob prevents zsh error when no COG_ files remain)
setopt nullglob 2>/dev/null; for f in Copernicus_DSM_COG_*.tif; do
    [ -s "$f" ] || continue
    newname=$(echo "$f" | sed 's/Copernicus_DSM_COG_/Copernicus_DSM_/' | sed 's/_DEM\.tif$/.tif/')
    mv "$f" "$newname"
done
echo "Tiles after rename: $(ls Copernicus_DSM_10_*.tif 2>/dev/null | wc -l)"
```

> **Rename required**: eodag (via AWS) delivers files with `COG_` and `_DEM` in the name, but
> eotile's `Product10` column contains `Copernicus_DSM_10_N41_00_E000_00` (no `COG_`, no `_DEM`).
> The rename above makes local filenames match what `dem_format = {Product10}.tif` resolves to.

> **Why `earth_search` and not `cop_dataspace`?**
> - `cop_dataspace` does **not** include `COP_DEM_GLO30_DGED` as a product type in eodag.
> - The CDSE STAC browser (`browser.stac.dataspace.copernicus.eu`) shows the collection exists,
>   but its S3 storage is either CDSE-authenticated or CREODIAS requester-pays — no free access.
> - `earth_search` (Element84) indexes the same tiles via public AWS S3
>   (`s3://copernicus-dem-30m`, `requester_pays: false`) — no credentials needed.

**Add: obtain eotile GeoPackage database**
S1-Tiling needs a GeoPackage (`dem_database`) that maps tile geometries to filenames. The `eotile` Python package bundles `DEM_Union.gpkg` for this purpose.

```bash
# On the host (not inside Docker)
# Install eotile data-only (--no-deps avoids building fiona/GDAL from source)
uv pip install eotile --no-deps --target /tmp/eotile_data -q
python3 -c "
import pathlib, shutil, os
gpkg = next(pathlib.Path('/tmp/eotile_data').rglob('DEM_Union.gpkg'))
dest = pathlib.Path(os.path.expandvars('\$S1T_WORKDIR/DEM/dem_db/DEM_Union.gpkg'))
dest.parent.mkdir(parents=True, exist_ok=True)
shutil.copy(gpkg, dest)
print('Copied:', gpkg)
"
```

> **Confirmed complete**: `DEM_Union.gpkg` populated — 17 MB, 26 470 entries (2026-05-01).
> Note: `N41E004` and `N42E004` tiles are absent from the GPKG (see Known GPKG gap below).

> **Verify**: run the script below — uses stdlib `sqlite3` (no GDAL required):

```bash
python3 <<'PYEOF'
import sqlite3, pathlib, os

gpkg = pathlib.Path(os.path.expandvars("$S1T_WORKDIR/DEM/dem_db/DEM_Union.gpkg"))
dem_dir = pathlib.Path(os.path.expandvars("$S1T_WORKDIR/DEM/COP_DEM_GLO30"))

con = sqlite3.connect(gpkg)

# Find the DEM table (any table with a Product10 column)
tables = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")]
dem_table = next((t for t in tables
                  if "Product10" in [c[1] for c in con.execute(f"PRAGMA table_info({t})")]), None)
if not dem_table:
    raise RuntimeError(f"No table with Product10 column found in {gpkg}. Tables: {tables}")
print(f"Found table: {dem_table}")

gpkg_products = {r[0] for r in con.execute(f"SELECT Product10 FROM {dem_table}") if r[0]}
local_stems   = {p.stem for p in dem_dir.glob("Copernicus_DSM_10_*.tif")}

missing_from_gpkg  = local_stems - gpkg_products
missing_from_local = gpkg_products - local_stems

print(f"GPKG entries : {len(gpkg_products)}")
print(f"Local tiles  : {len(local_stems)}")
if local_stems:
    if missing_from_gpkg:
        print(f"WARNING — local files not in GPKG ({len(missing_from_gpkg)}): {sorted(missing_from_gpkg)[:5]} ...")
    if missing_from_local:
        print(f"INFO    — GPKG entries with no local tile ({len(missing_from_local)}): {sorted(missing_from_local)[:5]} ...")
    if not missing_from_gpkg:
        print("OK: all local tiles have a matching Product10 entry in DEM_Union.gpkg")
else:
    print("ERROR: no local tiles found — download or rename step not done yet")
PYEOF
```
>
> **Known GPKG gap**: `N41E004` and `N42E004` are absent from eotile's `DEM_Union.gpkg`
> (confirmed by inspection). S1-Tiling may skip DEM lookup for those cells. Confirm with
> Emmanuel whether this affects the 31TCH swath before populating the PVC (Sub-issue 7a).

**Add: obtain EGM2008 geoid file**
S1-Tiling's `resources/Geoid/` directory ships only `egm96.grd` — **no EGM2008 file**.
Emmanuel's reference config also still uses `egm96.grd` (written for SRTM).
**Copernicus DEM requires EGM2008** (S1-Tiling docs: *"Make sure to use an EGM2008 model for Copernicus DEM files."*).

**Step 1 — check whether OTB bundles EGM2008 inside the Docker image** (run with Docker available):

```bash
docker run --rm --entrypoint find \
  registry.orfeo-toolbox.org/s1-tiling/s1tiling:1.4.0-ubuntu-otb9.1.1 \
  /usr /opt -name "egm2008*" 2>/dev/null
```

**Confirmed: NOT in the 1.4.0 image** — `find` returns nothing. The bind-mount path is
required.

The OTB superbuild `.grd` format is a big-endian binary grid:
6× float32 header `[lat_min, lat_max, lon_min, lon_max, dlat, dlon]` followed by float32
geoid undulations in north→south, west→east order.

**Conversion from GeographicLib EGM2008 PGM** (no OTB superbuild archive needed):
```bash
# 1. Download 1-arcmin EGM2008 PGM from GeographicLib (~445 MB):
#    https://sourceforge.net/projects/geographiclib/files/geoids-distrib/egm2008-1.zip/download
#    Unzip to ~/Downloads/geoids/egm2008-1.pgm

# 2. Convert to OTB .grd at 15-arcmin resolution (same as bundled egm96.grd, ~4 MB):
uv run python scripts/convert_egm2008_pgm_to_grd.py \
    --pgm ~/Downloads/geoids/egm2008-1.pgm \
    --out "$S1T_WORKDIR/geoid/egm2008.grd" \
    --step 15 \
    --egm96-grd /path/to/egm96.grd   # optional size check
```

`scripts/convert_egm2008_pgm_to_grd.py` is in this repo. It validates file size and
5 spot-check values against the source PGM (all diff = 0.000 m at ±0.001 m tolerance).

> **Confirmed complete** (2026-05-01): `$S1T_WORKDIR/geoid/egm2008.grd` present — 4 155 868 bytes,
> all validations pass.

**Step 2 — bind-mount required**:
```bash
mkdir -p "$S1T_WORKDIR/geoid"
# file already at $S1T_WORKDIR/geoid/egm2008.grd after conversion above
```

**Smoke-test** (expect ≥ 20 files):
```bash
ls -1 "$S1T_WORKDIR/DEM/COP_DEM_GLO30"/*.tif | wc -l
test -f "$S1T_WORKDIR/DEM/dem_db/DEM_Union.gpkg" && echo "OK: DEM_Union.gpkg present"
test -f "$S1T_WORKDIR/geoid/egm2008.grd" && echo "OK: EGM2008 geoid present"
```

**P3 — S1Tiling Docker image + EODAG patch file**

**Image:** `registry.orfeo-toolbox.org/s1-tiling/s1tiling:1.4.0-ubuntu-otb9.1.1` — this is the reference image. Per upstream, it ships EODAG 4.x and needs the patch for `cop_dataspace`.

> **Note on `cnes/s1tiling:1.4.1-ubuntu-otb9.1.1`**: this image has been pulled locally but
> ships **eodag 3.10.2** (older than 4.0.0). It is NOT the reference image for this spec.
> Use the OTB registry 1.4.0 image for all local and Argo runs.

Two tasks: (a) pull the image, (b) copy the EODAG 4.0 patch file into the repo.

**(a) Pull image:**
```bash
docker pull registry.orfeo-toolbox.org/s1-tiling/s1tiling:1.4.0-ubuntu-otb9.1.1
```

Check the EODAG version bundled in the image — this determines whether the patch is already
applied or still needed at runtime. The image uses `S1Processor` as `ENTRYPOINT`, so override
it to run Python (on Apple Silicon, add `--platform linux/amd64` if the image is amd64-only):
```bash
docker run --rm --entrypoint python \
  registry.orfeo-toolbox.org/s1-tiling/s1tiling:1.4.0-ubuntu-otb9.1.1 \
  -c "import eodag; print(eodag.__version__)"
# Confirmed: 4.0.0 in the 1.4.0 image. Patch injected unconditionally is a safe no-op.
# Confirm with Emmanuel whether injection can be removed (only once he confirms it's merged upstream).
```

**(b) Copy [`s1tiling_eodag4_patch.py`](https://github.com/EOPF-Explorer/data-model/blob/s1-tiling/analysis/s1tiling_eodag4_patch.py) into this repo and commit:**

```bash
mkdir -p analysis
wget -qO analysis/s1tiling_eodag4_patch.py \
  https://raw.githubusercontent.com/EOPF-Explorer/data-model/s1-tiling/analysis/s1tiling_eodag4_patch.py
git add analysis/s1tiling_eodag4_patch.py
git commit -m "chore: add S1Tiling EODAG 4.0 compatibility patch" --no-verify
```

**Smoke-test** — verify the patch loads without error inside the image:
```bash
docker run --rm --entrypoint python \
  -v "$(pwd)/analysis/s1tiling_eodag4_patch.py:/patches/eodag_patch.py:ro" \
  -e PYTHONSTARTUP=/patches/eodag_patch.py \
  registry.orfeo-toolbox.org/s1-tiling/s1tiling:1.4.0-ubuntu-otb9.1.1 \
  -c "print('patch OK')"
```

The patch is injected unconditionally by Script A at runtime — applying an already-applied
patch is a no-op. Remove the injection only after Emmanuel confirms it is merged upstream.

**P4 — `S1GRD_RTC.cfg` from Emmanuel**

Use the **`S1GRD_RTC.cfg` content Emmanuel validated** — either the file he shares directly,
or the canonical ini in [§4 of the docker instructions](https://github.com/EOPF-Explorer/data-model/blob/s1-tiling/analysis/s1tiling_docker_instructions.md)
(should match). Do **not** maintain a separate placeholder template unless you later need
parameterized runs.

**Task**:
1. Save it as **`$S1T_WORKDIR/config/S1GRD_RTC.cfg`**. With `$S1T_WORKDIR` mounted at
   `/data`, `S1Processor` reads `/data/config/S1GRD_RTC.cfg`. Key DEM/geoid paths in the
   file (Copernicus DEM keys):
   - `dem_dir      : /MNT/COP_DEM_GLO30`
   - `dem_database : /MNT/dem_db/DEM_Union.gpkg`
   - `dem_format   : {Product10}.tif`
   - `dem_info     : CopDEM GLO-30 30m`
   - `geoid_file   : /MNT/geoid/egm2008.grd` (bind-mounted from host — see P2 EGM2008 step;
     may resolve to an in-image OTB path if found there instead)
   - `eodag_config : /eo_config/eodag.yml`
2. **Optional but useful:** copy the same file into this repo as `config/S1GRD_RTC.cfg` and
   commit so `run_s1tiling.py` and reviewers share one pinned snapshot. When Emmanuel updates
   his file, replace and commit again.

```bash
mkdir -p "$S1T_WORKDIR/config"
cp ~/Downloads/S1GRD_RTC.cfg "$S1T_WORKDIR/config/S1GRD_RTC.cfg"
# or paste §4 into that path
mkdir -p config
# optional: cp "$S1T_WORKDIR/config/S1GRD_RTC.cfg" config/S1GRD_RTC.cfg
# git add config/S1GRD_RTC.cfg && git commit -m "chore: add S1GRD RTC config (Emmanuel reference)"
```

**Smoke-test** — `S1Tiling` reads **`$S1T_WORKDIR/config/S1GRD_RTC.cfg`**, not
`config/S1GRD_RTC.cfg` in the repo. If you only committed the latter, sync first:
`mkdir -p "$S1T_WORKDIR/config" && cp config/S1GRD_RTC.cfg "$S1T_WORKDIR/config/"`

```bash
test -f "$S1T_WORKDIR/config/S1GRD_RTC.cfg" \
  && grep -q '^\[Paths\]' "$S1T_WORKDIR/config/S1GRD_RTC.cfg" \
  && grep -q 'gamma_naught_rtc' "$S1T_WORKDIR/config/S1GRD_RTC.cfg" \
  && grep -q 'COP_DEM_GLO30' "$S1T_WORKDIR/config/S1GRD_RTC.cfg" \
  && grep -q 'egm2008' "$S1T_WORKDIR/config/S1GRD_RTC.cfg" \
  && echo "OK: S1GRD_RTC.cfg in workdir"
```

**P5 — Test S3 bucket + awscli**

Test bucket: **`esa-zarr-sentinel-explorer-tests`** (separate from production `esa-zarr-sentinel-explorer-fra`).

**Task 1** — install awscli v2 if not present:
```bash
aws --version   # must show aws-cli/2.x; install via: brew install awscli
```

**Task 2** — obtain OVH S3 credentials (ask Emmanuel/team for the `esa-zarr-sentinel-explorer-tests` access key and secret). Then configure:
```bash
aws configure --profile eopfexplorer
# AWS Access Key ID:     <ovh-access-key>
# AWS Secret Access Key: <ovh-secret-key>
# Default region name:   de           (or leave blank)
# Default output format: json
```

Or set as env vars for the session:
```bash
export AWS_ACCESS_KEY_ID=<ovh-access-key>
export AWS_SECRET_ACCESS_KEY=<ovh-secret-key>
```

**Smoke-test** — verify bucket is accessible before starting Sub-issue A:
```bash
aws s3 ls s3://esa-zarr-sentinel-explorer-tests/ \
  --endpoint-url https://s3.de.io.cloud.ovh.net --profile eopfexplorer
# expect: list of prefixes or empty output (no "Access Denied")
```

**P6 — Unblock data-model ingestion functions (prerequisite for Sub-issue 2)**

**Do:** Ask Emmanuel which data-model tag contains Phase 2–3 (`s1_ingest` module). Bump
`pyproject.toml` to pin it and test:
```bash
uv run python -c "from eopf_geozarr.conversion import s1_ingest; print('ok')"
```

> Current installed version is v0.9.0 — `s1_ingest` is absent from it.

---

## Sub-issue A — `scripts/run_s1tiling.py` (local Workflow 1 simulation)

**Repo**: `EOPF-Explorer/data-pipeline`

**What it is**: Thin script covering the same logical inputs as Argo Workflow 1
(`eopf-explorer-s1tiling`). Runs S1Tiling in Docker locally and uploads GeoTIFFs to S3.
No logic beyond config templating, one Docker call, and one S3 sync.

**Interface**:

```bash
uv run python scripts/run_s1tiling.py \
  --tile-id          31TCH \
  --orbit-direction  descending \
  --date-start       2025-02-01 \
  --date-end         2025-02-14 \
  --s3-bucket        esa-zarr-sentinel-explorer-tests \
  --s3-prefix        s1tiling-output \
  --s3-endpoint      https://s3.de.io.cloud.ovh.net \
  --eodag-cfg        "$S1T_WORKDIR/config/eodag.yml" \
  --dem-dir          "$S1T_WORKDIR/DEM/COP_DEM_GLO30" \
  --data-dir         "$S1T_WORKDIR" \
  --cfg              config/S1GRD_RTC.cfg \
  [--dry-run]
```

**Output**: prints the S3 prefix where GeoTIFFs were written — passed as `--s3-geotiff-prefix` to `run_ingest_register.py`.

**Behaviour** (≤ 60 lines of logic):

```
1. Ensure $S1T_WORKDIR/config/S1GRD_RTC.cfg matches the --cfg source (copy if needed)
2. docker run \
     --rm --platform linux/amd64 \
     --entrypoint bash \
     -v {abs_data_dir}:/data \
     -v {abs_dem_dir}:/MNT/COP_DEM_GLO30 \
     -v {abs_dem_db}:/MNT/dem_db:ro \
     -v {abs_geoid_dir}:/MNT/geoid:ro \
     -v {abs_eodag_cfg}:/eo_config/eodag.yml:ro \
     -v {abs_patch_dir}:/patch:ro \
     registry.orfeo-toolbox.org/s1-tiling/s1tiling:1.4.0-ubuntu-otb9.1.1 \
     -c 'python3 /patch/s1tiling_eodag4_patch.py && S1Processor /data/config/S1GRD_RTC.cfg'
   # IMPORTANT: docker options (--entrypoint, -v) must come BEFORE the image name.
   # No -e flags needed: PATH/OTB_*/GDAL_* are baked into the image; EODAG config
   # path is set via eodag_config in S1GRD_RTC.cfg (-> /eo_config/eodag.yml).
   # All mount paths must be absolute — use os.path.abspath() to expand ~ and relative refs.
   # Patch injected unconditionally — safe whether or not it's already in the image (see P3)
   # abs_dem_dir    = os.path.abspath(f"{data_dir}/DEM/COP_DEM_GLO30")
   # abs_dem_db     = os.path.abspath(f"{data_dir}/DEM/dem_db")
   # abs_geoid_dir  = os.path.abspath(f"{data_dir}/geoid")
3. On success: aws s3 sync {abs_data_dir}/data_out/{tile_id}/ \
                 s3://{bucket}/{prefix}/{tile_id}/{orbit}/{date_start}/ \
                 --endpoint-url {s3_endpoint} --profile eopfexplorer
              aws s3 sync {abs_data_dir}/data_gamma_area/ \
                 s3://{bucket}/{prefix}/{tile_id}/{orbit}/{date_start}/ \
                 --endpoint-url {s3_endpoint} --profile eopfexplorer
              # ↑ conditions synced INTO the same prefix as acquisitions so
              #   discover_s1tiling_conditions(prefix) finds them (see C1 fix)
4. Print: s3://{bucket}/{prefix}/{tile_id}/{orbit}/{date_start}/
```

where `{orbit}` is the lowercase full word (`descending`/`ascending`) — consistent with
the Argo output path convention and `ingest_v1_s1_rtc.py`'s expected prefix format.

**Expected S3 output structure** (everything under the same date prefix):

```
s3://esa-zarr-sentinel-explorer-tests/s1tiling-output/31TCH/descending/2025-02-01/
  s1a_31TCH_vv_DES_037_20250205T062921_GammaNaughtRTC.tif
  s1a_31TCH_vh_DES_037_20250205T062921_GammaNaughtRTC.tif
  s1a_31TCH_BorderMask_DES_037_20250205T062921.tif
  GAMMA_AREA_s1a_31TCH_DES_008.tif      ← conditions co-located with acquisitions
```

**Acceptance criteria**:
- [ ] `--dry-run` prints Docker command and S3 sync commands without executing
- [ ] Docker run completes; GeoTIFFs and GAMMA_AREA tif present locally
- [ ] All files present under the same S3 prefix; GeoTIFFs readable with rasterio (10980×10980)
- [ ] Script exits non-zero if Docker fails

**Depends on**: Prerequisites P1–P6
**Blocks**: Sub-issue 4, Sub-issue 10

---

## Sub-issue B — `scripts/run_ingest_register.py` (local Workflow 2 simulation)

**Repo**: `EOPF-Explorer/data-pipeline`

**What it is**: Thin orchestrator covering the same logical inputs as Argo Workflow 2
(`eopf-explorer-ingest-v1-s1rtc`). Calls `ingest_v1_s1_rtc.py` then
`register_v1_s1_rtc.py` in sequence. No logic of its own — just wiring.

**Interface**:

```bash
uv run python scripts/run_ingest_register.py \
  --s3-geotiff-prefix  s3://esa-zarr-sentinel-explorer-tests/s1tiling-output/31TCH/descending/2025-02-01/ \
  --tile-id            31TCH \
  --orbit-direction    descending \
  --collection         sentinel-1-grd-rtc-staging \
  --s3-output-bucket   esa-zarr-sentinel-explorer-tests \
  --s3-output-prefix   s1-rtc-test \
  --s3-endpoint        https://s3.de.io.cloud.ovh.net \
  --stac-api-url       https://api.explorer.eopf.copernicus.eu/stac \
  --raster-api-url     https://api.explorer.eopf.copernicus.eu/raster
```

Zarr store path is derived internally: `s3://{s3_output_bucket}/{s3_output_prefix}/s1-grd-rtc-{tile_id}.zarr`.
This matches the canonical convention in the specs and the Argo template — it is never
passed as an explicit argument.

**Behaviour** (≤ 40 lines of logic):

```
zarr_store = f"s3://{s3_output_bucket}/{s3_output_prefix}/s1-grd-rtc-{tile_id}.zarr"

Step 1 — ingest:
  result = subprocess.run(["uv", "run", "python", "scripts/ingest_v1_s1_rtc.py",
                           "--s3-geotiff-prefix", s3_geotiff_prefix,
                           "--s3-zarr-store", zarr_store,
                           "--tile-id", tile_id,
                           "--orbit-direction", orbit_direction])
  # subprocess.run() returns CompletedProcess — check .returncode, not the object itself
  if result.returncode == 2:
      log "no acquisitions found — skipping register"
      # Exit 0 locally so the watcher continues to the next product.
      # In Argo, exit 2 propagates to the retry policy directly; the
      # behaviour is equivalent but the mechanism differs.
      sys.exit(0)
  if result.returncode != 0: sys.exit(result.returncode)

Step 2 — register-stac (only reached if step 1 exited 0):
  result = subprocess.run(["uv", "run", "python", "scripts/register_v1_s1_rtc.py",
                           "--store", zarr_store,
                           "--collection", collection,
                           "--stac-api-url", stac_api_url,
                           "--raster-api-url", raster_api_url,
                           "--s3-endpoint", s3_endpoint,
                           "--s3-output-bucket", s3_output_bucket,
                           "--s3-output-prefix", s3_output_prefix])
  sys.exit(result.returncode)
```

**Acceptance criteria**:
- [ ] With real GeoTIFFs from Sub-issue A: step 1 ingests, step 2 registers, exits 0
- [ ] With an empty S3 prefix: step 1 exits 2, script logs "skipping" and exits 0
- [ ] If ingest fails (exit 1): register is not called, script exits 1
- [ ] Item `s1-rtc-31TCH` queryable at the staging STAC API after a successful run

**Depends on**: Sub-issues 2, 3, 5
**Blocks**: Sub-issue 4, Sub-issue 10

---

## Sub-issue 1 — [data-model] STAC item builder (`build_s1_rtc_stac_item`)

**Repo**: `EOPF-Explorer/data-model`, branch `s1-tiling`
(If the branch does not yet exist, create it from `main` as the first step.)

**New file**: `src/eopf_geozarr/stac/s1_rtc.py`

```python
def build_s1_rtc_stac_item(zarr_store: str, collection_id: str) -> pystac.Item:
    ...
```

- Opens Zarr via consolidated metadata (single request, no full scan)
- `tile_id` derived from store basename `s1-grd-rtc-{tile_id}.zarr`
- For each orbit direction (`ascending`, `descending`): UTM bbox → WGS84 via pyproj, `time` range, `platform`
- Item `id`: `s1-rtc-{tile_id}`; `datetime`: null; `start_datetime`/`end_datetime` = min/max across orbits
- Assets: `zarr-store`, `vv`, `vh` (ascending preferred, fallback descending)
- STAC extensions: `sar`, `sat`, `projection`

**Also**: add `generate-stac-s1` subcommand to `src/eopf_geozarr/cli.py`.

**Tests**: `tests/test_s1_stac.py`, 8 tests minimum (roundtrip, temporal, bbox, both orbits,
ascending-only, empty store → `ValueError`, asset subpaths, SAR extension fields).

**Acceptance criteria**:
- [ ] All 8 tests pass with synthetic Zarr fixture in `tmp_path`
- [ ] Pre-commit passes (ruff, mypy)
- [ ] New version tag published (e.g. `v0.10.0`)
- [ ] `pyproject.toml` in `data-pipeline` bumped to pin this tag (blocks Sub-issue 3 otherwise)

**Depends on**: nothing — start immediately
**Blocks**: Sub-issue 3, Sub-issue B

---

## Sub-issue 2 — [data-pipeline] `scripts/ingest_v1_s1_rtc.py`

**Repo**: `EOPF-Explorer/data-pipeline`

Discovers S1Tiling GeoTIFFs from an S3 prefix, appends to the per-tile Zarr, consolidates.
Imports from `eopf_geozarr.conversion.s1_ingest` — no subprocess calls.
Data-model Phases 2–3 (ingestion + conditions code) are already done in the data-model repo
but not yet released. The installed version is v0.9.0; `s1_ingest` is not present in it.

> **Unblocking step**: before `ingest_v1_s1_rtc.py` can run, confirm with Emmanuel which
> data-model tag includes Phase 2–3 code, bump `pyproject.toml` to pin it, and `uv pip install`
> the updated package. This is separate from the v0.10.0 STAC-builder bump in Sub-issue 1.

**Interface**:
```bash
uv run python scripts/ingest_v1_s1_rtc.py \
  --s3-geotiff-prefix  s3://esa-zarr-sentinel-explorer-tests/s1tiling-output/31TCH/descending/2025-02-01/ \
  --s3-zarr-store      s3://esa-zarr-sentinel-explorer-tests/s1-rtc-test/s1-grd-rtc-31TCH.zarr \
  --tile-id            31TCH \
  --orbit-direction    descending
```

**Behaviour**:
1. `discover_s1tiling_acquisitions(prefix)` — if empty: log + **exit 2** (clean skip)
2. For each acquisition: `ingest_s1tiling_acquisition(...)` — fail-fast on error (exit 1)
3. `discover_s1tiling_conditions(prefix)` — same prefix; GAMMA_AREA files co-located (see Sub-issue A); may be empty, non-fatal
4. For each condition group: `ingest_s1tiling_conditions(...)`
5. `consolidate_s1_store(zarr_store)`

**Exit codes**: 0 = success, 1 = error, 2 = no acquisitions found (Argo: no retry on 2)

**Tests**: `tests/test_ingest_v1_s1_rtc.py` — synthetic GeoTIFFs + GAMMA_AREA → local Zarr → `xr.open_zarr()` roundtrip

**Acceptance criteria**:
- [ ] Ingests ≥ 2 synthetic acquisitions; store readable by xarray
- [ ] `discover_s1tiling_conditions` finds GAMMA_AREA files in the same prefix as acquisitions
- [ ] Correct exit codes for all three states
- [ ] CI passes

**Depends on**: nothing — start immediately
**Blocks**: Sub-issue B

---

## Sub-issue 3 — [data-pipeline] `scripts/register_v1_s1_rtc.py`

**Repo**: `EOPF-Explorer/data-pipeline`

Builds STAC item from Zarr, augments with visualization links, upserts to STAC API.
Reuses helpers from `scripts/register_v1.py` — import, not copy.

> **Import path**: `register_v1.py` lives in `scripts/` and is not installed as a package.
> Add `sys.path.insert(0, str(Path(__file__).parent))` at the top of `register_v1_s1_rtc.py`
> so `from register_v1 import upsert_item, add_visualization_links, ...` resolves correctly.

**Interface**:
```bash
uv run python scripts/register_v1_s1_rtc.py \
  --store            s3://esa-zarr-sentinel-explorer-tests/s1-rtc-test/s1-grd-rtc-31TCH.zarr \
  --collection       sentinel-1-grd-rtc-staging \
  --stac-api-url     https://api.explorer.eopf.copernicus.eu/stac \
  --raster-api-url   https://api.explorer.eopf.copernicus.eu/raster \
  --s3-endpoint      https://s3.de.io.cloud.ovh.net \
  --s3-output-bucket esa-zarr-sentinel-explorer-tests \
  --s3-output-prefix s1-rtc-test
```

**Behaviour** (in order, all from existing `register_v1.py` helpers):
1. `build_s1_rtc_stac_item(store, collection)` — from `eopf_geozarr.stac.s1_rtc`
2. `add_store_link` → `add_alternate_s3_assets` → `add_visualization_links` → `add_thumbnail_asset`
3. `warm_thumbnail_cache`
4. `upsert_item`

Skip `consolidate_reflectance_assets` and `fix_zarr_asset_media_types` (S2-specific).

**Tests**: `tests/test_register_v1_s1_rtc.py` — mock STAC API via `respx`, real item build from synthetic Zarr fixture

**Acceptance criteria**:
- [ ] Upserts item without error; viewer link returns HTTP 200
- [ ] CI passes

**Depends on**: Sub-issue 1 (STAC builder tagged; `pyproject.toml` pin bumped as part of Sub-issue 1)
**Blocks**: Sub-issue B

---

## Sub-issue 4 — End-to-end validation: run Script A → Script B for tile 31TCH

**Repo**: `EOPF-Explorer/data-pipeline`

Run the two local workflow scripts back-to-back, passing Script A's printed S3 prefix
directly into Script B. Proof that all pieces integrate before any Argo YAML is written.

**Run sequence**:

```bash
# 1. Run local Workflow 1 (S1Tiling → GeoTIFFs on S3)
uv run python scripts/run_s1tiling.py \
  --tile-id 31TCH --orbit-direction descending \
  --date-start 2025-02-01 --date-end 2025-02-14 \
  --s3-bucket esa-zarr-sentinel-explorer-tests --s3-prefix s1tiling-output \
  --s3-endpoint https://s3.de.io.cloud.ovh.net \
  --eodag-cfg "$S1T_WORKDIR/config/eodag.yml" \
  --dem-dir "$S1T_WORKDIR/DEM/COP_DEM_GLO30" \
  --data-dir "$S1T_WORKDIR" \
  --cfg config/S1GRD_RTC.cfg

# Script A prints: s3://esa-zarr-sentinel-explorer-tests/s1tiling-output/31TCH/descending/2025-02-01/

# 2. Run local Workflow 2 (ingest + register)
uv run python scripts/run_ingest_register.py \
  --s3-geotiff-prefix s3://esa-zarr-sentinel-explorer-tests/s1tiling-output/31TCH/descending/2025-02-01/ \
  --tile-id 31TCH --orbit-direction descending \
  --collection sentinel-1-grd-rtc-staging \
  --s3-output-bucket esa-zarr-sentinel-explorer-tests --s3-output-prefix s1-rtc-test \
  --s3-endpoint https://s3.de.io.cloud.ovh.net \
  --stac-api-url https://api.explorer.eopf.copernicus.eu/stac \
  --raster-api-url https://api.explorer.eopf.copernicus.eu/raster

# 3. Verify
curl "https://api.explorer.eopf.copernicus.eu/stac/collections/sentinel-1-grd-rtc-staging/items/s1-rtc-31TCH"
```

**Acceptance criteria**:
- [ ] Script A produces GeoTIFFs + GAMMA_AREA under the same S3 prefix
- [ ] Script B ingests ≥ 2 acquisitions; Zarr readable by `xr.open_zarr()`
- [ ] `eopf-geozarr validate-s1` passes on the Zarr store
- [ ] Item `s1-rtc-31TCH` queryable at staging STAC API
- [ ] TiTiler viewer link for `vv` returns HTTP 200
- [ ] Issues reported to Emmanuel

**Depends on**: Sub-issues A, B, 5
**Blocks**: Sub-issues 6, 7

---

## Sub-issue 5 — [data-pipeline] STAC collection `sentinel-1-grd-rtc-staging.json`

**Repo**: `EOPF-Explorer/data-pipeline`
**New file**: `stac/sentinel-1-grd-rtc-staging.json` (model on `stac/sentinel-2-l2a-staging.json`)

Key fields: `id: sentinel-1-grd-rtc-staging`, temporal from `2014-04-03`, global bbox.

Create in staging API once:
```bash
uv run python operator-tools/manage_collections.py create \
  --collection stac/sentinel-1-grd-rtc-staging.json \
  --stac-api-url https://api.explorer.eopf.copernicus.eu/stac
```

**Tests**: add `pystac.Collection.from_file()` + validate to `tests/test_stac_collections.py`.

**Acceptance criteria**: schema-validates; created in staging API.

**Depends on**: nothing — start immediately
**Blocks**: Sub-issue B (register needs collection to exist before first upsert)

---

## Sub-issue 10 — `scripts/watch_cdse_and_process.py` (automated trigger)

**Repo**: `EOPF-Explorer/data-pipeline`

Queries CDSE STAC API for new S1 GRD products and calls Script A → Script B for each
new one. Local equivalent of the Argo CronWorkflow.

**Interface**:
```bash
uv run python scripts/watch_cdse_and_process.py \
  --tiles             31TCH \
  --orbit-direction   descending \
  --lookback-days     7 \
  --s3-bucket         esa-zarr-sentinel-explorer-tests \
  --s3-prefix         s1tiling-output \
  --s3-zarr-bucket    esa-zarr-sentinel-explorer-tests \
  --s3-zarr-prefix    s1-rtc-test \
  --s3-endpoint       https://s3.de.io.cloud.ovh.net \
  --collection        sentinel-1-grd-rtc-staging \
  --stac-api-url      https://api.explorer.eopf.copernicus.eu/stac \
  --raster-api-url    https://api.explorer.eopf.copernicus.eu/raster \
  [--dry-run]
```

**Behaviour**:
```
1. Query CDSE STAC API (catalogue.dataspace.copernicus.eu/stac)
   collection: SENTINEL-1-GRD
   bbox: tile WGS84 bbox  # 31TCH → [0.0, 42.0, 2.0, 43.0] (lon_min, lat_min, lon_max, lat_max)
                           # Derive from MGRS tile ID at runtime using mgrs or s2geometry lib,
                           # or hardcode a per-tile lookup dict for the initial scope (31TCH only)
   datetime: (now - lookback_days) / now

   ⚠️ VERIFY before implementing: confirm correct value of sat:orbit_state filter.
   EODAG 4.0 patch notes say orbit direction must be UPPERCASE ("DESCENDING") when
   passed through EODAG. Unclear if the raw CDSE STAC API also requires uppercase.
   Smoke-test both "descending" and "DESCENDING" against the live API before coding.

2. For each product:
   a. Skip if already in state file (data/.processed_products.json)
   b. Extract acquisition date from product datetime
   c. Run Script A (run_s1tiling.py) with --date-start/--date-end set to that date ± 1 day;
      capture the printed output prefix
   d. Run Script B (run_ingest_register.py) with Script A's output prefix and
      --s3-zarr-bucket / --s3-zarr-prefix for the Zarr output
      # NOTE: the watcher uses --s3-zarr-bucket/prefix internally but passes them to Script B
      # as --s3-output-bucket / --s3-output-prefix (Script B's actual arg names).
   e. On success: mark product in state file
   f. On failure: log error, continue to next product

3. Print summary: N found, M new, K processed, L failed
```

State file `data/.processed_products.json`:
```json
{"31TCH": {"descending": [{"product_id": "S1A_IW_GRDH_...", "date": "2025-02-05"}]}}
```

**Acceptance criteria**:
- [ ] `--dry-run` prints CDSE query results and planned runs without executing
- [ ] `sat:orbit_state` filter casing verified against live CDSE API before submission
- [ ] Idempotent: re-running with same `--lookback-days` skips already-processed products
- [ ] Processes at least one new product end-to-end

**Depends on**: Sub-issues A, B

---

## Sub-issue 6 — [platform-deploy] Argo WorkflowTemplate: `eopf-explorer-ingest-v1-s1rtc`

**Repo**: `EOPF-Explorer/platform-deploy`
**New file**: `workspaces/devseed-staging/data-pipeline/eopf-explorer-ingest-v1-s1rtc-template.yaml`

Argo translation of `run_ingest_register.py` (Sub-issue B). Written after Sub-issue 4 validates local scripts.

**Docker image**: both steps run the data-pipeline image (the same image used for existing
`convert_v1_s2.py` / `register_v1.py` jobs). `pipeline_image_version` selects the tag.
Look up the image name from an existing WorkflowTemplate in `platform-deploy` (e.g.
`eopf-explorer-convert-v1`).

**DAG** (2 steps, mirrors Sub-issue B):
1. `ingest` — `scripts/ingest_v1_s1_rtc.py`; 2 CPU, 8Gi; `activeDeadlineSeconds: 3600`
   - Retry on exit codes 1, 137, 143 — **no retry on exit code 2** (`asInt(lastRetry.exitCode) == 2`)
   - S3 credentials from `geozarr-s3-credentials` secret (keys: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. `register-stac` (depends-on `ingest`) — `scripts/register_v1_s1_rtc.py`; 1 CPU, 2Gi; retry 5×, always

**Parameters** (aligned with Script B, Zarr path derived — not passed explicitly):

| Parameter | Default |
|-----------|---------|
| `s3_geotiff_prefix` | — |
| `tile_id` | — |
| `orbit_direction` | `descending` |
| `collection` | `sentinel-1-grd-rtc-staging` |
| `s3_output_bucket` | `esa-zarr-sentinel-explorer-fra` |
| `s3_output_prefix` | `s1-rtc-staging` |
| `s3_endpoint` | `https://s3.de.io.cloud.ovh.net` |
| `stac_api_url` | `https://api.explorer.eopf.copernicus.eu/stac` |
| `raster_api_url` | `https://api.explorer.eopf.copernicus.eu/raster` |
| `pipeline_image_version` | — |
| `semaphore_key` | `v1-s1rtc-limit` |

**Acceptance criteria**: `argo lint --offline` passes; manually triggered run for 31TCH completes; exit-2 no-retry verified in cluster.

**Depends on**: Sub-issue 4

---

## Sub-issue 7 — [platform-deploy] Argo WorkflowTemplate: `eopf-explorer-s1tiling`

**Repo**: `EOPF-Explorer/platform-deploy`
**New file**: `workspaces/devseed-staging/data-pipeline/eopf-explorer-s1tiling-template.yaml`

Argo translation of `run_s1tiling.py` (Sub-issue A). Written after Sub-issue 4 validates local scripts.

**Single step** `run-s1tiling`: image `registry.orfeo-toolbox.org/s1tiling/s1tiling:1.4.0`; 4 CPU, 16Gi; `activeDeadlineSeconds: 7200`; retry 3×, exponential backoff.

**Parameters** (aligned with Script A logical inputs):

| Parameter | Default |
|-----------|---------|
| `tile_id` | — |
| `orbit_direction` | `descending` |
| `date_start` | — |
| `date_end` | — |
| `s3_geotiff_bucket` | `esa-zarr-sentinel-explorer-fra` |
| `s3_geotiff_prefix` | `s1tiling-output` |
| `s1tiling_image` | `registry.orfeo-toolbox.org/s1tiling/s1tiling:1.4.0` |
| `semaphore_key` | `v1-s1tiling-limit` |

**Config injection**: The `{tile_id}`-style Python placeholders in `S1GRD_RTC_template.cfg`
cannot be filled by Argo's `{{inputs.parameters.*}}` substitution directly (different syntax).
Recommended approach:
1. Commit `config/S1GRD_RTC_template.cfg` to `platform-deploy` as a ConfigMap, mounted
   read-only at `/config/template.cfg`.
2. Add an `init-container` (or `initContainers` in the pod spec) that runs a tiny Python
   one-liner to render the template and write it to an `emptyDir` volume:
   ```
   uv run python -c "
   import sys, pathlib
   t = pathlib.Path('/config/template.cfg').read_text()
   out = t.format(
     tile_id=sys.argv[1],
     orbit_direction_s1t=('DES' if sys.argv[2]=='descending' else 'ASC'),
     date_start=sys.argv[3], date_end=sys.argv[4])
   pathlib.Path('/rendered/run.cfg').write_text(out)
   " {{inputs.parameters.tile_id}} {{inputs.parameters.orbit_direction}} \
     {{inputs.parameters.date_start}} {{inputs.parameters.date_end}}
   ```
3. The main S1Tiling container mounts the same `emptyDir` at `/config/run.cfg`.

**DEM access in Argo — recommended approach: PersistentVolumeClaim (ReadOnlyMany)**

S1Tiling expects the DEM at a fixed path (`/MNT/COP_DEM_GLO30`). A PVC with `ReadOnlyMany`
access mode mounts directly there without an init container or per-run S3 download.
Multiple S1Tiling pods can read simultaneously with no contention.

One-time setup (before Sub-issue 7):
1. Create a PVC in the `devseed` namespace (e.g. `s1tiling-dem-pvc`, `ReadOnlyMany`, 5 Gi)
2. Populate it with Copernicus DEM GLO-30 COG GeoTIFF tiles for the tiles of interest (same set as P2)
3. Mount in the Argo pod spec:
   ```yaml
   volumes:
     - name: dem
       persistentVolumeClaim:
         claimName: s1tiling-dem-pvc
         readOnly: true
   containers:
     - volumeMounts:
         - name: dem
           mountPath: /MNT/COP_DEM_GLO30
           readOnly: true
   ```

**EODAG patch in Argo**: same inject-unconditionally approach as Sub-issue A.
Mount `analysis/s1tiling_eodag4_patch.py` as a ConfigMap and set
`PYTHONSTARTUP=/patches/eodag_patch.py` in the container env.

**Acceptance criteria**: `argo lint --offline` passes; run for 31TCH produces GeoTIFFs at `s3://{bucket}/s1tiling-output/31TCH/descending/{date_start}/`.

**Depends on**: Sub-issue 4 + DEM access strategy confirmed + EODAG patch question resolved

---

## Sub-issue 7a — [platform-deploy] DEM PVC: create, populate, verify

**Repo**: `EOPF-Explorer/platform-deploy`

One-time setup that must be done before Sub-issue 7 can be implemented. The S1Tiling pod
expects DEM tiles at `/MNT/COP_DEM_GLO30`; a `ReadOnlyMany` PVC is the recommended mount
strategy (no per-run download, multiple pods can read concurrently).

**Steps**:
1. Create PVC `s1tiling-dem-pvc` in the `devseed` namespace (`ReadOnlyMany`, 5 Gi):
   ```yaml
   apiVersion: v1
   kind: PersistentVolumeClaim
   metadata:
     name: s1tiling-dem-pvc
     namespace: devseed
   spec:
     accessModes: [ReadOnlyMany]
     resources:
       requests:
         storage: 5Gi
   ```
   Storage size stays 5 Gi (24 COG tiles ≈ 400–600 MB, fits comfortably).
2. Populate from the Copernicus DEM GLO-30 COG GeoTIFF tiles used locally (P2 — same ~24
   tiles for the 31TCH swath: 41–44°N, 3°W–5°E). Copy via a one-off pod or `kubectl cp`.
   Use the renamed filenames (no `COG_` infix, no `_DEM` suffix — see P2 rename step).
3. The eotile GPKG (`DEM_Union.gpkg`) and EGM2008 geoid should either go in the same PVC
   or a separate small PVC/ConfigMap — coordinate with Emmanuel before populating.
4. Commit the PVC manifest to `workspaces/devseed-staging/data-pipeline/`.

**Acceptance criteria**:
- [ ] PVC manifest committed and applied in `devseed` namespace; `kubectl get pvc s1tiling-dem-pvc -n devseed` shows `Bound`
- [ ] Test pod mounts the PVC at `/MNT/COP_DEM_GLO30` and lists ≥ 20 `.tif` files covering the 31TCH swath
- [ ] Coordinate with Emmanuel to confirm tile set and GPKG/geoid strategy before populating

**Depends on**: P2 (DEM tiles downloaded locally)
**Blocks**: Sub-issue 7

---

## Sub-issue 8 — [platform-deploy] CronWorkflow + Webhook Sensor

**CronWorkflow** (`0 6 * * *`, `concurrencyPolicy: Forbid`): triggers the **full pipeline** —
Workflow 1 (`eopf-explorer-s1tiling`) followed by Workflow 2 (`eopf-explorer-ingest-v1-s1rtc`)
— for tile 31TCH / descending. It runs daily; most days will find no new data (S1 cadence is
6 days) and S1Tiling will produce no output. This is intentional: the cost is low and
`concurrencyPolicy: Forbid` ensures no concurrent runs stack up.

**Sensor** (Webhook): filter `body.action == "^ingest-v1-s1rtc$"`, maps POST body fields to
**Workflow 2 only** (used for manual re-ingest when GeoTIFFs already exist on S3).

Manual trigger body:
```json
{
  "action": "ingest-v1-s1rtc",
  "s3_geotiff_prefix": "s3://esa-zarr-sentinel-explorer-fra/s1tiling-output/31TCH/descending/2025-02-05/",
  "tile_id": "31TCH",
  "orbit_direction": "descending"
}
```

**Acceptance criteria**:
- [ ] `argo lint --offline` passes for both the CronWorkflow and Sensor manifests
- [ ] CronWorkflow manually triggered in cluster completes end-to-end for tile 31TCH
- [ ] Webhook POST with the manual trigger body above fires `eopf-explorer-ingest-v1-s1rtc` and the run completes
- [ ] `concurrencyPolicy: Forbid` verified: a second manual trigger while first is running does not spawn a second run

**Depends on**: Sub-issue 6

---

## Sub-issue 9 — [platform-deploy] Concurrency configmap

Add to `eopf-workflow-concurrency-configmap.yaml`:
```yaml
v1-s1rtc-limit: "3"     # ingest+register — S3 writes are independent per tile
v1-s1tiling-limit: "2"  # CPU/memory heavy
```

Per-tile ZARR write isolation requirement (2026-04-23 meeting): only one ingest workflow
should write to a given tile's Zarr store at a time.

The Argo semaphore `v1-s1rtc-limit: "3"` is a **global** limit — it caps total concurrent
ingest workflows to 3, but does not prevent two workflows from writing to the same tile
simultaneously. True per-tile isolation requires the `semaphore_key` parameter to be
tile-specific (e.g. `v1-s1rtc-31TCH`), with each tile having its own ConfigMap key set
to `"1"`. Coordinate with Emmanuel to confirm whether:
(a) global limit of 1 (`"1"`) is acceptable for the initial scope (single tile), or
(b) per-tile keys are needed now (add one key per active tile to the ConfigMap).
Until confirmed, document the chosen approach here before implementing Sub-issue 9.

**Acceptance criteria**:
- [ ] ConfigMap committed and applied; `kubectl get configmap eopf-workflow-concurrency-configmap -n devseed -o yaml` shows both new keys
- [ ] Run 4 concurrent `eopf-explorer-ingest-v1-s1rtc` workflows: only 3 run in parallel (4th queues)
- [ ] Run 3 concurrent `eopf-explorer-s1tiling` workflows: only 2 run in parallel (3rd queues)

**Depends on**: Sub-issues 6, 7

---

## Summary

| # | Deliverable | Mirrors | Can start | Blocks |
|---|-------------|---------|-----------|--------|
| P1–P6 | CDSE account, DEM, Docker, config, test bucket + awscli, data-model unblock | — | now | A, 2 |
| **A** | `scripts/run_s1tiling.py` | Argo Workflow 1 | after P1–P5 | 4, 10 |
| **B** | `scripts/run_ingest_register.py` | Argo Workflow 2 | after 2, 3 | 4, 10 |
| 1 | data-model STAC builder + pyproject.toml pin bump | — | **now** | 3, B |
| 2 | `scripts/ingest_v1_s1_rtc.py` | Argo step 1 of Wf2 | **now** | B |
| 3 | `scripts/register_v1_s1_rtc.py` | Argo step 2 of Wf2 | after 1 | B |
| 5 | STAC collection JSON | — | **now** | B |
| **4** | **End-to-end validation (run A → B for 31TCH)** | Full pipeline | after A, B, 5 | 6, 7 |
| 10 | `scripts/watch_cdse_and_process.py` | Argo CronWorkflow | after A, B | — |
| 6 | Argo template: ingest+register | Script B | after 4 | 8, 9 |
| 7a | DEM PVC: create, populate, verify | — | **now** (parallel with local work) | 7 |
| 7 | Argo template: s1tiling | Script A | after 4 + 7a + EODAG patch resolved | 8 |
| 8 | CronWorkflow + Sensor | — | after 6 | — |
| 9 | Concurrency configmap | — | after 6, 7 | — |

**Critical path**: Prerequisites → A and (1, 2, 5 start immediately in parallel) → 3 → B → 4 → 6 → 8
