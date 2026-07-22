# TODO — Sub-issue 10 `watch_cdse_and_process.py`

Plan: `claude-docs/plans/subissue_10_watch_cdse.md`

- [x] **Task 1** — `mgrs` dep + `tile_bbox()` + CLI skeleton (no `--s3-zarr-prefix`)  ✅ `423f096`
- [x] **Task 2** — `query_cdse()` (CDSE STAC, isolated orbit filter)  ✅ `8e4339b`
- [x] **Task 3** — state file `data/.processed_products.json` + `.gitignore`  ✅ `f72b44c`
- [x] **Task 4** — `process_product()` (Script A → Script B; reconstruct prefix, stream A's logs)  ✅ `6211b02`
- [x] **Task 5** — `main()` wiring + `--dry-run` + summary line  ✅ `99a4c3a`
- [x] **Checkpoint** — 29 watcher tests + 378 full-suite green; ruff+mypy clean; `--dry-run` lists real products
- [~] **Task 6** — query side verified live (collection `sentinel-1-grd`, lowercase orbit, `query` filter).
      **Remaining (needs Sub-issue 4 env: CDSE creds + DEM + Docker + S3):** full A→B run + idempotent re-run.

Out of scope (flagged): fix stale sub-issue 10 interface block in the spec — separate one-line edit.
