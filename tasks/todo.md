# TODO — Sub-issue 10 `watch_cdse_and_process.py`

Plan: `claude-docs/plans/subissue_10_watch_cdse.md`

- [ ] **Task 1** — `mgrs` dep + `tile_bbox()` + CLI skeleton (no `--s3-zarr-prefix`)  ← NEXT
- [ ] **Task 2** — `query_cdse()` (CDSE STAC, `SENTINEL-1-GRD`, isolated orbit filter)
- [ ] **Task 3** — state file `data/.processed_products.json` + `.gitignore`
- [ ] **Task 4** — `process_product()` (Script A → Script B; reconstruct prefix, stream A's logs)
- [ ] **Task 5** — `main()` wiring + `--dry-run` + summary line
- [ ] **Checkpoint** — `uv run pytest` green; ruff+mypy; `--dry-run` end-to-end
- [ ] **Task 6** — live verify `sat:orbit_state` casing + mechanism; end-to-end + idempotent re-run

Out of scope (flagged): fix stale sub-issue 10 interface block in the spec — separate one-line edit.
