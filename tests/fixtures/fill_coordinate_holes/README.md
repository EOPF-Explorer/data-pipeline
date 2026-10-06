Real coordinate arrays (zarr.json plus the single chunk `c/0`), read through the gateway from
`sentinel-1-grd-rtc-staging`:

- `s1-rtc-32UPB/ascending/r10m/platform` holds `['s1a', 's1c', '']`; index 2 lost its value on S3
  (read on 1 Oct 2026);
- `s1-rtc-31TEL/descending/r10m/relative_orbit` holds `[110, 37, 110, 0]`; index 3 lost its value,
  and the 37 at index 1 is wrong (CDSE says 110 for that pass) (read on 1 Oct 2026);
- `s1-rtc-31TEL/descending/r10m/absolute_orbit` holds `[65107, 8258, 8258, 8360]`, intact (read on
  6 Oct 2026; unchanged since 1 Oct).

`ingest_v1_s1_rtc._sync_tree` skipped re-uploading the rewritten chunk because its compressed size had
not changed. `test_fill_coordinate_holes.py` fills these holes and checks the re-encoded chunk; it
also replaces the wrong 37, pinned to its slice by the absolute orbit.
