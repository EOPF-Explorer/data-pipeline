# GeoZarr Quicklook CRS Investigation

## TL;DR
- Quicklook assets are optional in upstream CPM publications; discovery must tolerate their absence.
- `scripts/convert.py` now infers quicklook groups only when the STAC item advertises them and soft-fails otherwise.
- Latest `data-model` CRS fallback ensures `/quality/l2a_quicklook` inherits the reflectance CRS when it is not encoded natively.

## What Broke
- Sentinel-2 GeoZarr releases tagged `cpm_v262` omitted the `/quality/l2a_quicklook` groups even though STAC assets still reference quicklooks.
- The convert workflow assumed quicklooks would always be published, so the register step failed on empty groups.
- Quicklook datasets that were published also lacked CRS metadata, blocking downstream tiling.

## Investigation Highlights
- Sampled six months of Sentinel-2 GeoZarr items across tiles and orbits; roughly half of the sampled items were missing quicklook data despite STAC advertising the asset.
- Confirmed that older `cpm_v256` items exhibit the same gap, so the issue predates the recent regression.
- Verified that STAC continues to list quicklook assets regardless of the dataset's presence, so workflow logic must perform existence checks.

> Sampling notes: raw JSON exports collected during the investigation are intentionally left out of the repository to keep the diff focused. They are available in the shared evidence bucket if deeper inspection is required.

## Fixes Landed
1. `scripts/convert.py`
   - Derives quicklook group names directly from the STAC assets.
   - Short-circuits when the quicklook dataset is absent instead of failing the run.
   - Logs actionable diagnostics for missing quicklook groups and for mismatched CRS.
2. `data-model` GeoZarr conversion
   - Adds a sibling CRS fallback so quicklook datasets inherit the reflectance CRS.
   - Ensures the CRS is propagated to variable attributes when inferred.

## Verification
- `uv run -q pytest -q tests/test_cli_e2e.py::TestCLIEndToEnd::test_cli_convert_real_sentinel2_data`
- Manual rerun of the devseed Argo workflow `geozarr-convert-quicklook`, confirming successful completion and STAC registration without quicklook datasets.
- Spot-checked freshly produced GeoZarr stores to confirm quicklook datasets (when present) now carry CRS metadata.

## Follow-Up
- Monitor CPM release notes for clarity on quicklook publication cadence.
- Backfill missing quicklooks only when explicitly prioritized; convert workflow can now operate without them.
