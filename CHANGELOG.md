# Changelog

## 1.12.0 (2026-07-14)

## What's Changed
* feat(demo): protect Fontainebleau wildfire scenes (T31UDP) by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/336
* feat(query-stac): add --max-cloud-cover filter to discover by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/335


**Full Changelog**: https://github.com/EOPF-Explorer/data-pipeline/compare/v1.11.1...v1.12.0

## 1.11.1 (2026-07-14)

## What's Changed
* fix(cleanup): datetime-based expiry + exclude-list-first demo protection by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/333


**Full Changelog**: https://github.com/EOPF-Explorer/data-pipeline/compare/v1.11.0...v1.11.1

## 1.11.0 (2026-07-14)

## What's Changed
* build(deps): bump the minor-and-patch group across 1 directory with 8 updates by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/277
* build(deps): consolidate Dependabot updates + group security PRs by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/286
* build(deps): bump the minor-and-patch group with 4 updates by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/289
* build(deps): update zarr[cast-value-rs] requirement from >=3.2.0 to >=3.2.1 by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/290
* feat(stac): eodash layerExclusive + style link for S2 L2A (#206) by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/278
* feat: add storage tier change events script by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/118
* feat(storage-tier): recurring tier-down selection (--min-age-days + tier-aware) by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/331
* feat: expiry-driven S2 retention cleanup (data-pipeline side) by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/329
* build(deps): bump the all group across 1 directory with 10 updates by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/332
* build(deps): bump the minor-and-patch group across 1 directory with 9 updates by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/328
* build(deps): bump uv from 0.11.21 to 0.11.26 in /docker in the all group across 1 directory by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/323
* build(deps): bump msgpack from 1.1.2 to 1.2.1 in the security group across 1 directory by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/299


**Full Changelog**: https://github.com/EOPF-Explorer/data-pipeline/compare/v1.10.0...v1.11.0

## 1.10.0 (2026-06-12)

## What's Changed
* build(deps): bump the all group across 1 directory with 2 updates by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/248
* feat(query-stac): batched fan-out + newest-first ordering + acquisition-age filter by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/257


**Full Changelog**: https://github.com/EOPF-Explorer/data-pipeline/compare/v1.9.0...v1.10.0

## 1.9.0 (2026-06-09)

## What's Changed
* ci: harden Docker image — switch to Alpine, fix CVE surface, clean Trivy scanning by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/160
* perf: improve eodc zarr store read reliability by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/168
* build(deps): bump uv from 0.11.2 to 0.11.3 in /docker in the all group by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/164
* build(deps): bump python from `18f00d3` to `70dd893` in /docker by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/165
* build(deps): bump docker/login-action from 3.7.0 to 4.1.0 in the all group by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/166
* build(deps): bump the minor-and-patch group across 1 directory with 5 updates by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/167
* fix: defer TCPConnector creation to async context (Python 3.13 crash) by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/170
* build(deps): bump uv from 0.11.3 to 0.11.6 in /docker by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/171
* fix: skip registry login on forks by @wietzesuijker in https://github.com/EOPF-Explorer/data-pipeline/pull/65
* fix: exclude dependabot from PUSH_IMAGE to prevent login failures by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/180
* build(deps): bump the all group across 1 directory with 4 updates by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/178
* build(deps): bump python from `70dd893` to `420cd0b` in /docker by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/175
* fix: fail fast when delete of existing item fails by @wietzesuijker in https://github.com/EOPF-Explorer/data-pipeline/pull/64
* fix: allow disabling sharding/dask/validation flags by @wietzesuijker in https://github.com/EOPF-Explorer/data-pipeline/pull/63
* build(deps): bump the minor-and-patch group across 1 directory with 7 updates by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/188
* build(deps): bump uv from 0.11.6 to 0.11.7 in /docker in the all group by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/187
* build(deps): bump nbconvert from 7.17.0 to 7.17.1 by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/183
* fix: handle None return from get_item in upsert_item existence check by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/191
* build(deps): bump pip from 26.0.1 to 26.1 by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/194
* build(deps-dev): bump notebook from 7.5.5 to 7.5.6 by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/195
* build(deps): bump the all group across 1 directory with 9 updates by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/214
* build(deps): bump python from 3.13-alpine to 3.14.5-alpine in /docker in the minor-and-patch group by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/208
* build(deps): bump the minor-and-patch group across 1 directory with 12 updates by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/209
* build(deps): bump idna from 3.11 to 3.15 by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/207
* build(deps): bump jupyter-server from 2.17.0 to 2.18.0 by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/197
* build(deps): bump mistune from 3.2.0 to 3.2.1 by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/200
* build(deps): bump urllib3 from 2.6.3 to 2.7.0 by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/203
* build(deps): bump uv from 0.11.14 to 0.11.15 by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/213
* fix(security): bump pip 26.0.1 -> 26.1.2 to clear pip-audit CVEs by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/216
* fix(deps): bump pillow + aiohttp to clear 7 GHAS alerts by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/222
* feat: pin data-model to scale-offset codec commit and add experimental flag by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/189
* chore: pin data-model to v0.10.1 for release by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/234


**Full Changelog**: https://github.com/EOPF-Explorer/data-pipeline/compare/v1.8.1...v1.9.0

## 1.8.1 (2026-04-08)

## What's Changed
* ci: fix build on release please tag by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/154
* ci: add fallback registry by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/156
* build(deps): bump actions/cache from 4.3.0 to 5.0.4 by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/153
* ci: update Dependabot configuration for version updates by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/157
* build(deps): bump the all group with 6 updates by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/158
* build(deps): bump the minor-and-patch group with 5 updates by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/159
* build(deps): bump python from `7d8999b` to `f1927c7` in /docker by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/150


**Full Changelog**: https://github.com/EOPF-Explorer/data-pipeline/compare/v1.8.0...v1.8.1

## 1.8.0 (2026-04-08)

## What's Changed
* Pin GitHub Actions to commit SHAs (coordination#239) by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/127
* ci: add Dependabot and least-privilege workflow permissions by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/128
* ci: add proper CI and security scanning workflows by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/129
* feat: harden docker image and add vuln scanners by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/148
* ci: skip build and push for dependabot PR by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/149
* build(deps): bump docker/setup-buildx-action from 3.12.0 to 4.0.0 by @dependabot[bot] in https://github.com/EOPF-Explorer/data-pipeline/pull/147
* feat: create cli migration tool by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/110
* feat: release v0.9.0 by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/152

## New Contributors
* @dependabot[bot] made their first contribution in https://github.com/EOPF-Explorer/data-pipeline/pull/147

**Full Changelog**: https://github.com/EOPF-Explorer/data-pipeline/compare/v1.7.0...v1.8.0

## 1.7.0 (2026-03-25)

## What's Changed
* feat: create cli command to change storage tier by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/103
* feat: add stac query item based on storage tier script by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/126


**Full Changelog**: https://github.com/EOPF-Explorer/data-pipeline/compare/v1.6.1...v1.7.0

## 1.6.1 (2026-03-16)

## What's Changed
* ci: fix docker build after release please by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/107
* fix: fix color_formula query by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/109
* fix: fix all zarr assets media type and remove zipped_product asset by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/111


**Full Changelog**: https://github.com/EOPF-Explorer/data-pipeline/compare/v1.6.0...v1.6.1

## 1.6.0 (2026-03-05)

## What's Changed
* fix(stac): reflectance media type version=2 -> version=3 by @wietzesuijker in https://github.com/EOPF-Explorer/data-pipeline/pull/94
* fix: correct zarr media type format in reflectance assets by @emmanuelmathot in https://github.com/EOPF-Explorer/data-pipeline/pull/100
* feat: add aggregation tool for daily and monthly item counts by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/106


**Full Changelog**: https://github.com/EOPF-Explorer/data-pipeline/compare/v1.5.0...v1.6.0

## 1.5.0 (2026-02-11)

## What's Changed
* ci: remove publish and publish-docker jobs by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/91
* feat: implement script to update stac storage metadata by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/67


**Full Changelog**: https://github.com/EOPF-Explorer/data-pipeline/compare/v1.4.0...v1.5.0

## 1.4.0 (2026-01-22)

## What's Changed
* feat(cache): add tile cache warming by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/87
* fix: use the correct OVH Cloud storage class STANDARD_IA instead of GLacier and improve performance by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/62
* feat: add s3 object cleanup while cleaning a STAC collection by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/89
* feat: release datamodel v0.8.0 by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/90


**Full Changelog**: https://github.com/EOPF-Explorer/data-pipeline/compare/v1.3.0...v1.4.0

## 1.3.0 (2026-01-15)

## What's Changed
* Update STAC item submission notebook and enhance error handling by @emmanuelmathot in https://github.com/EOPF-Explorer/data-pipeline/pull/76
* Update STAC item query filter to use 'between' operation by @emmanuelmathot in https://github.com/EOPF-Explorer/data-pipeline/pull/77
* Implement collection deletion functionality in STAC manager by @emmanuelmathot in https://github.com/EOPF-Explorer/data-pipeline/pull/58
* feat: bump data-model version to v0.7.0 by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/78
* ci: add release please by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/79
* ci: fix release please config by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/80
* ci: add changelog type to release please config by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/82
* fix: remove extra space in release-please-config.json causing parse error by @Copilot in https://github.com/EOPF-Explorer/data-pipeline/pull/83
* ci: fix formating issues release please config by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/84
* chore: fix release-please configuration for Python package by @lhoupert in https://github.com/EOPF-Explorer/data-pipeline/pull/85
* chore(deps): Update vdata-model version converter to 0.7.1 and fix item ID parsing by @emmanuelmathot in https://github.com/EOPF-Explorer/data-pipeline/pull/81


**Full Changelog**: https://github.com/EOPF-Explorer/data-pipeline/compare/v1.2.3...v1.3.0
