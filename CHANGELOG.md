# Changelog

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
