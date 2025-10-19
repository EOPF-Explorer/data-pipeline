# Changelog

All notable changes to data-pipeline.

Format: [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)

---

## [Unreleased]

### Added

- **Prometheus metrics endpoint** (422e438, 9d89a66, e4016de): `/metrics` exposed on port 8000 for observability
- **STAC spec validation** (1e630e0): `validate_geozarr.py` validates items against STAC 1.0 spec using pystac
- **TileMatrixSet validation** (1e630e0): Validates OGC TMS compliance using morecantile
- **CF-conventions validation** (1e630e0): Validates CF-1.8 metadata using cf-xarray
- **Docker CI/CD** (fb669cd): Automated builds push to `ghcr.io/eopf-explorer/data-pipeline:latest`
- **pystac-client for STAC registration** (9e33fca): `register_stac.py` now uses pystac-client with automatic validation

### Changed

- **Exception logging** (fix/error-logging): All scripts now log full stack traces with structured context for debugging
- **STAC operations** (9d89a66): Instrumented with Prometheus metrics for latency and success/failure tracking
- **AMQP publishing** (9d89a66): Instrumented with Prometheus metrics for message tracking

### Dependencies

- Added: `pystac-client>=0.8.0`, `morecantile>=5.0.0`, `cf-xarray>=0.9.0`

---

## [1.0.0] - Base Pipeline

See PRs #1-#11 for base pipeline features:
- Argo Workflows orchestration
- STAC registration and augmentation
- S1 GRD + S2 L2A support
- Prometheus metrics
- E2E tests

---

## Guidelines

**Adding entries:**
1. One entry per functional change
2. Link to commit/PR
3. Explain user impact

**Sections:**
- `Added` - New features
- `Changed` - Modifications to existing
- `Fixed` - Bug fixes
- `Removed` - Deleted features
- `Deprecated` - Soon-to-be removed

**Example:**
```markdown
### Added
#### feat/my-feature (2025-10-18)
- **file.py**: What changed
- **Impact**: Why it matters
```
