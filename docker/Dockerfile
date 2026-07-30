# Build for linux/amd64: docker buildx build --platform linux/amd64 -t <tag> . --push
#
# Base is Chainguard's Wolfi (glibc), not python:alpine (musl). Wolfi's apk repo is public and free
# (https://packages.wolfi.dev/os, definitions in github.com/wolfi-dev/os), so we get continuously
# rebuilt OS packages without a subscription. rasterio and pyproj are built from source against
# Wolfi's GDAL/PROJ rather than installed as manylinux wheels, so a GDAL or PROJ CVE is fixed by
# bumping the base instead of waiting for a rasterio release to re-vendor its bundled copy — and both
# libraries stay visible to Trivy and to any SBOM we generate (EU CRA).
#
# `:latest` is the only free Chainguard tag, so the digest pin below IS the reproducibility
# mechanism; Dependabot bumps it (docker ecosystem, /docker).
#
# docker/smoke-test.sh asserts the runtime contract this image owes Argo — run it after any change.

# --- Builder stage: installs build tools and Python dependencies ---
FROM cgr.dev/chainguard/wolfi-base:latest@sha256:003627df3c1e1bba0c4116afcddb314aca9594ee2328c7e876a8081a6c988b2e AS builder

# git: eopf-geozarr is a git dependency.
# proj-util: ships `projinfo`, which pyproj's sdist build requires — proj-dev alone is not enough.
RUN apk add --no-cache \
    python-3.14 \
    python-3.14-dev \
    py3.14-pip \
    git \
    ca-certificates-bundle \
    build-base \
    gdal-dev \
    proj-dev \
    proj-util

WORKDIR /app

# Pin build tools — versions tracked by Dependabot (docker/requirements-build.txt)
COPY docker/requirements-build.txt /app/
RUN pip install --no-cache-dir -r /app/requirements-build.txt

# Copy lockfile before source so this layer is cached independently of code changes
COPY pyproject.toml uv.lock README.md /app/
# --no-binary-package is a build-time flag rather than a [tool.uv] setting on purpose: local
# development on macOS has no system GDAL and must keep using wheels.
RUN uv sync --frozen --no-dev --no-editable --python /usr/bin/python3.14 \
    --no-binary-package rasterio \
    --no-binary-package pyproj

# --- Runtime stage: minimal production image without build tools ---
FROM cgr.dev/chainguard/wolfi-base:latest@sha256:003627df3c1e1bba0c4116afcddb314aca9594ee2328c7e876a8081a6c988b2e

# bash: Argo cron and job steps invoke `command: [bash, -c]`; wolfi-base already provides sh.
# gdal/proj: the shared libraries rasterio and pyproj were linked against (no -dev headers here).
# geos-3.14 is pinned deliberately. Wolfi's `gdal` declares only the soname `so:libgeos_c.so.1`,
# which geos-3.12, geos-3.13 and geos-3.14 all provide, and apk resolves it to geos-3.12 — whose ABI
# lacks GEOSGridIntersectionFractions_r. Without this pin the image builds green, scans clean, then
# fails at runtime on `import rasterio`. Keep it until wolfi-dev/os tightens that dependency.
RUN apk add --no-cache \
    python-3.14 \
    bash \
    ca-certificates-bundle \
    gdal \
    proj \
    geos-3.14

# Copy the virtualenv created by uv sync; activate it via PATH
COPY --from=builder /app/.venv /app/.venv
ENV VIRTUAL_ENV=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"

# A source-built pyproj looks for proj.db under its own package directory, not the system share dir.
ENV PROJ_DATA=/usr/share/proj

# uid 1000 is explicit: no platform-deploy template sets securityContext.runAsUser, so this uid is
# what every mounted volume sees.
RUN adduser -D -u 1000 -h /home/appuser -s /bin/sh appuser

WORKDIR /app

COPY scripts/ /app/scripts/
# Remove test helpers (no purpose in production) and set ownership in one layer
RUN find /app/scripts -name 'test_*.py' -delete \
    && chown -R appuser:appuser /app

USER appuser
