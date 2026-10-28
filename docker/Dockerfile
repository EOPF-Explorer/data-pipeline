# Build for linux/amd64: docker buildx build --platform linux/amd64 -t <tag> . --push

# --- Builder stage: installs build tools and Python dependencies ---
FROM python:3.14.8-alpine@sha256:f6a589d43c42b9e7f7dc67a12d37132491f362859a5d750607710cc56da3bc72 AS builder

RUN apk add --no-cache \
    git \
    curl \
    ca-certificates \
    build-base \
    gdal-dev \
    crc32c-dev

WORKDIR /app

# Pin build tools — versions tracked by Dependabot (docker/requirements-build.txt)
COPY docker/requirements-build.txt /app/
RUN pip install --no-cache-dir -r /app/requirements-build.txt

# Copy lockfile before source so this layer is cached independently of code changes
COPY pyproject.toml uv.lock README.md /app/
RUN uv sync --frozen --no-dev --no-editable

# --- Runtime stage: minimal production image without build tools ---
FROM python:3.14.8-alpine@sha256:f6a589d43c42b9e7f7dc67a12d37132491f362859a5d750607710cc56da3bc72 AS runtime

# apk upgrade refreshes the base image's own packages (musl, busybox, libssl3,
# ...), which apk add leaves alone; CI rebuilds this stage on every build. pip
# goes too: the app runs from /app/.venv, and the Trivy scan flags the certifi
# and urllib3 vendored in the base image's pip.
RUN apk upgrade --no-cache \
    && apk add --no-cache \
    bash \
    ca-certificates \
    gdal \
    gdal-dev \
    gdal-tools \
    crc32c \
    && pip uninstall -y pip

RUN adduser -D -h /home/appuser -s /bin/sh appuser

# Copy the virtualenv created by uv sync; activate it via PATH. Files are owned
# by appuser at copy time: a later `chown -R` would duplicate the whole venv in
# another layer.
COPY --from=builder --chown=appuser:appuser /app/.venv /app/.venv
ENV VIRTUAL_ENV=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"

WORKDIR /app

COPY --chown=appuser:appuser scripts/ /app/scripts/
# The operator CLIs ship too so bulk deletes can run as an in-cluster Argo
# workflow instead of a many-hour laptop session: in-region S3, credentials and
# the stac-writer OIDC secret already mounted, and no dependency on a desk
# machine staying awake. They import from /app/scripts via a relative path, so
# the two directories must stay siblings.
COPY --chown=appuser:appuser operator-tools/ /app/operator-tools/
# Remove test helpers (no purpose in production); /app itself was created by
# root, so hand it to appuser too
RUN find /app/scripts /app/operator-tools -name 'test_*.py' -delete \
    && chown appuser:appuser /app

USER appuser
