# Build for linux/amd64: docker buildx build --platform linux/amd64 -t <tag> . --push

# --- Builder stage: installs build tools and Python dependencies ---
FROM python:3.13-alpine@sha256:18f00d382da0de501bc850c1dd422eb4deea736f5ec45cb34e6a9cacd5199813 AS builder

RUN apk add --no-cache \
    git \
    curl \
    ca-certificates \
    build-base \
    gdal-dev

WORKDIR /app

# Pin build tools — versions tracked by Dependabot (docker/requirements-build.txt)
COPY docker/requirements-build.txt /app/
RUN pip install --no-cache-dir -r /app/requirements-build.txt

# Copy lockfile before source so this layer is cached independently of code changes
COPY pyproject.toml uv.lock README.md /app/
RUN uv sync --frozen --no-dev --system --no-editable

# --- Runtime stage: minimal production image without build tools ---
FROM python:3.13-alpine@sha256:18f00d382da0de501bc850c1dd422eb4deea736f5ec45cb34e6a9cacd5199813

RUN apk add --no-cache \
    ca-certificates \
    gdal

# Copy installed packages and any console-script entrypoints from builder
COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

RUN adduser -D -h /home/appuser -s /bin/sh appuser

WORKDIR /app

COPY scripts/ /app/scripts/
# Remove test helpers (no purpose in production) and set ownership in one layer
RUN find /app/scripts -name 'test_*.py' -delete \
    && chown -R appuser:appuser /app

USER appuser
