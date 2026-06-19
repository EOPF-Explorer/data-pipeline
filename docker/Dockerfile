# Build for linux/amd64: docker buildx build --platform linux/amd64 -t <tag> . --push

# --- Builder stage: installs build tools and Python dependencies ---
FROM python:3.14.6-alpine@sha256:26730869004e2b9c4b9ad09cab8625e81d256d1ce97e72df5520e806b1709f92 AS builder

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
FROM python:3.14.6-alpine@sha256:26730869004e2b9c4b9ad09cab8625e81d256d1ce97e72df5520e806b1709f92

RUN apk add --no-cache \
    bash \
    ca-certificates \
    gdal \
    gdal-dev \
    gdal-tools \
    crc32c

# Copy the virtualenv created by uv sync; activate it via PATH
COPY --from=builder /app/.venv /app/.venv
ENV VIRTUAL_ENV=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"

RUN adduser -D -h /home/appuser -s /bin/sh appuser

WORKDIR /app

COPY scripts/ /app/scripts/
# Remove test helpers (no purpose in production) and set ownership in one layer
RUN find /app/scripts -name 'test_*.py' -delete \
    && chown -R appuser:appuser /app

USER appuser
