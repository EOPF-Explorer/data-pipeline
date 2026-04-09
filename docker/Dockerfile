# Build for linux/amd64: docker buildx build --platform linux/amd64 -t <tag> . --push

# --- Builder stage: installs build tools and Python dependencies ---
FROM python:3.13-alpine AS builder

RUN apk add --no-cache \
    git \
    curl \
    ca-certificates \
    build-base \
    gdal-dev

WORKDIR /app

RUN pip install --no-cache-dir pip==26.0.1 uv==0.11.2

# Copy project files and install dependencies (includes eopf-geozarr from data-model via git)
COPY pyproject.toml README.md /app/
RUN uv pip install --system --no-cache /app

# --- Runtime stage: minimal production image without build tools ---
FROM python:3.13-alpine

RUN apk add --no-cache \
    ca-certificates \
    gdal

COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages

RUN adduser -D -h /home/appuser -s /bin/sh appuser

WORKDIR /app

COPY scripts/ /app/scripts/

RUN chown -R appuser:appuser /app

USER appuser

CMD ["/bin/sh"]
