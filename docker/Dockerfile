# Build for linux/amd64: docker buildx build --platform linux/amd64 -t <tag> . --push

# --- Builder stage: installs build tools and Python dependencies ---
FROM python:3.13.12-slim@sha256:739e7213785e88c0f702dcdc12c0973afcbd606dbf021a589cab77d6b00b579d AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    ca-certificates \
    build-essential \
    libgdal-dev \
    gdal-bin \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir pip==26.0.1 uv==0.11.2

# Copy project files and install dependencies (includes eopf-geozarr from data-model via git)
COPY pyproject.toml README.md /app/
RUN uv pip install --system --no-cache /app

# --- Runtime stage: minimal production image without build tools ---
FROM python:3.13.12-slim@sha256:739e7213785e88c0f702dcdc12c0973afcbd606dbf021a589cab77d6b00b579d

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libgdal-dev \
    gdal-bin \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages

RUN useradd --create-home --no-log-init --shell /bin/bash appuser

WORKDIR /app

COPY scripts/ /app/scripts/

RUN chown -R appuser:appuser /app

USER appuser

CMD ["/bin/bash"]
