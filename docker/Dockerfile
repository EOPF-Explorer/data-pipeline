# Build for linux/amd64: docker buildx build --platform linux/amd64 -t <tag> . --push
FROM python:3.13-slim

# System dependencies (including GDAL for rasterio)
RUN apt-get update && apt-get install -y \
    git \
    curl \
    ca-certificates \
    build-essential \
    libgdal-dev \
    gdal-bin \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install uv for fast dependency resolution
RUN pip install -U pip uv

# Copy project files and install dependencies (includes eopf-geozarr from data-model via git)
COPY pyproject.toml README.md /app/
RUN uv pip install --system --no-cache /app

# Copy scripts
COPY scripts/ /app/scripts/

# Copy workflows (example payloads and templates)
COPY workflows/ /app/workflows/

CMD ["/bin/bash"]
