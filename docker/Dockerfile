# Build for linux/amd64: docker buildx build --platform linux/amd64 -t <tag> . --push
FROM python:3.11-slim

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

# Use git commit SHA for precise cache control
# Update via: docker build --build-arg DATA_MODEL_COMMIT=$(git ls-remote https://github.com/EOPF-Explorer/data-model.git refs/heads/fix/s1-encoding-conflict | cut -f1)
ARG DATA_MODEL_COMMIT=fix/s1-encoding-conflict

# Install eopf-geozarr from data-model (includes dask[distributed])
RUN uv pip install --system --no-cache \
    git+https://github.com/EOPF-Explorer/data-model.git@${DATA_MODEL_COMMIT}

# Copy project files for dependency installation
COPY pyproject.toml README.md /app/
RUN uv pip install --system --no-cache /app

# Copy scripts (cache invalidated by content changes, not manual ARG)
ARG SCRIPTS_VERSION=auto

# Copy scripts
COPY scripts/ /app/scripts/
RUN chmod +x /app/scripts/*.py

# Copy workflows (example payloads and templates)
COPY workflows/ /app/workflows/

CMD ["/bin/bash"]
