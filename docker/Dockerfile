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

# Cachebust for data-model installation (change timestamp to force fresh install)
ARG CACHEBUST=2025-10-06T11:00:00Z

# Install eopf-geozarr from minimal fix branch
# Includes critical set_spatial_dims() fix before write_crs() calls
RUN uv pip install --system --no-cache \
    git+https://github.com/EOPF-Explorer/data-model.git@fix/spatial-dims-minimal \
    pystac>=1.10.0 \
    httpx>=0.27.0 \
    boto3>=1.34.0

# Force fresh copy of scripts (invalidate cache)
ARG SCRIPTS_VERSION=2025-10-06T02:05:00Z

# Copy scripts
COPY scripts/ /app/scripts/
RUN chmod +x /app/scripts/*.py

CMD ["/bin/bash"]
