#!/usr/bin/env python3
"""Prometheus metrics instrumentation for data-pipeline scripts.

This module provides shared metric definitions and a metrics server
for exposing metrics to the Prometheus scraper in Kubernetes.

Usage:
    from scripts.metrics import start_metrics_server, CONVERSION_DURATION

    start_metrics_server(port=8000)  # In main()

    with CONVERSION_DURATION.labels(collection="sentinel-2-l2a").time():
        convert_data()
"""

from __future__ import annotations

import logging
import os

from prometheus_client import Counter, Histogram, start_http_server

logger = logging.getLogger(__name__)

# Metrics port for Kubernetes ServiceMonitor to scrape
DEFAULT_METRICS_PORT = 8000

# Conversion workflow metrics
CONVERSION_DURATION = Histogram(
    "geozarr_conversion_seconds",
    "Time to convert source to GeoZarr format",
    labelnames=["collection", "resolution"],
)

CONVERSION_DATA_SIZE = Histogram(
    "geozarr_conversion_bytes",
    "Size of data converted in bytes",
    labelnames=["collection"],
    buckets=[1e6, 10e6, 100e6, 1e9, 10e9, 100e9],  # 1MB to 100GB
)

# STAC API interaction metrics
STAC_REGISTRATION_TOTAL = Counter(
    "stac_registration_total",
    "Total STAC item registration attempts",
    labelnames=["collection", "status"],  # status: success|failure|retry
)

STAC_HTTP_REQUEST_DURATION = Histogram(
    "stac_http_request_seconds",
    "STAC API HTTP request duration",
    labelnames=["method", "endpoint", "status_code"],
)

# Preview generation metrics
PREVIEW_GENERATION_DURATION = Histogram(
    "preview_generation_seconds",
    "Time to generate preview images",
    labelnames=["collection", "preview_type"],  # preview_type: true_color|quicklook|s1_grd
)

PREVIEW_HTTP_REQUEST_DURATION = Histogram(
    "preview_http_request_seconds",
    "HTTP request duration for preview-related operations",
    labelnames=["operation", "status_code"],
)

# AMQP workflow metrics
AMQP_PUBLISH_TOTAL = Counter(
    "amqp_publish_total",
    "Total AMQP messages published",
    labelnames=["exchange", "status"],  # status: success|failure
)


def start_metrics_server(port: int | None = None) -> None:
    """Start Prometheus metrics HTTP server.

    Args:
        port: Port to listen on. Defaults to METRICS_PORT env var or 8000.

    Note:
        Should only be called once per process. Safe to call in Kubernetes
        pod startup. Metrics exposed at http://localhost:<port>/metrics
    """
    if port is None:
        port = int(os.getenv("METRICS_PORT", str(DEFAULT_METRICS_PORT)))

    try:
        start_http_server(port)
        logger.info("Metrics server started on port %d", port)
    except OSError as e:
        # Port already in use (e.g., from previous run)
        logger.warning("Failed to start metrics server on port %d: %s", port, e)


def is_metrics_enabled() -> bool:
    """Check if metrics collection is enabled.

    Returns:
        True if ENABLE_METRICS env var is set to "true" (case-insensitive).
        Defaults to True if not set (opt-out model).
    """
    return os.getenv("ENABLE_METRICS", "true").lower() == "true"
