"""Preview configuration registry for different collections."""

from dataclasses import dataclass


@dataclass
class PreviewConfig:
    """Preview rendering configuration for a collection."""

    variables: list[str]  # Zarr paths to variables
    rescale: str  # Rescale range (e.g., "0,0.1")
    fallback_variable: str | None = None  # Fallback if variables not found


# Collection registry
PREVIEW_CONFIGS = {
    "sentinel-2-l2a": PreviewConfig(
        variables=[
            "/measurements/reflectance/r10m/0:b04",  # Red
            "/measurements/reflectance/r10m/0:b03",  # Green
            "/measurements/reflectance/r10m/0:b02",  # Blue
        ],
        rescale="0,0.1",
    ),
    "sentinel-1-grd": PreviewConfig(
        variables=[],  # Auto-detect from assets
        rescale="0,219",
        fallback_variable="/measurements:grd",
    ),
}


def get_preview_config(collection_id: str) -> PreviewConfig | None:
    """Get preview config for collection, trying normalized variants."""
    normalized = collection_id.lower().replace("_", "-")

    # Direct match
    if normalized in PREVIEW_CONFIGS:
        return PREVIEW_CONFIGS[normalized]

    # Prefix match (sentinel-2-l2a matches sentinel-2*)
    for key, config in PREVIEW_CONFIGS.items():
        if normalized.startswith(key.split("-")[0]):
            return config

    return None
