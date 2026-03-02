#!/usr/bin/env python3
"""STAC registration entry point - orchestrates item creation and registration."""

from __future__ import annotations

import argparse
import logging
import os
import sys
import urllib.parse
from urllib.parse import urlparse

import httpx
import zarr
from pystac import Asset, Item, Link
from pystac.extensions.projection import ProjectionExtension
from pystac_client import Client
from storage_tier_utils import extract_region_from_endpoint, get_s3_storage_class

# Configure logging (set LOG_LEVEL=DEBUG for verbose output)
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Suppress verbose library logging
for lib in ["botocore", "s3fs", "aiobotocore", "urllib3", "httpx", "httpcore"]:
    logging.getLogger(lib).setLevel(logging.WARNING)

EXPLORER_BASE = os.getenv("EXPLORER_BASE_URL", "https://explorer.eopf.copernicus.eu")


# === Utilities ===


def s3_to_https(s3_url: str, gateway_url: str = "https://s3.explorer.eopf.copernicus.eu") -> str:
    """Convert s3:// URL to https:// using S3 gateway.

    Uses gateway format: https://s3.explorer.eopf.copernicus.eu/bucket/path

    Args:
        s3_url: S3 URL (s3://bucket/path)
        gateway_url: S3 gateway base URL (default: https://s3.explorer.eopf.copernicus.eu)

    Returns:
        HTTPS URL with bucket as path prefix
    """
    if not s3_url.startswith("s3://"):
        return s3_url

    parsed = urlparse(s3_url)
    bucket = parsed.netloc
    path = parsed.path

    gateway_base = gateway_url.rstrip("/")
    return f"{gateway_base}/{bucket}{path}"


def https_to_s3(
    https_url: str, gateway_url: str = "https://s3.explorer.eopf.copernicus.eu"
) -> str | None:
    """Convert https:// URL back to s3:// URL.

    Handles both formats:
    - New gateway format: https://s3.explorer.eopf.copernicus.eu/bucket/path
    - Old S3 format: https://bucket.s3.endpoint.com/path

    Args:
        https_url: HTTPS URL potentially pointing to S3
        gateway_url: S3 gateway base URL (default: https://s3.explorer.eopf.copernicus.eu)

    Returns:
        S3 URL if conversion is possible, None otherwise
    """
    if not https_url.startswith("https://"):
        return None

    parsed = urlparse(https_url)
    gateway_parsed = urlparse(gateway_url)
    gateway_host = gateway_parsed.netloc

    # Check if URL matches the new gateway format: gateway-host/bucket/path
    if parsed.netloc == gateway_host:
        # Extract bucket from path (first component)
        path_parts = parsed.path.lstrip("/").split("/", 1)
        if len(path_parts) >= 1:
            bucket = path_parts[0]
            remaining_path = "/" + path_parts[1] if len(path_parts) > 1 else ""
            return f"s3://{bucket}{remaining_path}"

    # Check if URL matches old S3 endpoint pattern: bucket.endpoint-host/path
    # This is for backwards compatibility
    if ".s3." in parsed.netloc or "s3." in parsed.netloc:
        # Try to extract bucket name (everything before .s3.)
        parts = parsed.netloc.split(".s3.")
        if len(parts) == 2:
            bucket = parts[0]
            return f"s3://{bucket}{parsed.path}"

    return None


def rewrite_asset_hrefs(item: Item, old_base: str, new_base: str) -> None:
    """Rewrite all asset hrefs from old_base to new_base (in place)."""
    old_base = old_base.rstrip("/") + "/"
    new_base = new_base.rstrip("/") + "/"

    for key, asset in item.assets.items():
        if asset.href and asset.href.startswith(old_base):
            new_href = new_base + asset.href[len(old_base) :]
            if new_href.startswith("s3://"):
                new_href = s3_to_https(new_href)
            logger.debug(f"  {key}: {asset.href} -> {new_href}")
            asset.href = new_href


# === Registration ===


def upsert_item(client: Client, collection_id: str, item: Item) -> None:
    """Register or update STAC item using pystac-client session."""
    # Check if exists
    try:
        client.get_collection(collection_id).get_item(item.id)
        exists = True
    except Exception:
        exists = False

    # Use client's base URL directly (includes /stac if present)
    base_url = str(client.self_href).rstrip("/")
    if exists:
        # DELETE then POST (pgstac doesn't support PUT for items)
        delete_url = f"{base_url}/collections/{collection_id}/items/{item.id}"
        client._stac_io.session.delete(delete_url, timeout=30)
        logger.info(f"Deleted existing {item.id}")

    # POST new/updated item
    create_url = f"{base_url}/collections/{collection_id}/items"
    resp = client._stac_io.session.post(
        create_url,
        json=item.to_dict(),
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    logger.info(f"✅ Registered {item.id} (HTTP {resp.status_code})")


# === Augmentation ===


def add_projection_from_zarr(item: Item) -> None:
    """Add ProjectionExtension from first zarr asset's spatial_ref attribute."""
    for asset in item.assets.values():
        if asset.media_type and asset.media_type.startswith("application/vnd.zarr") and asset.href:
            try:
                store = zarr.open(asset.href, mode="r")
                if (spatial_ref := store.attrs.get("spatial_ref")) and (
                    epsg := spatial_ref.get("spatial_ref")
                ):
                    proj = ProjectionExtension.ext(item, add_if_missing=True)
                    proj.epsg = int(epsg)
                    if wkt := spatial_ref.get("crs_wkt"):
                        proj.wkt2 = wkt
                    logger.debug(f"Added projection EPSG:{epsg}")
                    return
            except Exception as e:
                logger.debug(f"Could not read zarr projection: {e}")


def add_visualization_links(item: Item, raster_base: str, collection_id: str) -> None:
    """Add viewer/xyz/tilejson links for TiTiler visualization."""
    base_url = f"{raster_base}/collections/{collection_id}/items/{item.id}"
    item.add_link(Link("viewer", f"{base_url}/viewer", "text/html", f"Viewer for {item.id}"))

    # Mission-specific tile configurations
    coll_lower = collection_id.lower()
    if coll_lower.startswith(("sentinel-1", "sentinel1")):
        # S1: VH band visualization
        if (vh := item.assets.get("vh")) and ".zarr/" in (vh.href or ""):
            var_path = f"/{vh.href.split('.zarr/')[1]}:grd"
            query = f"variables={urllib.parse.quote(var_path, safe='')}&bidx=1&rescale=0%2C219&assets=vh"
            item.add_link(
                Link(
                    "xyz",
                    f"{base_url}/tiles/WebMercatorQuad/{{z}}/{{x}}/{{y}}.png?{query}",
                    "image/png",
                    "Sentinel-1 GRD VH",
                )
            )
            item.add_link(
                Link(
                    "tilejson",
                    f"{base_url}/WebMercatorQuad/tilejson.json?{query}",
                    "application/json",
                    f"TileJSON for {item.id}",
                )
            )
    elif coll_lower.startswith(("sentinel-2", "sentinel2")):
        # S2: True color from reflectance bands (B04=Red, B03=Green, B02=Blue)
        query = "rescale=0%2C1&color_formula=gamma+rgb+1.3%2C+sigmoidal+rgb+6+0.1%2C+saturation+1.2&variables=%2Fmeasurements%2Freflectance%3Ab04&variables=%2Fmeasurements%2Freflectance%3Ab03&variables=%2Fmeasurements%2Freflectance%3Ab02&bidx=1"
        item.add_link(
            Link(
                "xyz",
                f"{base_url}/tiles/WebMercatorQuad/{{z}}/{{x}}/{{y}}.png?{query}",
                "image/png",
                "Sentinel-2 L2A True Color",
            )
        )
        item.add_link(
            Link(
                "tilejson",
                f"{base_url}/WebMercatorQuad/tilejson.json?{query}",
                "application/json",
                f"TileJSON for {item.id}",
            )
        )

    # Add EOPF Explorer link
    item.add_link(
        Link(
            "via",
            f"{EXPLORER_BASE}/collections/{collection_id.lower().replace('_', '-')}/items/{item.id}",
            title="EOPF Explorer",
        )
    )


def add_thumbnail_asset(item: Item, raster_base: str, collection_id: str) -> None:
    """Add thumbnail preview asset for STAC browsers."""
    if "thumbnail" in item.assets:
        return

    base_url = f"{raster_base}/collections/{collection_id}/items/{item.id}"
    coll_lower = collection_id.lower()

    # Mission-specific thumbnail parameters
    if coll_lower.startswith(("sentinel-2", "sentinel2")):
        params = "format=png&rescale=0%2C1&color_formula=gamma+rgb+1.3%2C+sigmoidal+rgb+6+0.1%2C+saturation+1.2&variables=%2Fmeasurements%2Freflectance%3Ab04&variables=%2Fmeasurements%2Freflectance%3Ab03&variables=%2Fmeasurements%2Freflectance%3Ab02&bidx=1"
        title = "Sentinel-2 L2A True Color Preview"
    elif coll_lower.startswith(("sentinel-1", "sentinel1")):
        # Use VH band for S-1 thumbnail
        if (vh := item.assets.get("vh")) and ".zarr/" in (vh.href or ""):
            var_path = f"/{vh.href.split('.zarr/')[1]}:grd"
            params = f"format=png&variables={urllib.parse.quote(var_path, safe='')}&bidx=1&rescale=0%2C219&assets=vh"
            title = "Sentinel-1 GRD VH Preview"
        else:
            logger.debug("No VH asset found for S-1 thumbnail")
            return
    else:
        logger.debug(f"Unknown mission for thumbnail: {collection_id}")
        return

    thumbnail = Asset(
        href=f"{base_url}/preview?{params}",
        media_type="image/png",
        roles=["thumbnail"],
        title=title,
    )
    item.add_asset("thumbnail", thumbnail)
    logger.debug(f"Added thumbnail asset: {title}")


def warm_thumbnail_cache(item: Item) -> None:
    """Request thumbnail URL to warm the cache.

    This makes a single request to the thumbnail asset URL, which triggers
    titiler to generate and cache the thumbnail in Redis. Subsequent requests
    will get instant responses from the cache.

    Failures are logged but don't stop registration - cache warming is best-effort.
    """
    thumbnail = item.assets.get("thumbnail")
    if not thumbnail or not thumbnail.href:
        logger.debug("No thumbnail asset to warm cache")
        return

    thumbnail_url = thumbnail.href
    logger.info(f"   🔥 Warming cache: {thumbnail_url}")

    try:
        # Make request with generous timeout (first generation can be slow)
        with httpx.Client(timeout=60.0, follow_redirects=True) as http:
            resp = http.get(thumbnail_url)
            resp.raise_for_status()

            # Log success with response size
            size_kb = len(resp.content) / 1024
            logger.info(f"   ✅ Cache warmed: {size_kb:.1f} KB thumbnail generated")

    except httpx.TimeoutException:
        logger.warning("   ⚠️  Cache warming timed out (thumbnail may be very large)")
    except httpx.HTTPError as e:
        logger.warning(f"   ⚠️  Cache warming failed: {e}")
    except Exception as e:
        logger.warning(f"   ⚠️  Cache warming error: {e}")


def add_store_link(item: Item, geozarr_url: str) -> None:
    """Add store link pointing to the root Zarr location.

    Following the Multiscale reflectance group representation best practices,
    the store link provides the root Zarr location for clients to discover
    the Zarr hierarchy.
    """
    # Remove existing store links to avoid duplicates
    item.links = [link for link in item.links if link.rel != "store"]

    # Convert S3 URL to HTTPS for the store link
    store_href = geozarr_url
    if store_href.startswith("s3://"):
        store_href = s3_to_https(store_href)

    item.add_link(
        Link(
            "store",
            store_href,
            # TODO remove temporary comment when application/vnd.zarr; version=3 is supported by zarr-extension
            # "application/vnd.zarr; version=3",
            "application/octet-stream",
            "Zarr Store",
        )
    )
    logger.debug(f"Added store link: {store_href}")


def add_derived_from_link(item: Item, source_url: str) -> None:
    """Add derived_from link pointing to original source item."""
    # Remove existing derived_from links to avoid duplicates
    item.links = [link for link in item.links if link.rel != "derived_from"]

    item.add_link(
        Link(
            "derived_from",
            source_url,
            "application/json",
            "Derived from original Zarr STAC item",
        )
    )
    logger.debug(f"Added derived_from link: {source_url}")


def remove_xarray_integration(item: Item) -> None:
    """Remove XArray-specific fields from assets (ADR-111 compliance)."""
    removed_count = 0
    for asset in item.assets.values():
        if hasattr(asset, "extra_fields"):
            # Remove xarray-specific fields
            if asset.extra_fields.pop("xarray:open_dataset_kwargs", None):
                removed_count += 1
            if asset.extra_fields.pop("xarray:open_datatree_kwargs", None):
                removed_count += 1

            # Remove alternate xarray configurations
            if "alternate" in asset.extra_fields and isinstance(
                asset.extra_fields["alternate"], dict
            ):
                if asset.extra_fields["alternate"].pop("xarray", None):
                    removed_count += 1
                # Remove empty alternate section
                if not asset.extra_fields["alternate"]:
                    asset.extra_fields.pop("alternate")

    if removed_count > 0:
        logger.debug(f"Removed {removed_count} XArray integration field(s)")


def add_alternate_s3_assets(item: Item, s3_endpoint: str) -> None:
    """Add alternate S3 URLs to assets using the alternate and storage extensions.

    For each asset with an HTTPS URL pointing to S3, adds an alternate representation
    with the S3 URI and storage extension metadata.

    Args:
        item: STAC item to modify
        s3_endpoint: S3 endpoint URL (used to extract region metadata)
    """
    # Add alternate and storage extensions to the item if not present
    extensions = [
        "https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json",
        "https://stac-extensions.github.io/storage/v2.0.0/schema.json",
    ]

    if not hasattr(item, "stac_extensions"):
        item.stac_extensions = []

    for ext in extensions:
        if ext not in item.stac_extensions:
            item.stac_extensions.append(ext)

    # Extract region from endpoint
    region = extract_region_from_endpoint(s3_endpoint)

    # Add alternate to each asset with data role that has an HTTPS URL
    modified_count = 0
    for asset in item.assets.values():
        if not asset.href or not asset.href.startswith("https://"):
            continue

        # Skip thumbnail and other non-data assets
        if asset.roles and "thumbnail" in asset.roles:
            continue

        # Convert HTTPS URL to S3 URL
        s3_url = https_to_s3(asset.href)
        if not s3_url:
            continue

        # Query storage class for this asset
        storage_tier = get_s3_storage_class(s3_url, s3_endpoint)

        # Add alternate with storage extension fields (v2.0 format)
        if not hasattr(asset, "extra_fields"):
            asset.extra_fields = {}

        # Preserve existing alternate structure if present
        existing_alternate = asset.extra_fields.get("alternate", {})
        if not isinstance(existing_alternate, dict):
            existing_alternate = {}

        # Get existing s3 alternate or create new one
        existing_s3 = existing_alternate.get("s3", {})
        if not isinstance(existing_s3, dict):
            existing_s3 = {}

        # Get or create storage:scheme object (v2.0 format)
        storage_scheme = existing_s3.get("storage:scheme", {})
        if not isinstance(storage_scheme, dict):
            storage_scheme = {}

        # Update scheme fields
        storage_scheme["platform"] = "OVHcloud"
        storage_scheme["region"] = region
        storage_scheme["requester_pays"] = False

        # Add tier to scheme (standard field in v2.0)
        if storage_tier:
            storage_scheme["tier"] = storage_tier

        # Update s3 alternate (preserving any existing fields)
        s3_alternate = {
            **existing_s3,  # Preserve existing fields
            "href": s3_url,
            "storage:scheme": storage_scheme,
        }

        # Preserve other alternate formats (e.g., alternate.xarray if it exists)
        existing_alternate["s3"] = s3_alternate
        asset.extra_fields["alternate"] = existing_alternate
        modified_count += 1

    if modified_count > 0:
        logger.info(f"   🔗 Added S3 alternates to {modified_count} asset(s)")


def consolidate_reflectance_assets(item: Item, geozarr_url: str) -> None:
    """Consolidate multiple resolution/band assets into single reflectance asset.

    Transforms old structure (SR_10m, SR_20m, SR_60m, B01_20m, etc.) into new
    structure with single 'reflectance' asset containing bands, cube:variables,
    and cube:dimensions following the Multiscale reflectance group representation
    best practices.
    """
    # Check if there's already a reflectance asset with the new structure
    if "reflectance" in item.assets:
        reflectance = item.assets["reflectance"]
        if hasattr(reflectance, "extra_fields") and "cube:variables" in reflectance.extra_fields:
            logger.debug("Reflectance asset already in new format, skipping consolidation")
            return

    # Collect band information from existing assets
    bands_info = {}  # Use dict to deduplicate by band name
    resolutions = {"r10m": 10, "r20m": 20, "r60m": 60}

    # Gather all reflectance bands from old-style assets
    for key, asset in list(item.assets.items()):
        # Extract band info from individual band assets (e.g., B01_20m, B02_10m)
        if key.startswith("B") and "_" in key and asset.roles and "reflectance" in asset.roles:
            # Parse resolution from key (e.g., B01_20m -> 20m)
            parts = key.split("_")
            if len(parts) == 2:
                band_name = parts[0].lower()
                res = parts[1]  # e.g., "10m", "20m", "60m"
                res_key = f"r{res}"  # e.g., "r10m", "r20m"

                # Get band metadata from the asset's bands array
                if hasattr(asset, "extra_fields") and "bands" in asset.extra_fields:
                    band_list = asset.extra_fields["bands"]
                    if band_list and len(band_list) > 0:
                        band_data = band_list[0].copy()
                        # Use just the band name without resolution prefix (best practice)
                        band_data["name"] = band_name
                        band_data["gsd"] = resolutions.get(res_key, 10)
                        if "proj:shape" in asset.extra_fields:
                            band_data["proj:shape"] = asset.extra_fields["proj:shape"]
                        bands_info[band_name] = band_data

    # If no bands found from individual assets, try SR_* assets
    if not bands_info:
        for key, asset in list(item.assets.items()):
            if (
                key.startswith("SR_")
                and hasattr(asset, "extra_fields")
                and "bands" in asset.extra_fields
            ):
                res = key.replace("SR_", "").lower()  # e.g., "10m", "20m"
                res_key = f"r{res}"
                gsd = resolutions.get(res_key, 10)

                for band in asset.extra_fields.get("bands", []):
                    band_data = band.copy()
                    band_name = band_data.get("name", "").lower()
                    # Use just the band name without resolution prefix (best practice)
                    band_data["name"] = band_name
                    band_data["gsd"] = gsd
                    # Only add if not already present (prefer higher resolution)
                    if band_name not in bands_info or bands_info[band_name]["gsd"] > gsd:
                        bands_info[band_name] = band_data

    # Remove all old reflectance assets
    assets_removed = []
    for key in list(item.assets.keys()):
        if (
            key.startswith("SR_")
            or (
                key.startswith("B")
                and "_" in key
                and any(key.endswith(f"_{res}") for res in ["10m", "20m", "60m"])
            )
            or key == "TCI_10m"
            or key == "product"
            or key == "product_metadata"
        ):
            assets_removed.append(key)
            item.assets.pop(key)

    if not bands_info:
        logger.warning("No band information found to create reflectance asset")
        return

    # Convert dict to sorted list
    bands_list = sorted(bands_info.values(), key=lambda x: x.get("name", ""))

    # Build cube:variables from bands
    cube_variables = {}
    for band in bands_list:
        band_name = band["name"]
        cube_variables[band_name] = {
            "dimensions": ["y", "x"],
            "description": band.get("description", ""),
            "type": "data",
        }

    # Build cube:dimensions with extent (use projection info from item if available)
    proj_bbox = item.properties.get("proj:bbox", [])
    cube_dimensions = {
        "x": {
            "type": "spatial",
            "axis": "x",
            "reference_system": item.properties.get("proj:code", "EPSG:32632"),
        },
        "y": {
            "type": "spatial",
            "axis": "y",
            "reference_system": item.properties.get("proj:code", "EPSG:32632"),
        },
    }

    # Add extent if proj:bbox is available (assuming it's in projected coordinates)
    if len(proj_bbox) >= 4 and isinstance(proj_bbox[0], int | float):
        # proj:bbox might be in lat/lon, need to check if we have projected bbox
        # For now, we'll use proj:transform and proj:shape to compute extent
        proj_transform = item.properties.get("proj:transform")
        proj_shape = item.properties.get("proj:shape", [10980, 10980])

        if proj_transform and len(proj_transform) >= 6:
            # GeoTransform: [x_min, pixel_width, 0, y_max, 0, -pixel_height]
            x_min = proj_transform[0]
            pixel_width = proj_transform[1]
            y_max = proj_transform[3]
            pixel_height = abs(proj_transform[5])

            x_max = x_min + (proj_shape[1] * pixel_width)
            y_min = y_max - (proj_shape[0] * pixel_height)

            cube_dimensions["x"]["extent"] = [x_min, x_max]
            cube_dimensions["y"]["extent"] = [y_min, y_max]

    # Create new reflectance asset
    reflectance_href = f"{geozarr_url}/measurements/reflectance"
    if reflectance_href.startswith("s3://"):
        reflectance_href = s3_to_https(reflectance_href)

    reflectance_asset = Asset(
        href=reflectance_href,
        media_type="application/vnd+zarr; version=3; profile=multiscales",
        title="Surface Reflectance",
        roles=["data", "reflectance"],
        extra_fields={
            "gsd": 10,
            "proj:code": item.properties.get("proj:code", "EPSG:32632"),
            "proj:shape": [10980, 10980],
            "bands": bands_list,
            "cube:dimensions": cube_dimensions,
            "cube:variables": cube_variables,
        },
    )

    item.assets["reflectance"] = reflectance_asset

    logger.info(
        f"   🔧 Consolidated {len(assets_removed)} assets into single 'reflectance' asset with {len(bands_list)} bands"
    )


# === Registration Workflow ===


def run_registration(
    source_url: str,
    collection: str,
    stac_api_url: str,
    raster_api_url: str,
    s3_endpoint: str,
    s3_output_bucket: str,
    s3_output_prefix: str,
) -> None:
    """Run STAC registration workflow with asset rewriting and augmentation.

    Args:
        source_url: Source STAC item URL
        collection: Target collection ID
        stac_api_url: STAC API base URL
        raster_api_url: TiTiler raster API base URL
        s3_endpoint: S3 endpoint for HTTP access
        s3_output_bucket: S3 bucket name
        s3_output_prefix: S3 prefix path

    Raises:
        RuntimeError: If registration fails
    """
    path_segment = urlparse(source_url).path.rstrip("/").split("/")[-1]
    item_id = os.path.splitext(path_segment)[0]
    geozarr_url = f"s3://{s3_output_bucket}/{s3_output_prefix}/{collection}/{item_id}.zarr"

    logger.info(f"📝 Registering: {item_id}")
    logger.info(f"   Collection: {collection}")

    # 1. Fetch source item and clone with new collection
    with httpx.Client(timeout=30.0, follow_redirects=True) as http:
        resp = http.get(source_url)
        resp.raise_for_status()

        # Filter out assets with missing href to handle malformed source items
        source_data = resp.json()
        if "assets" in source_data:
            original_asset_count = len(source_data["assets"])
            # Remove assets that don't have an href
            source_data["assets"] = {
                key: asset for key, asset in source_data["assets"].items() if "href" in asset
            }
            removed_count = original_asset_count - len(source_data["assets"])
            if removed_count > 0:
                logger.warning(
                    f"   ⚠️  Removed {removed_count} asset(s) with missing href from source item"
                )

        source_item = Item.from_dict(source_data)

    item = source_item.clone()
    item.collection_id = collection
    logger.info(f"   📥 Fetched source item with {len(item.assets)} assets")

    # 2. Rewrite asset hrefs from source zarr to output geozarr
    source_zarr_base = next(
        (
            a.href.split(".zarr/")[0] + ".zarr"
            for a in item.assets.values()
            if a.href and ".zarr/" in a.href
        ),
        None,
    )
    if source_zarr_base:
        logger.info(f"   🔗 Rewriting {len(item.assets)} asset hrefs")
        logger.debug(f"      From: {source_zarr_base}")
        logger.debug(f"      To:   {geozarr_url}")
        rewrite_asset_hrefs(item, source_zarr_base, geozarr_url)
    else:
        logger.warning("   ⚠️  No source zarr found - assets not rewritten")

    # 3. Add store link to root Zarr location (best practice)
    add_store_link(item, geozarr_url)

    # 4. Consolidate reflectance assets into single asset with bands/cube metadata
    consolidate_reflectance_assets(item, geozarr_url)

    # 5. Add projection metadata from zarr
    add_projection_from_zarr(item)

    # 6. Remove XArray integration fields (ADR-111 compliance)
    remove_xarray_integration(item)

    # 7. Add alternate S3 URLs to assets (alternate-assets + storage extensions)
    # This also queries and adds storage:tier to each asset's alternate
    add_alternate_s3_assets(item, s3_endpoint)

    # 8. Add visualization links (viewer, xyz, tilejson)
    add_visualization_links(item, raster_api_url, collection)
    logger.info("   🎨 Added visualization links")

    # 10. Add thumbnail asset for STAC browsers
    add_thumbnail_asset(item, raster_api_url, collection)

    # 11. Warm thumbnail cache
    warm_thumbnail_cache(item)

    # 12. Add derived_from link to source item
    add_derived_from_link(item, source_url)

    # 13. Register to STAC API
    client = Client.open(stac_api_url)
    upsert_item(client, collection, item)

    logger.info(
        f"✅ Registration complete → {stac_api_url}/collections/{collection}/items/{item_id}"
    )


def main(argv: list[str] | None = None) -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Register GeoZarr product to STAC")
    parser.add_argument("--source-url", required=True, help="Source STAC item URL")
    parser.add_argument("--collection", required=True, help="Target collection ID")
    parser.add_argument("--stac-api-url", required=True, help="STAC API base URL")
    parser.add_argument("--raster-api-url", required=True, help="TiTiler raster API base URL")
    parser.add_argument("--s3-endpoint", required=True, help="S3 endpoint for HTTP access")
    parser.add_argument("--s3-output-bucket", required=True, help="S3 bucket name")
    parser.add_argument("--s3-output-prefix", required=True, help="S3 prefix path")

    args = parser.parse_args(argv)

    try:
        run_registration(
            args.source_url,
            args.collection,
            args.stac_api_url,
            args.raster_api_url,
            args.s3_endpoint,
            args.s3_output_bucket,
            args.s3_output_prefix,
        )
        return 0
    except Exception as e:
        logger.error(f"Registration failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
