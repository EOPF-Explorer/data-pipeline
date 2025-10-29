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
from pystac import Item, Link
from pystac.extensions.projection import ProjectionExtension
from pystac_client import Client

# Configure logging (set LOG_LEVEL=DEBUG for verbose output)
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)
for lib in ["botocore", "s3fs", "aiobotocore", "urllib3"]:
    logging.getLogger(lib).setLevel(logging.WARNING)

EXPLORER_BASE = os.getenv("EXPLORER_BASE_URL", "https://explorer.eopf.copernicus.eu")


# === Utilities ===


def s3_to_https(s3_url: str, endpoint: str) -> str:
    """Convert s3:// URL to https:// using endpoint."""
    if not s3_url.startswith("s3://"):
        return s3_url
    parsed = urlparse(s3_url)
    host = urlparse(endpoint).netloc or urlparse(endpoint).path
    return f"https://{parsed.netloc}.{host}{parsed.path}"


def rewrite_asset_hrefs(item: Item, old_base: str, new_base: str, s3_endpoint: str) -> None:
    """Rewrite all asset hrefs from old_base to new_base (in place)."""
    old_base = old_base.rstrip("/") + "/"
    new_base = new_base.rstrip("/") + "/"

    for key, asset in item.assets.items():
        if asset.href and asset.href.startswith(old_base):
            new_href = new_base + asset.href[len(old_base) :]
            if new_href.startswith("s3://"):
                new_href = s3_to_https(new_href, s3_endpoint)
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

    stac_url = str(client.self_href).rstrip("/stac")  # Remove /stac suffix if present
    if exists:
        # DELETE then POST (pgstac doesn't support PUT for items)
        delete_url = f"{stac_url}/collections/{collection_id}/items/{item.id}"
        client._stac_io.session.delete(delete_url, timeout=30)
        logger.info(f"Deleted existing {item.id}")

    # POST new/updated item
    create_url = f"{stac_url}/collections/{collection_id}/items"
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
        if asset.media_type == "application/vnd+zarr" and asset.href:
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
        # S2: True color quicklook
        var_path = "/quality/l2a_quicklook/r10m:tci"
        query = (
            f"variables={urllib.parse.quote(var_path, safe='')}&bidx=1&bidx=2&bidx=3&assets=TCI_10m"
        )
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
    item_id = urlparse(source_url).path.rstrip("/").split("/")[-1]
    geozarr_url = f"s3://{s3_output_bucket}/{s3_output_prefix}/{collection}/{item_id}.zarr"

    logger.info(f"Registering {item_id} → {collection}")

    # 1. Fetch source item and clone with new collection
    with httpx.Client(timeout=30.0, follow_redirects=True) as http:
        resp = http.get(source_url)
        resp.raise_for_status()
        source_item = Item.from_dict(resp.json())

    item = source_item.clone()
    item.collection_id = collection

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
        logger.info(f"Rewriting assets: {source_zarr_base} → {geozarr_url}")
        rewrite_asset_hrefs(item, source_zarr_base, geozarr_url, s3_endpoint)
    else:
        logger.warning("No source zarr found - assets not rewritten")

    # 3. Add projection metadata from zarr
    add_projection_from_zarr(item)

    # 4. Add visualization links (viewer, xyz, tilejson)
    add_visualization_links(item, raster_api_url, collection)

    # 5. Register to STAC API
    client = Client.open(stac_api_url)
    upsert_item(client, collection, item)

    logger.info(f"✅ Complete: {stac_api_url}/collections/{collection}/items/{item_id}")


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
