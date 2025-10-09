#!/usr/bin/env python3
"""STAC item augmentation utilities."""

from __future__ import annotations

import argparse
import os
import sys
import urllib.parse
from collections.abc import Sequence
from typing import Any

import httpx
import s3fs
import zarr
from pystac import Asset, Item, Link
from pystac.extensions.projection import ProjectionExtension

_TRUE_COLOR_BANDS = ["b04", "b03", "b02"]
_TRUE_COLOR_FORMULA = "Gamma RGB 1.4"
_DEFAULT_TRUE_COLOR_RESCALE = "0,0.1"


def _encode_true_color_query(rescale: str) -> str:
    # Use /0 subgroup to access overview level 0 (native resolution with overviews)
    pairs = [
        ("variables", f"/measurements/reflectance/r10m/0:{band}") for band in _TRUE_COLOR_BANDS
    ]
    pairs.extend(("rescale", rescale) for _ in _TRUE_COLOR_BANDS)
    pairs.append(("color_formula", _TRUE_COLOR_FORMULA))
    return "&".join(f"{key}={urllib.parse.quote_plus(value)}" for key, value in pairs)


DEFAULT_TRUE_COLOR_QUERY = _encode_true_color_query(_DEFAULT_TRUE_COLOR_RESCALE)


def _encode_quicklook_query() -> str:
    # TCI quicklook in converted GeoZarr (r10m has no overview subdirs)
    pairs = [
        ("variables", "/quality/l2a_quicklook/r10m:tci"),
        ("bidx", "1"),
        ("bidx", "2"),
        ("bidx", "3"),
    ]
    return "&".join(f"{key}={urllib.parse.quote_plus(value)}" for key, value in pairs)


DEFAULT_QUICKLOOK_QUERY = _encode_quicklook_query()


def _get_s1_polarization(item: Item) -> str:
    """Extract first available polarization from S1 item assets.

    Args:
        item: PySTAC Item with S1 assets

    Returns:
        Uppercase polarization code (VH, VV, HH, or HV). Defaults to VH.
    """
    for pol in _S1_POLARIZATIONS:
        if pol in item.assets:
            return pol.upper()
    return "VH"


def _encode_s1_preview_query(item: Item) -> str:
    """Generate S1 GRD preview query for TiTiler.

    S1 GRD structure in converted GeoZarr:
    /S01SIWGRD_{timestamp}_{id}_VH/measurements with grd variable

    TiTiler needs the full path to the measurements group with the grd variable.

    Args:
        item: PySTAC Item with S1 GRD data

    Returns:
        Query string for TiTiler (variables, bidx, rescale)
    """
    pol = _get_s1_polarization(item)
    asset = item.assets.get(pol.lower())

    if not asset or not asset.href:
        # Fallback to simple path
        pairs = [
            ("variables", "/measurements:grd"),
            ("bidx", "1"),
            ("rescale", "0,219"),
        ]
        return "&".join(f"{key}={urllib.parse.quote_plus(value)}" for key, value in pairs)

    # Extract group path from asset href
    # Example: s3://.../S01SIWGRD_..._VH/measurements -> /S01SIWGRD_..._VH/measurements:grd
    href = asset.href
    if ".zarr/" in href:
        # Extract path after .zarr/
        zarr_path = href.split(".zarr/")[1]
        # zarr_path is like: S01SIWGRD_..._VH/measurements
        # Build variable reference: /S01SIWGRD_..._VH/measurements:grd
        variable_path = f"/{zarr_path}:grd"
    else:
        # Fallback
        variable_path = "/measurements:grd"

    pairs = [
        ("variables", variable_path),
        ("bidx", "1"),
        ("rescale", "0,219"),  # Typical S1 GRD range
    ]
    return "&".join(f"{key}={urllib.parse.quote_plus(value)}" for key, value in pairs)


_ALLOWED_SCHEMES = {"http", "https"}
_USER_AGENT = "augment-stac-item/1.0"
_DEFAULT_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "30"))

_PROJECTION_EXTRA_KEYS = (
    "proj:code",
    "proj:epsg",
    "proj:shape",
    "proj:transform",
    "proj:bbox",
)

_ITEM_PROJECTION_FIELDS = frozenset({"code", "bbox", "shape", "transform"})

_S2_COLLECTION_ID = "sentinel-2-l2a"
_S2_DATASET_KEYS = ("SR_10m", "SR_20m", "SR_60m")
_S2_QUICKLOOK_KEYS = ("TCI_10m", "TCI", "TCI_20m")

_S1_COLLECTION_ID = "sentinel-1-l1-grd"
_S1_POLARIZATIONS = ("vh", "vv", "hh", "hv")


def _is_s1_collection(collection_id: str) -> bool:
    """Check if collection is Sentinel-1 GRD."""
    return collection_id.startswith("sentinel-1-l1-grd")


def _coerce_epsg(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        trimmed = value.strip()
        if not trimmed:
            return None
        if trimmed.isdigit():
            return int(trimmed)
        upper = trimmed.upper()
        if upper.startswith("EPSG:"):
            suffix = upper.split("EPSG:", 1)[1]
            return _coerce_epsg(suffix)
    return None


def warn(message: str) -> None:
    print(f"[augment] {message}", file=sys.stderr)


def _resolve_preview_query(
    env_value: str | None,
    *,
    default_query: str,
) -> str:
    if env_value is None:
        return default_query
    trimmed = env_value.strip()
    if not trimmed:
        return ""
    return trimmed


def _asset_extras(asset: Asset) -> dict[str, Any] | None:
    extra = getattr(asset, "extra_fields", None)
    return extra if isinstance(extra, dict) else None


def clean_xarray_metadata(asset: Asset) -> None:
    """Remove deprecated xarray metadata from asset.

    Cleans up metadata from the legacy eopf-zarr xarray engine which is no
    longer used. The xarray integration was deprecated in favor of direct
    Zarr access via zarr-python and kerchunk for cloud-optimized access.

    Specifically removes:
    - xarray:open_datatree_kwargs from asset extra_fields
    - xarray alternate from asset alternates

    This cleanup prevents confusion for STAC clients and ensures only
    current, supported access methods are advertised.

    Args:
        asset: PySTAC Asset object (modified in place)

    Example:
        Input asset.extra_fields:
        {
            "xarray:open_datatree_kwargs": {"engine": "eopf-zarr"},
            "alternate": {
                "xarray": {"href": "..."},
                "s3": {"href": "..."}
            }
        }

        After cleanup:
        {
            "alternate": {
                "s3": {"href": "..."}
            }
        }
    """
    extra = _asset_extras(asset)
    if extra is None:
        return
    extra.pop("xarray:open_datatree_kwargs", None)
    alt = extra.get("alternate")
    if isinstance(alt, dict):
        alt.pop("xarray", None)
        if not alt:
            extra.pop("alternate", None)


def normalize_href_scheme(href: str) -> str:
    """Normalize asset href to canonical S3 scheme.

    Converts various HTTPS S3 URL patterns to canonical s3:// format for consistency.
    This normalization enables uniform handling of cloud storage references across
    different URL representations from OVH Cloud Storage and similar providers.

    Handles these URL patterns:
    - https://s3.region.cloud.ovh.net/bucket/key → s3://bucket/key
    - https://bucket.s3.region.cloud.ovh.net/key → s3://bucket/key

    Args:
        href: Asset href URL (s3://, https://, or other scheme)

    Returns:
        Normalized href with s3:// scheme if applicable, otherwise unchanged

    Examples:
        >>> normalize_href_scheme("https://s3.gra.cloud.ovh.net/mybucket/data.zarr")
        "s3://mybucket/data.zarr"
        >>> normalize_href_scheme("https://mybucket.s3.gra.cloud.ovh.net/data.zarr")
        "s3://mybucket/data.zarr"
        >>> normalize_href_scheme("s3://mybucket/data.zarr")
        "s3://mybucket/data.zarr"
    """
    if not href or href.startswith("s3://"):
        return href
    try:
        parsed = urllib.parse.urlparse(href)
    except Exception:
        return href
    if parsed.scheme not in _ALLOWED_SCHEMES:
        return href
    host = parsed.netloc.split(":", 1)[0].lower()
    path = parsed.path.lstrip("/")
    allowed_suffixes = (".cloud.ovh.net", ".io.cloud.ovh.net")
    if not any(host.endswith(suffix) for suffix in allowed_suffixes):
        return href
    if host.startswith("s3.") and "/" in path:
        bucket, key = path.split("/", 1)
        return f"s3://{bucket}/{key}" if bucket and key else href
    if ".s3." in host:
        bucket = host.split(".s3.", 1)[0]
        if bucket:
            return f"s3://{bucket}/{path}" if path else f"s3://{bucket}"
    return href


def resolve_preview_asset_href(href: str) -> str:
    """Resolve preview asset href to full-resolution dataset location.

    Converts preview asset paths to their full-resolution equivalents by:
    - Replacing /previews/ directory with /sentinel-2-l2a/
    - Removing _preview.zarr suffix to reference the complete dataset

    This transformation enables preview items (which contain downsampled/overview
    data for faster loading) to reference the full-resolution dataset for
    complete visualization and analysis.

    Args:
        href: S3 URL to asset (may be preview or full resolution)

    Returns:
        S3 URL to full-resolution dataset, or original href if not a preview

    Examples:
        >>> resolve_preview_asset_href("s3://bucket/previews/S2B_20250518_preview.zarr/data")
        "s3://bucket/sentinel-2-l2a/S2B_20250518.zarr/data"
        >>> resolve_preview_asset_href("s3://bucket/sentinel-2-l2a/S2B_20250518.zarr/data")
        "s3://bucket/sentinel-2-l2a/S2B_20250518.zarr/data"
        >>> resolve_preview_asset_href("https://example.com/data")
        "https://example.com/data"
    """
    if not href or not href.startswith("s3://"):
        return href
    try:
        parsed = urllib.parse.urlsplit(href)
    except Exception:
        return href
    bucket = parsed.netloc
    path = parsed.path.lstrip("/")
    if not bucket or not path:
        return href
    parts = path.split("/")
    try:
        previews_idx = parts.index("previews")
    except ValueError:
        return href
    if previews_idx + 1 >= len(parts):
        return href
    store = parts[previews_idx + 1]
    suffix = "_preview.zarr"
    if not store.endswith(suffix):
        return href
    promoted_store = f"{store[: -len(suffix)]}.zarr"
    parts[previews_idx] = "sentinel-2-l2a"
    parts[previews_idx + 1] = promoted_store
    new_path = "/".join(parts)
    return urllib.parse.urlunsplit((parsed.scheme, bucket, f"/{new_path}", "", ""))


def normalize_asset_alternate_schemes(asset: Asset) -> None:
    """Normalize alternate asset hrefs to canonical scheme.

    Ensures all alternate hrefs in asset.extra_fields['alternate'] use
    consistent s3:// scheme and reference full-resolution datasets.

    This normalization:
    - Converts HTTPS S3 URLs to canonical s3:// format
    - Resolves preview paths to full-resolution datasets
    - Removes empty alternate entries

    Alternate hrefs are used by clients to access the same data through
    different protocols or locations. Normalizing ensures consistent
    behavior across different access patterns.

    Args:
        asset: PySTAC Asset object (modified in place)

    Example:
        Input asset with alternate:
        {
            "s3": {"href": "https://bucket.s3.ovh.net/previews/data_preview.zarr"},
            "https": {"href": "https://example.com/data"}
        }

        After normalization:
        {
            "s3": {"href": "s3://bucket/sentinel-2-l2a/data.zarr"},
            "https": {"href": "https://example.com/data"}
        }
    """
    extra = _asset_extras(asset)
    if not extra:
        return
    alternates = extra.get("alternate")
    if not isinstance(alternates, dict):
        return
    for name, data in list(alternates.items()):
        href = data.get("href") if isinstance(data, dict) else None
        if isinstance(href, str):
            new_href = resolve_preview_asset_href(normalize_href_scheme(href))
            if new_href != href:
                data["href"] = new_href
                alternates[name] = data
    if not alternates:
        extra.pop("alternate", None)


def add_asset_title(asset_key: str, asset: Asset) -> None:
    href = (asset.href or "").lower()
    lowered = asset_key.lower()
    title: str | None = None
    if "tci" in lowered or any(marker in href for marker in (":tci", "/tci")):
        title = os.getenv("PREVIEW_XYZ_TITLE", "True Color Image (10m)")
    elif "scl" in lowered or "scene_classification" in href:
        title = "Scene Classification (SCL)"
    if not title:
        return
    try:
        asset.title = title
    except Exception:  # pragma: no cover - pystac < 1.9 compatibility
        extra = _asset_extras(asset)
        if extra is not None:
            extra["title"] = title


def normalize_zarr_asset_roles(asset: Asset) -> None:
    try:
        href = (asset.href or "").lower()
        media = str(asset.media_type or "").lower()
        if ".zarr" not in href and "zarr" not in media and not asset.roles:
            return
        roles = [role for role in (asset.roles or []) if role != "geozarr"]
        is_metadata = "metadata" in roles or any(
            href.endswith(suffix)
            for suffix in (
                "/.zmetadata",
                ".zmetadata",
            )
        )
        if not is_metadata and "data" not in roles:
            roles.insert(0, "data")
        asset.roles = roles
    except Exception as exc:
        warn(f"normalizing zarr roles failed: {exc}")


def rewrite_asset_alternates(asset: Asset) -> None:
    normalize_asset_alternate_schemes(asset)
    clean_xarray_metadata(asset)


def add_zarr_dataset_hints(asset: Asset) -> None:
    """Add xarray engine configuration hints for Zarr dataset assets.

    Adds xarray:open_dataset_kwargs to asset extra_fields to configure
    the xarray engine for reading Zarr datasets. Uses the eopf-zarr engine
    which provides optimized access to EOPF Zarr stores.

    Only adds hints if:
    - Asset has 'dataset' or 'reflectance' role
    - Asset href points to a Zarr store (.zarr extension or media type)
    - No engine is already configured

    This enables xarray-based clients to properly open the Zarr store
    without manual configuration.

    Args:
        asset: PySTAC Asset object (modified in place)

    Example:
        Input asset:
        {
            "href": "s3://bucket/data.zarr",
            "roles": ["dataset"],
            "extra_fields": {}
        }

        After adding hints:
        {
            "href": "s3://bucket/data.zarr",
            "roles": ["dataset"],
            "extra_fields": {
                "xarray:open_dataset_kwargs": {
                    "chunks": {},
                    "engine": "eopf-zarr",
                    "op_mode": "native"
                }
            }
        }
    """
    extra = _asset_extras(asset)
    if extra is None:
        asset.extra_fields = extra = {}
    current = extra.get("xarray:open_dataset_kwargs")
    if isinstance(current, dict) and current.get("engine"):
        return
    roles = {role.lower() for role in asset.roles or ()}
    if "dataset" not in roles and "reflectance" not in roles:
        return
    href = (asset.href or "").lower()
    media = str(asset.media_type or "").lower()
    if not (href.endswith(".zarr") or ".zarr/" in href or "zarr" in media):
        return
    extra["xarray:open_dataset_kwargs"] = {
        "chunks": {},
        "engine": "eopf-zarr",
        "op_mode": "native",
    }


def _normalize_collection_slug(identifier: str) -> str:
    slug = identifier.strip().lower()
    # Normalize all variations to canonical form with hyphens
    normalized = slug.replace("-", "")
    if normalized == "sentinel2l2a":
        return _S2_COLLECTION_ID
    return slug or _S2_COLLECTION_ID


def _is_quicklook_asset(asset: Asset | None) -> bool:
    if asset is None:
        return False
    roles = {role.lower() for role in asset.roles or ()}
    if any(tag in roles for tag in ("quicklook", "visual")):
        return True
    href = (asset.href or "").lower()
    if "/quality/" in href and "quicklook" in href:
        return True
    title = (asset.title or "").lower()
    return "true color" in title or "quicklook" in title


def _select_quicklook_asset(item: Item) -> str | None:
    for key in _S2_QUICKLOOK_KEYS:
        if _is_quicklook_asset(item.assets.get(key)):
            return key
    for key, asset in item.assets.items():
        if _is_quicklook_asset(asset):
            return key
    return None


def _select_preview_asset(item: Item) -> str | None:
    quicklook = _select_quicklook_asset(item)
    if quicklook:
        return quicklook
    for key in _S2_DATASET_KEYS:
        if key in item.assets:
            return key
    for key, asset in item.assets.items():
        if asset.roles and any(role.lower() == "dataset" for role in asset.roles):
            return key
    return next(iter(item.assets), None)


def add_visualization_links(
    item: Item,
    base_raster_url: str,
    *,
    collection_id: str | None = None,
) -> None:
    coll = collection_id or item.collection_id or "sentinel-2-l2a"
    coll = _normalize_collection_slug(coll)
    filtered_rels = {"viewer", "xyz", "tilejson", "ogc-wmts", "ogc-wms"}
    item.links = [link for link in item.links if link.rel not in filtered_rels]
    item_id = item.id
    viewer_href = f"{base_raster_url}/collections/{coll}/items/{item_id}/viewer"

    # Determine preview query based on collection type
    asset_key: str | None
    if _is_s1_collection(coll):
        # Sentinel-1: Use GRD polarization preview
        default_query = _encode_s1_preview_query(item)
        xyz_title = os.getenv("PREVIEW_XYZ_TITLE", f"GRD {_get_s1_polarization(item)}")
        asset_key = _get_s1_polarization(item).lower()  # vh or vv
    else:
        # Sentinel-2: Use quicklook or true color
        asset_key = _select_preview_asset(item)
        preview_asset = item.assets.get(asset_key) if asset_key else None
        is_quicklook = _is_quicklook_asset(preview_asset)
        default_query = DEFAULT_QUICKLOOK_QUERY if is_quicklook else DEFAULT_TRUE_COLOR_QUERY
        xyz_title = os.getenv("PREVIEW_XYZ_TITLE", "True Color Image (10m)")

    xyz_query = _resolve_preview_query(
        os.getenv("PREVIEW_XYZ_QUERY"),
        default_query=default_query,
    )

    def _add_link(rel: str, target: str, media_type: str, title: str | None = None) -> None:
        item.add_link(
            Link(
                rel=rel,
                target=target,
                media_type=media_type,
                title=title or f"{rel.title()} for {item_id}",
            )
        )

    _add_link("viewer", viewer_href, "text/html")
    item_root = f"{base_raster_url}/collections/{coll}/items/{item_id}"
    xyz_href = f"{item_root}/tiles/WebMercatorQuad/{{z}}/{{x}}/{{y}}.png"
    tilejson_href = f"{item_root}/WebMercatorQuad/tilejson.json"

    # Build query string with asset key for TiTiler
    query_parts = []
    if xyz_query:
        query_parts.append(xyz_query)
    if asset_key:
        query_parts.append(f"assets={asset_key}")

    if query_parts:
        full_query = "&".join(query_parts)
        xyz_href = f"{xyz_href}?{full_query}"
        tilejson_href = f"{tilejson_href}?{full_query}"

    _add_link("xyz", xyz_href, "image/png", xyz_title)
    _add_link("tilejson", tilejson_href, "application/json")
    wmts_href = f"{item_root}/WebMercatorQuad/WMTSCapabilities.xml"
    _add_link("ogc-wmts", wmts_href, "application/xml", "WMTS capabilities")


def dedupe_stac_extensions(item: Item) -> None:
    extensions = list(dict.fromkeys(ext for ext in item.stac_extensions or [] if ext))
    item.stac_extensions = extensions
    if not extensions:
        item.extra_fields.pop("stac_extensions", None)


def normalize_item_assets(item: Item, verbose: bool) -> None:
    for asset_key, asset in list(item.assets.items()):
        original = asset.href
        asset.href = resolve_preview_asset_href(normalize_href_scheme(asset.href or ""))
        if verbose and original != asset.href:
            print(f"[augment] Rewrote href for asset '{asset_key}': {original} -> {asset.href}")
        rewrite_asset_alternates(asset)
        add_zarr_dataset_hints(asset)
        add_asset_title(asset_key, asset)
        normalize_zarr_asset_roles(asset)


def _asset_gsd(asset: Asset) -> float | None:
    gsd = asset.common_metadata.gsd
    if gsd is None:
        extra = _asset_extras(asset)
        raw = extra.get("gsd") if extra else None
        if isinstance(raw, int | float):
            gsd = float(raw)
    return float(gsd) if gsd is not None else None


def _has_projection_metadata(asset: Asset | None) -> bool:
    if asset is None:
        return False
    try:
        ext = ProjectionExtension.ext(asset, add_if_missing=False)
    except Exception:
        ext = None
    if ext is not None:
        projection_values = (ext.code, ext.epsg, ext.transform, ext.bbox, ext.shape)
        if any(value not in (None, [], ()) for value in projection_values):
            return True
    extra = _asset_extras(asset)
    if not extra:
        return False
    return any(extra.get(key) not in (None, [], ()) for key in _PROJECTION_EXTRA_KEYS)


def _projection_snapshot(
    ext: ProjectionExtension[Asset] | ProjectionExtension[Item] | None,
) -> dict[str, object]:
    if ext is None:
        return {}
    return {
        "code": ext.code,
        "epsg": ext.epsg,
        "bbox": ext.bbox,
        "shape": ext.shape,
        "transform": ext.transform,
    }


def _projection_score(snapshot: dict[str, object]) -> int:
    return sum(1 for value in snapshot.values() if value not in (None, [], ()))


def _apply_projection(
    ext: ProjectionExtension[Asset] | ProjectionExtension[Item],
    snapshot: dict[str, object],
    *,
    allow_epsg: bool = True,
) -> None:
    code = snapshot.get("code")
    epsg_value = _coerce_epsg(snapshot.get("epsg"))
    if epsg_value is None:
        epsg_value = _coerce_epsg(code)
    normalized_code: str | None = None
    if isinstance(code, str) and code.strip():
        normalized_code = code.strip()
    elif isinstance(code, int):
        normalized_code = f"EPSG:{code}"
    elif isinstance(code, float):
        normalized_code = f"EPSG:{int(code)}"
    elif epsg_value is not None:
        normalized_code = f"EPSG:{epsg_value}"
    if normalized_code and ext.code is None:
        ext.code = normalized_code
    if allow_epsg and epsg_value is not None and ext.epsg is None:
        ext.epsg = epsg_value

    bbox = snapshot.get("bbox")
    if isinstance(bbox, Sequence) and ext.bbox is None:
        ext.bbox = list(bbox)

    shape = snapshot.get("shape")
    if isinstance(shape, Sequence) and ext.shape is None:
        ext.shape = list(shape)

    transform = snapshot.get("transform")
    if isinstance(transform, Sequence) and ext.transform is None:
        ext.transform = list(transform)


def _read_geozarr_spatial_metadata(item: Item, *, verbose: bool = False) -> None:
    """Read spatial metadata from GeoZarr and populate proj:shape and proj:transform."""
    geozarr_asset = item.assets.get("geozarr")
    if not geozarr_asset or not geozarr_asset.href:
        if verbose:
            warn("No geozarr asset found, skipping spatial metadata extraction")
        return

    href = geozarr_asset.href
    if not href.startswith("s3://"):
        if verbose:
            warn(f"GeoZarr href is not s3:// (got {href}), skipping spatial metadata extraction")
        return

    try:
        # Parse s3://bucket/key path
        s3_parts = href.replace("s3://", "").split("/", 1)
        if len(s3_parts) != 2:
            if verbose:
                warn(f"Invalid S3 path format: {href}")
            return
        bucket, key = s3_parts

        # Determine endpoint from environment or defaults
        endpoint = os.environ.get("AWS_ENDPOINT_URL", "https://s3.de.io.cloud.ovh.net")

        # Create S3 filesystem
        fs = s3fs.S3FileSystem(anon=False, client_kwargs={"endpoint_url": endpoint})

        # Open the Zarr store
        store = s3fs.S3Map(root=f"{bucket}/{key}", s3=fs, check=False)
        root = zarr.open(store, mode="r")

        # Try to read spatial_ref from common paths
        # After conversion changes for TiTiler compatibility:
        # - r10m/r20m: Bands directly in resolution group (no overview subdirs)
        # - r60m: Has overview levels as subdirectories (0, 1, 2, etc.)
        spatial_ref_groups = [
            "/measurements/reflectance/r10m",  # r10m has no /0 (flattened)
            "/measurements/reflectance/r20m",  # r20m has no /0 (flattened)
            "/measurements/reflectance/r60m/0",  # r60m has /0 (overview level 0)
            "/measurements/reflectance/r60m",
        ]

        spatial_ref_attrs = None
        spatial_ref_group = None
        for group_path in spatial_ref_groups:
            try:
                group = root[group_path.lstrip("/")]
                if "spatial_ref" in group:
                    spatial_ref_var = group["spatial_ref"]
                    spatial_ref_attrs = dict(spatial_ref_var.attrs)
                    spatial_ref_group = group
                    if verbose:
                        warn(f"Found spatial_ref in {group_path}")
                    break
            except (KeyError, AttributeError):
                continue

        if not spatial_ref_attrs:
            if verbose:
                warn("No spatial_ref variable found in GeoZarr")
            return

        # Extract GeoTransform (GDAL format: [x_min, pixel_width, 0, y_max, 0, -pixel_height])
        geotransform = spatial_ref_attrs.get("GeoTransform")
        if geotransform and isinstance(geotransform, list | tuple) and len(geotransform) == 6:
            # Convert GDAL GeoTransform to Affine transform (rasterio format)
            # [a, b, c, d, e, f] where:
            # x = a*col + b*row + c
            # y = d*col + e*row + f
            transform = list(geotransform)
            if verbose:
                warn(f"Extracted proj:transform from GeoTransform: {transform}")
        else:
            transform = None
            if verbose:
                warn("No valid GeoTransform found in spatial_ref")

        # Try to get shape from coordinate dimensions
        # Look for x/y coordinates in the group where we found spatial_ref
        shape = None
        if spatial_ref_group is not None:
            try:
                # Look for x and y coordinates
                if "x" in spatial_ref_group and "y" in spatial_ref_group:
                    y_size = len(spatial_ref_group["y"])
                    x_size = len(spatial_ref_group["x"])
                    shape = [y_size, x_size]
                    if verbose:
                        warn(f"Extracted proj:shape from coordinates: {shape}")
            except (KeyError, AttributeError, TypeError):
                pass

        if not shape and verbose:
            warn("Could not determine proj:shape from coordinates")

        # Populate the geozarr asset with projection metadata
        if transform or shape:
            extra = (
                geozarr_asset.extra_fields if isinstance(geozarr_asset.extra_fields, dict) else {}
            )
            if extra is not geozarr_asset.extra_fields:
                geozarr_asset.extra_fields = extra

            if transform:
                extra["proj:transform"] = transform
            if shape:
                extra["proj:shape"] = shape

            # Also try to get EPSG code
            epsg_code = spatial_ref_attrs.get("spatial_ref")
            if isinstance(epsg_code, int | str):
                epsg_value = _coerce_epsg(epsg_code)
                if epsg_value:
                    extra["proj:epsg"] = epsg_value
                    extra["proj:code"] = f"EPSG:{epsg_value}"
                    if verbose:
                        warn(f"Extracted proj:epsg: {epsg_value}")

            if verbose:
                warn("Populated geozarr asset with projection metadata")

    except Exception as exc:
        if verbose:
            warn(f"Failed to read GeoZarr spatial metadata: {exc}")


def propagate_projection_metadata(item: Item) -> None:
    donors: dict[float, dict[str, object]] = {}
    for asset in item.assets.values():
        gsd = _asset_gsd(asset)
        if gsd is None:
            continue
        snapshot = _projection_snapshot(ProjectionExtension.ext(asset, add_if_missing=False))
        if not snapshot:
            continue
        score = _projection_score(snapshot)
        if score == 0:
            continue
        existing = donors.get(gsd)
        if existing is None or _projection_score(existing) < score:
            donors[gsd] = snapshot

    for asset in item.assets.values():
        roles = tuple(asset.roles or ())
        if "dataset" not in roles:
            continue
        gsd = _asset_gsd(asset)
        if gsd is None:
            continue
        candidate = donors.get(gsd)
        if not candidate:
            continue
        extra = asset.extra_fields if isinstance(asset.extra_fields, dict) else {}
        if extra is not asset.extra_fields:
            asset.extra_fields = extra
        candidate_score = _projection_score(candidate)
        if candidate_score == 0:
            continue
        existing_ext = ProjectionExtension.ext(asset, add_if_missing=False)
        ext = existing_ext or ProjectionExtension.ext(asset, add_if_missing=True)
        _apply_projection(ext, candidate, allow_epsg=True)

        proj_code = candidate.get("code")
        epsg_value = _coerce_epsg(candidate.get("epsg"))
        if epsg_value is None:
            epsg_value = _coerce_epsg(candidate.get("code"))
        if (not proj_code or proj_code in (None, "")) and epsg_value is not None:
            proj_code = f"EPSG:{epsg_value}"
        if isinstance(proj_code, str) and proj_code:
            extra["proj:code"] = proj_code
        if epsg_value is not None:
            extra["proj:epsg"] = epsg_value
        for field in ("bbox", "shape", "transform"):
            value = candidate.get(field)
            if value in (None, [], ()):  # skip empty values
                continue
            key = f"proj:{field}"
            if key in extra and extra[key] not in (None, [], ()):  # keep existing values
                continue
            if isinstance(value, Sequence) and not isinstance(value, str | bytes):
                extra[key] = list(value)
            else:
                extra[key] = value

    if not donors:
        return

    best_candidate = max(donors.values(), key=_projection_score)

    current_snapshot = _projection_snapshot(ProjectionExtension.ext(item, add_if_missing=False))
    needs_update = _projection_score(current_snapshot) < _projection_score(best_candidate)
    item_ext = ProjectionExtension.ext(item, add_if_missing=True)
    if needs_update:
        _apply_projection(item_ext, best_candidate, allow_epsg=False)

    item_extra = item.extra_fields if isinstance(item.extra_fields, dict) else {}
    if item_extra is not item.extra_fields:
        item.extra_fields = item_extra
    item_props = item.properties if isinstance(item.properties, dict) else {}
    if item_props is not item.properties:
        item.properties = item_props

    dominant_code: str | None = None
    for asset in item.assets.values():
        roles = tuple(asset.roles or ())
        if "dataset" not in roles:
            continue
        try:
            dataset_ext = ProjectionExtension.ext(asset, add_if_missing=False)
        except Exception:
            dataset_ext = None
        if dataset_ext and isinstance(dataset_ext.code, str) and dataset_ext.code.strip():
            dominant_code = dataset_ext.code.strip()
            break
    if not dominant_code:
        for snapshot in donors.values():
            candidate_code = snapshot.get("code")
            if isinstance(candidate_code, str) and candidate_code.strip():
                dominant_code = candidate_code.strip()
                break
    if not dominant_code:
        for snapshot in donors.values():
            epsg_value = _coerce_epsg(snapshot.get("epsg") or snapshot.get("code"))
            if epsg_value is not None:
                dominant_code = f"EPSG:{epsg_value}"
                break

    stored_code: str | None = None
    if isinstance(dominant_code, str) and dominant_code.strip():
        stored_code = dominant_code.strip()
    else:
        fallback_code = item_extra.get("proj:code")
        if isinstance(fallback_code, str) and fallback_code.strip():
            stored_code = fallback_code.strip()

    if stored_code:
        item_props["proj:code"] = stored_code
        item_extra["proj:code"] = stored_code
        if getattr(item_ext, "code", None) != stored_code:
            _apply_projection(item_ext, {"code": stored_code}, allow_epsg=False)
    else:
        item_props.pop("proj:code", None)
        item_extra.pop("proj:code", None)
        if getattr(item_ext, "code", None) is not None:
            item_ext.code = None

    # Omit proj:epsg at item level to conform to projection extension 2.0 schema
    if getattr(item_ext, "epsg", None) is not None:
        item_ext.epsg = None
    item_props.pop("proj:epsg", None)
    item_extra.pop("proj:epsg", None)

    for field in ("bbox", "shape", "transform"):
        value = best_candidate.get(field)
        if value in (None, [], ()):  # skip empty values
            continue
        key = f"proj:{field}"
        if key in item_props and item_props[key] not in (None, [], ()):  # keep existing
            continue
        if isinstance(value, Sequence) and not isinstance(value, str | bytes):
            cast_value: object = list(value)
        else:
            cast_value = value
        item_props[key] = cast_value
        if key not in item_extra or item_extra[key] in (None, [], ()):
            item_extra[key] = cast_value

    final_code = item_extra.get("proj:code")
    if isinstance(final_code, str) and final_code.strip():
        item_props["proj:code"] = final_code.strip()
    elif "proj:code" in item_props and item_props["proj:code"] in (None, ""):
        item_props.pop("proj:code", None)

    # Item-level proj:epsg intentionally omitted; assets provide compatibility


def _ensure_preview_projection(item: Item) -> None:
    preview_key = _select_quicklook_asset(item) or _select_preview_asset(item)
    if not preview_key:
        return
    asset = item.assets.get(preview_key)
    if asset is None:
        return
    roles = {role.lower() for role in asset.roles or ()}
    if "dataset" in roles:
        return

    extra = _asset_extras(asset)
    if not isinstance(extra, dict):
        asset.extra_fields = extra = {}

    try:
        proj_ext = ProjectionExtension.ext(asset, add_if_missing=True)
    except Exception as exc:
        warn(f"unable to populate proj:code for preview asset '{preview_key}': {exc}")
        return

    def _normalize_code(value: Any) -> str | None:
        if isinstance(value, str) and value.strip():
            return value.strip()
        epsg_value = _coerce_epsg(value)
        if epsg_value is not None:
            return f"EPSG:{epsg_value}"
        return None

    code_sources: tuple[Any, ...] = (
        extra.get("proj:code"),
        getattr(proj_ext, "code", None),
        extra.get("proj:epsg"),
        getattr(proj_ext, "epsg", None),
        item.properties.get("proj:code"),
        item.properties.get("proj:epsg"),
    )

    candidate_code = next((code for code in map(_normalize_code, code_sources) if code), None)
    if not candidate_code:
        return

    if getattr(proj_ext, "code", None) != candidate_code:
        try:
            proj_ext.code = candidate_code
        except Exception as exc:
            warn(
                "unable to assign proj:code "
                f"'{candidate_code}' for preview asset '{preview_key}': {exc}"
            )
    extra["proj:code"] = candidate_code

    epsg_value = _coerce_epsg(candidate_code)
    if epsg_value is None:
        return
    if getattr(proj_ext, "epsg", None) != epsg_value:
        proj_ext.epsg = epsg_value
    extra["proj:epsg"] = epsg_value


def _request(
    method: str,
    url: str,
    headers: dict[str, str],
    *,
    json_body: dict[str, Any] | None = None,
) -> Any:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in _ALLOWED_SCHEMES:
        raise ValueError(f"unsupported scheme for {method}: {parsed.scheme}")
    request_headers = {"User-Agent": _USER_AGENT, **headers}
    response = httpx.request(
        method,
        url,
        headers=request_headers,
        json=json_body,
        timeout=_DEFAULT_TIMEOUT,
    )
    response.raise_for_status()
    return response


def http_get(url: str, headers: dict[str, str]) -> dict[str, Any]:
    data = _request("GET", url, headers).json()
    if isinstance(data, dict):
        return data
    raise ValueError("unexpected non-mapping response body")


def http_put(url: str, data: dict[str, Any], headers: dict[str, str]) -> int:
    return int(
        _request(
            "PUT",
            url,
            {**headers, "Content-Type": "application/json"},
            json_body=data,
        ).status_code
    )


def ensure_collection_thumbnail(
    stac_base: str,
    collection_id: str,
    headers: dict[str, str],
) -> None:
    thumb = os.getenv("PREVIEW_COLLECTION_THUMBNAIL", "").strip()
    if not thumb:
        return
    coll_url = f"{stac_base.rstrip('/')}/collections/{collection_id}"
    try:
        coll = http_get(coll_url, headers)
    except Exception as exc:
        warn(f"unable to fetch collection {coll_url}: {exc}")
        return
    assets = dict(coll.get("assets") or {})
    thumb_asset = assets.get("thumbnail")
    current = thumb_asset.get("href") if isinstance(thumb_asset, dict) else None
    if current == thumb:
        return
    assets["thumbnail"] = {
        "href": thumb,
        "type": "image/png",
        "roles": ["thumbnail"],
        "title": "Collection thumbnail",
    }
    coll["assets"] = assets
    try:
        code = http_put(coll_url, coll, headers)
        print(f"[augment] PUT collection thumbnail {coll_url} -> {code}")
    except Exception as exc:
        warn(f"failed to PUT collection thumbnail: {exc}")


def _augment_item(
    item: Item,
    *,
    raster_base: str,
    collection_id: str,
    verbose: bool,
) -> Item:
    normalize_item_assets(item, verbose)
    _read_geozarr_spatial_metadata(item, verbose=verbose)
    propagate_projection_metadata(item)
    _ensure_preview_projection(item)
    add_visualization_links(item, raster_base, collection_id=collection_id)
    dedupe_stac_extensions(item)
    return item


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Augment a STAC item with GeoZarr metadata")
    parser.add_argument("--stac", required=True, help="STAC API base, e.g. https://api/.../stac")
    parser.add_argument("--collection", required=True, help="Collection id used in register step")
    parser.add_argument("--item-id", required=True, help="Item identifier to augment")
    parser.add_argument("--bearer", default="", help="Bearer token for Transactions API")
    parser.add_argument(
        "--raster-base",
        default="https://api.explorer.eopf.copernicus.eu/raster",
        help="Base raster API for visualization links",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    args = parser.parse_args(argv)

    headers: dict[str, str] = {}
    if args.bearer:
        headers["Authorization"] = f"Bearer {args.bearer}"
    item_url = f"{args.stac.rstrip('/')}/collections/{args.collection}/items/{args.item_id}"
    if args.verbose:
        print(f"[augment] GET {item_url}")
    try:
        payload = http_get(item_url, headers)
    except Exception as exc:
        warn(f"unable to fetch item {item_url}: {exc}")
        return 0

    item = Item.from_dict(payload)
    target_collection = item.collection_id or args.collection
    _augment_item(
        item,
        raster_base=args.raster_base,
        collection_id=target_collection,
        verbose=args.verbose,
    )

    target_url = f"{args.stac.rstrip('/')}/collections/{target_collection}/items/{item.id}"
    try:
        code = http_put(target_url, item.to_dict(), headers)
        print(f"[augment] PUT {target_url} -> {code}")
    except Exception as exc:
        warn(f"failed to PUT updated item: {exc}")
        return 1

    try:
        ensure_collection_thumbnail(args.stac, target_collection, headers)
    except Exception as exc:
        warn(f"collection thumbnail update skipped/failed: {exc}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
