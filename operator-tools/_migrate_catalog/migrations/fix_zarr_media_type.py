from typing import Any

from _migrate_catalog.migrations._registry import migration
from _migrate_catalog.types import apply_item_transform


def _transform(item: dict[str, Any]) -> bool:
    changed = False

    assets = item.get("assets", {})

    for asset in assets.values():
        media_type = asset.get("type", "")

        # Fix non-standard MIME prefix inherited from source collection
        if "application/vnd+zarr" in media_type:
            media_type = media_type.replace("application/vnd+zarr", "application/vnd.zarr")
            asset["type"] = media_type
            changed = True

        # Source declares version=2 but our pipeline converts to Zarr v3
        if "application/vnd.zarr" in media_type and "version=2" in media_type:
            media_type = media_type.replace("version=2", "version=3")
            asset["type"] = media_type
            changed = True

        # Add missing version for bare vnd.zarr (our data is Zarr v3)
        if media_type == "application/vnd.zarr":
            asset["type"] = "application/vnd.zarr; version=3"
            changed = True

    # Remove zipped_product asset inherited from source (points to CDSE
    # zip download service, unrelated to our converted zarr data)
    if "zipped_product" in assets:
        del assets["zipped_product"]
        changed = True

    return changed


@migration(
    "fix_zarr_media_type",
    "Fix zarr media types (vnd+zarr prefix, version=2, missing version) and remove zipped_product asset",
)
def fix_zarr_media_type(item: dict[str, Any]) -> dict[str, Any] | None:
    """Fix zarr media type issues inherited from the source STAC collection.

    Context: Our pipeline clones items from a source STAC collection (EODC/CDSE),
    then converts their Zarr v2 data to Zarr v3 (GeoZarr). Two problems carried
    over from the source items into our target catalogue:

    1. Wrong MIME prefix — the source collection uses ``application/vnd+zarr``
       (non-standard) instead of ``application/vnd.zarr``.
    2. Wrong or missing version — the source declares ``version=2`` (matching
       their original Zarr v2 store), but our converted data is Zarr v3.
       Some assets have no version parameter at all.
    3. Stale ``zipped_product`` asset — the source items include an asset
       pointing to an external zip download service (CDSE zipper). This asset
       is unrelated to our converted zarr data and should not appear in the
       target catalogue.

    Fixes applied:
    - ``application/vnd+zarr`` → ``application/vnd.zarr``
    - ``version=2`` → ``version=3``
    - bare ``application/vnd.zarr`` → ``application/vnd.zarr; version=3``
    - Remove ``zipped_product`` asset

    See: https://github.com/EOPF-Explorer/data-pipeline/issues/97
    See: https://github.com/EOPF-Explorer/data-pipeline/pull/94
    See: https://github.com/EOPF-Explorer/data-pipeline/pull/111
    """
    return apply_item_transform(item, _transform)
