from typing import Any

from migrate_catalog.migrations._registry import migration
from migrate_catalog.types import apply_item_transform


def _transform(item: dict[str, Any]) -> bool:
    changed = False

    for asset in item.get("assets", {}).values():
        media_type = asset.get("type", "")
        if "application/vnd+zarr" in media_type:
            asset["type"] = media_type.replace("application/vnd+zarr", "application/vnd.zarr")
            changed = True

    return changed


@migration(
    "fix_zarr_media_type",
    "Replace application/vnd+zarr with application/vnd.zarr (MIME convention)",
)
def fix_zarr_media_type(item: dict[str, Any]) -> dict[str, Any] | None:
    """Replace application/vnd+zarr with application/vnd.zarr in asset type fields."""
    return apply_item_transform(item, _transform)
