from typing import Any

from _migrate_catalog.migrations._registry import migration
from _migrate_catalog.types import apply_item_transform


def _transform(item: dict[str, Any]) -> bool:
    changed = False

    for asset in item.get("assets", {}).values():
        media_type = asset.get("type", "")
        if (
            "application/vnd.zarr" in media_type or "application/vnd+zarr" in media_type
        ) and "version=2" in media_type:
            asset["type"] = media_type.replace("version=2", "version=3")
            changed = True

    return changed


@migration(
    "fix_zarr_version",
    "Replace version=2 with version=3 in application/vnd.zarr media types",
)
def fix_zarr_version(item: dict[str, Any]) -> dict[str, Any] | None:
    """Fix hardcoded version=2 in zarr asset media types — store writes Zarr v3.

    See: https://github.com/EOPF-Explorer/data-pipeline/pull/94
    """
    return apply_item_transform(item, _transform)
