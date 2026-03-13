from typing import Any

from _migrate_catalog.migrations._registry import migration
from _migrate_catalog.types import apply_item_transform


def _transform(item: dict[str, Any]) -> bool:
    changed = False

    def _fix(href: str) -> str:
        nonlocal changed
        if "?" not in href:
            return href
        path, query = href.split("?", 1)
        if "+" not in query:
            return href
        changed = True
        return f"{path}?{query.replace('+', '%20')}"

    for asset in item.get("assets", {}).values():
        if isinstance(asset.get("href"), str):
            asset["href"] = _fix(asset["href"])

    for link in item.get("links", []):
        if isinstance(link.get("href"), str):
            link["href"] = _fix(link["href"])

    return changed


@migration(
    "fix_url_encoding",
    "Replace + with %20 in asset/link href query strings (RFC 3986 compliance)",
)
def fix_url_encoding(item: dict[str, Any]) -> dict[str, Any] | None:
    """Replace + with %20 in query string portions of asset and link hrefs (RFC 3986).

    See: https://github.com/EOPF-Explorer/data-pipeline/issues/104
    """
    return apply_item_transform(item, _transform)
