from typing import Any

from _migrate_catalog.migrations._registry import migration
from _migrate_catalog.types import apply_item_transform

_FILTER_TITLE = "Per-acquisition items (filter by tile grid:code)"
_PARENT_TITLE = "Parent tile datacube"


def _transform(item: dict[str, Any]) -> bool:
    links = item.get("links", [])

    # Idempotent: never add a second filter link.
    if any(lk.get("rel") == "related" and lk.get("title") == _FILTER_TITLE for lk in links):
        return False

    # Only acquisition items — identified by their parent-datacube related link. Cube and S2 items
    # don't carry it, so they're left alone (the cube already has its own filter link).
    parent = next(
        (lk for lk in links if lk.get("rel") == "related" and lk.get("title") == _PARENT_TITLE),
        None,
    )
    collection = item.get("collection")
    if parent is None or not collection or "/collections/" not in parent.get("href", ""):
        return False

    # Derive the STAC API base from the parent link href (…/collections/{cube}/items/{id}); the
    # filter link points at this acquisition's own (sibling) collection.
    stac_base = parent["href"].split("/collections/", 1)[0]
    parent_index = links.index(parent)
    links.insert(
        parent_index + 1,
        {
            "rel": "related",
            "type": "application/json",
            "href": f"{stac_base}/collections/{collection}",
            "title": _FILTER_TITLE,
        },
    )
    return True


@migration(
    "add_acquisitions_filter_link",
    "Add the sibling-collection 'Per-acquisition items (filter by tile grid:code)' related link to "
    "acquisition items, so a rel group has >=2 entries and STAC Browser renders grouped categories",
)
def add_acquisitions_filter_link(item: dict[str, Any]) -> dict[str, Any] | None:
    """Give acquisition items a second ``related`` link (the sibling acquisitions collection).

    STAC Browser's LinkList only renders the grouped "Additional Resources" section (category
    headers) when some ``rel`` has >=2 links (its ``hasGroups`` check). Acquisition items otherwise
    carry exactly one link per rel, so they render as a flat list. Adding the sibling-collection
    filter link (mirroring the cube via register_v1_s1_rtc) gives ``related`` two entries and flips
    the rendering to grouped.

    Idempotent (skips items that already have the filter link) and scoped to acquisition items
    (identified by their "Parent tile datacube" related link); no-op on cube and S2 items.
    """
    return apply_item_transform(item, _transform)
