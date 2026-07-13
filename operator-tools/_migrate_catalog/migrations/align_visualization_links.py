from typing import Any

from _migrate_catalog.migrations._registry import migration
from _migrate_catalog.types import apply_item_transform

# STAC Browser renders each non-navigation link's title as an "Additional Resources" header. The cube
# path (register_v1) is the canonical form; this brings older per-acquisition items to the same shape.
_NAV = {"self", "root", "parent", "collection"}
# Canonical order of the non-nav visualization links (unknown rels sort last, keeping their order).
_ORDER = {"store": 0, "viewer": 1, "tilejson": 2, "xyz": 3, "via": 4, "related": 5}


def _transform(item: dict[str, Any]) -> bool:
    render = item.get("properties", {}).get("renders", {}).get("rgb")
    if not isinstance(render, dict):
        # No producer render config (e.g. S2 hardcoded visualization) — nothing to align.
        return False
    links = item.get("links", [])
    item_id = item.get("id", "")
    render_title = render.get("title") or f"Visualization for {item_id}"
    targets = {
        "viewer": render_title,
        "tilejson": f"TileJSON for {item_id}",
        "xyz": render_title,
    }

    changed = False
    for link in links:
        rel = link.get("rel")
        if rel in targets and link.get("title") != targets[rel]:
            link["title"] = targets[rel]
            changed = True

    # Reorder the non-nav links into the canonical sequence; nav links keep their (front) positions.
    before = [link.get("rel") for link in links]
    nav = [link for link in links if link.get("rel") in _NAV]
    rest = [link for link in links if link.get("rel") not in _NAV]
    rest.sort(key=lambda link: _ORDER.get(link.get("rel"), 99))  # stable: ties keep input order
    reordered = nav + rest
    if [link.get("rel") for link in reordered] != before:
        item["links"] = reordered
        changed = True

    return changed


@migration(
    "align_visualization_links",
    "Align viewer/tilejson/xyz link titles + order to the canonical cube form "
    "(render title for viewer/xyz, 'TileJSON for {id}', order store→viewer→tilejson→xyz)",
)
def align_visualization_links(item: dict[str, Any]) -> dict[str, Any] | None:
    """Normalize an S1 RTC item's visualization links to the cube convention.

    STAC Browser shows each non-nav link's title as an "Additional Resources" header, so older
    per-acquisition items (tilejson titled "tilejson", viewer/xyz hardcoded, order
    tilejson→xyz→viewer) read differently from the cube. This retitles viewer/xyz to the render
    composite title, tilejson to "TileJSON for {id}", and reorders to store→viewer→tilejson→xyz.

    No-op (returns None) on items already in canonical form (cube items) and on items without a
    producer render config (S2). Compose with ``add_xyz_link`` to also backfill a missing xyz link.
    """
    return apply_item_transform(item, _transform)
