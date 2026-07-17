import logging
from typing import Any

from _migrate_catalog.migrations._registry import migration
from _migrate_catalog.types import apply_item_transform

logger = logging.getLogger(__name__)

_TILEJSON_MARK = "/WebMercatorQuad/tilejson.json?"
_XYZ_MARK = "/tiles/WebMercatorQuad/{z}/{x}/{y}.png?"


def _xyz_title(item: dict[str, Any], links: list[dict[str, Any]]) -> str:
    """Title for the xyz link: viewer-link title first, else the render title, else a fallback.

    Viewer-first gives exact parity for both item types: fresh per-acquisition registrations hardcode
    the viewer title ("Sentinel-1 GRD RGB composite") while carrying a *different* renders.rgb.title
    ("VV, VH, VV/VH composite"), and cube items use the render title for both viewer and xyz — so
    deriving from the viewer link matches fresh registrations in every case.
    """
    for link in links:
        if link.get("rel") == "viewer" and link.get("title"):
            return str(link["title"])
    render = item.get("properties", {}).get("renders", {}).get("rgb", {})
    if isinstance(render, dict) and render.get("title"):
        return str(render["title"])
    return "XYZ tile template"


def _transform(item: dict[str, Any]) -> bool:
    links = item.get("links", [])

    # 1. Idempotent + inherently S2-safe: never add a second xyz.
    if any(link.get("rel") == "xyz" for link in links):
        return False

    # 2. Find the tilejson link; skip (don't emit garbage) if it isn't the render-path form.
    tj_index = next((i for i, link in enumerate(links) if link.get("rel") == "tilejson"), None)
    if tj_index is None:
        logger.warning("Skipping %s: no tilejson link", item.get("id", "unknown"))
        return False
    tj_href = links[tj_index].get("href", "")
    if _TILEJSON_MARK not in tj_href:
        # The 3 known-legacy items (31TCJ/30TXM/30TXN + their acqs) carry bare .../WebMercatorQuad
        # hrefs — deriving xyz would be garbage; they're slated for wipe+re-ingest.
        logger.warning(
            "Skipping %s: tilejson href is not the render-path form (%r)",
            item.get("id", "unknown"),
            tj_href,
        )
        return False

    # 3. Exact substitution validated live (query, incl. sel=time, unchanged).
    xyz_href = tj_href.replace(_TILEJSON_MARK, _XYZ_MARK, 1)

    # 4. Insert immediately after tilejson.
    links.insert(
        tj_index + 1,
        {
            "rel": "xyz",
            "type": "image/png",
            "href": xyz_href,
            "title": _xyz_title(item, links),
        },
    )
    return True


@migration(
    "add_xyz_link",
    "Add a rel=xyz {z}/{x}/{y}.png tile template after tilejson (derived from the tilejson href)",
)
def add_xyz_link(item: dict[str, Any]) -> dict[str, Any] | None:
    """Backfill the machine-facing rel=xyz tile-template link on S1 RTC items missing it.

    Derives the xyz href from the item's existing tilejson link (same endpoint + query, incl.
    sel=time), so it matches what fresh registrations now emit natively. Idempotent (skips items that
    already carry xyz — including S2, which keeps its own hardcoded xyz) and skips the known-legacy
    items whose tilejson is a bare .../WebMercatorQuad href.

    See: https://github.com/EOPF-Explorer/data-pipeline (S1 RTC xyz tile links)
    """
    return apply_item_transform(item, _transform)
