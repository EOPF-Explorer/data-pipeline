"""Wipe (delete) all STAC items for a set of S1 RTC tiles — both orbits (plan T1/T3, issue #306).

To reprocess a tile with the corrected -180..180 geoid, its already-published STAC items must be
removed first: ``trigger_cdse.py`` dedups against the per-acquisition collection, so leaving the
per-acq items in place would make ``discover`` skip the tile. For each ``--tiles`` entry this deletes:

  * the **cube** item ``s1-rtc-<tile>`` in ``--cube-collection``; and
  * **every** per-acquisition item ``s1-rtc-<tile>-<datetime>`` (both ascending and descending) in
    ``--acq-collection``.

Per-acq items are STAC-only pointers that render the shared cube via ``sel=time`` (no per-item S3
data), so deleting the STAC item is a clean wipe; the cube store itself is removed separately (the
in-cluster wipe Workflow does the S3 ``cp`` backup + ``rm`` before calling this script).

SCOPE: only the listed tiles, **both orbits** (no ``sat:orbit_state`` filter — unlike
``retract_ascending_acquisitions.py``, this is a full per-tile wipe). Per-acq items are enumerated by
a server-side ``grid:code = MGRS-<tile>`` filter, then guarded client-side by
``id.startswith("s1-rtc-<tile>-")`` so an over-broad server response can never delete another tile's
items.

SAFETY: **dry-run by default** — lists the resolved cube + per-acq ids and exits without writing. Pass
``--execute`` to actually delete (needs sign-off; the deletes are immediate). Idempotent: a missing
item (HTTP 404) counts as already-wiped.

Usage:
    # dry-run (read-only — lists exactly what would be deleted):
    uv run python scripts/wipe_s1rtc_tiles.py --tiles 30UVU --stac-api-url https://api.../stac
    # delete (destructive — after sign-off):
    uv run python scripts/wipe_s1rtc_tiles.py --tiles 30UVU,30UWB --stac-api-url https://api.../stac --execute
"""

from __future__ import annotations

import argparse
import logging
from typing import Any

import stac_auth

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("wipe_s1rtc_tiles")

DEFAULT_CUBE_COLLECTION = "sentinel-1-grd-rtc-staging"
DEFAULT_ACQ_COLLECTION = "sentinel-1-grd-rtc-acquisitions-staging"
_OK_STATUS = (200, 202, 204, 404)  # 404 == already gone (idempotent)


def cube_item_id(tile: str) -> str:
    """The cube STAC item id for a tile (e.g. ``s1-rtc-30UVU``)."""
    return f"s1-rtc-{tile}"


def grid_code_filter(tile: str) -> dict:
    """CQL2-json filter selecting items of a single MGRS tile (both orbits — no orbit constraint)."""
    return {"op": "=", "args": [{"property": "grid:code"}, f"MGRS-{tile}"]}


def item_delete_url(base: str, collection: str, item_id: str) -> str:
    return f"{base.rstrip('/')}/collections/{collection}/items/{item_id}"


def filter_tile_items(items: list[dict[str, Any]], tile: str) -> list[str]:
    """Keep only this tile's per-acquisition ids (``s1-rtc-<tile>-<datetime>``), drop everything else.

    The trailing ``-`` is the safety net: it excludes the bare cube item (``s1-rtc-<tile>``) and any
    superstring tile (``s1-rtc-<tile>X-…``) an over-broad server filter might return.
    """
    prefix = f"s1-rtc-{tile}-"
    return [it["id"] for it in items if str(it["id"]).startswith(prefix)]


def find_acquisition_items(stac_api_url: str, acq_collection: str, tile: str) -> list[str]:
    """Per-acq item ids in ``acq_collection`` for ``tile`` (both orbits; follows pagination)."""
    from pystac_client import Client

    client = Client.open(stac_api_url)
    search = client.search(
        collections=[acq_collection],
        filter=grid_code_filter(tile),
        filter_lang="cql2-json",
    )
    return filter_tile_items(list(search.items_as_dicts()), tile)


def _open_session(stac_api_url: str) -> tuple[Any, str]:
    """Return ``(session, self_href)`` from pystac-client; the session carries a Bearer via
    ``stac_auth.open_client`` so the DELETEs authenticate once enforcement is on (no-op when
    OIDC env is unset)."""
    client = stac_auth.open_client(stac_api_url)
    io = client._stac_io
    assert io is not None  # noqa: S101  # nosec B101 -- pystac-client sets this after open()
    return io.session, str(client.self_href).rstrip("/")


def delete_items(
    session: Any, base: str, collection: str, item_ids: list[str], *, execute: bool
) -> int:
    """DELETE each item id from ``collection`` (idempotent). Dry-run (``execute=False``) deletes nothing.

    Returns the number of in-scope items (processed/deleted).
    """
    for item_id in item_ids:
        url = item_delete_url(base, collection, item_id)
        if not execute:
            log.info("[dry-run] would DELETE %s/%s", collection, item_id)
            continue
        resp = session.delete(url, timeout=30)
        if resp.status_code not in _OK_STATUS:
            resp.raise_for_status()
        log.info("deleted %s/%s (HTTP %s)", collection, item_id, resp.status_code)
    return len(item_ids)


def wipe_tile(
    session: Any,
    base: str,
    *,
    cube_collection: str,
    acq_collection: str,
    cube_id: str,
    acq_ids: list[str],
    execute: bool,
) -> dict[str, int]:
    """Delete the cube item, then every per-acq item (both orbits). Returns per-collection counts."""
    cube = delete_items(session, base, cube_collection, [cube_id], execute=execute)
    acq = delete_items(session, base, acq_collection, acq_ids, execute=execute)
    return {"cube": cube, "acq": acq}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--stac-api-url", required=True)
    ap.add_argument("--tiles", required=True, help="comma-separated MGRS tiles, e.g. 30UVU,30UWB")
    ap.add_argument("--cube-collection", default=DEFAULT_CUBE_COLLECTION)
    ap.add_argument("--acq-collection", default=DEFAULT_ACQ_COLLECTION)
    ap.add_argument(
        "--execute",
        action="store_true",
        help="actually delete (default: dry-run, which only lists the resolved cube + per-acq ids)",
    )
    args = ap.parse_args()

    tiles = [t.strip() for t in args.tiles.split(",") if t.strip()]

    # Enumeration is read-only; only open a write session when actually deleting.
    session, base = (None, args.stac_api_url)
    if args.execute:
        session, base = _open_session(args.stac_api_url)

    grand_total = 0
    for tile in tiles:
        cube_id = cube_item_id(tile)
        acq_ids = find_acquisition_items(args.stac_api_url, args.acq_collection, tile)
        verb = "deleting" if args.execute else "[dry-run] would delete"
        log.info(
            "%s tile %s: 1 cube item (%s) + %d per-acq items (both orbits)",
            verb,
            tile,
            cube_id,
            len(acq_ids),
        )
        counts = wipe_tile(
            session,
            base,
            cube_collection=args.cube_collection,
            acq_collection=args.acq_collection,
            cube_id=cube_id,
            acq_ids=acq_ids,
            execute=args.execute,
        )
        grand_total += counts["cube"] + counts["acq"]

    if not args.execute:
        log.info(
            "[dry-run] %d items across %d tile(s) would be wiped — pass --execute to delete",
            grand_total,
            len(tiles),
        )
        return
    log.info("wiped %d items across %d tile(s)", grand_total, len(tiles))


if __name__ == "__main__":
    main()
