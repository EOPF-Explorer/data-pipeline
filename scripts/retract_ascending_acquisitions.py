"""Retract (delete) ascending per-acquisition S1 RTC STAC items (plan T2, issue #306).

OTB 9.1.1's SAR sensor model mis-geolocates Sentinel-1 ascending acquisitions (~47 km range shift).
Ascending ingestion is suspended (T1); the already-published ascending **per-acquisition** items
must be retracted so they are no longer served/rendered. These items are STAC-only pointers that
render the shared cube via ``sel=time`` (no per-item S3 data) — deleting the STAC item is a clean
retract. The cube ascending data itself is removed separately by T2a (the ``ascending`` zarr group)
and the cube items are re-registered (T2b); T10 re-adds *correct* ascending after the OTB fix.

SCOPE: only ascending items (``sat:orbit_state == "ascending"``) in ``--collection`` (default the
staging acquisitions collection). This tool never deletes descending items or cube items — it has no
option to; descending is the good data.

SAFETY: **dry-run by default** — lists what it would delete and exits without writing. Pass
``--execute`` to actually delete (needs sign-off; the deletes are immediate and irreversible).
Idempotent: a missing item (HTTP 404) counts as already-retracted.

Usage:
    # dry-run (read-only — lists the in-scope ascending items, deletes nothing):
    uv run python scripts/retract_ascending_acquisitions.py --stac-api-url https://api.../stac
    # delete (destructive — after sign-off):
    uv run python scripts/retract_ascending_acquisitions.py --stac-api-url https://api.../stac --execute
"""

from __future__ import annotations

import argparse
import logging
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("retract_ascending")

DEFAULT_ACQ_COLLECTION = "sentinel-1-grd-rtc-acquisitions-staging"
# Hardcoded — this tool retracts ONLY the affected (ascending) pass. Deleting descending (the good
# data) must never be a one-flag typo away, so there is deliberately no --orbit option.
ORBIT = "ascending"
ORBIT_PROPERTY = "sat:orbit_state"
_OK_STATUS = (200, 202, 204, 404)  # 404 == already gone (idempotent)


def orbit_filter(orbit: str) -> dict:
    """CQL2-json filter selecting items of a single orbit direction."""
    return {"op": "=", "args": [{"property": ORBIT_PROPERTY}, orbit]}


def item_delete_url(base: str, collection: str, item_id: str) -> str:
    return f"{base.rstrip('/')}/collections/{collection}/items/{item_id}"


def find_orbit_items(stac_api_url: str, collection: str, orbit: str) -> list[str]:
    """Item ids in ``collection`` with ``sat:orbit_state == orbit`` (follows pagination)."""
    from pystac_client import Client

    client = Client.open(stac_api_url)
    search = client.search(
        collections=[collection],
        filter=orbit_filter(orbit),
        filter_lang="cql2-json",
    )
    return [it["id"] for it in search.items_as_dicts()]


def _open_session(stac_api_url: str) -> tuple[Any, str]:
    """Return ``(session, self_href)`` from pystac-client (mirrors register_per_acquisition)."""
    from pystac_client import Client

    client = Client.open(stac_api_url)
    io = client._stac_io
    assert io is not None  # noqa: S101  # nosec B101 -- pystac-client sets this after open()
    return io.session, str(client.self_href).rstrip("/")


def retract(session: Any, base: str, collection: str, item_ids: list[str], *, execute: bool) -> int:
    """DELETE each item id (idempotent). Dry-run (``execute=False``) deletes nothing.

    Returns the number of in-scope items (processed/deleted).
    """
    for item_id in item_ids:
        url = item_delete_url(base, collection, item_id)
        if not execute:
            log.info("[dry-run] would DELETE %s", item_id)
            continue
        resp = session.delete(url, timeout=30)
        if resp.status_code not in _OK_STATUS:
            resp.raise_for_status()
        log.info("deleted %s (HTTP %s)", item_id, resp.status_code)
    return len(item_ids)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--stac-api-url", required=True)
    ap.add_argument("--collection", default=DEFAULT_ACQ_COLLECTION)
    ap.add_argument(
        "--execute",
        action="store_true",
        help="actually delete (default: dry-run, which only lists the in-scope items)",
    )
    args = ap.parse_args()

    item_ids = find_orbit_items(args.stac_api_url, args.collection, ORBIT)
    verb = "deleting" if args.execute else "[dry-run] would delete"
    log.info("%s %d %s items in %s", verb, len(item_ids), ORBIT, args.collection)

    # Only open a write session when actually deleting; dry-run never needs the base URL.
    session, base = (None, args.stac_api_url)
    if args.execute:
        session, base = _open_session(args.stac_api_url)

    n = retract(session, base, args.collection, item_ids, execute=args.execute)

    if not args.execute:
        log.info("[dry-run] %d %s items would be retracted — pass --execute to delete", n, ORBIT)
        return
    log.info("retracted %d %s items from %s", n, ORBIT, args.collection)


if __name__ == "__main__":
    main()
