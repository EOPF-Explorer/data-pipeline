"""Probe the deployed titiler render endpoints for every item in a STAC collection (read-only).

Answers "do the previews actually render?" — for each item it fetches a real **tile PNG** (from the
item's ``xyz`` link) and its **thumbnail PNG** (asset), and reports the HTTP status + content-type.
No credentials: the titiler raster API and the STAC API are public read.

Cube items carry an ``xyz`` link with no time selection (renders 200). Per-acquisition items carry a
``sel=time`` xyz link + thumbnail (register_per_acquisition.sel_time_xyz / sel_time_thumbnail). This is
the harness that documents — and later confirms the fix of — the titiler ``sel=time`` tile-render gap
(it 500s today; see the titiler-eopf issue). Re-run after the titiler fix ships to close CP-2.

Usage:
    uv run python scripts/verify_titiler_preview.py --collection sentinel-1-grd-rtc-tests \
      --stac-api-url https://api.explorer.eopf.copernicus.eu/stac \
      --raster-api-url https://api.explorer.eopf.copernicus.eu/raster [--limit 20] [--ids id1,id2]
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import urllib.error
import urllib.request


def tile_for_bbox(bbox: list[float], zoom: int = 8) -> tuple[int, int, int]:
    """A WebMercatorQuad tile covering the bbox centre at ``zoom`` (enough to hit data)."""
    lon = (bbox[0] + bbox[2]) / 2
    lat = (bbox[1] + bbox[3]) / 2
    n = 2**zoom
    x = int((lon + 180.0) / 360.0 * n)
    y = int((1.0 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2.0 * n)
    return zoom, x, y


def probe(url: str, timeout: int = 45) -> tuple[int | None, str, int]:
    """GET ``url``; return (http_status, content_type, body_bytes). Status None on transport error."""
    req = urllib.request.Request(url, headers={"Accept": "*/*"})  # noqa: S310 -- fixed https API
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
            return resp.status, resp.headers.get("Content-Type", ""), len(resp.read())
    except urllib.error.HTTPError as exc:
        return exc.code, exc.headers.get("Content-Type", "") if exc.headers else "", 0
    except Exception as exc:  # noqa: BLE001 -- a probe never crashes the report
        return None, type(exc).__name__, 0


def _get_json(url: str, timeout: int = 45) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})  # noqa: S310 -- https
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
        data = json.load(resp)
    return data if isinstance(data, dict) else {}


def fetch_items(stac_api_url: str, collection: str, limit: int, ids: list[str]) -> list[dict]:
    base = stac_api_url.rstrip("/")
    if ids:
        items = []
        for item_id in ids:
            try:
                items.append(_get_json(f"{base}/collections/{collection}/items/{item_id}"))
            except urllib.error.HTTPError as exc:
                print(f"  ! {item_id}: GET item -> HTTP {exc.code}", file=sys.stderr)
        return items
    features = _get_json(f"{base}/collections/{collection}/items?limit={limit}").get("features", [])
    return features if isinstance(features, list) else []


def _is_render_ok(status: int | None, ctype: str) -> bool:
    return status == 200 and ctype.startswith("image/")


def verify_item(item: dict) -> bool:
    """Probe one item's tile + thumbnail render endpoints. Returns True if both render an image."""
    item_id = item.get("id", "?")
    bbox = item.get("bbox") or [0, 0, 1, 1]
    ok = True

    xyz = next((link["href"] for link in item.get("links", []) if link.get("rel") == "xyz"), None)
    if xyz:
        z, x, y = tile_for_bbox(bbox)
        tile_url = xyz.replace("{z}", str(z)).replace("{x}", str(x)).replace("{y}", str(y))
        status, ctype, nbytes = probe(tile_url)
        rendered = _is_render_ok(status, ctype)
        ok &= rendered
        sel = "sel=time" if "sel=" in xyz else "no-sel"
        print(
            f"  {item_id}  tile({sel})  -> {status} {ctype} {nbytes}B  {'OK' if rendered else 'FAIL'}"
        )
    else:
        print(f"  {item_id}  tile  -> (no xyz link)")

    thumb = item.get("assets", {}).get("thumbnail", {}).get("href")
    if thumb:
        status, ctype, nbytes = probe(thumb)
        rendered = _is_render_ok(status, ctype)
        ok &= rendered
        print(
            f"  {item_id}  thumbnail  -> {status} {ctype} {nbytes}B  {'OK' if rendered else 'FAIL'}"
        )
    return ok


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--collection", required=True)
    ap.add_argument("--stac-api-url", default="https://api.explorer.eopf.copernicus.eu/stac")
    ap.add_argument("--raster-api-url", default="https://api.explorer.eopf.copernicus.eu/raster")
    ap.add_argument("--limit", type=int, default=20)
    ap.add_argument(
        "--ids", default="", help="comma-separated item ids (overrides --limit listing)"
    )
    args = ap.parse_args()

    ids = [i.strip() for i in args.ids.split(",") if i.strip()]
    items = fetch_items(args.stac_api_url, args.collection, args.limit, ids)
    print(f"probing {len(items)} item(s) in {args.collection}:")
    n_ok = sum(verify_item(item) for item in items)
    total = len(items)
    print(f"\n{n_ok}/{total} item(s) render both tile + thumbnail as image/png")
    # Non-zero exit when any preview is broken, so the harness gates CP-2 in CI/manual re-runs.
    sys.exit(0 if n_ok == total and total > 0 else 1)


if __name__ == "__main__":
    main()
