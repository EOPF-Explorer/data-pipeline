import logging
import re
from typing import Any

from _migrate_catalog.migrations._registry import migration
from _migrate_catalog.types import apply_item_transform

logger = logging.getLogger(__name__)

_GROUP_SUFFIX = "/quality/atmosphere"
# Array-level href each asset was registered with. Both cpm_v262 and cpm_v270 put the
# 10 m array under r10m/ (tests/unit/test_prestage_source.py:107); the bare form names
# the same variable in the same group, so it is accepted too. Host-agnostic: the old
# bucket host and the gateway host both exist in the wild, and the same rule applies
# to the `s3://` form in alternate.s3.href.
_ARRAY_HREF = {
    "AOT_10m": re.compile(r"/quality/atmosphere(?:/r10m)?/aot/?$"),
    "WVP_10m": re.compile(r"/quality/atmosphere(?:/r10m)?/wvp/?$"),
}


def _transform(item: dict[str, Any]) -> bool:
    changed = False
    assets = item.get("assets")
    if not isinstance(assets, dict):
        return False

    for key, array_href in _ARRAY_HREF.items():
        asset = assets.get(key)
        if not isinstance(asset, dict):
            continue
        holders = [asset]
        alternate = asset.get("alternate")
        if isinstance(alternate, dict) and isinstance(alternate.get("s3"), dict):
            holders.append(alternate["s3"])

        # All-or-nothing per asset: href and alternate.s3.href must never end up at
        # different depths (s3_item_cleanup prefers the alternate).
        rewrites = []
        for holder in holders:
            href = holder.get("href")
            if not isinstance(href, str) or href.rstrip("/").endswith(_GROUP_SUFFIX):
                continue  # absent, or already at the group
            if not array_href.search(href):
                logger.warning(
                    "Skipping %s/%s: unrecognised href %r", item.get("id", "unknown"), key, href
                )
                break
            rewrites.append((holder, array_href.sub(_GROUP_SUFFIX, href)))
        else:
            for holder, href in rewrites:
                holder["href"] = href
                changed = True

    return changed


@migration(
    "repoint_atmosphere_assets",
    "Point S2 L2A AOT_10m/WVP_10m hrefs (and alternate.s3.href) at the quality/atmosphere "
    "group instead of the r10m array so titiler can open them",
)
def repoint_atmosphere_assets(item: dict[str, Any]) -> dict[str, Any] | None:
    """Repoint AOT_10m/WVP_10m from a Zarr array to its parent group.

    titiler's GeoZarrReader opens every asset as a DataTree, so an href ending in
    ``…/quality/atmosphere/r10m/aot`` fails (``/assets/AOT_10m/info`` 500s and
    ``/info?assets=AOT_10m`` silently returns ``{}``). The parent group
    ``…/quality/atmosphere`` opens and exposes ``/r10m:aot``, ``/r10m:wvp``, … as
    variables, so the fix is STAC-only: point the asset at the group and let the
    client select with ``assets=AOT_10m|variables=/r10m:aot``.

    Idempotent by construction: once the suffix is stripped it no longer matches,
    so a second pass returns ``None``. An asset whose href is neither an array nor
    the group is logged and left alone (visible in ``--dry-run``), and an asset is
    rewritten only when *both* its href and its S3 alternate can be. ``SCL_20m`` is
    intentionally untouched — its group lacks the spatial/proj attrs until the
    data-model fix lands, and repointing it early turns the whole item ``/info``
    into a 500.

    See: https://github.com/EOPF-Explorer/titiler-eopf/issues/163
    """
    return apply_item_transform(item, _transform)
