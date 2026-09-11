import logging
import re
from typing import Any

from _migrate_catalog.migrations._registry import migration
from _migrate_catalog.types import apply_item_transform

logger = logging.getLogger(__name__)

# Href suffix to strip so the asset lands on the store root. Both cpm_v262 and cpm_v270
# put the 10 m array under r10m/ (tests/unit/test_prestage_source.py:107); the bare form
# names the same variable in the same group, so it is accepted too. The optional trailing
# group alone is accepted so an item repointed by an earlier revision of this migration
# (which stopped at `…/quality/atmosphere`, a node with no consolidated metadata and so
# unopenable) is carried forward rather than skipped. Host-agnostic: the old bucket host
# and the gateway host both exist in the wild, and the same rule applies to the `s3://`
# form in alternate.s3.href.
_ARRAY_HREF = {
    "AOT_10m": re.compile(r"/quality/atmosphere(?:(?:/r10m)?/aot)?/?$"),
    "WVP_10m": re.compile(r"/quality/atmosphere(?:(?:/r10m)?/wvp)?/?$"),
}
# A store root ends at the `.zarr` node; reaching it means the item is already migrated.
_STORE_ROOT_SUFFIX = ".zarr"


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
            if not isinstance(href, str) or href.rstrip("/").endswith(_STORE_ROOT_SUFFIX):
                continue  # absent, or already at the store root
            if not array_href.search(href):
                logger.warning(
                    "Skipping %s/%s: unrecognised href %r", item.get("id", "unknown"), key, href
                )
                break
            # Strip to the store root *with* its trailing slash. A bare `…/X.zarr`
            # is rejected by `s3_item_cleanup.check_urls_confined` as
            # `bare_zarr_store`, which would hard-abort `manage_collections clean`
            # for the whole batch and stall the purge drain.
            rewrites.append((holder, array_href.sub("/", href)))
        else:
            for holder, href in rewrites:
                holder["href"] = href
                changed = True

    return changed


@migration(
    "repoint_atmosphere_assets",
    "Point S2 L2A AOT_10m/WVP_10m hrefs (and alternate.s3.href) at the store root "
    "instead of the r10m array so titiler can open them",
)
def repoint_atmosphere_assets(item: dict[str, Any]) -> dict[str, Any] | None:
    """Repoint AOT_10m/WVP_10m from a Zarr array to the store root.

    titiler's GeoZarrReader opens every asset as a DataTree and never falls back to
    the store root, so an href ending in ``…/quality/atmosphere/r10m/aot`` fails
    (``/assets/AOT_10m/info`` 500s and ``/info?assets=AOT_10m`` silently returns
    ``{}``). The parent group does not work either: over HTTP a group is only
    discoverable through its own ``consolidated_metadata``, and the converter writes
    that for exactly two nodes — the store root and ``measurements/reflectance``
    (eopf-geozarr ``s2_optimization/s2_converter.py:322,325``). Opening
    ``…/quality/atmosphere`` therefore yields an empty tree. The store root does
    carry it and exposes ``/quality/atmosphere/r10m:aot``, ``…:wvp``, … as variables,
    so the fix is STAC-only: point the asset at the root and let the client select
    with ``assets=AOT_10m|variables=/quality/atmosphere/r10m:aot``.

    Idempotent by construction: once the suffix is stripped the href ends at the
    ``.zarr`` root and is skipped, so a second pass returns ``None``. An asset whose
    href is neither an array nor the group is logged and left alone (visible in
    ``--dry-run``), and an asset is rewritten only when *both* its href and its S3
    alternate can be. ``SCL_20m`` is intentionally untouched — its group lacks the
    spatial/proj attrs until data-model#262 lands, and repointing it early turns the
    whole item ``/info`` into a 500.

    See: https://github.com/EOPF-Explorer/titiler-eopf/issues/163
    """
    return apply_item_transform(item, _transform)
