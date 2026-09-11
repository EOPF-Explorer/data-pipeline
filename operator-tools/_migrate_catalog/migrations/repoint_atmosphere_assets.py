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
# and the gateway host both exist in the wild.
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
        # Only `href` moves. `alternate.s3.href` stays on the array, because it is
        # what the S3 tooling consumes and it wants the narrowest accurate prefix:
        # `s3_item_cleanup` reads it in preference to `href` (and never reads `href`
        # here, which is https, not s3://), and `update_stac_storage_tier` samples
        # storage class under it — pointing it at the store root would list the whole
        # store per item and report MIXED for a straggler anywhere in it.
        item_id = item.get("id", "unknown")
        href = asset.get("href")
        if not isinstance(href, str) or href.rstrip("/").endswith(_STORE_ROOT_SUFFIX):
            continue  # absent, or already at the store root
        if not array_href.search(href):
            logger.warning("Skipping %s/%s: unrecognised href %r", item_id, key, href)
            continue

        # An asset with no `alternate.s3.href` has no narrow S3 pointer to fall back
        # on, and `update_stac_storage_tier --add-missing` would later derive one
        # from this href (`update_stac_storage_tier.py:244`) — i.e. from the store
        # root, listing the whole store per asset and stamping
        # `storage:refs: ["mixed"]` for any straggler in it. Leave those items alone
        # and say so, rather than trading a titiler fix for a tiering regression.
        alternate = asset.get("alternate")
        s3 = alternate.get("s3") if isinstance(alternate, dict) else None
        if not (isinstance(s3, dict) and isinstance(s3.get("href"), str)):
            logger.warning(
                "Skipping %s/%s: no alternate.s3.href to keep the narrow S3 prefix", item_id, key
            )
            continue

        # Strip to the store root *with* its trailing slash: a bare `…/X.zarr` is
        # rejected by `s3_item_cleanup.check_urls_confined` as `bare_zarr_store`
        # on the fallback path.
        root = array_href.sub("/", href)
        # The regex matches on suffix alone, so a non-zarr or source-store layout
        # (`…/product/quality/atmosphere/r10m/aot`) would otherwise be mangled into
        # `…/product/` and written back. Only ever land on a `.zarr` root.
        if not root.rstrip("/").endswith(_STORE_ROOT_SUFFIX):
            logger.warning(
                "Skipping %s/%s: %r does not strip to a .zarr store root", item_id, key, href
            )
            continue
        asset["href"] = root
        changed = True

    return changed


@migration(
    "repoint_atmosphere_assets",
    "Point S2 L2A AOT_10m/WVP_10m hrefs at the store root instead of the r10m array "
    "so titiler can open them; alternate.s3.href is left on the array",
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
    ``--dry-run``). ``alternate.s3.href`` is deliberately out of scope: it is what
    ``s3_item_cleanup`` (which prefers it over ``href``) and
    ``update_stac_storage_tier`` consume, and both want the narrowest accurate
    prefix — the store root would list the whole store per item and report MIXED
    for a straggler anywhere in it. ``SCL_20m`` is intentionally untouched, and that
    still holds at the root: the reader exposes ``/quality/atmosphere/r10m`` with real
    bounds but omits ``/conditions/mask/l2a_classification/r20m``, which has no
    spatial attrs until data-model#262 lands. SCL is unrenderable wherever its href
    points, so repointing it early only turns a silent skip into a 500.

    See: https://github.com/EOPF-Explorer/titiler-eopf/issues/163
    """
    return apply_item_transform(item, _transform)
