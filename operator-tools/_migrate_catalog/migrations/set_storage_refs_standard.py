"""Catch ``storage:refs`` up after the OVH lifecycle rule moved every S2 object to STANDARD.

A lifecycle transition writes nothing to STAC, so moved items still claim
``["performance"]`` / ``["mixed"]`` (or carry the legacy per-asset ``storage:scheme``
and no refs at all), and ``is_already_migrated`` keeps reading them as not standard.
This migration lists nothing in S3: it records what the bucket inventory already
proved, so run it only once that inventory reads 0 non-STANDARD objects.

- Every asset with a non-empty ``alternate.s3`` gets ``storage:refs = ["standard"]``,
  its ``objects_per_storage_class`` folds into ``{"STANDARD": total}``, and the
  legacy ``storage:scheme`` is dropped (as ``update_stac_storage_tier`` does).
- ``no_s3_assets``: nothing to record, so the item is skipped, not written.
- ``other_bucket``: an ``alternate.s3`` outside ``_BUCKET``, whose inventory proved nothing
  about it, so the item is skipped, not written (cleanup's ``wrong_bucket`` test).
- The demo denylist is deliberately NOT consulted: this field cannot affect deletion.
"""

import copy
import sys
from collections import Counter
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from _migrate_catalog.migrations._registry import migration
from _migrate_catalog.types import MigrationResult, apply_item_transform

_scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
if str(_scripts_dir) not in sys.path:
    sys.path.insert(0, str(_scripts_dir))

# The bucket the inventory covered; also the one the storage:schemes written below name.
_BUCKET = "esa-zarr-sentinel-explorer-fra"

# The pair update_stac_storage_tier.update_item_storage_tiers declares.
_EXTENSIONS = (
    "https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json",
    "https://stac-extensions.github.io/storage/v2.0.0/schema.json",
)

HISTOGRAM: Counter[str] = Counter()


def _s3_alternates(item: dict[str, Any]) -> list[dict[str, Any]]:
    """The non-empty ``alternate.s3`` dicts: the assets ``is_already_migrated`` checks."""
    assets = item.get("assets")
    found = []
    for asset in assets.values() if isinstance(assets, dict) else []:
        alternate = asset.get("alternate") if isinstance(asset, dict) else None
        s3 = alternate.get("s3") if isinstance(alternate, dict) else None
        if isinstance(s3, dict) and s3:
            found.append(s3)
    return found


def _storage_schemes() -> Any:
    # Imported on first use: update_stac_storage_tier (and the register_v1 it imports)
    # call logging.basicConfig at import, which at module level would turn on INFO
    # logging for every migrate_catalog command, flooding restamp/stamp runs.
    from storage_tier_utils import extract_region_from_endpoint
    from update_stac_storage_tier import STORAGE_SCHEMES_PLATFORM, _build_storage_schemes

    return _build_storage_schemes(extract_region_from_endpoint(STORAGE_SCHEMES_PLATFORM))


def _transform(item: dict[str, Any]) -> bool:
    before = copy.deepcopy(item)
    for s3 in _s3_alternates(item):
        s3["storage:refs"] = ["standard"]
        s3.pop("storage:scheme", None)
        counts = s3.get("objects_per_storage_class")
        if isinstance(counts, dict):
            s3["objects_per_storage_class"] = {"STANDARD": sum(counts.values())}
    properties = item.setdefault("properties", {})
    if not properties.get("storage:schemes"):
        properties["storage:schemes"] = _storage_schemes()
    extensions = item.setdefault("stac_extensions", [])
    for ext in _EXTENSIONS:
        if ext not in extensions:
            extensions.append(ext)
    return item != before


def report(result: MigrationResult) -> str:
    """The histogram, cross-checked against the runner's counts: only ``needs_refs``
    items are written, so they must end up modified or failed."""
    lines = ["Outcome histogram:"]
    lines += [f"  {reason:<16} {HISTOGRAM[reason]}" for reason in sorted(HISTOGRAM)]
    written = HISTOGRAM["needs_refs"]
    skipped = sum(HISTOGRAM.values()) - written
    if written != result.items_modified + result.items_failed or skipped != result.items_skipped:
        lines.append(
            "  WARNING: histogram does not reconcile with run counts "
            f"(processed={result.items_processed}, modified={result.items_modified}, "
            f"skipped={result.items_skipped}, failed={result.items_failed})"
        )
    return "\n".join(lines)


@migration(
    "set_storage_refs_standard",
    'Set every alternate.s3 storage:refs to ["standard"] after the lifecycle move to '
    "STANDARD (no S3 listing); skips items with no S3 asset or one outside " + _BUCKET,
    reporter=report,
    reset=HISTOGRAM.clear,
)
def set_storage_refs_standard(item: dict[str, Any]) -> dict[str, Any] | None:
    result = None
    alternates = _s3_alternates(item)
    if not alternates:
        reason = "no_s3_assets"
    elif any(urlparse(str(s3.get("href") or "")).netloc != _BUCKET for s3 in alternates):
        reason = "other_bucket"
    elif (result := apply_item_transform(item, _transform)) is None:
        reason = "already_standard"
    else:
        reason = "needs_refs"
    HISTOGRAM[reason] += 1
    return result
