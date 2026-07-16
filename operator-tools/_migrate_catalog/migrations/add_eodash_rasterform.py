import logging
import sys
from pathlib import Path
from typing import Any

from _migrate_catalog.migrations._registry import migration
from _migrate_catalog.types import apply_item_transform

# The emitters live in scripts/ (baked into the runtime image); reuse their constants rather
# than restating the URLs here, so the migration cannot disagree with what fresh
# registrations write. Same bootstrap as migrations/stamp_expires.py.
_scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
if str(_scripts_dir) not in sys.path:
    sys.path.insert(0, str(_scripts_dir))

from eodash_rasterform import rasterform_for_orbit  # noqa: E402

logger = logging.getLogger(__name__)


def _transform(item: dict[str, Any]) -> bool:
    props = item.get("properties", {})
    orbit = props.get("sat:orbit_state")
    form = rasterform_for_orbit(orbit)

    # No orbit -> no single render target, so there is no correct form to write. Also what
    # makes this inherently S2-safe: S2 items carry no sat:orbit_state.
    if form is None:
        logger.warning(
            "Skipping %s: no/unknown sat:orbit_state (%r)", item.get("id", "unknown"), orbit
        )
        return False

    # Idempotent by VALUE, not key-presence: an item whose orbit flipped but whose form did
    # not is wrong, and comparing keys would leave it wrong forever.
    if props.get("eodash:rasterform") == form:
        return False

    props["eodash:rasterform"] = form
    return True


@migration(
    "add_eodash_rasterform",
    "Set eodash:rasterform on S1 RTC items to the bands form matching sat:orbit_state",
)
def add_eodash_rasterform(item: dict[str, Any]) -> dict[str, Any] | None:
    """Backfill eodash's per-orbit bands form onto existing S1 RTC items (issue #348).

    The form configures eodash's TiTiler controls and must match the item's render target
    (/ascending:* vs /descending:*), so it is keyed off sat:orbit_state — which every live S1
    item carries. Idempotent by value, so it also corrects an item whose form no longer matches
    its orbit. Items without sat:orbit_state are skipped, which leaves S2 untouched.

    See: https://github.com/EOPF-Explorer/data-pipeline/issues/348
    """
    return apply_item_transform(item, _transform)
