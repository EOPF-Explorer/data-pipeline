"""Shared eodash ``eodash:rasterform`` URLs for the S1 RTC registration paths.

The bands form configures eodash's TiTiler controls. For S1 it cannot live on the
collection the way S2's does: the render targets one orbit group (``/ascending:vv`` vs
``/descending:vv``), so the form has to match each *item*'s orbit.

Defined once so the cube emitter (``register_v1_s1_rtc``), the per-acquisition emitter
(``register_per_acquisition``) and the backfill migration
(``_migrate_catalog.migrations.add_eodash_rasterform`` — bound here by a drift-guard test)
all use the identical strings. The migration compares against these values to decide
whether an item is already correct, so a silent rename would make it rewrite every item.

See issue #348. This is deliberately a plain property rather than a declared STAC
extension: the eodash fields are a temporary measure pending a real extension, and S2
carries ``eodash:rasterform`` undeclared in exactly the same way.
"""

_BASE = "https://raw.githubusercontent.com/EOPF-Explorer/eodash-assets/refs/heads/main/forms"

# Keyed by `sat:orbit_state`, whose only values on these collections are
# "ascending"/"descending" (verified across all 1590 live items).
RASTERFORM_BY_ORBIT = {
    "ascending": f"{_BASE}/s1-asc-bandsform.json",
    "descending": f"{_BASE}/s1-desc-bandsform.json",
}


def rasterform_for_orbit(orbit: str | None) -> str | None:
    """The eodash bands-form URL for an orbit, or None when the orbit is unknown.

    None means "emit nothing": an item with no ``sat:orbit_state`` (a dual-orbit cube
    whose preview slice could not be picked) has no single render target, so any form
    would be a guess. eodash falls back to its default.
    """
    return RASTERFORM_BY_ORBIT.get(orbit or "")
