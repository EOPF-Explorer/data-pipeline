"""Shared STAC link titles for the S1 RTC cube and per-acquisition registration paths.

Defined once so the cube emitter (``register_v1_s1_rtc``), the per-acquisition emitter
(``register_per_acquisition``) and the backfill migration
(``_migrate_catalog.migrations.add_acquisitions_filter_link`` — bound here by a drift-guard test)
all use the identical strings. The migration *gates* on :data:`PARENT_DATACUBE_TITLE`, so a silent
rename would otherwise make it skip every item.
"""

# The acquisition item's link up to its parent tile datacube (register_per_acquisition emits it;
# add_acquisitions_filter_link uses it to recognise a fully-structured acquisition item).
PARENT_DATACUBE_TITLE = "Parent tile datacube"

# The link out to the sibling acquisitions collection, carried by both the cube and each acquisition
# item so STAC Browser has a rel group with >=2 entries (its grouped-rendering condition).
ACQUISITIONS_FILTER_TITLE = "Per-acquisition items (filter by tile grid:code)"
