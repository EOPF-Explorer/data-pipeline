# Import migration modules for their @migration registration side effect.
# Each binds to its own module name (no shared throwaway alias) so that adding
# a migration — or merging two branches that each add one — cannot collide.
from _migrate_catalog.migrations import (
    add_acquisitions_filter_link,  # noqa: F401
    add_eodash_rasterform,  # noqa: F401
    add_xyz_link,  # noqa: F401
    align_visualization_links,  # noqa: F401
    fix_url_encoding,  # noqa: F401
    fix_zarr_media_type,  # noqa: F401
    stamp_expires,  # noqa: F401
)
from _migrate_catalog.migrations._registry import MIGRATIONS, Migration, migration

__all__ = ["MIGRATIONS", "Migration", "migration"]
