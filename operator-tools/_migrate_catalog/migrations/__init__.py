# Import migration modules to trigger @migration registration
from _migrate_catalog.migrations import fix_url_encoding as _  # noqa: F401
from _migrate_catalog.migrations import fix_zarr_media_type as __  # noqa: F401
from _migrate_catalog.migrations import fix_zarr_version as ___  # noqa: F401
from _migrate_catalog.migrations._registry import MIGRATIONS, migration

__all__ = ["MIGRATIONS", "migration"]
