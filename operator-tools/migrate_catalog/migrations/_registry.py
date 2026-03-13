from collections.abc import Callable

from migrate_catalog.types import MigrationFn

MIGRATIONS: dict[str, tuple[MigrationFn, str]] = {}


def migration(name: str, description: str) -> Callable[[MigrationFn], MigrationFn]:
    def decorator(fn: MigrationFn) -> MigrationFn:
        MIGRATIONS[name] = (fn, description)
        return fn

    return decorator
