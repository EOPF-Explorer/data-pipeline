from collections.abc import Callable
from dataclasses import dataclass

from _migrate_catalog.types import MigrationFn, MigrationResult

# A migration may optionally surface an end-of-run report (e.g. an outcome
# histogram) and a per-run reset for any state it accumulates.
Reporter = Callable[[MigrationResult], str]


@dataclass(frozen=True)
class Migration:
    fn: MigrationFn
    description: str
    reporter: Reporter | None = None
    reset: Callable[[], None] | None = None


MIGRATIONS: dict[str, Migration] = {}


def migration(
    name: str,
    description: str,
    *,
    reporter: Reporter | None = None,
    reset: Callable[[], None] | None = None,
) -> Callable[[MigrationFn], MigrationFn]:
    def decorator(fn: MigrationFn) -> MigrationFn:
        MIGRATIONS[name] = Migration(fn, description, reporter, reset)
        return fn

    return decorator
