import copy
import dataclasses
from collections.abc import Callable
from typing import Any

MigrationFn = Callable[[dict[str, Any]], dict[str, Any] | None]


@dataclasses.dataclass
class MigrationResult:
    migration_name: str
    collection_id: str
    started_at: str
    completed_at: str
    items_processed: int
    items_modified: int
    items_skipped: int
    items_failed: int
    dry_run: bool
    errors: list[dict[str, str]]


def apply_item_transform(
    item: dict[str, Any], transform_fn: Callable[[dict[str, Any]], bool]
) -> dict[str, Any] | None:
    """Deep-copy item, apply transform_fn in place (must return True if changed), return item or None."""
    item = copy.deepcopy(item)
    changed = transform_fn(item)
    return item if changed else None
