import dataclasses
import json
from pathlib import Path
from typing import Any

from migrate_catalog.types import MigrationResult


def load_history(history_file: Path) -> dict[str, Any]:
    if not history_file.exists():
        return {"runs": []}
    with open(history_file) as f:
        data: dict[str, Any] = json.load(f)
    return data


def save_history(history_file: Path, history: dict[str, Any]) -> None:
    with open(history_file, "w") as f:
        json.dump(history, f, indent=2)
        f.write("\n")


def record_run(history_file: Path, result: MigrationResult) -> None:
    history = load_history(history_file)
    history["runs"].append(dataclasses.asdict(result))
    save_history(history_file, history)


def was_migration_run(history_file: Path, migration_name: str, collection_id: str) -> bool:
    """Return True if this migration was previously applied (non-dry-run) to this collection."""
    for run in load_history(history_file)["runs"]:
        if (
            run.get("migration_name") == migration_name
            and run.get("collection_id") == collection_id
            and not run.get("dry_run", True)
        ):
            return True
    return False
