import dataclasses
import json
from pathlib import Path
from typing import Any

from _migrate_catalog.types import MigrationResult


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
    """Return True if this migration previously ran to completion on this collection.

    Runs that stopped early do NOT count, because they applied only part of the
    migration:

    - ``reached_max_writes`` — a deliberate bounded chunk. Chunking a large backfill
      is the designed workflow (~170k items is ~17 runs of ``--max-writes 10000``).
    - ``aborted`` — the circuit breaker stopped a run whose writes were failing; that
      is the run that most needs re-running.

    This matters because the caller warns "already applied" and prompts "Run again?"
    defaulting to *No* — so counting a partial run here would tell the operator the
    work was done and let the safe-looking default abandon the rest of the backfill.

    Entries written before these fields existed have neither key; they were full runs,
    so the defaults below keep them counting as applied.
    """
    for run in load_history(history_file)["runs"]:
        if (
            run.get("migration_name") == migration_name
            and run.get("collection_id") == collection_id
            and not run.get("dry_run", True)
            and not run.get("reached_max_writes", False)
            and not run.get("aborted", False)
        ):
            return True
    return False
