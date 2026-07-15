"""Static checks that `operator-tools/` is baked into the runtime image.

coordination#183: the in-cluster `stamp_expires` backfill needs
`operator-tools/migrate_catalog.py` inside the image, as a sibling of the
already-baked `scripts/` directory. These tests pin the two file edits that
make that true; the actual build is verified separately with `docker build`.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _dockerignore_lines() -> list[str]:
    return (REPO_ROOT / ".dockerignore").read_text().splitlines()


def _dockerfile_lines() -> list[str]:
    return (REPO_ROOT / "docker" / "Dockerfile").read_text().splitlines()


def test_dockerignore_does_not_exclude_operator_tools():
    lines = {line.strip() for line in _dockerignore_lines()}
    assert "operator-tools/" not in lines


def test_dockerfile_copies_operator_tools_as_sibling_of_scripts():
    lines = [line.strip() for line in _dockerfile_lines()]
    assert "COPY scripts/ /app/scripts/" in lines
    assert "COPY operator-tools/ /app/operator-tools/" in lines

    scripts_idx = lines.index("COPY scripts/ /app/scripts/")
    operator_tools_idx = lines.index("COPY operator-tools/ /app/operator-tools/")
    assert operator_tools_idx > scripts_idx


def test_dockerfile_chown_runs_after_both_copies():
    text = "\n".join(_dockerfile_lines())
    scripts_pos = text.index("COPY scripts/ /app/scripts/")
    operator_tools_pos = text.index("COPY operator-tools/ /app/operator-tools/")
    chown_pos = text.index("chown -R appuser:appuser /app")
    assert scripts_pos < chown_pos
    assert operator_tools_pos < chown_pos
