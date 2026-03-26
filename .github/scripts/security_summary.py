"""Build a markdown security report from bandit and pip-audit JSON outputs.

Reads:
  bandit-report.json    — produced by: bandit -r scripts/ -f json -o bandit-report.json
  pip-audit-report.json — produced by: pip-audit -f json -o pip-audit-report.json

Writes:
  security-summary.md          — used by the Post PR comment step
  $GITHUB_STEP_SUMMARY (env)   — appended for the Actions job summary tab

Environment variables:
  REPO     — e.g. "owner/repo"   (${{ github.repository }})
  RUN_ID   — e.g. "1234567890"   (${{ github.run_id }})
  GITHUB_STEP_SUMMARY — path to the step summary file (set by Actions runner)
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

MARKER = "<!-- security-scan-results -->"


def _bandit_section(artifact_url: str) -> list[str]:
    lines: list[str] = ["### Bandit — Static Security Analysis\n"]
    try:
        issues: list[dict] = json.loads(Path("bandit-report.json").read_text()).get("results", [])
    except Exception as exc:
        lines.append(f":warning: Could not read bandit report: {exc}")
        return lines

    if not issues:
        lines.append(":white_check_mark: No issues found")
        return lines

    by_sev: dict[str, list[dict]] = {}
    for issue in issues:
        by_sev.setdefault(issue["issue_severity"].upper(), []).append(issue)

    summary = ", ".join(f"{len(by_sev[k])} {k}" for k in ("HIGH", "MEDIUM", "LOW") if k in by_sev)
    lines.append(f"**:x: {len(issues)} issue(s) found — {summary}**\n")

    high = by_sev.get("HIGH", [])
    if high:
        lines += [
            "#### High Severity\n",
            "| Confidence | File | Line | Issue |",
            "|------------|------|------|-------|",
        ]
        for issue in high:
            lines.append(
                f"| {issue['issue_confidence']}"
                f" | `{issue['filename']}`"
                f" | {issue['line_number']}"
                f" | {issue['issue_text']} |"
            )

    lower = [(k, len(by_sev[k])) for k in ("MEDIUM", "LOW") if k in by_sev]
    if lower:
        detail = ", ".join(f"{n} {k.lower()}" for k, n in lower)
        lines.append(
            f"\n> Additionally {detail} issue(s) found (non-blocking)."
            f" See the [full report]({artifact_url}) for details."
        )

    return lines


def _pip_audit_section() -> list[str]:
    lines: list[str] = ["\n### pip-audit — Dependency Vulnerability Scan\n"]
    try:
        deps: list[dict] = json.loads(Path("pip-audit-report.json").read_text()).get(
            "dependencies", []
        )
    except Exception as exc:
        lines.append(f":warning: Could not read pip-audit report: {exc}")
        return lines

    vulnerable = [d for d in deps if d.get("vulns")]
    if not vulnerable:
        lines.append(":white_check_mark: No vulnerabilities found")
        return lines

    fixable: list[tuple[str, str, dict]] = []
    unfixable: list[tuple[str, str, dict]] = []
    for dep in vulnerable:
        for vuln in dep["vulns"]:
            entry = (dep["name"], dep["version"], vuln)
            (fixable if vuln.get("fix_versions") else unfixable).append(entry)

    if fixable:
        lines += [
            f"**:x: {len(fixable)} fixable vulnerability/ies found**\n",
            "| Package | Version | ID | Fix versions | Description |",
            "|---------|---------|-----|-------------|-------------|",
        ]
        for name, version, vuln in fixable:
            fixes = ", ".join(vuln["fix_versions"])
            desc = vuln.get("description", "")[:120]
            lines.append(f"| {name} | {version} | {vuln['id']} | {fixes} | {desc} |")

    if unfixable:
        items = ", ".join(f"{name}@{ver} ({v['id']})" for name, ver, v in unfixable)
        lines.append(
            f"\n> :warning: {len(unfixable)} vulnerability/ies have no fix available"
            f" yet (non-blocking): {items}"
        )

    return lines


def build_markdown(artifact_url: str) -> str:
    lines = ["## Security Scan Results\n"]
    lines += _bandit_section(artifact_url)
    lines += _pip_audit_section()
    return "\n".join(lines) + "\n"


def main() -> None:
    repo = os.environ.get("REPO", "")
    run_id = os.environ.get("RUN_ID", "")
    step_summary_path = os.environ.get("GITHUB_STEP_SUMMARY", "")

    if not repo or not run_id:
        print("Error: REPO and RUN_ID environment variables must be set.", file=sys.stderr)
        sys.exit(1)

    artifact_url = f"https://github.com/{repo}/actions/runs/{run_id}"
    body = MARKER + "\n" + build_markdown(artifact_url)

    Path("security-summary.md").write_text(body)

    if step_summary_path:
        with open(step_summary_path, "a") as f:
            f.write(body)


if __name__ == "__main__":
    main()
