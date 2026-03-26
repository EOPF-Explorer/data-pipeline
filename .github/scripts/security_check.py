"""Exit with code 1 if blocking security issues are found.

Blocking conditions:
  - bandit:    any HIGH severity issue
  - pip-audit: any vulnerability that has a known fix version available

Non-blocking (printed as informational, no failure):
  - bandit:    MEDIUM or LOW severity issues
  - pip-audit: vulnerabilities with no fix version available yet

Reads:
  bandit-report.json    — produced by: bandit -r scripts/ -f json -o bandit-report.json
  pip-audit-report.json — produced by: pip-audit -f json -o pip-audit-report.json
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


def check_bandit() -> bool:
    """Return True if blocking bandit issues are found."""
    try:
        issues: list[dict] = json.loads(Path("bandit-report.json").read_text()).get("results", [])
    except Exception as exc:
        print(f"Could not read bandit report: {exc}", file=sys.stderr)
        return True

    high = [i for i in issues if i.get("issue_severity", "").upper() == "HIGH"]
    lower = [i for i in issues if i.get("issue_severity", "").upper() in ("MEDIUM", "LOW")]

    if high:
        print(f"bandit: {len(high)} HIGH severity issue(s) — blocking")
    if lower:
        print(f"bandit: {len(lower)} MEDIUM/LOW issue(s) — non-blocking (see artifact)")

    return bool(high)


def check_pip_audit() -> bool:
    """Return True if fixable pip-audit vulnerabilities are found."""
    try:
        deps: list[dict] = json.loads(Path("pip-audit-report.json").read_text()).get(
            "dependencies", []
        )
    except Exception as exc:
        print(f"Could not read pip-audit report: {exc}", file=sys.stderr)
        return True

    fixable: list[tuple[str, str]] = []
    unfixable: list[tuple[str, str]] = []
    for dep in deps:
        for vuln in dep.get("vulns", []):
            entry = (dep["name"], vuln["id"])
            (fixable if vuln.get("fix_versions") else unfixable).append(entry)

    if fixable:
        print(f"pip-audit: {len(fixable)} fixable vulnerability/ies — blocking")
    if unfixable:
        print(f"pip-audit: {len(unfixable)} vulnerability/ies with no fix available — non-blocking")

    return bool(fixable)


def main() -> None:
    failed = check_bandit() | check_pip_audit()
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
