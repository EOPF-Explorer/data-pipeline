"""Type-safe wrapper for eopf-geozarr CLI commands."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass


@dataclass
class CLIResult:
    """Result from CLI command execution."""

    success: bool
    exit_code: int
    stdout: str
    stderr: str
    error: str | None = None


class EOPFGeozarrCLI:
    """Wrapper for eopf-geozarr CLI with consistent error handling."""

    def __init__(self, default_timeout: int = 300):
        self.default_timeout = default_timeout

    def _run(self, cmd: list[str], timeout: int | None = None) -> CLIResult:
        """Run command with error handling."""
        timeout = timeout or self.default_timeout

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,  # Don't raise on non-zero exit
            )
            return CLIResult(
                success=result.returncode == 0,
                exit_code=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr,
            )

        except subprocess.TimeoutExpired as e:
            return CLIResult(
                success=False,
                exit_code=-1,
                stdout=e.stdout.decode() if e.stdout else "",
                stderr=e.stderr.decode() if e.stderr else "",
                error=f"Command timed out after {timeout}s",
            )

        except FileNotFoundError:
            return CLIResult(
                success=False,
                exit_code=-1,
                stdout="",
                stderr="",
                error="eopf-geozarr command not found. Is it installed? (pip install eopf-geozarr)",
            )

        except Exception as e:
            return CLIResult(
                success=False,
                exit_code=-1,
                stdout="",
                stderr="",
                error=f"Unexpected error: {e}",
            )

    def validate(
        self,
        dataset_path: str,
        verbose: bool = False,
        timeout: int | None = None,
    ) -> CLIResult:
        """Run eopf-geozarr validate."""
        cmd = ["eopf-geozarr", "validate", dataset_path]
        if verbose:
            cmd.append("--verbose")
        return self._run(cmd, timeout=timeout)

    def convert(
        self,
        source: str,
        target: str,
        groups: str | None = None,
        verbose: bool = False,
        timeout: int = 1800,
    ) -> CLIResult:
        """Run eopf-geozarr convert (default 30min timeout)."""
        cmd = ["eopf-geozarr", "convert", source, target]
        if groups:
            cmd.extend(["--groups", groups])
        if verbose:
            cmd.append("--verbose")
        return self._run(cmd, timeout=timeout)
