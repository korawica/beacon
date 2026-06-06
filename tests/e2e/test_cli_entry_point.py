"""Subprocess smoke test for the installed ``beacon`` entry point.

The full ``beacon serve`` subprocess test promised by §1.5.2 is blocked
on §2.4 (process model). Until then this is the narrow promise we can
keep today: the entry point exists, ``--help`` succeeds, ``config show``
succeeds, and exit codes follow the contract documented in
``beacon.cli.main``.
"""

import shutil
import subprocess

import pytest


pytestmark = pytest.mark.skipif(
    shutil.which("beacon") is None,
    reason="`beacon` script not on PATH (package not installed in env)",
)


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["beacon", *args],
        capture_output=True,
        text=True,
        timeout=15,
    )


def test_beacon_help_exits_zero() -> None:
    proc = _run("--help")
    assert proc.returncode == 0, proc.stderr
    assert "Beacon" in proc.stdout
    # Every shipped top-level command appears in --help.
    for cmd in (
        "config",
        "deploy",
        "deployment",
        "list",
        "logs",
        "plan",
        "run",
        "scheduler",
        "sync",
        "test",
        "trigger",
    ):
        assert f" {cmd}" in proc.stdout, f"`beacon {cmd}` missing from --help"


def test_beacon_config_show_exits_zero() -> None:
    proc = _run("config", "show")
    assert proc.returncode == 0, proc.stderr
    assert "BEACON_METADATA_PATH" in proc.stdout
    assert "(env)" in proc.stdout or "(default)" in proc.stdout


def test_beacon_unknown_command_exits_2() -> None:
    proc = _run("not-a-real-command")
    assert proc.returncode == 2, proc.stderr
    # Click writes the usage error to stderr.
    assert "No such command" in proc.stderr or "Usage:" in proc.stderr
