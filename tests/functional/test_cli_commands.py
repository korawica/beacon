"""Functional tests for the click ``beacon`` CLI.

Uses ``click.testing.CliRunner`` so we don't shell out.
"""

import textwrap
from pathlib import Path

import pytest
from click.testing import CliRunner

from beacon.cli import cli


DAG_OK = """
from beacon import Dag, Task
dag = Dag(id="hello", actions=[
    Task(id="a", uses="empty"),
    Task(id="b", uses="empty", upstream=["a"]),
])
"""

DAG_BAD = """
from beacon import Dag, Task
# Cycle: a -> b -> a
dag = Dag(id="bad", actions=[
    Task(id="a", uses="empty", upstream=["b"]),
    Task(id="b", uses="empty", upstream=["a"]),
])
"""


def _write(path: Path, body: str) -> Path:
    path.write_text(textwrap.dedent(body).lstrip())
    return path


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


# ---------- root group ----------------------------------------------------


def test_help_lists_commands(runner: CliRunner) -> None:
    res = runner.invoke(cli, ["--help"])
    assert res.exit_code == 0
    for name in ("plan", "test", "run", "config"):
        assert name in res.output


# ---------- plan ----------------------------------------------------------


def test_plan_success(runner: CliRunner, tmp_path: Path) -> None:
    f = _write(tmp_path / "d.py", DAG_OK)
    res = runner.invoke(cli, ["plan", str(f)])
    assert res.exit_code == 0
    assert "hello" in res.output
    assert "PASS" in res.output


def test_plan_invalid_dag_exits_nonzero(
    runner: CliRunner, tmp_path: Path
) -> None:
    f = _write(tmp_path / "d.py", DAG_BAD)
    res = runner.invoke(cli, ["plan", str(f)])
    assert res.exit_code != 0


def test_plan_missing_dag_id(runner: CliRunner, tmp_path: Path) -> None:
    f = _write(tmp_path / "d.py", DAG_OK)
    res = runner.invoke(cli, ["plan", str(f), "--dag-id", "ghost"])
    assert res.exit_code != 0
    assert "ghost" in res.output


def test_test_runs_dag_to_success(runner: CliRunner, tmp_path: Path) -> None:
    f = _write(tmp_path / "d.py", DAG_OK)
    res = runner.invoke(cli, ["test", str(f)])
    assert res.exit_code == 0
    assert "state  : success" in res.output


# ---------- run -----------------------------------------------------------


def test_run_persists_metadata(runner: CliRunner, tmp_path: Path) -> None:
    f = _write(tmp_path / "d.py", DAG_OK)
    meta = tmp_path / "meta"
    res = runner.invoke(cli, ["run", str(f), "--metadata-path", str(meta)])
    assert res.exit_code == 0, res.output
    assert "state  : success" in res.output
    # Metadata dir was created and populated (hive-style partitioning).
    assert (meta / "dag_runs" / "dag_id=hello").exists()


# ---------- config show ---------------------------------------------------


def test_config_show_prints_all_known_settings(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("BEACON_METADATA_PATH", "/tmp/zzz")
    res = runner.invoke(cli, ["config", "show"])
    assert res.exit_code == 0
    assert "BEACON_METADATA_PATH" in res.output
    assert "/tmp/zzz" in res.output
    assert "(env)" in res.output
    assert "(default)" in res.output
