"""Functional tests for ``beacon config show`` + ``beacon logs --logical-date``.

These exercise behaviours already shipped but missing direct coverage,
closing out the Phase 1.5 §1.5.2 DoD checklist.
"""

import asyncio
from datetime import datetime
from pathlib import Path

import pytest
from click.testing import CliRunner

from beacon.cli import cli
from beacon.metadata import LocalMetadata


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


# --- config show ----------------------------------------------------------


def test_config_show_lists_every_known_setting_with_source(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("BEACON_LOG_LEVEL", "DEBUG")
    monkeypatch.delenv("BEACON_METADATA_PATH", raising=False)

    res = runner.invoke(cli, ["config", "show"])
    assert res.exit_code == 0, res.output

    # Every known setting appears with a source label.
    for name in (
        "BEACON_METADATA_PATH",
        "BEACON_LOG_DIR",
        "BEACON_LOG_LEVEL",
        "BEACON_SCHEDULER_TICK_SECONDS",
    ):
        assert name in res.output, f"{name} missing from output"

    # Source labels are present.
    assert "(env)" in res.output
    assert "(default)" in res.output

    # The env-sourced one shows the env value.
    for line in res.output.splitlines():
        if line.startswith("BEACON_LOG_LEVEL"):
            assert "DEBUG" in line
            assert "(env)" in line
            break
    else:
        pytest.fail("BEACON_LOG_LEVEL row not found")


# --- logs --logical-date --------------------------------------------------


def test_logs_logical_date_resolves_run_id(
    runner: CliRunner, tmp_path: Path
) -> None:
    meta_path = tmp_path / "meta"
    log_dir = tmp_path / "logs"
    meta = LocalMetadata(meta_path)

    # Seed a DagRun with a known logical_date + a fake attempt log file.
    dag_id, task_id, run_id = (
        "dag-x",
        "task-y",
        "scheduled-dag-x-20260603T020000",
    )
    asyncio.run(
        meta.create_dag_run(
            run_id=run_id,
            dag_id=dag_id,
            dag_version="v1",
            logical_date=datetime(2026, 6, 3, 2, 0, 0),
        )
    )
    attempt_dir = log_dir / dag_id / run_id / task_id
    attempt_dir.mkdir(parents=True)
    (attempt_dir / "attempt_1.jsonl").write_text('{"msg": "hello-from-task"}\n')

    res = runner.invoke(
        cli,
        [
            "logs",
            dag_id,
            task_id,
            "--logical-date",
            "2026-06-03",
            "--metadata-path",
            str(meta_path),
            "--log-dir",
            str(log_dir),
        ],
    )
    assert res.exit_code == 0, res.output
    assert "hello-from-task" in res.output
    # Diagnostic line on stderr confirms resolution path was used.
    assert "resolved run_id" in res.output


def test_logs_requires_run_or_logical_date(
    runner: CliRunner, tmp_path: Path
) -> None:
    res = runner.invoke(cli, ["logs", "x", "y"])
    assert res.exit_code == 2, res.output
    assert "--run" in res.output or "--logical-date" in res.output


def test_logs_logical_date_with_no_match_exits_1(
    runner: CliRunner, tmp_path: Path
) -> None:
    meta_path = tmp_path / "meta"
    LocalMetadata(meta_path)  # init dirs
    res = runner.invoke(
        cli,
        [
            "logs",
            "unknown",
            "task",
            "--logical-date",
            "2026-06-03",
            "--metadata-path",
            str(meta_path),
            "--log-dir",
            str(tmp_path / "logs"),
        ],
    )
    assert res.exit_code == 1, res.output
