"""Tests for ``beacon logs``."""

import asyncio
import json
from datetime import datetime
from pathlib import Path

import pytest
from click.testing import CliRunner

from beacon.cli import cli
from beacon.metadata import LocalMetadata


def _write_attempt(
    base: Path,
    *,
    dag_id: str,
    run_id: str,
    task_id: str,
    attempt: int,
    lines: list[dict],
) -> Path:
    d = base / dag_id / run_id / task_id
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"attempt_{attempt}.jsonl"
    p.write_text("\n".join(json.dumps(line) for line in lines) + "\n")
    return p


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def test_requires_run_or_logical_date(
    runner: CliRunner, tmp_path: Path
) -> None:
    res = runner.invoke(
        cli, ["logs", "dag", "task", "--log-dir", str(tmp_path)]
    )
    assert res.exit_code != 0
    assert "either --run" in res.output.lower()


def test_missing_log_dir_exits_nonzero(
    runner: CliRunner, tmp_path: Path
) -> None:
    res = runner.invoke(
        cli,
        [
            "logs",
            "dag",
            "task",
            "--run",
            "r1",
            "--log-dir",
            str(tmp_path),
        ],
    )
    assert res.exit_code != 0
    assert "No log dir" in res.output


def test_dumps_latest_attempt_by_default(
    runner: CliRunner, tmp_path: Path
) -> None:
    base = tmp_path / "logs"
    _write_attempt(
        base,
        dag_id="d",
        run_id="r1",
        task_id="t",
        attempt=1,
        lines=[{"msg": "first"}],
    )
    _write_attempt(
        base,
        dag_id="d",
        run_id="r1",
        task_id="t",
        attempt=2,
        lines=[{"msg": "second"}],
    )
    res = runner.invoke(
        cli,
        ["logs", "d", "t", "--run", "r1", "--log-dir", str(base)],
    )
    assert res.exit_code == 0, res.output
    assert "second" in res.output
    assert "first" not in res.output  # default is latest, not all


def test_specific_attempt(runner: CliRunner, tmp_path: Path) -> None:
    base = tmp_path / "logs"
    _write_attempt(
        base,
        dag_id="d",
        run_id="r1",
        task_id="t",
        attempt=1,
        lines=[{"msg": "first"}],
    )
    _write_attempt(
        base,
        dag_id="d",
        run_id="r1",
        task_id="t",
        attempt=2,
        lines=[{"msg": "second"}],
    )
    res = runner.invoke(
        cli,
        [
            "logs",
            "d",
            "t",
            "--run",
            "r1",
            "--attempt",
            "1",
            "--log-dir",
            str(base),
        ],
    )
    assert res.exit_code == 0
    assert "first" in res.output
    assert "second" not in res.output


def test_missing_attempt_file(runner: CliRunner, tmp_path: Path) -> None:
    base = tmp_path / "logs"
    _write_attempt(
        base,
        dag_id="d",
        run_id="r1",
        task_id="t",
        attempt=1,
        lines=[{"msg": "x"}],
    )
    res = runner.invoke(
        cli,
        [
            "logs",
            "d",
            "t",
            "--run",
            "r1",
            "--attempt",
            "99",
            "--log-dir",
            str(base),
        ],
    )
    assert res.exit_code != 0
    assert "No such file" in res.output


def test_resolve_run_id_by_logical_date(
    runner: CliRunner, tmp_path: Path
) -> None:
    meta_path = tmp_path / "m"
    log_dir = tmp_path / "logs"
    meta = LocalMetadata(meta_path)
    asyncio.run(
        meta.create_dag_run(
            run_id="r-2026-06-01",
            dag_id="d",
            dag_version="v",
            logical_date=datetime(2026, 6, 1, 14, 0),
        )
    )
    _write_attempt(
        log_dir,
        dag_id="d",
        run_id="r-2026-06-01",
        task_id="t",
        attempt=1,
        lines=[{"msg": "hello"}],
    )
    res = runner.invoke(
        cli,
        [
            "logs",
            "d",
            "t",
            "--logical-date",
            "2026-06-01",
            "--log-dir",
            str(log_dir),
            "--metadata-path",
            str(meta_path),
        ],
    )
    assert res.exit_code == 0, res.output
    assert "hello" in res.output
    assert "resolved run_id=r-2026-06-01" in res.output


def test_resolve_run_id_no_match(runner: CliRunner, tmp_path: Path) -> None:
    res = runner.invoke(
        cli,
        [
            "logs",
            "d",
            "t",
            "--logical-date",
            "2099-01-01",
            "--log-dir",
            str(tmp_path / "logs"),
            "--metadata-path",
            str(tmp_path / "m"),
        ],
    )
    assert res.exit_code != 0
    assert "No run" in res.output
