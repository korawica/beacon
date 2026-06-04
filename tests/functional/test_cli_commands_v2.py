"""Functional tests for commit-2 CLI commands: deploy / list / sync / trigger."""

import asyncio
import textwrap
from pathlib import Path

import pytest
from click.testing import CliRunner

from beacon.cli import cli
from beacon.metadata import LocalMetadata


DAG_OK = """
from beacon import Dag, Task
dag = Dag(id="hello", actions=[Task(id="a", uses="empty")])
"""

DAG_BAD = """
from beacon import Dag, Task
dag = Dag(id="bad", actions=[
    Task(id="a", uses="empty", upstream=["b"]),
    Task(id="b", uses="empty", upstream=["a"]),
])
"""


def _write(path: Path, body: str) -> Path:
    path.write_text(textwrap.dedent(body).lstrip())
    return path


def _bundle(tmp_path: Path, dags: dict[str, str]) -> Path:
    """Make a LocalBundle at tmp_path with the given dag files."""
    dags_dir = tmp_path / "dags"
    dags_dir.mkdir()
    for name, body in dags.items():
        _write(dags_dir / name, body)
    return tmp_path


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


# ---------- deploy --------------------------------------------------------


def test_deploy_creates_record(runner: CliRunner, tmp_path: Path) -> None:
    meta = tmp_path / "m"
    res = runner.invoke(
        cli,
        [
            "deploy",
            "--id",
            "d1",
            "--dag-id",
            "etl",
            "--cron",
            "0 * * * *",
            "--param",
            "src=pg",
            "--owner",
            "alice",
            "--metadata-path",
            str(meta),
        ],
    )
    assert res.exit_code == 0, res.output
    rec = asyncio.run(LocalMetadata(meta).get_deployment("d1"))
    assert rec["dag_id"] == "etl"
    assert rec["cron"] == "0 * * * *"
    assert rec["params"] == {"src": "pg"}
    assert rec["owners"] == ["alice"]
    assert rec["enabled"] is True


def test_deploy_rejects_bad_cron(runner: CliRunner, tmp_path: Path) -> None:
    res = runner.invoke(
        cli,
        [
            "deploy",
            "--id",
            "d",
            "--dag-id",
            "x",
            "--cron",
            "not a cron",
            "--metadata-path",
            str(tmp_path / "m"),
        ],
    )
    assert res.exit_code != 0
    assert "Invalid cron" in res.output


def test_deploy_upsert_preserves_scheduler_bookkeeping(
    runner: CliRunner, tmp_path: Path
) -> None:
    meta_path = tmp_path / "m"
    meta = LocalMetadata(meta_path)
    runner.invoke(
        cli,
        [
            "deploy",
            "--id",
            "d",
            "--dag-id",
            "x",
            "--cron",
            "* * * * *",
            "--metadata-path",
            str(meta_path),
        ],
    )
    from datetime import datetime

    asyncio.run(
        meta.update_deployment_scheduler_state(
            "d", last_scheduled_at=datetime(2026, 1, 1)
        )
    )
    # Re-deploy with a different cron.
    res = runner.invoke(
        cli,
        [
            "deploy",
            "--id",
            "d",
            "--dag-id",
            "x",
            "--cron",
            "0 * * * *",
            "--metadata-path",
            str(meta_path),
        ],
    )
    assert res.exit_code == 0
    rec = asyncio.run(meta.get_deployment("d"))
    assert rec["cron"] == "0 * * * *"
    assert rec["_scheduler"]["last_scheduled_at"] == "2026-01-01T00:00:00"


def test_deploy_disabled_flag(runner: CliRunner, tmp_path: Path) -> None:
    meta_path = tmp_path / "m"
    res = runner.invoke(
        cli,
        [
            "deploy",
            "--id",
            "d",
            "--dag-id",
            "x",
            "--disabled",
            "--metadata-path",
            str(meta_path),
        ],
    )
    assert res.exit_code == 0
    rec = asyncio.run(LocalMetadata(meta_path).get_deployment("d"))
    assert rec["enabled"] is False


# ---------- list ----------------------------------------------------------


def test_list_dags(runner: CliRunner, tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, {"d.py": DAG_OK})
    res = runner.invoke(cli, ["list", "dags", str(bundle)])
    assert res.exit_code == 0
    assert "hello" in res.output


def test_list_deployments_empty(runner: CliRunner, tmp_path: Path) -> None:
    res = runner.invoke(
        cli, ["list", "deployments", "--metadata-path", str(tmp_path / "m")]
    )
    assert res.exit_code == 0
    assert "no deployments" in res.output


def test_list_deployments_after_deploy(
    runner: CliRunner, tmp_path: Path
) -> None:
    meta = tmp_path / "m"
    runner.invoke(
        cli,
        [
            "deploy",
            "--id",
            "abc",
            "--dag-id",
            "etl",
            "--metadata-path",
            str(meta),
        ],
    )
    res = runner.invoke(
        cli, ["list", "deployments", "--metadata-path", str(meta)]
    )
    assert res.exit_code == 0
    assert "abc" in res.output and "etl" in res.output


def test_list_runs_empty(runner: CliRunner, tmp_path: Path) -> None:
    res = runner.invoke(
        cli, ["list", "runs", "--metadata-path", str(tmp_path / "m")]
    )
    assert res.exit_code == 0
    assert "no runs" in res.output


def test_list_runs_after_run(runner: CliRunner, tmp_path: Path) -> None:
    f = _write(tmp_path / "d.py", DAG_OK)
    meta = tmp_path / "m"
    runner.invoke(cli, ["run", str(f), "--metadata-path", str(meta)])
    res = runner.invoke(cli, ["list", "runs", "--metadata-path", str(meta)])
    assert res.exit_code == 0
    assert "hello" in res.output
    assert "success" in res.output


# ---------- sync ----------------------------------------------------------


def test_sync_validates_bundle(runner: CliRunner, tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, {"d.py": DAG_OK})
    res = runner.invoke(cli, ["sync", str(bundle)])
    assert res.exit_code == 0
    assert "hello" in res.output
    assert "failed=0" in res.output


def test_sync_fails_on_invalid_dag(runner: CliRunner, tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, {"d.py": DAG_BAD})
    res = runner.invoke(cli, ["sync", str(bundle)])
    assert res.exit_code != 0
    assert "failed=1" in res.output


def test_sync_empty_bundle(runner: CliRunner, tmp_path: Path) -> None:
    (tmp_path / "dags").mkdir()
    res = runner.invoke(cli, ["sync", str(tmp_path)])
    assert res.exit_code != 0


# ---------- trigger -------------------------------------------------------


def test_trigger_unknown_deployment(runner: CliRunner, tmp_path: Path) -> None:
    res = runner.invoke(
        cli,
        ["trigger", "ghost", "--metadata-path", str(tmp_path / "m")],
    )
    assert res.exit_code != 0
    assert "Unknown deployment" in res.output


def test_trigger_enqueues(runner: CliRunner, tmp_path: Path) -> None:
    meta = tmp_path / "m"
    runner.invoke(
        cli,
        ["deploy", "--id", "d", "--dag-id", "x", "--metadata-path", str(meta)],
    )
    res = runner.invoke(
        cli,
        ["trigger", "d", "--param", "k=v", "--metadata-path", str(meta)],
    )
    assert res.exit_code == 0
    assert "trigger enqueued" in res.output

    drained = asyncio.run(LocalMetadata(meta).drain_triggers("d"))
    assert len(drained) == 1
    assert drained[0]["params"] == {"k": "v"}
