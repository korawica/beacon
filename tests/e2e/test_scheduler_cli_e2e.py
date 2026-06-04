"""End-to-end smoke: deploy → trigger → scheduler runs it → list it."""

import textwrap
from pathlib import Path

import pytest
from click.testing import CliRunner

from beacon.cli import cli


DAG = """
from beacon import Dag, Task
dag = Dag(id="hello", actions=[Task(id="t", uses="empty")])
"""


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def test_deploy_trigger_scheduler_run_then_list(
    runner: CliRunner, tmp_path: Path
) -> None:
    # 1. Set up bundle on disk.
    bundle = tmp_path / "bundle"
    (bundle / "dags").mkdir(parents=True)
    (bundle / "dags" / "d.py").write_text(textwrap.dedent(DAG).lstrip())
    meta = tmp_path / "meta"

    # 2. Validate via sync.
    res = runner.invoke(cli, ["sync", str(bundle)])
    assert res.exit_code == 0

    # 3. Register a deployment (manual-trigger only, no cron).
    res = runner.invoke(
        cli,
        [
            "deploy",
            "--id",
            "d1",
            "--dag-id",
            "hello",
            "--metadata-path",
            str(meta),
        ],
    )
    assert res.exit_code == 0

    # 4. Enqueue a manual trigger.
    res = runner.invoke(cli, ["trigger", "d1", "--metadata-path", str(meta)])
    assert res.exit_code == 0
    assert "trigger enqueued" in res.output

    # 5. Drive ONE scheduler tick. We do this in-process via the module
    # so we don't need a subprocess.
    import asyncio
    from beacon.metadata import LocalMetadata
    from beacon.scheduler import DeploymentScheduler

    async def drive() -> None:
        sched = DeploymentScheduler(bundle, LocalMetadata(meta))
        sched.reload()
        await sched._tick()
        if sched._tasks:
            await asyncio.gather(*sched._tasks, return_exceptions=True)

    asyncio.run(drive())

    # 6. List runs — there should be exactly one, success.
    res = runner.invoke(cli, ["list", "runs", "--metadata-path", str(meta)])
    assert res.exit_code == 0
    assert "hello" in res.output
    assert "success" in res.output
