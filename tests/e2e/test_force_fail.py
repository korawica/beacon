"""Force-fail + teardown re-fire — the 'kill stuck task, clean up resource' flow.

User story:
    "I have a process task running a Spark job. The cluster is sick.
    I force-fail the task → the teardown (stop-spark) fires automatically
    to clean up, without me having to manually clear the teardown."

Development journey comparison:

    Airflow:
        1. Open UI → find the stuck task → click "Mark Failed"
        2. The scheduler sees a newly-FAILED task
        3. If it has `as_teardown(setups=...)`, the setup/teardown logic
           re-evaluates and fires cleanup

    Beacon:
        1. dag.fail(run_id=..., task_id="process", metadata_path=...)
        2. Internally: mark FAILED → auto-clear affected teardown → resume
        3. Teardown fires, reads setup outputs, cleans up the resource
"""

import asyncio
from typing import ClassVar

import pytest

from beacon import BasePlugin, Dag, DagRunner, Task
from beacon.core.state import TaskState
from beacon.metadata import LocalMetadata


_EVENTS: list[str] = []


@pytest.fixture(autouse=True)
def _reset():
    _EVENTS.clear()
    yield


class _Launch(BasePlugin):
    plugin_name: ClassVar[str] = "_ff_launch"

    async def execute(self, context):
        _EVENTS.append("launch")
        return {"app_id": f"spark-{context['run_id']}"}


class _Process(BasePlugin):
    plugin_name: ClassVar[str] = "_ff_process"
    app_id: str = ""

    async def execute(self, context):
        _EVENTS.append(f"process:{self.app_id}")
        return {"processed": True}


class _Stop(BasePlugin):
    plugin_name: ClassVar[str] = "_ff_stop"
    app_id: str = ""

    async def execute(self, context):
        _EVENTS.append(f"stop:{self.app_id}")
        return {"stopped": self.app_id}


def _spark_dag() -> Dag:
    return Dag(
        id="spark-fail",
        owners=["de"],
        actions=[
            Task(id="launch", uses="_ff_launch"),
            Task(
                id="process",
                uses="_ff_process",
                upstream=["launch"],
                inputs={"app_id": "{{ outputs.launch.app_id }}"},
            ),
            Task(
                id="stop",
                uses="_ff_stop",
                teardown="launch",
                inputs={"app_id": "{{ outputs.launch.app_id }}"},
            ),
        ],
    )


# ---------------------------------------------------------------------------
# 1. Normal run — everything succeeds, teardown fires once
# ---------------------------------------------------------------------------


def test_normal_run_baseline(tmp_path):
    dag = _spark_dag()
    runner = DagRunner(dag, meta=LocalMetadata(tmp_path / "m"))
    result = asyncio.run(runner.run(run_id="manual-spark-fail-1"))
    assert result.state == "success"
    assert _EVENTS == [
        "launch",
        "process:spark-manual-spark-fail-1",
        "stop:spark-manual-spark-fail-1",
    ]


# ---------------------------------------------------------------------------
# 2. Force-fail `process` → teardown `stop` re-fires automatically
# ---------------------------------------------------------------------------


def test_force_fail_process_fires_teardown(tmp_path):
    dag = _spark_dag()
    meta = LocalMetadata(tmp_path / "m")
    runner = DagRunner(dag, meta=meta)

    # Initial run — all success.
    asyncio.run(runner.run(run_id="manual-spark-fail-1"))
    assert _EVENTS[-1].startswith("stop:")  # teardown fired

    _EVENTS.clear()

    # Force-fail `process`. This should clear `stop` and resume.
    info = asyncio.run(
        runner.fail(run_id="manual-spark-fail-1", task_ids="process")
    )
    assert info["marked"] == ["process"]
    assert info["teardowns_cleared"] == ["stop"]

    # Verify metadata: process=FAILED, stop=NONE (cleared).
    states = asyncio.run(
        meta.get_all_task_states("manual-spark-fail-1", "spark-fail")
    )
    assert states["process"] == TaskState.FAILED
    assert states["stop"] == TaskState.NONE
    assert states["launch"] == TaskState.SUCCESS  # untouched

    # Resume → only teardown fires, process stays FAILED, launch untouched.
    result = asyncio.run(runner.run(run_id="manual-spark-fail-1", resume=True))

    # DAG state is "failed" because process is FAILED.
    assert result.state == "failed"
    # But teardown DID fire — the resource was cleaned up.
    assert _EVENTS == ["stop:spark-manual-spark-fail-1"]
    assert result.states["stop"] == TaskState.SUCCESS


# ---------------------------------------------------------------------------
# 3. Dag.fail() one-liner: force-fail + cleanup in one call
# ---------------------------------------------------------------------------


def test_dag_fail_one_liner(tmp_path):
    dag = _spark_dag()
    meta_path = str(tmp_path / "m")
    initial = dag.run(metadata_path=meta_path)
    run_id = initial["run_id"]

    _EVENTS.clear()

    out = dag.fail(run_id=run_id, task_id="process", metadata_path=meta_path)

    # DAG is "failed" (process is FAILED).
    assert out["state"] == "failed"
    # But teardown fired.
    assert out["teardowns_fired"] == ["stop"]
    assert out["states"]["stop"] == TaskState.SUCCESS
    # Only teardown re-ran.
    assert _EVENTS == [f"stop:spark-{run_id}"]


# ---------------------------------------------------------------------------
# 4. Force-fail launch (the setup) → teardown also fires
# ---------------------------------------------------------------------------


def test_force_fail_setup_fires_teardown(tmp_path):
    dag = _spark_dag()
    meta_path = str(tmp_path / "m")
    initial = dag.run(metadata_path=meta_path)
    run_id = initial["run_id"]
    _EVENTS.clear()

    out = dag.fail(run_id=run_id, task_id="launch", metadata_path=meta_path)
    assert "stop" in out["teardowns_fired"]
    assert _EVENTS == [f"stop:spark-{run_id}"]


# ---------------------------------------------------------------------------
# 5. Force-fail a task with NO teardown → just marks FAILED, no crash
# ---------------------------------------------------------------------------


def test_force_fail_no_teardown_is_clean(tmp_path):
    dag = Dag(
        id="no-td",
        owners=["de"],
        actions=[
            Task(id="a", uses="_ff_launch"),
            Task(id="b", uses="_ff_process", upstream=["a"]),
        ],
    )
    meta_path = str(tmp_path / "m")
    initial = dag.run(metadata_path=meta_path)
    run_id = initial["run_id"]

    out = dag.fail(run_id=run_id, task_id="b", metadata_path=meta_path)
    # No teardowns to fire.
    assert out["teardowns_fired"] == []
    assert out["states"]["b"] == TaskState.FAILED
    assert out["state"] == "failed"


# ---------------------------------------------------------------------------
# 6. Force-fail multiple tasks at once
# ---------------------------------------------------------------------------


def test_force_fail_multiple_tasks(tmp_path):
    dag = _spark_dag()
    meta_path = str(tmp_path / "m")
    initial = dag.run(metadata_path=meta_path)
    run_id = initial["run_id"]
    _EVENTS.clear()

    out = dag.fail(
        run_id=run_id,
        task_id=["launch", "process"],
        metadata_path=meta_path,
    )
    assert set(out["marked"]) == {"launch", "process"}
    assert out["teardowns_fired"] == ["stop"]
    # Teardown fired once (not twice).
    assert len([e for e in _EVENTS if e.startswith("stop:")]) == 1
