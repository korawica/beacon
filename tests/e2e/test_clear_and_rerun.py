"""End-to-end tests for clear + resume (backfill / fix-and-rerun).

User story:
    "I ran the DAG last week. task2 had a bug. I fixed the code, want to
    clear task2 (and downstream task3 which used task2's outputs), then
    re-run. task1 already succeeded — I don't want to re-run it. task2
    should re-read task1's outputs from metadata."
"""

import asyncio
from datetime import datetime
from typing import Any, ClassVar

import pytest

from beacon import BasePlugin, Dag, DagRunner, Task
from beacon.core.state import TaskState
from beacon.metadata import LocalMetadata


# --- Counter plugins ------------------------------------------------------

_CALL_COUNTS: dict[str, int] = {}
_LAST_INPUTS: dict[str, dict[str, Any]] = {}


class _Counter(BasePlugin):
    """Records how many times execute() has been called per task_id and
    captures the resolved inputs each call so we can assert on them."""

    plugin_name: ClassVar[str] = "_clear_counter"
    label: str = "x"
    upstream_value: Any = None

    async def execute(self, context):
        tid = context["task_id"]
        _CALL_COUNTS[tid] = _CALL_COUNTS.get(tid, 0) + 1
        _LAST_INPUTS[tid] = {
            "label": self.label,
            "upstream_value": self.upstream_value,
        }
        return {"label": self.label, "n_calls": _CALL_COUNTS[tid]}


@pytest.fixture(autouse=True)
def _reset_counters():
    _CALL_COUNTS.clear()
    _LAST_INPUTS.clear()
    yield


def _make_pipeline_dag() -> Dag:
    return Dag(
        id="clear-pipeline",
        owners=["de"],
        actions=[
            Task(id="task1", uses="_clear_counter", inputs={"label": "one"}),
            Task(
                id="task2",
                uses="_clear_counter",
                upstream=["task1"],
                inputs={
                    "label": "two",
                    "upstream_value": "{{ outputs.task1.label }}",
                },
            ),
            Task(
                id="task3",
                uses="_clear_counter",
                upstream=["task2"],
                inputs={
                    "label": "three",
                    "upstream_value": "{{ outputs.task2.label }}",
                },
            ),
        ],
    )


# ---------------------------------------------------------------------------
# 1. Initial run executes everything once
# ---------------------------------------------------------------------------


def test_initial_run_executes_each_task_once(tmp_path):
    dag = _make_pipeline_dag()
    meta = LocalMetadata(tmp_path / "meta")
    runner = DagRunner(dag, meta=meta)

    result = asyncio.run(runner.run(run_id="run-week-1"))

    assert result.state == "success"
    assert _CALL_COUNTS == {"task1": 1, "task2": 1, "task3": 1}
    assert result.outputs["task1"]["label"] == "one"
    # Upstream values flow at execute time (recorded in _LAST_INPUTS).
    assert _LAST_INPUTS["task2"]["upstream_value"] == "one"
    assert _LAST_INPUTS["task3"]["upstream_value"] == "two"


# ---------------------------------------------------------------------------
# 2. Clear task2 with downstream=True, resume → task1 NOT re-run,
#    task2+task3 re-run; task2 still sees task1's outputs.
# ---------------------------------------------------------------------------


def test_clear_task_with_downstream_and_resume(tmp_path):
    dag = _make_pipeline_dag()
    meta = LocalMetadata(tmp_path / "meta")
    runner = DagRunner(dag, meta=meta)

    # Initial run — last week.
    asyncio.run(runner.run(run_id="run-week-1"))
    assert _CALL_COUNTS == {"task1": 1, "task2": 1, "task3": 1}

    # Clear task2 + its downstream (task3) — backfill ask.
    cleared = asyncio.run(
        runner.clear(run_id="run-week-1", task_ids="task2", downstream=True)
    )
    assert cleared == ["task2", "task3"]

    # Verify metadata: task1 still SUCCESS, task2/task3 NONE, attempts cleared.
    states = asyncio.run(
        meta.get_all_task_states("run-week-1", "clear-pipeline")
    )
    assert states["task1"] == TaskState.SUCCESS
    assert states["task2"] == TaskState.NONE
    assert states["task3"] == TaskState.NONE

    ctx2 = asyncio.run(
        meta.get_task_context("run-week-1", "clear-pipeline", "task2")
    )
    assert ctx2.attempts == []
    assert ctx2.outputs == {}

    # Resume — task1 must NOT run; task2 + task3 MUST run again.
    result = asyncio.run(runner.run(run_id="run-week-1", resume=True))

    assert result.state == "success"
    assert _CALL_COUNTS == {"task1": 1, "task2": 2, "task3": 2}

    # task2 on its rerun must have read task1's outputs from metadata.
    assert _LAST_INPUTS["task2"]["upstream_value"] == "one"
    # task3 on its rerun must have read task2's FRESH outputs.
    assert _LAST_INPUTS["task3"]["upstream_value"] == "two"

    # Result.outputs surfaces all tasks (terminal-from-metadata + rerun).
    assert "task1" in result.outputs
    assert "task2" in result.outputs
    assert "task3" in result.outputs


# ---------------------------------------------------------------------------
# 3. Clear task2 WITHOUT downstream → task3 stays SUCCESS, only task2 reruns
# ---------------------------------------------------------------------------


def test_clear_single_task_no_downstream(tmp_path):
    dag = _make_pipeline_dag()
    meta = LocalMetadata(tmp_path / "meta")
    runner = DagRunner(dag, meta=meta)

    asyncio.run(runner.run(run_id="run-week-1"))
    assert _CALL_COUNTS == {"task1": 1, "task2": 1, "task3": 1}

    cleared = asyncio.run(runner.clear(run_id="run-week-1", task_ids="task2"))
    assert cleared == ["task2"]

    result = asyncio.run(runner.run(run_id="run-week-1", resume=True))

    assert result.state == "success"
    # task1: not rerun. task2: rerun once. task3: NOT rerun (stayed SUCCESS).
    assert _CALL_COUNTS == {"task1": 1, "task2": 2, "task3": 1}


# ---------------------------------------------------------------------------
# 4. Resume without clearing is a no-op: every task is already terminal
# ---------------------------------------------------------------------------


def test_resume_without_clear_is_noop(tmp_path):
    dag = _make_pipeline_dag()
    meta = LocalMetadata(tmp_path / "meta")
    runner = DagRunner(dag, meta=meta)

    asyncio.run(runner.run(run_id="run-week-1"))
    counts_before = dict(_CALL_COUNTS)

    result = asyncio.run(runner.run(run_id="run-week-1", resume=True))
    assert result.state == "success"
    assert _CALL_COUNTS == counts_before  # nothing re-ran


# ---------------------------------------------------------------------------
# 5. Resume of a non-existent run raises
# ---------------------------------------------------------------------------


def test_resume_unknown_run_raises(tmp_path):
    dag = _make_pipeline_dag()
    runner = DagRunner(dag, meta=LocalMetadata(tmp_path / "meta"))

    with pytest.raises(ValueError, match="no DagRun"):
        asyncio.run(runner.run(run_id="never-existed", resume=True))


# ---------------------------------------------------------------------------
# 6. Clear an unknown task_id raises (don't silently no-op)
# ---------------------------------------------------------------------------


def test_clear_unknown_task_raises(tmp_path):
    dag = _make_pipeline_dag()
    meta = LocalMetadata(tmp_path / "meta")
    runner = DagRunner(dag, meta=meta)
    asyncio.run(runner.run(run_id="run-week-1"))

    with pytest.raises(ValueError, match="not found in DAG"):
        asyncio.run(runner.clear(run_id="run-week-1", task_ids="ghost-task"))


# ---------------------------------------------------------------------------
# 7. Resume preserves the ORIGINAL params (not whatever caller passes)
# ---------------------------------------------------------------------------


def test_resume_uses_original_params_not_new(tmp_path):
    class _ParamCapture(BasePlugin):
        plugin_name: ClassVar[str] = "_param_capture"
        which: str = ""

        async def execute(self, context):
            _LAST_INPUTS.setdefault(context["task_id"], {})["which"] = (
                self.which
            )
            return {"which": self.which}

    dag = Dag(
        id="param-resume",
        owners=["de"],
        actions=[
            Task(
                id="t",
                uses="_param_capture",
                inputs={"which": "{{ params.which }}"},
            ),
        ],
    )
    meta = LocalMetadata(tmp_path / "meta")
    runner = DagRunner(dag, meta=meta)

    # Initial run with params={"which": "original"}.
    asyncio.run(runner.run(run_id="run-1", params={"which": "original"}))
    assert _LAST_INPUTS["t"]["which"] == "original"

    # Clear, then resume. Even if a caller passes a new params dict,
    # resume must use the persisted ORIGINAL params for determinism.
    asyncio.run(runner.clear(run_id="run-1", task_ids="t"))
    asyncio.run(
        runner.run(
            run_id="run-1",
            params={"which": "DIFFERENT"},  # intentionally wrong
            resume=True,
        )
    )
    assert _LAST_INPUTS["t"]["which"] == "original"  # original wins


# ---------------------------------------------------------------------------
# 8. Dag.clear convenience: clear+rerun in one call from the public API
# ---------------------------------------------------------------------------


def test_dag_clear_convenience(tmp_path):
    dag = _make_pipeline_dag()
    meta_path = str(tmp_path / "meta")

    # Initial run via the public API.
    initial = dag.run(metadata_path=meta_path)
    run_id = initial["run_id"]
    assert _CALL_COUNTS == {"task1": 1, "task2": 1, "task3": 1}

    # Fix-and-rerun via Dag.clear in one shot.
    out = dag.clear(
        run_id=run_id,
        task_id="task2",
        downstream=True,
        metadata_path=meta_path,
    )
    assert out["state"] == "success"
    assert out["cleared"] == ["task2", "task3"]
    assert _CALL_COUNTS == {"task1": 1, "task2": 2, "task3": 2}


# ---------------------------------------------------------------------------
# 10. Backfill scenario: clear+rerun across multiple historic runs
# ---------------------------------------------------------------------------


def test_backfill_multiple_runs(tmp_path):
    """Simulates: every Monday for 3 weeks we ran the DAG; today we
    discover task2 had a bug. Clear & rerun task2+task3 in each historic
    run. task1 must NEVER re-execute."""
    dag = _make_pipeline_dag()
    meta = LocalMetadata(tmp_path / "meta")
    runner = DagRunner(dag, meta=meta)

    run_ids = []
    for week in (1, 2, 3):
        rid = f"run-week-{week}"
        run_ids.append(rid)
        asyncio.run(
            runner.run(
                run_id=rid,
                logical_date=datetime(2026, 5, week * 7),
            )
        )

    assert _CALL_COUNTS == {"task1": 3, "task2": 3, "task3": 3}

    # Backfill: clear+rerun task2 (with downstream) in each run.
    for rid in run_ids:
        asyncio.run(runner.clear(run_id=rid, task_ids="task2", downstream=True))
        result = asyncio.run(runner.run(run_id=rid, resume=True))
        assert result.state == "success"

    # task1 never re-ran (still 3 from the originals).
    # task2 + task3 each re-ran once per week (3 more apiece).
    assert _CALL_COUNTS == {"task1": 3, "task2": 6, "task3": 6}
