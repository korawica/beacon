"""End-to-end test: Backfill preserves upstream outputs.

Scenario:
  - DAG has task1 (produces a random value) and task2 (uses task1's output).
  - After a full run, backfilling only task2 should reuse task1's original
    stored output from metadata (not re-run task1).
  - Backfilling both task1 and task2 should produce a new random value from
    task1, and task2 should use the new value.
"""

import asyncio
from datetime import datetime

import pytest

from beacon.core import TaskContext, TaskState
from beacon.metadata.local_store import LocalMetadata
from beacon.worker import Worker
from beacon.providers.standard.plugins.task.python import PythonPlugin  # noqa: F401


SCRIPT_TASK1_RANDOM = """\
import random

def main():
    value = random.randint(1, 1_000_000)
    return {"result": value}
"""

SCRIPT_TASK2_USE_UPSTREAM = """\
from beacon import load_context

def main():
    ctx = load_context()
    upstream_result = ctx.upstream_outputs["task1"]["result"]
    return {"received": upstream_result}
"""


@pytest.fixture
def workspace(tmp_path):
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "task1_random.py").write_text(SCRIPT_TASK1_RANDOM)
    (scripts / "task2_use_upstream.py").write_text(SCRIPT_TASK2_USE_UPSTREAM)
    return {
        "scripts": scripts,
        "metadata": tmp_path / "metadata.db",
    }


def _make_ctx(
    run_id: str, task_id: str, py_statement: str, logical_date: datetime
) -> TaskContext:
    return TaskContext(
        run_id=run_id,
        dag_id="backfill-dag",
        task_id=task_id,
        dag_version="v1",
        run_date=logical_date,
        logical_date=logical_date,
        data_interval_start=logical_date,
        data_interval_end=logical_date,
        params={},
        inputs={
            "py_statement": py_statement,
            "py_function": "main",
            "params": {},
        },
        plugin_name="py",
    )


async def _run_task(meta, task_ctx, upstream_task_ids=None):
    """Helper to submit and run a single task to completion."""
    worker = Worker(meta, max_concurrent=5)
    await worker.submit(task_ctx, upstream_task_ids=upstream_task_ids)

    async def stop():
        await asyncio.sleep(0.5)
        await worker.shutdown()

    await asyncio.gather(worker.run(), stop())


class TestBackfillUpstreamOutputs:
    """Backfill scenarios for upstream output preservation."""

    def test_backfill_task2_only_reuses_original_task1_output(self, workspace):
        """When backfilling only task2, it uses the already-stored task1 output.

        This simulates: task1 ran on day1 and produced result=X.
        We want to re-run task2 only — it should still see result=X.
        """
        meta = LocalMetadata(workspace["metadata"])
        run_id = "run-day1"
        day1 = datetime(2026, 6, 1)

        task1_ctx = _make_ctx(
            run_id, "task1", str(workspace["scripts"] / "task1_random.py"), day1
        )
        task2_ctx = _make_ctx(
            run_id,
            "task2",
            str(workspace["scripts"] / "task2_use_upstream.py"),
            day1,
        )

        # Step 1: Run task1 (produces a random value)
        asyncio.run(_run_task(meta, task1_ctx))

        state = asyncio.run(
            meta.get_task_state(run_id, "backfill-dag", "task1")
        )
        assert state == TaskState.SUCCESS

        stored_ctx = asyncio.run(
            meta.get_task_context(run_id, "backfill-dag", "task1")
        )
        original_value = stored_ctx.outputs["result"]
        assert isinstance(original_value, int)

        # Step 2: Run task2 with upstream=["task1"] (first time)
        asyncio.run(_run_task(meta, task2_ctx, upstream_task_ids=["task1"]))

        state = asyncio.run(
            meta.get_task_state(run_id, "backfill-dag", "task2")
        )
        assert state == TaskState.SUCCESS

        task2_result = asyncio.run(
            meta.get_task_context(run_id, "backfill-dag", "task2")
        )
        assert task2_result.outputs["received"] == original_value

        # Step 3: Backfill — re-run task2 ONLY (task1 is NOT re-run)
        # Create a fresh task2 context (simulates backfill of task2 only)
        task2_backfill_ctx = _make_ctx(
            run_id,
            "task2",
            str(workspace["scripts"] / "task2_use_upstream.py"),
            day1,
        )

        asyncio.run(
            _run_task(meta, task2_backfill_ctx, upstream_task_ids=["task1"])
        )

        state = asyncio.run(
            meta.get_task_state(run_id, "backfill-dag", "task2")
        )
        assert state == TaskState.SUCCESS

        task2_backfill_result = asyncio.run(
            meta.get_task_context(run_id, "backfill-dag", "task2")
        )
        # Key assertion: task2 still receives the ORIGINAL value from task1
        assert task2_backfill_result.outputs["received"] == original_value
        assert (
            task2_backfill_result.upstream_outputs["task1"]["result"]
            == original_value
        )

    def test_backfill_both_tasks_produces_new_value(self, workspace):
        """When backfilling both task1 and task2, task1 produces a new random
        value and task2 uses that new value.
        """
        meta = LocalMetadata(workspace["metadata"])
        run_id = "run-day1"
        day1 = datetime(2026, 6, 1)

        task1_ctx = _make_ctx(
            run_id, "task1", str(workspace["scripts"] / "task1_random.py"), day1
        )
        task2_ctx = _make_ctx(
            run_id,
            "task2",
            str(workspace["scripts"] / "task2_use_upstream.py"),
            day1,
        )

        # Step 1: Initial full run
        asyncio.run(_run_task(meta, task1_ctx))
        asyncio.run(_run_task(meta, task2_ctx, upstream_task_ids=["task1"]))

        # Step 2: Backfill both task1 AND task2
        # Re-run task1 — gets a new random value
        task1_backfill_ctx = _make_ctx(
            run_id, "task1", str(workspace["scripts"] / "task1_random.py"), day1
        )
        asyncio.run(_run_task(meta, task1_backfill_ctx))

        stored_ctx_after = asyncio.run(
            meta.get_task_context(run_id, "backfill-dag", "task1")
        )
        new_value = stored_ctx_after.outputs["result"]
        # The new value could theoretically be the same (1 in 1M chance),
        # but we verify the mechanism works by checking task2 gets whatever
        # task1 now has stored.

        # Re-run task2 — should pick up the NEW task1 output from metadata
        task2_backfill_ctx = _make_ctx(
            run_id,
            "task2",
            str(workspace["scripts"] / "task2_use_upstream.py"),
            day1,
        )
        asyncio.run(
            _run_task(meta, task2_backfill_ctx, upstream_task_ids=["task1"])
        )

        task2_result = asyncio.run(
            meta.get_task_context(run_id, "backfill-dag", "task2")
        )
        # task2 should use the NEW value from the re-run of task1
        assert task2_result.outputs["received"] == new_value
        assert task2_result.upstream_outputs["task1"]["result"] == new_value

    def test_backfill_task2_across_multiple_days_uses_correct_day(
        self, workspace
    ):
        """Each day's run has independent outputs. Backfilling task2 for day1
        uses day1's task1 output, not day2's.
        """
        meta = LocalMetadata(workspace["metadata"])
        day1 = datetime(2026, 6, 1)
        day2 = datetime(2026, 6, 2)

        # Run both days fully
        for run_id, day in [("run-day1", day1), ("run-day2", day2)]:
            t1 = _make_ctx(
                run_id,
                "task1",
                str(workspace["scripts"] / "task1_random.py"),
                day,
            )
            t2 = _make_ctx(
                run_id,
                "task2",
                str(workspace["scripts"] / "task2_use_upstream.py"),
                day,
            )
            asyncio.run(_run_task(meta, t1))
            asyncio.run(_run_task(meta, t2, upstream_task_ids=["task1"]))

        # Get each day's task1 output
        day1_task1 = asyncio.run(
            meta.get_task_context("run-day1", "backfill-dag", "task1")
        )
        day1_value = day1_task1.outputs["result"]

        # Backfill task2 for day1 only
        task2_backfill = _make_ctx(
            "run-day1",
            "task2",
            str(workspace["scripts"] / "task2_use_upstream.py"),
            day1,
        )
        asyncio.run(
            _run_task(meta, task2_backfill, upstream_task_ids=["task1"])
        )

        result = asyncio.run(
            meta.get_task_context("run-day1", "backfill-dag", "task2")
        )
        # Must use day1's task1 output, NOT day2's
        assert result.outputs["received"] == day1_value
        assert result.upstream_outputs["task1"]["result"] == day1_value
