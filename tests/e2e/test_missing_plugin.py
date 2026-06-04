"""Regression: a Task referencing a non-existent plugin must FAIL the DAG
cleanly, not deadlock the runner.

This was a real bug — the executor raised ``NotImplementedError`` from
plugin lookup *before* opening an attempt, so the worker's per-task
coroutine died and the runner waited forever on its wake event.
"""

import asyncio


from beacon import Dag, DagRunner, Task
from beacon.metadata import LocalMetadata


def test_missing_plugin_fails_dag_does_not_hang(tmp_path) -> None:
    dag = Dag(
        id="bad-plugin",
        actions=[Task(id="t", uses="definitely-not-registered")],
    )

    async def go() -> None:
        runner = DagRunner(dag, meta=LocalMetadata(tmp_path))
        result = await asyncio.wait_for(runner.run(), timeout=10)
        assert result.state == "failed"
        from beacon.core.state import TaskState

        assert result.states["t"] == TaskState.FAILED

    asyncio.run(go())


def test_missing_plugin_does_not_retry(tmp_path) -> None:
    """retries=5 on a missing-plugin task — still exactly 1 attempt."""
    dag = Dag(
        id="bad-plugin-noretry",
        actions=[Task(id="t", uses="nope", retries=5, retry_delay=0)],
    )
    meta = LocalMetadata(tmp_path)

    async def go() -> None:
        runner = DagRunner(dag, meta=meta)
        result = await asyncio.wait_for(runner.run(), timeout=10)
        assert result.state == "failed"
        ctx = await meta.get_task_context(
            result.run_id, "bad-plugin-noretry", "t"
        )
        assert ctx is not None
        assert len(ctx.attempts) == 1
        assert (
            ctx.attempts[0].error
            and "not found" in ctx.attempts[0].error.lower()
        )

    asyncio.run(go())


def test_buggy_executor_raising_does_not_hang(tmp_path) -> None:
    """Worker safety net: a misbehaving executor that *raises* (instead of
    returning a failed TaskContext) must still produce a terminal state."""
    from beacon.core.executor import BaseExecutor

    class BoomExecutor(BaseExecutor):
        executor_type = "boom"

        async def run_task(self, task_ctx):  # type: ignore[override]
            raise RuntimeError("kaboom")

    dag = Dag(id="boom", actions=[Task(id="t", uses="empty")])

    async def go() -> None:
        runner = DagRunner(
            dag, meta=LocalMetadata(tmp_path), executor=BoomExecutor()
        )
        result = await asyncio.wait_for(runner.run(), timeout=10)
        assert result.state == "failed"

    asyncio.run(go())
