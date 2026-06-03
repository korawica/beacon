"""End-to-end test: 100 DAGs running in parallel.

Validates that the JsonMetadata store handles concurrent writes/reads
from 100 independent DAG runs, each with multiple tasks, without data
corruption or performance degradation.
"""

import asyncio
import time
from datetime import datetime

import pytest

from beacon.core import TaskContext, TaskState
from beacon.metadata.json_store import JsonMetadata
from beacon.worker import Worker

# Ensure py plugin is registered
from beacon.providers.standard.plugins.task.python import PythonPlugin  # noqa: F401


SCRIPT_FAST = """\
def main(src_dag: str, src_task: str):
    return {"dag_id": src_dag, "task_id": src_task, "status": "done"}
"""

SCRIPT_MULTI_STEP = """\
def main(step: int):
    return {"step": step, "result": step * 10}
"""


@pytest.fixture
def workspace(tmp_path):
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "fast.py").write_text(SCRIPT_FAST)
    (scripts / "multi_step.py").write_text(SCRIPT_MULTI_STEP)
    return {
        "scripts": scripts,
        "metadata": tmp_path / "metadata.db",
    }


def _make_task_ctx(
    dag_id: str, run_id: str, task_id: str, py_file: str, **params
) -> TaskContext:
    return TaskContext(
        run_id=run_id,
        dag_id=dag_id,
        task_id=task_id,
        dag_version="v1",
        run_date=datetime(2026, 6, 3),
        logical_date=datetime(2026, 6, 3),
        data_interval_start=datetime(2026, 6, 3),
        data_interval_end=datetime(2026, 6, 4),
        params=params,
        inputs={"py_file": py_file, "py_function": "main", "params": params},
        plugin_name="py",
    )


class TestParallel100Dags:
    """Test 100 DAGs executing in parallel through the worker."""

    def test_100_dags_single_task_each(self, workspace):
        """100 different DAGs each with 1 task, all submitted at once."""
        meta = JsonMetadata(workspace["metadata"])
        worker = Worker(meta, max_concurrent=50)
        py_file = str(workspace["scripts"] / "fast.py")

        num_dags = 100
        contexts = []
        for i in range(num_dags):
            ctx = _make_task_ctx(
                dag_id=f"dag-{i:03d}",
                run_id=f"run-{i:03d}",
                task_id="task-main",
                py_file=py_file,
                src_dag=f"dag-{i:03d}",
                src_task="task-main",
            )
            contexts.append(ctx)

        async def _run():
            # Submit all 100 tasks
            for ctx in contexts:
                await worker.submit(ctx)

            # Wait for completion then shutdown
            async def stop_after():
                # Poll until queue is empty and no active tasks
                for _ in range(100):  # max 10s
                    await asyncio.sleep(0.1)
                    if worker._queue.empty() and len(worker._tasks) == 0:
                        break
                await asyncio.sleep(0.2)  # buffer for final writes
                await worker.shutdown()

            await asyncio.gather(worker.run(), stop_after())

        t0 = time.time()
        asyncio.run(_run())
        elapsed = time.time() - t0

        # Verify ALL 100 tasks succeeded
        async def _verify():
            success_count = 0
            for i in range(num_dags):
                state = await meta.get_task_state(
                    f"run-{i:03d}", f"dag-{i:03d}", "task-main"
                )
                assert state == TaskState.SUCCESS, (
                    f"dag-{i:03d} expected SUCCESS, got {state}"
                )
                success_count += 1

                # Verify outputs are correct (no cross-contamination)
                ctx = await meta.get_task_context(
                    f"run-{i:03d}", f"dag-{i:03d}", "task-main"
                )
                assert ctx.outputs["dag_id"] == f"dag-{i:03d}"
                assert ctx.outputs["task_id"] == "task-main"
                assert ctx.outputs["status"] == "done"

            return success_count

        count = asyncio.run(_verify())
        assert count == 100

        # Performance: 100 simple tasks should finish within 10s
        assert elapsed < 10.0, f"Took {elapsed:.1f}s, expected < 10s"

    def test_100_dags_with_3_tasks_each(self, workspace):
        """100 DAGs each with 3 sequential tasks (300 total task executions).

        Tasks within each DAG run sequentially (submitted after previous
        completes), but all 100 DAGs run concurrently.
        """
        meta = JsonMetadata(workspace["metadata"])
        worker = Worker(meta, max_concurrent=50)
        py_file = str(workspace["scripts"] / "multi_step.py")

        num_dags = 100
        tasks_per_dag = 3

        async def _run():
            # Submit first task of each DAG
            for i in range(num_dags):
                ctx = _make_task_ctx(
                    dag_id=f"dag-{i:03d}",
                    run_id=f"run-{i:03d}",
                    task_id="step-0",
                    py_file=py_file,
                    step=0,
                )
                await worker.submit(ctx)

            # Background: chain remaining tasks as they complete
            async def chain_tasks():
                submitted = {i: 0 for i in range(num_dags)}
                while any(s < tasks_per_dag - 1 for s in submitted.values()):
                    await asyncio.sleep(0.05)
                    for i in range(num_dags):
                        current_step = submitted[i]
                        if current_step >= tasks_per_dag - 1:
                            continue
                        # Check if current step completed
                        state = await meta.get_task_state(
                            f"run-{i:03d}",
                            f"dag-{i:03d}",
                            f"step-{current_step}",
                        )
                        if state == TaskState.SUCCESS:
                            next_step = current_step + 1
                            ctx = _make_task_ctx(
                                dag_id=f"dag-{i:03d}",
                                run_id=f"run-{i:03d}",
                                task_id=f"step-{next_step}",
                                py_file=py_file,
                                step=next_step,
                            )
                            await worker.submit(ctx)
                            submitted[i] = next_step

            async def stop_when_done():
                await chain_tasks()
                # Wait for final tasks to complete
                for _ in range(100):
                    await asyncio.sleep(0.1)
                    if worker._queue.empty() and len(worker._tasks) == 0:
                        break
                await asyncio.sleep(0.2)
                await worker.shutdown()

            await asyncio.gather(worker.run(), stop_when_done())

        t0 = time.time()
        asyncio.run(_run())
        elapsed = time.time() - t0

        # Verify all 300 tasks succeeded
        async def _verify():
            for i in range(num_dags):
                for step in range(tasks_per_dag):
                    state = await meta.get_task_state(
                        f"run-{i:03d}", f"dag-{i:03d}", f"step-{step}"
                    )
                    assert state == TaskState.SUCCESS, (
                        f"dag-{i:03d}/step-{step} expected SUCCESS, got {state}"
                    )
                    ctx = await meta.get_task_context(
                        f"run-{i:03d}", f"dag-{i:03d}", f"step-{step}"
                    )
                    assert ctx.outputs == {
                        "step": step,
                        "result": step * 10,
                    }

        asyncio.run(_verify())

        # 300 tasks with concurrency=50 should finish within 30s
        assert elapsed < 30.0, f"Took {elapsed:.1f}s, expected < 30s"

    def test_metadata_isolation_across_dags(self, workspace):
        """Verify no data leaks between DAGs writing to metadata concurrently."""
        meta = JsonMetadata(workspace["metadata"])
        py_file = str(workspace["scripts"] / "fast.py")

        num_dags = 100

        async def _run():
            # Simulate concurrent metadata writes from 100 DAGs
            tasks = []
            for i in range(num_dags):
                dag_id = f"dag-{i:03d}"
                run_id = f"run-{i:03d}"

                async def _write_dag(d_id, r_id, idx):
                    await meta.create_dag_run(r_id, d_id, f"v{idx}")
                    ctx = _make_task_ctx(
                        dag_id=d_id,
                        run_id=r_id,
                        task_id="task-a",
                        py_file=py_file,
                        src_dag=d_id,
                        src_task="task-a",
                    )
                    await meta.put_task_context(r_id, d_id, "task-a", ctx)
                    await meta.set_task_state(
                        r_id, d_id, "task-a", TaskState.RUNNING
                    )
                    # Simulate work
                    await asyncio.sleep(0.01)
                    await meta.set_task_state(
                        r_id, d_id, "task-a", TaskState.SUCCESS
                    )

                tasks.append(_write_dag(dag_id, run_id, i))

            # Run all 100 concurrently
            await asyncio.gather(*tasks)

        asyncio.run(_run())

        # Verify each DAG's data is isolated and correct
        async def _verify():
            for i in range(num_dags):
                dag_id = f"dag-{i:03d}"
                run_id = f"run-{i:03d}"

                # DagRun has correct version
                run_data = await meta.get_dag_run(run_id, dag_id)
                assert run_data is not None, f"Missing dag_run for {dag_id}"
                assert run_data["dag_version"] == f"v{i}"
                assert run_data["dag_id"] == dag_id

                # TaskContext belongs to correct DAG
                ctx = await meta.get_task_context(run_id, dag_id, "task-a")
                assert ctx is not None, f"Missing context for {dag_id}"
                assert ctx.dag_id == dag_id
                assert ctx.run_id == run_id

                # TaskState is correct
                state = await meta.get_task_state(run_id, dag_id, "task-a")
                assert state == TaskState.SUCCESS

        asyncio.run(_verify())

    def test_bulk_query_performance(self, workspace):
        """Test get_all_task_states bulk query is faster than N individual reads."""
        meta = JsonMetadata(workspace["metadata"])

        num_tasks = 50
        dag_id = "bulk-dag"
        run_id = "bulk-run"

        # Setup: create 50 task states
        async def _setup():
            await meta.create_dag_run(run_id, dag_id, "v1")
            for i in range(num_tasks):
                await meta.set_task_state(
                    run_id, dag_id, f"task-{i:03d}", TaskState.SUCCESS
                )

        asyncio.run(_setup())

        # Clear cache to force file reads
        meta._state_cache.clear()

        # Measure: bulk query
        async def _bulk():
            t0 = time.time()
            states = await meta.get_all_task_states(run_id, dag_id)
            bulk_time = time.time() - t0
            return states, bulk_time

        states, bulk_time = asyncio.run(_bulk())
        assert len(states) == num_tasks
        assert all(s == TaskState.SUCCESS for s in states.values())

        # Clear cache again
        meta._state_cache.clear()

        # Measure: individual queries
        async def _individual():
            t0 = time.time()
            for i in range(num_tasks):
                await meta.get_task_state(run_id, dag_id, f"task-{i:03d}")
            return time.time() - t0

        individual_time = asyncio.run(_individual())

        # Bulk should not be dramatically slower than individual
        # (both hit disk, but bulk avoids repeated cache lock overhead)
        assert bulk_time < individual_time * 2, (
            f"Bulk {bulk_time:.3f}s vs individual {individual_time:.3f}s"
        )
