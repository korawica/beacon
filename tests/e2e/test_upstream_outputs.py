"""End-to-end test: Upstream output transfer to downstream tasks.

Tests that outputs from an upstream task are available in downstream tasks
via load_context().upstream_outputs.
"""

import asyncio
from datetime import datetime

import pytest

from beacon.core import TaskContext, TaskState
from beacon.metadata.local_store import LocalMetadata
from beacon.worker import Worker
from beacon.providers.standard.plugins.task.python import PythonPlugin  # noqa: F401


SCRIPT_EXTRACT = """\
def main():
    return {"files": ["a.csv", "b.csv"], "row_count": 1000}
"""

SCRIPT_TRANSFORM = """\
from beacon import load_context

def main():
    ctx = load_context()
    # Read upstream outputs
    extract_out = ctx.upstream_outputs.get("extract", {})
    files = extract_out.get("files", [])
    row_count = extract_out.get("row_count", 0)
    return {
        "processed_files": files,
        "input_rows": row_count,
        "output_rows": row_count * 2,
    }
"""

SCRIPT_LOAD = """\
from beacon import load_context

def main():
    ctx = load_context()
    # Can read from multiple upstream tasks
    transform_out = ctx.upstream_outputs.get("transform", {})
    extract_out = ctx.upstream_outputs.get("extract", {})
    return {
        "loaded_rows": transform_out.get("output_rows", 0),
        "source_files": extract_out.get("files", []),
    }
"""


@pytest.fixture
def workspace(tmp_path):
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "extract.py").write_text(SCRIPT_EXTRACT)
    (scripts / "transform.py").write_text(SCRIPT_TRANSFORM)
    (scripts / "load.py").write_text(SCRIPT_LOAD)
    return {
        "scripts": scripts,
        "metadata": tmp_path / "metadata.db",
    }


def _ctx(task_id: str, py_statement: str) -> TaskContext:
    return TaskContext(
        run_id="run-dag-001",
        dag_id="etl-pipeline",
        task_id=task_id,
        dag_version="v1",
        run_date=datetime(2026, 6, 3),
        logical_date=datetime(2026, 6, 3),
        data_interval_start=datetime(2026, 6, 3),
        data_interval_end=datetime(2026, 6, 4),
        params={},
        inputs={
            "py_statement": py_statement,
            "py_function": "main",
            "params": {},
        },
        plugin_name="py",
    )


def test_upstream_outputs_available_in_downstream(workspace):
    """Downstream task can read upstream task outputs via load_context()."""
    meta = LocalMetadata(workspace["metadata"])
    worker = Worker(meta, max_concurrent=5)

    extract_ctx = _ctx("extract", str(workspace["scripts"] / "extract.py"))
    transform_ctx = _ctx(
        "transform", str(workspace["scripts"] / "transform.py")
    )
    load_ctx = _ctx("load", str(workspace["scripts"] / "load.py"))

    async def _run():
        # Step 1: Run extract (no upstream)
        await worker.submit(extract_ctx)

        async def run_extract():
            await asyncio.sleep(0.3)
            await worker.shutdown()

        await asyncio.gather(worker.run(), run_extract())

    asyncio.run(_run())

    # Verify extract succeeded with outputs
    state = asyncio.run(
        meta.get_task_state("run-dag-001", "etl-pipeline", "extract")
    )
    assert state == TaskState.SUCCESS
    ctx = asyncio.run(
        meta.get_task_context("run-dag-001", "etl-pipeline", "extract")
    )
    assert ctx.outputs == {"files": ["a.csv", "b.csv"], "row_count": 1000}

    # Step 2: Run transform with extract as upstream
    worker2 = Worker(meta, max_concurrent=5)

    async def _run2():
        await worker2.submit(
            transform_ctx,
            upstream_task_ids=["extract"],
        )

        async def stop():
            await asyncio.sleep(0.3)
            await worker2.shutdown()

        await asyncio.gather(worker2.run(), stop())

    asyncio.run(_run2())

    state = asyncio.run(
        meta.get_task_state("run-dag-001", "etl-pipeline", "transform")
    )
    assert state == TaskState.SUCCESS
    ctx = asyncio.run(
        meta.get_task_context("run-dag-001", "etl-pipeline", "transform")
    )
    assert ctx.outputs == {
        "processed_files": ["a.csv", "b.csv"],
        "input_rows": 1000,
        "output_rows": 2000,
    }
    # Verify upstream_outputs was populated
    assert ctx.upstream_outputs == {
        "extract": {"files": ["a.csv", "b.csv"], "row_count": 1000}
    }

    # Step 3: Run load with both extract and transform as upstream
    worker3 = Worker(meta, max_concurrent=5)

    async def _run3():
        await worker3.submit(
            load_ctx,
            upstream_task_ids=["extract", "transform"],
        )

        async def stop():
            await asyncio.sleep(0.3)
            await worker3.shutdown()

        await asyncio.gather(worker3.run(), stop())

    asyncio.run(_run3())

    state = asyncio.run(
        meta.get_task_state("run-dag-001", "etl-pipeline", "load")
    )
    assert state == TaskState.SUCCESS
    ctx = asyncio.run(
        meta.get_task_context("run-dag-001", "etl-pipeline", "load")
    )
    assert ctx.outputs == {
        "loaded_rows": 2000,
        "source_files": ["a.csv", "b.csv"],
    }


def test_empty_upstream_outputs_when_no_upstream(workspace):
    """Task with no upstream has empty upstream_outputs."""
    meta = LocalMetadata(workspace["metadata"])
    worker = Worker(meta, max_concurrent=5)

    extract_ctx = _ctx("extract", str(workspace["scripts"] / "extract.py"))

    async def _run():
        await worker.submit(extract_ctx)

        async def stop():
            await asyncio.sleep(0.3)
            await worker.shutdown()

        await asyncio.gather(worker.run(), stop())

    asyncio.run(_run())

    ctx = asyncio.run(
        meta.get_task_context("run-dag-001", "etl-pipeline", "extract")
    )
    assert ctx.upstream_outputs == {}
    assert ctx.outputs == {"files": ["a.csv", "b.csv"], "row_count": 1000}
