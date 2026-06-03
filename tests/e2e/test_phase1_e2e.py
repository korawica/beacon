"""End-to-end test for Phase 1: Worker + Metadata + Callbacks."""

import asyncio
import json
from datetime import datetime

import pytest

from beacon.callback import OnTaskEvent
from beacon.providers.standard.hooks import JsonFileHook
from beacon.core import TaskContext, TaskState
from beacon.metadata.json_store import JsonMetadata
from beacon.worker import Worker

# Ensure py plugin registered
from beacon.providers.standard.plugins.task.python import PythonPlugin  # noqa: F401


USER_SCRIPT_OK = """\
def main(value: str):
    return {"result": value}
"""

USER_SCRIPT_FAIL_TWICE = """\
import os

_counter_file = os.environ.get("COUNTER_FILE", "/tmp/_beacon_test_counter")

def main():
    # Fail first 2 calls, succeed on 3rd
    try:
        count = int(open(_counter_file).read())
    except FileNotFoundError:
        count = 0
    count += 1
    open(_counter_file, "w").write(str(count))
    if count < 3:
        raise RuntimeError(f"Failing attempt {count}")
    return {"attempt_succeeded": count}
"""


@pytest.fixture
def workspace(tmp_path):
    """Create workspace with metadata and alert dirs."""
    return {
        "metadata_path": tmp_path / "metadata.db",
        "alert_path": tmp_path / "alerts",
        "scripts_path": tmp_path / "scripts",
    }


def _make_ctx(py_file: str, retries: int = 0, **params) -> TaskContext:
    return TaskContext(
        run_id="run-001",
        dag_id="test-dag",
        task_id="process",
        dag_version="v1",
        run_date=datetime(2026, 6, 3),
        logical_date=datetime(2026, 6, 3),
        data_interval_start=datetime(2026, 6, 3),
        data_interval_end=datetime(2026, 6, 4),
        params=params,
        inputs={"py_file": py_file, "py_function": "main", "params": params},
        plugin_name="py",
        retries=retries,
        retry_delay=0,  # No delay in tests
    )


def test_json_metadata_crud(workspace):
    """Test metadata store basic operations."""
    meta = JsonMetadata(workspace["metadata_path"])

    async def _test():
        # DagRun
        await meta.create_dag_run("run-1", "dag-1", "v1")
        run = await meta.get_dag_run("run-1", "dag-1")
        assert run["dag_id"] == "dag-1"
        assert run["state"] == "running"

        await meta.update_dag_run_state("run-1", "dag-1", "success")
        run = await meta.get_dag_run("run-1", "dag-1")
        assert run["state"] == "success"

        # TaskContext
        ctx = TaskContext(
            run_id="run-1",
            dag_id="dag-1",
            task_id="t1",
            dag_version="v1",
            run_date=datetime(2026, 6, 3),
            logical_date=datetime(2026, 6, 3),
            data_interval_start=datetime(2026, 6, 3),
            data_interval_end=datetime(2026, 6, 4),
            inputs={},
            plugin_name="empty",
        )
        await meta.put_task_context("run-1", "dag-1", "t1", ctx)
        loaded = await meta.get_task_context("run-1", "dag-1", "t1")
        assert loaded.dag_id == "dag-1"

        # TaskState
        await meta.set_task_state("run-1", "dag-1", "t1", TaskState.RUNNING)
        state = await meta.get_task_state("run-1", "dag-1", "t1")
        assert state == TaskState.RUNNING

    asyncio.run(_test())


def test_json_file_hook(workspace):
    """Test JsonFileHook writes alert files."""
    hook = JsonFileHook(alert_dir=str(workspace["alert_path"]))

    async def _test():
        await hook.notify(
            "failure",
            {
                "dag_id": "my-dag",
                "task_id": "my-task",
                "error": "something broke",
            },
        )

    asyncio.run(_test())

    alerts = list(workspace["alert_path"].glob("*.json"))
    assert len(alerts) == 1
    content = json.loads(alerts[0].read_text())
    assert content["event"] == "failure"
    assert content["dag_id"] == "my-dag"
    assert content["error"] == "something broke"


def test_worker_success(workspace):
    """Worker executes task and persists SUCCESS state."""
    scripts = workspace["scripts_path"]
    scripts.mkdir()
    (scripts / "ok.py").write_text(USER_SCRIPT_OK)

    meta = JsonMetadata(workspace["metadata_path"])
    worker = Worker(meta, max_concurrent=2)
    task_ctx = _make_ctx(str(scripts / "ok.py"), value="hello")

    async def _test():
        await worker.submit(task_ctx)

        # Run worker with auto-shutdown after queue drains
        async def run_and_stop():
            # Give worker time to process
            await asyncio.sleep(0.1)
            await worker.shutdown()

        await asyncio.gather(worker.run(), run_and_stop())

        # Verify state
        state = await meta.get_task_state("run-001", "test-dag", "process")
        assert state == TaskState.SUCCESS

        # Verify context persisted with outputs
        ctx = await meta.get_task_context("run-001", "test-dag", "process")
        assert ctx.outputs == {"result": "hello"}

    asyncio.run(_test())


def test_worker_with_callbacks(workspace):
    """Worker fires callbacks on events."""
    scripts = workspace["scripts_path"]
    scripts.mkdir()
    (scripts / "ok.py").write_text(USER_SCRIPT_OK)

    meta = JsonMetadata(workspace["metadata_path"])
    alert_dir = str(workspace["alert_path"])
    worker = Worker(meta)

    callbacks = [
        OnTaskEvent(
            on_event="start", hook="json-file", inputs={"alert_dir": alert_dir}
        ),
        OnTaskEvent(
            on_event="success",
            hook="json-file",
            inputs={"alert_dir": alert_dir},
        ),
    ]
    task_ctx = _make_ctx(str(scripts / "ok.py"), value="cb_test")

    async def _test():
        await worker.submit(task_ctx, callbacks=callbacks)

        async def run_and_stop():
            await asyncio.sleep(0.1)
            await worker.shutdown()

        await asyncio.gather(worker.run(), run_and_stop())

    asyncio.run(_test())

    alerts = sorted(workspace["alert_path"].glob("*.json"))
    assert len(alerts) == 2
    events = [json.loads(a.read_text())["event"] for a in alerts]
    assert "start" in events
    assert "success" in events


def test_worker_retry_flow(workspace):
    """Worker retries failed tasks and eventually succeeds."""
    scripts = workspace["scripts_path"]
    scripts.mkdir()
    (scripts / "flaky.py").write_text(USER_SCRIPT_FAIL_TWICE)

    # Use a unique counter file for this test
    counter_file = str(workspace["metadata_path"] / "_counter")

    meta = JsonMetadata(workspace["metadata_path"])
    alert_dir = str(workspace["alert_path"])
    worker = Worker(meta)

    callbacks = [
        OnTaskEvent(
            on_event="retry", hook="json-file", inputs={"alert_dir": alert_dir}
        ),
        OnTaskEvent(
            on_event="success",
            hook="json-file",
            inputs={"alert_dir": alert_dir},
        ),
    ]

    task_ctx = TaskContext(
        run_id="run-retry",
        dag_id="test-dag",
        task_id="flaky",
        dag_version="v1",
        run_date=datetime(2026, 6, 3),
        logical_date=datetime(2026, 6, 3),
        data_interval_start=datetime(2026, 6, 3),
        data_interval_end=datetime(2026, 6, 4),
        params={},
        inputs={
            "py_file": str(scripts / "flaky.py"),
            "py_function": "main",
            "params": {},
            "env": {"COUNTER_FILE": counter_file},
        },
        plugin_name="py",
        retries=3,
        retry_delay=0,  # instant retry for test
    )

    async def _test():
        await worker.submit(task_ctx, callbacks=callbacks)

        async def run_and_stop():
            await asyncio.sleep(0.5)  # enough for 3 attempts
            await worker.shutdown()

        await asyncio.gather(worker.run(), run_and_stop())

        # Should succeed on 3rd attempt
        state = await meta.get_task_state("run-retry", "test-dag", "flaky")
        assert state == TaskState.SUCCESS

        ctx = await meta.get_task_context("run-retry", "test-dag", "flaky")
        assert ctx.current_attempt == 3
        assert ctx.outputs == {"attempt_succeeded": 3}

    asyncio.run(_test())

    # Check retry alerts were fired
    alerts = list(workspace["alert_path"].glob("*retry*"))
    assert len(alerts) == 2  # 2 retries before success


def test_worker_final_failure(workspace):
    """Worker marks task FAILED when retries exhausted."""
    scripts = workspace["scripts_path"]
    scripts.mkdir()
    (scripts / "always_fail.py").write_text(
        'def main():\n    raise RuntimeError("permanent error")\n'
    )

    meta = JsonMetadata(workspace["metadata_path"])
    alert_dir = str(workspace["alert_path"])
    worker = Worker(meta)

    callbacks = [
        OnTaskEvent(
            on_event="failure",
            hook="json-file",
            inputs={"alert_dir": alert_dir},
        ),
    ]

    task_ctx = _make_ctx(str(scripts / "always_fail.py"), retries=1)

    async def _test():
        await worker.submit(task_ctx, callbacks=callbacks)

        async def run_and_stop():
            await asyncio.sleep(0.3)
            await worker.shutdown()

        await asyncio.gather(worker.run(), run_and_stop())

        state = await meta.get_task_state("run-001", "test-dag", "process")
        assert state == TaskState.FAILED

        ctx = await meta.get_task_context("run-001", "test-dag", "process")
        assert ctx.current_attempt == 2  # original + 1 retry
        assert "permanent error" in ctx.last_attempt.error

    asyncio.run(_test())

    # Failure callback fired
    alerts = list(workspace["alert_path"].glob("*failure*"))
    assert len(alerts) == 1
