"""End-to-end test: All callback events with retry and long-running task.

This test proves the full worker lifecycle fires every event correctly:
  - start: fired when task begins execution
  - retry: fired when task fails but has retries left
  - success: fired when task completes successfully
  - failure: fired when task exhausts all retries

Scenario:
  Task 1 (long-running): sleeps 3 seconds then succeeds.
  Task 2 (retry-then-succeed): fails twice, succeeds on 3rd attempt.
  Task 3 (always-fail): fails all attempts, triggers failure callback.

All callbacks write to alert directory so we can verify events fired.
"""

import asyncio
import json
import time
from datetime import datetime
from pathlib import Path

import pytest

from beacon.callback import OnTaskEvent
from beacon.core import TaskContext, TaskState
from beacon.metadata.json_store import JsonMetadata
from beacon.worker import Worker

# Ensure py plugin is registered
from beacon.providers.standard.plugins.task.python import PythonPlugin  # noqa: F401


# --- User scripts ---

SCRIPT_LONG_RUNNING = """\
import time

def main():
    time.sleep(3)
    return {"status": "completed_after_3s"}
"""

SCRIPT_FAIL_THEN_SUCCEED = """\
import os

def main():
    counter_file = os.environ["COUNTER_FILE"]
    try:
        count = int(open(counter_file).read())
    except FileNotFoundError:
        count = 0
    count += 1
    open(counter_file, "w").write(str(count))
    if count < 3:
        raise RuntimeError(f"Intentional failure on attempt {count}")
    return {"succeeded_on_attempt": count}
"""

SCRIPT_ALWAYS_FAIL = """\
def main():
    raise RuntimeError("permanent failure - will never succeed")
"""


@pytest.fixture
def workspace(tmp_path):
    """Set up workspace with scripts, metadata, and alert dirs."""
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "long_running.py").write_text(SCRIPT_LONG_RUNNING)
    (scripts / "fail_then_succeed.py").write_text(SCRIPT_FAIL_THEN_SUCCEED)
    (scripts / "always_fail.py").write_text(SCRIPT_ALWAYS_FAIL)

    return {
        "scripts": scripts,
        "metadata": tmp_path / "metadata.db",
        "alerts": tmp_path / "alerts",
        "counters": tmp_path / "counters",
    }


def _all_callbacks(alert_dir: str) -> list[OnTaskEvent]:
    """Create callbacks for ALL events pointing to the same alert dir."""
    return [
        OnTaskEvent(
            on_event="start", hook="json-file", inputs={"alert_dir": alert_dir}
        ),
        OnTaskEvent(
            on_event="success",
            hook="json-file",
            inputs={"alert_dir": alert_dir},
        ),
        OnTaskEvent(
            on_event="failure",
            hook="json-file",
            inputs={"alert_dir": alert_dir},
        ),
        OnTaskEvent(
            on_event="retry", hook="json-file", inputs={"alert_dir": alert_dir}
        ),
    ]


def _read_alerts(alert_dir: Path) -> list[dict]:
    """Read all alert JSON files, sorted by creation time."""
    files = sorted(alert_dir.glob("*.json"), key=lambda f: f.stat().st_mtime_ns)
    return [json.loads(f.read_text()) for f in files]


def _make_task_ctx(
    task_id: str,
    py_file: str,
    retries: int = 0,
    env: dict | None = None,
) -> TaskContext:
    return TaskContext(
        run_id="run-events-001",
        dag_id="events-test-dag",
        task_id=task_id,
        dag_version="v1",
        run_date=datetime(2026, 6, 3),
        logical_date=datetime(2026, 6, 3),
        data_interval_start=datetime(2026, 6, 3),
        data_interval_end=datetime(2026, 6, 4),
        params={},
        inputs={
            "py_file": py_file,
            "py_function": "main",
            "params": {},
            "env": env or {},
        },
        plugin_name="py",
        retries=retries,
        retry_delay=0,  # instant retry for test speed
    )


class TestAllCallbackEvents:
    """Test all 4 callback events fire correctly."""

    def test_long_running_task_fires_start_and_success(self, workspace):
        """A 3-second task fires 'start' then 'success'."""
        meta = JsonMetadata(workspace["metadata"])
        alert_dir = str(workspace["alerts"])
        worker = Worker(meta, max_concurrent=5)

        task_ctx = _make_task_ctx(
            "long-task",
            str(workspace["scripts"] / "long_running.py"),
        )
        callbacks = _all_callbacks(alert_dir)

        async def _run():
            await worker.submit(task_ctx, callbacks=callbacks)

            async def stop_after():
                await asyncio.sleep(5)  # 3s task + buffer
                await worker.shutdown()

            await asyncio.gather(worker.run(), stop_after())

        t0 = time.time()
        asyncio.run(_run())
        elapsed = time.time() - t0

        # Task took ~3 seconds
        assert elapsed >= 3.0

        # Verify final state
        state = asyncio.run(meta.get_task_state("run-events-001", "long-task"))
        assert state == TaskState.SUCCESS

        # Verify outputs
        ctx = asyncio.run(meta.get_task_context("run-events-001", "long-task"))
        assert ctx.outputs == {"status": "completed_after_3s"}
        assert ctx.current_attempt == 1

        # Verify callbacks: start + success
        alerts = _read_alerts(workspace["alerts"])
        events = [a["event"] for a in alerts]
        assert events == ["start", "success"]

    def test_retry_task_fires_start_retry_retry_start_success(self, workspace):
        """Task that fails 2x then succeeds fires: start, retry, start, retry, start, success."""
        meta = JsonMetadata(workspace["metadata"])
        alert_dir = str(workspace["alerts"])
        worker = Worker(meta, max_concurrent=5)

        counter_file = str(workspace["counters"] / "retry_counter")
        workspace["counters"].mkdir(exist_ok=True)

        task_ctx = _make_task_ctx(
            "retry-task",
            str(workspace["scripts"] / "fail_then_succeed.py"),
            retries=3,
            env={"COUNTER_FILE": counter_file},
        )
        callbacks = _all_callbacks(alert_dir)

        async def _run():
            await worker.submit(task_ctx, callbacks=callbacks)

            async def stop_after():
                await asyncio.sleep(2)
                await worker.shutdown()

            await asyncio.gather(worker.run(), stop_after())

        asyncio.run(_run())

        # Verify final state
        state = asyncio.run(meta.get_task_state("run-events-001", "retry-task"))
        assert state == TaskState.SUCCESS

        # Verify attempt count
        ctx = asyncio.run(meta.get_task_context("run-events-001", "retry-task"))
        assert ctx.current_attempt == 3
        assert ctx.outputs == {"succeeded_on_attempt": 3}

        # Verify callback events
        alerts = _read_alerts(workspace["alerts"])
        events = [a["event"] for a in alerts]

        # Each attempt fires "start", failed attempts also fire "retry"
        # Attempt 1: start → fail → retry
        # Attempt 2: start → fail → retry
        # Attempt 3: start → success
        assert events.count("start") == 3
        assert events.count("retry") == 2
        assert events.count("success") == 1
        assert events.count("failure") == 0

    def test_permanent_failure_fires_start_and_failure(self, workspace):
        """Task with 1 retry that always fails fires: start, retry, start, failure."""
        meta = JsonMetadata(workspace["metadata"])
        alert_dir = str(workspace["alerts"])
        worker = Worker(meta, max_concurrent=5)

        task_ctx = _make_task_ctx(
            "fail-task",
            str(workspace["scripts"] / "always_fail.py"),
            retries=1,  # 1 retry = 2 total attempts
        )
        callbacks = _all_callbacks(alert_dir)

        async def _run():
            await worker.submit(task_ctx, callbacks=callbacks)

            async def stop_after():
                await asyncio.sleep(1)
                await worker.shutdown()

            await asyncio.gather(worker.run(), stop_after())

        asyncio.run(_run())

        # Verify final state
        state = asyncio.run(meta.get_task_state("run-events-001", "fail-task"))
        assert state == TaskState.FAILED

        # Verify attempt count
        ctx = asyncio.run(meta.get_task_context("run-events-001", "fail-task"))
        assert ctx.current_attempt == 2
        assert "permanent failure" in ctx.last_attempt.error

        # Verify callback events
        alerts = _read_alerts(workspace["alerts"])
        events = [a["event"] for a in alerts]

        # Attempt 1: start → fail → retry
        # Attempt 2: start → fail → failure (terminal)
        assert events.count("start") == 2
        assert events.count("retry") == 1
        assert events.count("failure") == 1
        assert events.count("success") == 0

    def test_no_retry_immediate_failure(self, workspace):
        """Task with 0 retries fires: start, failure."""
        meta = JsonMetadata(workspace["metadata"])
        alert_dir = str(workspace["alerts"])
        worker = Worker(meta, max_concurrent=5)

        task_ctx = _make_task_ctx(
            "no-retry-task",
            str(workspace["scripts"] / "always_fail.py"),
            retries=0,
        )
        callbacks = _all_callbacks(alert_dir)

        async def _run():
            await worker.submit(task_ctx, callbacks=callbacks)

            async def stop_after():
                await asyncio.sleep(0.5)
                await worker.shutdown()

            await asyncio.gather(worker.run(), stop_after())

        asyncio.run(_run())

        state = asyncio.run(
            meta.get_task_state("run-events-001", "no-retry-task")
        )
        assert state == TaskState.FAILED

        alerts = _read_alerts(workspace["alerts"])
        events = [a["event"] for a in alerts]
        assert events == ["start", "failure"]

    def test_callback_data_contains_error_on_failure(self, workspace):
        """Failure callback includes error details."""
        meta = JsonMetadata(workspace["metadata"])
        alert_dir = str(workspace["alerts"])
        worker = Worker(meta, max_concurrent=5)

        task_ctx = _make_task_ctx(
            "error-data-task",
            str(workspace["scripts"] / "always_fail.py"),
            retries=0,
        )
        callbacks = _all_callbacks(alert_dir)

        async def _run():
            await worker.submit(task_ctx, callbacks=callbacks)

            async def stop_after():
                await asyncio.sleep(0.5)
                await worker.shutdown()

            await asyncio.gather(worker.run(), stop_after())

        asyncio.run(_run())

        alerts = _read_alerts(workspace["alerts"])
        failure_alert = next(a for a in alerts if a["event"] == "failure")
        assert "permanent failure" in failure_alert["error"]
        assert failure_alert["dag_id"] == "events-test-dag"
        assert failure_alert["task_id"] == "error-data-task"
        assert failure_alert["attempt"] == 1

    def test_callback_data_contains_outputs_on_success(self, workspace):
        """Success callback includes task outputs."""
        meta = JsonMetadata(workspace["metadata"])
        alert_dir = str(workspace["alerts"])
        worker = Worker(meta, max_concurrent=5)

        task_ctx = _make_task_ctx(
            "output-task",
            str(workspace["scripts"] / "long_running.py"),
        )
        callbacks = _all_callbacks(alert_dir)

        async def _run():
            await worker.submit(task_ctx, callbacks=callbacks)

            async def stop_after():
                await asyncio.sleep(5)
                await worker.shutdown()

            await asyncio.gather(worker.run(), stop_after())

        asyncio.run(_run())

        alerts = _read_alerts(workspace["alerts"])
        success_alert = next(a for a in alerts if a["event"] == "success")
        assert success_alert["outputs"] == {"status": "completed_after_3s"}


class TestConcurrentExecution:
    """Test that worker handles multiple tasks."""

    def test_multiple_tasks_all_complete(self, workspace):
        """Multiple tasks submitted to worker all complete successfully."""
        meta = JsonMetadata(workspace["metadata"])
        alert_dir = str(workspace["alerts"])
        worker = Worker(meta, max_concurrent=5)

        # Use a simple fast script instead of long-running for this test
        fast_script = workspace["scripts"] / "fast.py"
        fast_script.write_text('def main():\n    return {"done": True}\n')

        tasks = []
        for i in range(5):
            ctx = _make_task_ctx(f"task-{i}", str(fast_script))
            tasks.append(ctx)

        callbacks = _all_callbacks(alert_dir)

        async def _run():
            for ctx in tasks:
                await worker.submit(ctx, callbacks=callbacks)

            async def stop_after():
                await asyncio.sleep(1)
                await worker.shutdown()

            await asyncio.gather(worker.run(), stop_after())

        asyncio.run(_run())

        # All 5 tasks should be SUCCESS
        for i in range(5):
            state = asyncio.run(
                meta.get_task_state("run-events-001", f"task-{i}")
            )
            assert state == TaskState.SUCCESS

        # Should have 5 start + 5 success = 10 alerts
        alerts = _read_alerts(workspace["alerts"])
        events = [a["event"] for a in alerts]
        assert events.count("start") == 5
        assert events.count("success") == 5
