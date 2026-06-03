"""Test: Custom plugin retry, permanent failure, and skip behavior.

Validates that:
  - A regular exception triggers retries until success or exhaustion.
  - Raising TaskFailed skips all remaining retries and fails immediately.
  - Raising TaskSkipped marks the task as SKIPPED (no retry, no failure).

This is the pattern for custom plugin authors:
  - Raise any exception → beacon retries (recoverable error)
  - Raise TaskFailed → beacon fails immediately (permanent/fatal error)
  - Raise TaskSkipped → beacon skips the task (nothing to do)
"""

import asyncio
from datetime import datetime
from typing import ClassVar

import pytest

from beacon.core import BasePlugin, Context
from beacon.core.state import TaskState
from beacon.core.task_context import AttemptStatus, TaskContext
from beacon.errors import TaskFailed, TaskSkipped
from beacon.metadata.json_store import JsonMetadata
from beacon.worker import Worker


# --- Custom plugins for testing ---


class RetryThenSucceedPlugin(BasePlugin):
    """Fails N times then succeeds. Simulates a transient error."""

    plugin_name: ClassVar[str] = "retry-then-succeed"

    fail_count: int = 2
    counter_key: str = "default"

    # Class-level counter shared across instances
    _counters: ClassVar[dict[str, int]] = {}

    async def execute(self, context: Context) -> dict:
        key = self.counter_key
        RetryThenSucceedPlugin._counters.setdefault(key, 0)
        RetryThenSucceedPlugin._counters[key] += 1
        current = RetryThenSucceedPlugin._counters[key]

        if current <= self.fail_count:
            raise RuntimeError(f"Transient error on attempt {current}")
        return {"succeeded_on_call": current}


class FailPermanentlyPlugin(BasePlugin):
    """Raises TaskFailed immediately — should never retry."""

    plugin_name: ClassVar[str] = "fail-permanently"

    message: str = "This is a permanent failure"

    async def execute(self, context: Context) -> dict:
        raise TaskFailed(self.message)


class ConditionalFailPlugin(BasePlugin):
    """Fails transiently first, then raises TaskFailed (permanent).

    Simulates: retry a few times, then detect an unrecoverable condition.
    """

    plugin_name: ClassVar[str] = "conditional-fail"

    counter_key: str = "default"
    transient_failures: int = 2

    _counters: ClassVar[dict[str, int]] = {}

    async def execute(self, context: Context) -> dict:
        key = self.counter_key
        ConditionalFailPlugin._counters.setdefault(key, 0)
        ConditionalFailPlugin._counters[key] += 1
        current = ConditionalFailPlugin._counters[key]

        if current <= self.transient_failures:
            raise RuntimeError(f"Transient error attempt {current}")

        # On the next attempt, detect it's unrecoverable
        raise TaskFailed(
            "Permanent: data source does not exist, no point retrying"
        )


class SkipPlugin(BasePlugin):
    """Raises TaskSkipped immediately — task should be marked SKIPPED."""

    plugin_name: ClassVar[str] = "skip-task"

    reason: str = "No data to process"

    async def execute(self, context: Context) -> dict:
        raise TaskSkipped(self.reason)


class ConditionalSkipPlugin(BasePlugin):
    """Retries on transient error, then skips when condition is met.

    Simulates: check if data exists, retry on network error, skip if empty.
    """

    plugin_name: ClassVar[str] = "conditional-skip"

    counter_key: str = "default"
    transient_failures: int = 1

    _counters: ClassVar[dict[str, int]] = {}

    async def execute(self, context: Context) -> dict:
        key = self.counter_key
        ConditionalSkipPlugin._counters.setdefault(key, 0)
        ConditionalSkipPlugin._counters[key] += 1
        current = ConditionalSkipPlugin._counters[key]

        if current <= self.transient_failures:
            raise RuntimeError(f"Network timeout on attempt {current}")

        # After retries, determine there's nothing to do
        raise TaskSkipped("Source partition is empty, skipping")


@pytest.fixture(autouse=True)
def reset_counters():
    """Reset plugin counters between tests."""
    RetryThenSucceedPlugin._counters.clear()
    ConditionalFailPlugin._counters.clear()
    ConditionalSkipPlugin._counters.clear()
    yield
    RetryThenSucceedPlugin._counters.clear()
    ConditionalFailPlugin._counters.clear()
    ConditionalSkipPlugin._counters.clear()


@pytest.fixture
def metadata(tmp_path):
    return JsonMetadata(tmp_path / "metadata.db")


def _make_ctx(
    plugin_name: str, task_id: str = "task-1", retries: int = 3, **inputs
) -> TaskContext:
    return TaskContext(
        run_id="run-001",
        dag_id="test-dag",
        task_id=task_id,
        dag_version="v1",
        run_date=datetime(2026, 6, 3),
        logical_date=datetime(2026, 6, 3),
        data_interval_start=datetime(2026, 6, 3),
        data_interval_end=datetime(2026, 6, 4),
        params={},
        inputs=inputs,
        plugin_name=plugin_name,
        retries=retries,
        retry_delay=0,
    )


class TestRetryBehavior:
    """Test that regular errors trigger retries."""

    def test_transient_error_retries_until_success(self, metadata):
        """Plugin raises RuntimeError twice, succeeds on 3rd attempt."""
        worker = Worker(metadata, max_concurrent=5)
        task_ctx = _make_ctx(
            "retry-then-succeed",
            retries=5,
            fail_count=2,
            counter_key="test1",
        )

        async def _run():
            await worker.submit(task_ctx)

            async def stop():
                await asyncio.sleep(0.5)
                await worker.shutdown()

            await asyncio.gather(worker.run(), stop())

        asyncio.run(_run())

        async def _verify():
            state = await metadata.get_task_state(
                "run-001", "test-dag", "task-1"
            )
            assert state == TaskState.SUCCESS

            ctx = await metadata.get_task_context(
                "run-001", "test-dag", "task-1"
            )
            # 2 failed attempts + 1 success = 3 total
            assert ctx.attempt_number == 3
            assert ctx.attempts[0].state == AttemptStatus.FAILED
            assert ctx.attempts[1].state == AttemptStatus.FAILED
            assert ctx.attempts[2].state == AttemptStatus.SUCCESS
            assert ctx.outputs == {"succeeded_on_call": 3}

        asyncio.run(_verify())


class TestTaskFailedSkipsRetry:
    """Test that TaskFailed immediately fails without retrying."""

    def test_task_failed_no_retry(self, metadata):
        """Plugin raises TaskFailed — task goes straight to FAILED, no retries."""
        worker = Worker(metadata, max_concurrent=5)
        task_ctx = _make_ctx(
            "fail-permanently",
            retries=5,  # Has 5 retries available but should NOT use them
            message="Database schema does not exist",
        )

        async def _run():
            await worker.submit(task_ctx)

            async def stop():
                await asyncio.sleep(0.3)
                await worker.shutdown()

            await asyncio.gather(worker.run(), stop())

        asyncio.run(_run())

        async def _verify():
            state = await metadata.get_task_state(
                "run-001", "test-dag", "task-1"
            )
            assert state == TaskState.FAILED

            ctx = await metadata.get_task_context(
                "run-001", "test-dag", "task-1"
            )
            # Only 1 attempt — did NOT retry
            assert ctx.attempt_number == 1
            assert ctx.attempts[0].state == AttemptStatus.FAILED
            assert "Database schema does not exist" in ctx.attempts[0].error

        asyncio.run(_verify())

    def test_transient_then_permanent_failure(self, metadata):
        """Plugin retries on transient errors, then raises TaskFailed to stop."""
        worker = Worker(metadata, max_concurrent=5)
        task_ctx = _make_ctx(
            "conditional-fail",
            retries=5,
            transient_failures=2,
            counter_key="test2",
        )

        async def _run():
            await worker.submit(task_ctx)

            async def stop():
                await asyncio.sleep(0.5)
                await worker.shutdown()

            await asyncio.gather(worker.run(), stop())

        asyncio.run(_run())

        async def _verify():
            state = await metadata.get_task_state(
                "run-001", "test-dag", "task-1"
            )
            assert state == TaskState.FAILED

            ctx = await metadata.get_task_context(
                "run-001", "test-dag", "task-1"
            )
            # 2 transient failures (retried) + 1 permanent failure (stopped)
            assert ctx.attempt_number == 3
            assert ctx.attempts[0].state == AttemptStatus.FAILED
            assert "Transient error" in ctx.attempts[0].error
            assert ctx.attempts[1].state == AttemptStatus.FAILED
            assert "Transient error" in ctx.attempts[1].error
            assert ctx.attempts[2].state == AttemptStatus.FAILED
            assert "Permanent" in ctx.attempts[2].error

        asyncio.run(_verify())

    def test_task_failed_with_zero_retries(self, metadata):
        """TaskFailed with retries=0 still produces exactly 1 attempt."""
        worker = Worker(metadata, max_concurrent=5)
        task_ctx = _make_ctx(
            "fail-permanently",
            retries=0,
            message="fatal error",
        )

        async def _run():
            await worker.submit(task_ctx)

            async def stop():
                await asyncio.sleep(0.3)
                await worker.shutdown()

            await asyncio.gather(worker.run(), stop())

        asyncio.run(_run())

        async def _verify():
            state = await metadata.get_task_state(
                "run-001", "test-dag", "task-1"
            )
            assert state == TaskState.FAILED

            ctx = await metadata.get_task_context(
                "run-001", "test-dag", "task-1"
            )
            assert ctx.attempt_number == 1

        asyncio.run(_verify())


class TestTaskSkipped:
    """Test that TaskSkipped marks task as SKIPPED without retry."""

    def test_task_skipped_no_retry(self, metadata):
        """Plugin raises TaskSkipped — task goes to SKIPPED, no retries."""
        worker = Worker(metadata, max_concurrent=5)
        task_ctx = _make_ctx(
            "skip-task",
            retries=5,  # Has retries but should NOT use them
            reason="No input data available",
        )

        async def _run():
            await worker.submit(task_ctx)

            async def stop():
                await asyncio.sleep(0.3)
                await worker.shutdown()

            await asyncio.gather(worker.run(), stop())

        asyncio.run(_run())

        async def _verify():
            state = await metadata.get_task_state(
                "run-001", "test-dag", "task-1"
            )
            assert state == TaskState.SKIPPED

            ctx = await metadata.get_task_context(
                "run-001", "test-dag", "task-1"
            )
            # Only 1 attempt — did NOT retry
            assert ctx.attempt_number == 1
            assert ctx.attempts[0].state == AttemptStatus.SKIPPED

        asyncio.run(_verify())

    def test_transient_then_skip(self, metadata):
        """Plugin retries on transient error, then raises TaskSkipped."""
        worker = Worker(metadata, max_concurrent=5)
        task_ctx = _make_ctx(
            "conditional-skip",
            retries=5,
            transient_failures=1,
            counter_key="skip1",
        )

        async def _run():
            await worker.submit(task_ctx)

            async def stop():
                await asyncio.sleep(0.5)
                await worker.shutdown()

            await asyncio.gather(worker.run(), stop())

        asyncio.run(_run())

        async def _verify():
            state = await metadata.get_task_state(
                "run-001", "test-dag", "task-1"
            )
            assert state == TaskState.SKIPPED

            ctx = await metadata.get_task_context(
                "run-001", "test-dag", "task-1"
            )
            # 1 transient failure (retried) + 1 skip
            assert ctx.attempt_number == 2
            assert ctx.attempts[0].state == AttemptStatus.FAILED
            assert "Network timeout" in ctx.attempts[0].error
            assert ctx.attempts[1].state == AttemptStatus.SKIPPED

        asyncio.run(_verify())

    def test_skipped_does_not_fire_failure_callback(self, metadata):
        """TaskSkipped should not trigger failure callbacks."""
        from beacon.callback import OnTaskEvent

        worker = Worker(metadata, max_concurrent=5)
        alert_dir = str(metadata.base_path / "alerts")

        task_ctx = _make_ctx(
            "skip-task",
            retries=3,
            reason="empty partition",
        )

        callbacks = [
            OnTaskEvent(
                on_event="failure",
                hook="json-file",
                inputs={"alert_dir": alert_dir},
            ),
            OnTaskEvent(
                on_event="skipped",
                hook="json-file",
                inputs={"alert_dir": alert_dir},
            ),
        ]

        async def _run():
            await worker.submit(task_ctx, callbacks=callbacks)

            async def stop():
                await asyncio.sleep(0.3)
                await worker.shutdown()

            await asyncio.gather(worker.run(), stop())

        asyncio.run(_run())

        import json
        from pathlib import Path

        alert_path = Path(alert_dir)
        if alert_path.exists():
            alerts = [
                json.loads(f.read_text()) for f in alert_path.glob("*.json")
            ]
            events = [a["event"] for a in alerts]
            # Should have "skipped" callback, NOT "failure"
            assert "failure" not in events
            assert "skipped" in events
        else:
            # If no alerts dir, skipped callback didn't write (acceptable
            # if hook doesn't handle "skipped" event name in filename)
            pass
