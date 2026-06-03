"""End-to-end test for the py plugin with full TaskContext lifecycle."""

import asyncio
import os
from datetime import datetime

import pytest

from beacon.core import (
    LocalExecutor,
    TaskContext,
    TaskState,
    AttemptStatus,
)

# Ensure py plugin is registered
from beacon.providers.standard.plugins import EmptyPlugin  # noqa: F401
from beacon.providers.standard.plugins.task.python import PythonPlugin  # noqa: F401


USER_SCRIPT_SIMPLE = """\
def main(source_system: str):
    return {"processed": source_system, "rows": 42}
"""

USER_SCRIPT_WITH_CONTEXT = """\
from beacon.runtime import load_context

def main(source_system: str):
    ctx = load_context()
    ctx.logger.info("Processing %s in run %s", source_system, ctx.run_id)
    return {
        "source": source_system,
        "run_id": ctx.run_id,
        "dag_id": ctx.dag_id,
        "task_id": ctx.task_id,
        "attempt": ctx.attempt_number,
    }
"""

USER_SCRIPT_FAILING = """\
def main():
    raise ValueError("Something went wrong")
"""

USER_SCRIPT_WITH_ENV = """\
import os

def main():
    return {"my_var": os.environ.get("MY_VAR", "")}
"""


@pytest.fixture
def tmp_script(tmp_path):
    """Helper to write a script and return its path."""

    def _write(content: str, name: str = "task_script.py") -> str:
        p = tmp_path / name
        p.write_text(content)
        return str(p)

    return _write


def _make_task_context(py_file: str, **input_overrides) -> TaskContext:
    """Create a minimal TaskContext for the py plugin."""
    inputs = {
        "py_file": py_file,
        "py_function": "main",
        "params": {},
        "env": {},
        **input_overrides,
    }
    return TaskContext(
        run_id="run-test-001",
        dag_id="test-dag",
        task_id="process",
        dag_version="v1",
        run_date=datetime(2026, 6, 3, 0, 0, 0),
        logical_date=datetime(2026, 6, 2, 0, 0, 0),
        data_interval_start=datetime(2026, 6, 2, 0, 0, 0),
        data_interval_end=datetime(2026, 6, 3, 0, 0, 0),
        params={"source_system": "example"},
        inputs=inputs,
        plugin_name="py",
        retries=1,
        retry_delay=1,
    )


def test_py_plugin_simple(tmp_script):
    """Test basic function execution with params."""
    script = tmp_script(USER_SCRIPT_SIMPLE)
    task_ctx = _make_task_context(
        script,
        params={"source_system": "my_source"},
    )

    executor = LocalExecutor()
    result = asyncio.run(executor.run_task(task_ctx))

    assert result.last_attempt.state == AttemptStatus.SUCCESS
    assert result.outputs == {"processed": "my_source", "rows": 42}


def test_py_plugin_with_load_context(tmp_script):
    """Test that load_context() works inside user code."""
    script = tmp_script(USER_SCRIPT_WITH_CONTEXT)
    task_ctx = _make_task_context(
        script,
        params={"source_system": "ctx_test"},
    )

    executor = LocalExecutor()
    result = asyncio.run(executor.run_task(task_ctx))

    assert result.last_attempt.state == AttemptStatus.SUCCESS
    assert result.outputs["source"] == "ctx_test"
    assert result.outputs["run_id"] == "run-test-001"
    assert result.outputs["dag_id"] == "test-dag"
    assert result.outputs["task_id"] == "process"
    assert result.outputs["attempt"] == 1


def test_py_plugin_failure_and_retry(tmp_script):
    """Test that failures are captured and retry tracking works."""
    script = tmp_script(USER_SCRIPT_FAILING)
    task_ctx = _make_task_context(script)

    executor = LocalExecutor()

    # First attempt — fails
    result = asyncio.run(executor.run_task(task_ctx))
    assert result.last_attempt.state == AttemptStatus.FAILED
    assert "Something went wrong" in result.last_attempt.error
    assert result.has_retries_left is True  # retries=1, attempt=1

    # Second attempt — fails again
    result = asyncio.run(executor.run_task(result))
    assert result.current_attempt == 2
    assert result.last_attempt.state == AttemptStatus.FAILED
    assert result.has_retries_left is False  # no more retries


def test_py_plugin_env_vars(tmp_script):
    """Test that env vars are set and cleaned up."""
    script = tmp_script(USER_SCRIPT_WITH_ENV)
    task_ctx = _make_task_context(
        script,
        env={"MY_VAR": "hello_beacon"},
    )

    assert os.environ.get("MY_VAR") is None

    executor = LocalExecutor()
    result = asyncio.run(executor.run_task(task_ctx))

    assert result.last_attempt.state == AttemptStatus.SUCCESS
    assert result.outputs == {"my_var": "hello_beacon"}
    # Env var cleaned up
    assert os.environ.get("MY_VAR") is None


def test_py_plugin_file_not_found(tmp_script):
    """Test error when py_file doesn't exist."""
    task_ctx = _make_task_context("/nonexistent/path/script.py")

    executor = LocalExecutor()
    result = asyncio.run(executor.run_task(task_ctx))

    assert result.last_attempt.state == AttemptStatus.FAILED
    assert "not found" in result.last_attempt.error


def test_py_plugin_function_not_found(tmp_script):
    """Test error when function doesn't exist in the file."""
    script = tmp_script("def other_func(): pass")
    task_ctx = _make_task_context(script)  # looks for "main"

    executor = LocalExecutor()
    result = asyncio.run(executor.run_task(task_ctx))

    assert result.last_attempt.state == AttemptStatus.FAILED
    assert "not found" in result.last_attempt.error


def test_full_action_lifecycle(tmp_script):
    """Test the full warp_execute lifecycle through BaseAction."""
    from beacon.models.task import Task

    script = tmp_script(USER_SCRIPT_SIMPLE)

    task = Task(
        id="process",
        uses="py",
        retries=2,
    )

    # In real flow, the scheduler builds TaskContext with fully rendered inputs
    task_ctx = task.build_task_context(
        run_id="run-lifecycle-001",
        dag_id="test-dag",
        dag_version="v1",
        run_date=datetime(2026, 6, 3),
        logical_date=datetime(2026, 6, 2),
        data_interval_start=datetime(2026, 6, 2),
        data_interval_end=datetime(2026, 6, 3),
        params={"source_system": "lifecycle_test"},
        rendered_inputs={
            "py_file": script,
            "py_function": "main",
            "params": {"source_system": "lifecycle_test"},
        },
    )

    states_recorded = []

    async def mock_set_state(ctx, state):
        states_recorded.append(state)

    async def run():
        return await task.warp_execute(
            task_ctx,
            set_state=mock_set_state,
        )

    result_state = asyncio.run(run())

    assert result_state == TaskState.SUCCESS
    assert TaskState.RUNNING in states_recorded
    assert TaskState.SUCCESS in states_recorded
    assert task_ctx.outputs == {"processed": "lifecycle_test", "rows": 42}
