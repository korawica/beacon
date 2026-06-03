"""Quick smoke test for TaskContext and state machine."""

from datetime import datetime
from beacon.core import (
    TaskContext,
    AttemptStatus,
    validate_transition,
    TaskState,
)


def test_task_context_lifecycle():
    ctx = TaskContext(
        run_id="run-001",
        dag_id="hello-world",
        task_id="process",
        dag_version="v1-abc123",
        run_date=datetime.now(),
        logical_date=datetime.now(),
        data_interval_start=datetime.now(),
        data_interval_end=datetime.now(),
        params={"source_system": "example"},
        inputs={"py_file": "./process.py"},
        plugin_name="py",
        retries=2,
    )

    assert ctx.attempt_number == 0
    assert ctx.has_retries_left is True

    # First attempt fails
    ctx.start_attempt(executor="local")
    assert ctx.attempt_number == 1
    ctx.finish_attempt(state=AttemptStatus.FAILED, error="connection refused")
    assert ctx.last_attempt.state == AttemptStatus.FAILED
    assert ctx.has_retries_left is True

    # Second attempt succeeds on k8s
    ctx.start_attempt(executor="k8s", executor_ref="pod-xyz")
    ctx.finish_attempt(state=AttemptStatus.SUCCESS, outputs={"rows": 100})
    assert ctx.outputs == {"rows": 100}
    assert ctx.last_attempt.executor == "k8s"
    assert ctx.last_attempt.executor_ref == "pod-xyz"

    # Serializable
    json_str = ctx.model_dump_json()
    restored = TaskContext.model_validate_json(json_str)
    assert restored.run_id == "run-001"
    assert len(restored.attempts) == 2


def test_state_transitions():
    validate_transition(TaskState.NONE, TaskState.SCHEDULED)
    validate_transition(TaskState.SCHEDULED, TaskState.QUEUED)
    validate_transition(TaskState.QUEUED, TaskState.RUNNING)
    validate_transition(TaskState.RUNNING, TaskState.SUCCESS)
    validate_transition(TaskState.RUNNING, TaskState.UP_FOR_RETRY)
    validate_transition(TaskState.UP_FOR_RETRY, TaskState.QUEUED)

    # Invalid transitions
    try:
        validate_transition(TaskState.SUCCESS, TaskState.RUNNING)
        assert False, "Should have raised"
    except ValueError:
        pass

    try:
        validate_transition(TaskState.NONE, TaskState.RUNNING)
        assert False, "Should have raised"
    except ValueError:
        pass


if __name__ == "__main__":
    test_task_context_lifecycle()
    test_state_transitions()
    print("All tests passed!")
