"""Quick smoke test for TaskContext lifecycle."""

from datetime import datetime
from beacon.core import (
    TaskContext,
)
from beacon.core.task_context import AttemptStatus


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
        inputs={"py_statement": "./process.py"},
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


if __name__ == "__main__":
    test_task_context_lifecycle()
    print("All tests passed!")
