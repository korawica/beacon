"""Task Context.

This module defines the TaskContext model — the serializable unit of work
that gets persisted to the metadata store and transmitted to any executor
(local, Docker, Kubernetes, AWS Batch, Cloud Batch).

The TaskContext is NOT the same as Airflow's XCom. It does not pass data
between tasks. Instead, it carries everything a single task instance needs
to execute in any environment, and accumulates attempt history for retries.

Design:
    - Serializable (JSON) so it can be stored in metadata and sent to remote
      executors via queue message, API call, or environment injection.
    - Immutable per attempt — each retry creates a new Attempt record.
    - The executor reads TaskContext from metadata, runs the plugin, writes
      the result back. The executor never needs local DAG files.
"""

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class AttemptStatus(StrEnum):
    """Status of a single execution attempt."""

    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    TIMED_OUT = "timed_out"


class Attempt(BaseModel):
    """A single execution attempt record.

    One Attempt is created per retry. All attempts are stored in TaskContext
    so remote executors and the UI can inspect retry history.
    """

    attempt_number: int = Field(description="1-based attempt number")
    state: AttemptStatus = Field(description="Outcome of this attempt")
    started_at: datetime | None = Field(default=None)
    ended_at: datetime | None = Field(default=None)
    duration_sec: float | None = Field(default=None)
    error: str | None = Field(
        default=None,
        description="Error message if attempt failed",
    )
    error_traceback: str | None = Field(
        default=None,
        description="Full traceback string if attempt failed",
    )
    executor: str | None = Field(
        default=None,
        description="Executor type that ran this attempt (local, docker, k8s)",
    )
    executor_ref: str | None = Field(
        default=None,
        description="External reference (pod name, container id, batch job id)",
    )


class TaskContext(BaseModel):
    """The complete context for a task instance execution.

    This object is:
      - Created by the scheduler when a task is enqueued.
      - Stored in the metadata store (serialized as JSON).
      - Sent to the executor (local process, k8s pod, docker container).
      - Updated by the executor after execution completes.
      - Read by the API server/UI to display task state and history.

    Unlike Airflow's XCom, TaskContext does NOT shuttle data between tasks.
    It carries execution context TO a task and records results FROM a task.
    """

    # --- Identity ---
    run_id: str = Field(description="The DagRun ID this task belongs to")
    dag_id: str = Field(description="The DAG ID")
    task_id: str = Field(description="The task/action ID within the DAG")
    dag_version: str = Field(description="DAG bundle version at trigger time")

    # --- Time ---
    run_date: datetime = Field(description="Wall-clock when DagRun was created")
    logical_date: datetime = Field(
        description="Schedule logical date (= data_interval_start)"
    )
    data_interval_start: datetime = Field(description="Data interval start")
    data_interval_end: datetime = Field(description="Data interval end")

    # --- Inputs (resolved at enqueue time) ---
    variables: dict[str, Any] = Field(
        default_factory=dict,
        description="Variables from scoped chain + run-time overrides",
    )
    inputs: dict[str, Any] = Field(
        default_factory=dict,
        description="Task inputs, Jinja-rendered with vars/secrets at pre-execute",
    )
    plugin_name: str = Field(
        description="Resolved plugin name to execute",
    )

    # --- Execution Config ---
    retries: int = Field(default=0, description="Max retry attempts allowed")
    retry_delay: int = Field(
        default=10, description="Base delay in seconds between retries"
    )
    execution_timeout: int | None = Field(
        default=None, description="Timeout in seconds for each attempt"
    )
    exponential_backoff: bool = Field(
        default=True,
        description="Whether to exponentially increase retry delay",
    )

    # --- Attempt History ---
    attempts: list[Attempt] = Field(
        default_factory=list,
        description="Ordered list of execution attempts (1 per try)",
    )

    # --- Outputs ---
    outputs: dict[str, Any] = Field(
        default_factory=dict,
        description="Task outputs written by the plugin after success",
    )

    # --- Upstream Outputs (populated before execution) ---
    upstream_outputs: dict[str, dict[str, Any]] = Field(
        default_factory=dict,
        description="Outputs from upstream tasks: {task_id: {key: value}}",
    )

    @property
    def attempt_number(self) -> int:
        """Number of the most recent attempt (1-based). 0 if not started."""
        return len(self.attempts)

    @property
    def has_retries_left(self) -> bool:
        """Whether the task can still retry."""
        return self.attempt_number <= self.retries

    @property
    def last_attempt(self) -> Attempt | None:
        """The most recent attempt, or None if not started."""
        return self.attempts[-1] if self.attempts else None

    @property
    def next_retry_delay(self) -> float:
        """Compute next retry delay with optional exponential backoff."""
        if not self.exponential_backoff:
            return float(self.retry_delay)
        return float(self.retry_delay * (2 ** (self.attempt_number - 1)))

    def start_attempt(
        self, executor: str, executor_ref: str | None = None
    ) -> Attempt:
        """Create and append a new attempt. Called by the executor."""
        attempt = Attempt(
            attempt_number=self.attempt_number + 1,
            state=AttemptStatus.RUNNING,
            started_at=datetime.now(),
            executor=executor,
            executor_ref=executor_ref,
        )
        self.attempts.append(attempt)
        return attempt

    def finish_attempt(
        self,
        *,
        state: AttemptStatus,
        error: str | None = None,
        error_traceback: str | None = None,
        outputs: dict[str, Any] | None = None,
    ) -> None:
        """Mark the current attempt as finished. Called by the executor."""
        attempt = self.attempts[-1]
        attempt.state = state
        attempt.ended_at = datetime.now()
        if attempt.started_at:
            attempt.duration_sec = (
                attempt.ended_at - attempt.started_at
            ).total_seconds()
        attempt.error = error
        attempt.error_traceback = error_traceback
        if outputs:
            self.outputs = outputs
