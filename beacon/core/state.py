"""Task Instance States.

This module defines all possible states a task instance can be in during
its lifecycle, from initial scheduling through terminal completion.

The design references Apache Airflow's task state model but removes the
abstract mixin layers and consolidates into a single flat enum with
helper sets for state classification.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html#task-instance-states
"""

from enum import StrEnum


class TaskState(StrEnum):
    """All possible states a task instance can occupy.

    Lifecycle states (non-terminal):
        A task moves through these states during normal execution.

    Terminal states:
        A task reaches one of these and will not transition further
        without external intervention (retry, clear, etc.).

    Attributes:
        NONE: The task is registered but not yet scheduled.
        SCHEDULED: The task is scheduled and waiting for a worker slot.
        QUEUED: The task has been sent to a worker and is waiting to run.
        RUNNING: The task is actively executing.
        SUCCESS: The task completed successfully (terminal).
        FAILED: The task raised an unhandled exception (terminal).
        SKIPPED: The task was intentionally skipped (terminal).
        UPSTREAM_FAILED: The task was not run because an upstream
            dependency failed (terminal).
        UP_FOR_RETRY: The task failed but has retries remaining and will
            be rescheduled.
        REMOVED: The task was removed from the DAG while a run was
            active (terminal).
    """

    # -- Non-terminal (lifecycle) states --
    NONE = "none"
    SCHEDULED = "scheduled"
    QUEUED = "queued"
    RUNNING = "running"
    UP_FOR_RETRY = "up_for_retry"

    # -- Terminal states --
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    UPSTREAM_FAILED = "upstream_failed"
    REMOVED = "removed"


#: States that represent a finished task (no further transitions).
TERMINAL_STATES: frozenset[TaskState] = frozenset(
    {
        TaskState.SUCCESS,
        TaskState.FAILED,
        TaskState.SKIPPED,
        TaskState.UPSTREAM_FAILED,
        TaskState.REMOVED,
    }
)

#: States considered "unfinished" — the task may still transition.
UNFINISHED_STATES: frozenset[TaskState] = frozenset(
    {
        TaskState.NONE,
        TaskState.SCHEDULED,
        TaskState.QUEUED,
        TaskState.RUNNING,
        TaskState.UP_FOR_RETRY,
    }
)
