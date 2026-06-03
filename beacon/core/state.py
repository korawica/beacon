"""Task Instance States.

This module defines all possible states a task instance can be in during
its lifecycle, from initial scheduling through terminal completion.

It also defines the valid state transitions as a directed graph, so that
any state change can be validated before persisting to the metadata store.
"""

from enum import StrEnum


class TaskState(StrEnum):
    """All possible states a task instance can occupy.

    Lifecycle:
        NONE → SCHEDULED → QUEUED → RUNNING → SUCCESS
                                            → FAILED
                                            → UP_FOR_RETRY → QUEUED (retry loop)
        NONE → SKIPPED (trigger rule not met)
        NONE → UPSTREAM_FAILED (upstream failed, no skip)
        RUNNING → REMOVED (DAG edited mid-run)

    Attributes:
        NONE: The task is registered but not yet evaluated by the scheduler.
        SCHEDULED: The scheduler determined this task should run (deps met).
        QUEUED: The task has been sent to an executor queue, waiting for a slot.
        RUNNING: The executor is actively running the plugin.
        SUCCESS: The plugin completed without error (terminal).
        FAILED: All retries exhausted, plugin raised an error (terminal).
        SKIPPED: Trigger rule evaluated to skip (terminal).
        UPSTREAM_FAILED: An upstream dependency is in a failed state (terminal).
        UP_FOR_RETRY: The attempt failed but retries remain. Will re-queue.
        REMOVED: The task was removed from DAG while run was active (terminal).
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


#: Valid state transitions. Key = current state, value = set of allowed next states.
VALID_TRANSITIONS: dict[TaskState, frozenset[TaskState]] = {
    TaskState.NONE: frozenset(
        {
            TaskState.SCHEDULED,
            TaskState.SKIPPED,
            TaskState.UPSTREAM_FAILED,
            TaskState.REMOVED,
        }
    ),
    TaskState.SCHEDULED: frozenset(
        {
            TaskState.QUEUED,
            TaskState.REMOVED,
        }
    ),
    TaskState.QUEUED: frozenset(
        {
            TaskState.RUNNING,
            TaskState.REMOVED,
        }
    ),
    TaskState.RUNNING: frozenset(
        {
            TaskState.SUCCESS,
            TaskState.FAILED,
            TaskState.UP_FOR_RETRY,
            TaskState.REMOVED,
        }
    ),
    TaskState.UP_FOR_RETRY: frozenset(
        {
            TaskState.QUEUED,
            TaskState.FAILED,  # manual mark-failed or system limit
            TaskState.REMOVED,
        }
    ),
    # Terminal states have no outgoing transitions (except manual clear)
    TaskState.SUCCESS: frozenset(),
    TaskState.FAILED: frozenset(),
    TaskState.SKIPPED: frozenset(),
    TaskState.UPSTREAM_FAILED: frozenset(),
    TaskState.REMOVED: frozenset(),
}

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


def validate_transition(current: TaskState, target: TaskState) -> bool:
    """Check whether a state transition is valid.

    Args:
        current: The current task state.
        target: The desired next state.

    Returns:
        True if the transition is allowed.

    Raises:
        ValueError: If the transition is not allowed.
    """
    allowed = VALID_TRANSITIONS.get(current, frozenset())
    if target not in allowed:
        raise ValueError(
            f"Invalid state transition: {current!r} → {target!r}. "
            f"Allowed from {current!r}: {sorted(allowed)}"
        )
    return True
