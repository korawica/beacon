"""Task instance states.

Beacon tracks every task through a small enum and exposes one helper set
(``TERMINAL_STATES``) consumed by the runner. State *transitions* are
enforced implicitly by the runner / worker — there is no per-transition
validator because every write site already knows the next legal state.
"""

from enum import StrEnum


class TaskState(StrEnum):
    """All possible states a task instance can occupy.

    Lifecycle::

        NONE → SCHEDULED → QUEUED → RUNNING → SUCCESS
                                            → FAILED
                                            → UP_FOR_RETRY → QUEUED (retry)
        NONE → SKIPPED                (trigger rule not met)
        NONE → UPSTREAM_FAILED        (upstream failed, no skip)
        RUNNING → REMOVED             (DAG edited mid-run)
    """

    # -- Non-terminal --
    NONE = "none"
    SCHEDULED = "scheduled"
    QUEUED = "queued"
    RUNNING = "running"
    UP_FOR_RETRY = "up_for_retry"

    # -- Terminal --
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


# =============================================================================
# State Helpers
# =============================================================================


def is_terminal(state: TaskState) -> bool:
    """Check if a state is terminal (no further transitions possible).

    Args:
        state: The task state to check.

    Returns:
        True if the state is terminal, False otherwise.

    Example:
        >>> is_terminal(TaskState.SUCCESS)
        True
        >>> is_terminal(TaskState.RUNNING)
        False
    """
    return state in TERMINAL_STATES


def can_transition(from_state: TaskState, to_state: TaskState) -> bool:
    """Check if a state transition is valid.

    This is a helper for logging and debugging — the runner/worker do not
    enforce transitions at runtime. It documents the expected state machine.

    Args:
        from_state: The current state.
        to_state: The target state.

    Returns:
        True if the transition is valid, False otherwise.

    Example:
        >>> can_transition(TaskState.NONE, TaskState.SCHEDULED)
        True
        >>> can_transition(TaskState.SUCCESS, TaskState.FAILED)
        False
    """
    VALID_TRANSITIONS: dict[TaskState, frozenset[TaskState]] = {
        TaskState.NONE: frozenset(
            {
                TaskState.SCHEDULED,
                TaskState.SKIPPED,
                TaskState.UPSTREAM_FAILED,
            }
        ),
        TaskState.SCHEDULED: frozenset({TaskState.QUEUED}),
        TaskState.QUEUED: frozenset({TaskState.RUNNING}),
        TaskState.RUNNING: frozenset(
            {
                TaskState.SUCCESS,
                TaskState.FAILED,
                TaskState.SKIPPED,
                TaskState.UP_FOR_RETRY,
            }
        ),
        TaskState.UP_FOR_RETRY: frozenset(
            {
                TaskState.QUEUED,
                TaskState.FAILED,
            }
        ),
    }
    return to_state in VALID_TRANSITIONS.get(from_state, frozenset())
