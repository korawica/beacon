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
