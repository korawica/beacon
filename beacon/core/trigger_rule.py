"""Dependency Trigger Rules.

This module provides task terminal states, trigger rules, and the evaluation
engine that determines whether a downstream task should run based on the
states of its upstream dependencies.

References:
    https://www.astronomer.io/docs/learn/airflow-trigger-rules
"""

from collections.abc import Callable, Sequence
from enum import StrEnum


class TaskState(StrEnum):
    """All possible terminal states a task can reach.

    Attributes:
        SUCCESS: The task completed successfully.
        FAILED: The task raised an unhandled exception.
        SKIPPED: The task was intentionally skipped.
        UPSTREAM_FAILED: The task was not run because an upstream failed.
    """

    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    UPSTREAM_FAILED = "upstream_failed"


class TriggerRule(StrEnum):
    """Trigger rules for dependency-based task execution.

    These control when a downstream task is allowed to run based on the
    terminal states of its upstream (dependency) tasks.

    Attributes:
        ALL_SUCCESS: Run when all upstream tasks succeeded (default).
        ALL_FAILED: Run when all upstream tasks failed.
        ALL_DONE: Run when all upstream tasks reached any terminal state.
        ALL_SKIPPED: Run when all upstream tasks were skipped.
        ONE_SUCCESS: Run when at least one upstream task succeeded.
            Fires immediately without waiting for all upstreams.
        ONE_FAILED: Run when at least one upstream task failed.
            Fires immediately without waiting for all upstreams.
        NONE_FAILED: Run when no upstream task failed. Skipped is OK.
        NONE_SKIPPED: Run when no upstream task was skipped. Failed is OK.
        NONE_FAILED_MIN_ONE_SUCCESS: Run when no upstream task failed and
            at least one succeeded.
        NONE_FAILED_OR_SKIPPED: Run only when no upstream task failed or
            was skipped — every upstream must have succeeded.
    """

    ALL_SUCCESS = "all_success"
    ALL_FAILED = "all_failed"
    ALL_DONE = "all_done"
    ALL_SKIPPED = "all_skipped"
    ONE_SUCCESS = "one_success"
    ONE_FAILED = "one_failed"
    NONE_FAILED = "none_failed"
    NONE_SKIPPED = "none_skipped"
    NONE_FAILED_MIN_ONE_SUCCESS = "none_failed_min_one_success"
    NONE_FAILED_OR_SKIPPED = "none_failed_or_skipped"


class _Counts:
    """Aggregated counts of upstream task terminal states.

    Args:
        states: Sequence of resolved upstream task states.
        total: Total number of upstream tasks (may exceed ``len(states)``
            when not all upstreams have reported yet).

    Attributes:
        success: Number of upstream tasks that succeeded.
        failed: Number of upstream tasks that failed.
        skipped: Number of upstream tasks that were skipped.
        upstream_failed: Number of upstream tasks that were not run due to
            an upstream failure.
        total: Total expected upstream task count.
        done: Number of upstream tasks that have reached a terminal state.
    """

    __slots__ = (
        "success",
        "failed",
        "skipped",
        "upstream_failed",
        "total",
        "done",
    )

    def __init__(self, states: Sequence[TaskState], total: int) -> None:
        self.success = 0
        self.failed = 0
        self.skipped = 0
        self.upstream_failed = 0

        for s in states:
            if s is TaskState.SUCCESS:
                self.success += 1
            elif s is TaskState.FAILED:
                self.failed += 1
            elif s is TaskState.SKIPPED:
                self.skipped += 1
            else:
                self.upstream_failed += 1

        self.done = (
            self.success + self.failed + self.skipped + self.upstream_failed
        )
        self.total = total

    @property
    def all_done(self) -> bool:
        """Return True if all upstream tasks have reached a terminal state."""
        return self.done >= self.total


_RULES: dict[TriggerRule, Callable[[_Counts], bool]] = {
    TriggerRule.ALL_SUCCESS: lambda c: c.all_done and c.success == c.total,
    TriggerRule.ALL_FAILED: lambda c: c.all_done and c.failed == c.total,
    TriggerRule.ALL_DONE: lambda c: c.all_done,
    TriggerRule.ALL_SKIPPED: lambda c: c.all_done and c.skipped == c.total,
    TriggerRule.ONE_SUCCESS: lambda c: c.success >= 1,
    TriggerRule.ONE_FAILED: lambda c: c.failed >= 1,
    TriggerRule.NONE_FAILED: lambda c: c.all_done and c.failed == 0,
    TriggerRule.NONE_SKIPPED: lambda c: c.all_done and c.skipped == 0,
    TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS: lambda c: (
        c.all_done and c.failed == 0 and c.success >= 1
    ),
    TriggerRule.NONE_FAILED_OR_SKIPPED: lambda c: (
        c.all_done and c.failed == 0 and c.skipped == 0
    ),
}


def is_trigger_satisfied(
    rule: TriggerRule | str,
    upstream_states: Sequence[TaskState | str],
    *,
    total_upstreams: int | None = None,
) -> bool:
    """Evaluate whether a trigger rule is satisfied.

    Args:
        rule: The trigger rule to evaluate. Accepts a ``TriggerRule`` enum
            member or its string value (e.g., ``"all_success"``).
        upstream_states: Terminal states reported so far by upstream tasks.
            Accepts ``TaskState`` enum members or their string values.
            For ``one_success`` / ``one_failed`` rules this may be a
            partial list (not all upstreams have finished yet).
        total_upstreams: Total number of upstream tasks. Defaults to
            ``len(upstream_states)`` which assumes all upstreams have
            already reported their state.

    Returns:
        True if the downstream task should run, False otherwise.

    Raises:
        ValueError: If ``rule`` or any state string is not a recognised
            enum value.

    Examples:
        >>> is_trigger_satisfied(TriggerRule.ALL_SUCCESS, [TaskState.SUCCESS, TaskState.SUCCESS])
        True

        >>> is_trigger_satisfied("none_failed", ["success", "skipped"])
        True

        >>> is_trigger_satisfied(TriggerRule.ONE_SUCCESS, [TaskState.SUCCESS], total_upstreams=5)
        True
    """
    if isinstance(rule, str):
        rule = TriggerRule(rule)

    states = [
        TaskState(s) if isinstance(s, str) else s for s in upstream_states
    ]
    total = total_upstreams if total_upstreams is not None else len(states)

    if total == 0:
        return True

    handler = _RULES.get(rule)
    if handler is None:
        raise ValueError(f"Unknown trigger rule: {rule!r}")

    return handler(_Counts(states, total))


def evaluate_all_rules(
    upstream_states: Sequence[TaskState | str],
    *,
    total_upstreams: int | None = None,
) -> dict[TriggerRule, bool]:
    """Evaluate every trigger rule against the given upstream states.

    Args:
        upstream_states: Terminal states reported so far by upstream tasks.
            Accepts ``TaskState`` enum members or their string values.
        total_upstreams: Total number of upstream tasks. Defaults to
            ``len(upstream_states)`` which assumes all upstreams have
            already reported their state.

    Returns:
        A dict mapping each ``TriggerRule`` to whether it is satisfied.

    Examples:
        >>> results = evaluate_all_rules(["success", "failed", "skipped"])
        >>> results[TriggerRule.ALL_DONE]
        True
        >>> results[TriggerRule.ALL_SUCCESS]
        False
    """
    states = [
        TaskState(s) if isinstance(s, str) else s for s in upstream_states
    ]
    total = total_upstreams if total_upstreams is not None else len(states)

    if total == 0:
        return {rule: True for rule in TriggerRule}

    counts = _Counts(states, total)
    return {rule: handler(counts) for rule, handler in _RULES.items()}
