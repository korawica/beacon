"""Dependency Trigger Rules.

This module provides all possible task terminal states, trigger rules,
and the evaluation engine that determines whether a downstream task
should run based on the states of its upstream dependencies.

References:
    https://www.astronomer.io/docs/learn/airflow-trigger-rules
"""

from enum import Enum
from collections.abc import Sequence, Callable


class TaskState(str, Enum):
    """All possible terminal states a task can reach."""

    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    UPSTREAM_FAILED = "upstream_failed"

    def __str__(self) -> str:
        return self.value


class TriggerRule(str, Enum):
    """All 10 official trigger rules for dependency-based task execution.

    These control *when* a downstream task is allowed to run based on
    the terminal states of its upstream (dependency) tasks.
    """

    ALL_SUCCESS = "all_success"
    """(Default) Run when ALL upstream tasks succeeded."""

    ALL_FAILED = "all_failed"
    """Run when ALL upstream tasks failed."""

    ALL_DONE = "all_done"
    """Run when ALL upstream tasks reached any terminal state."""

    ALL_SKIPPED = "all_skipped"
    """Run when ALL upstream tasks were skipped."""

    ONE_SUCCESS = "one_success"
    """Run when AT LEAST ONE upstream task succeeded.
    Fires immediately — does not wait for all upstreams to finish."""

    ONE_FAILED = "one_failed"
    """Run when AT LEAST ONE upstream task failed.
    Fires immediately — does not wait for all upstreams to finish."""

    NONE_FAILED = "none_failed"
    """Run when NO upstream task failed. Skipped tasks are OK."""

    NONE_SKIPPED = "none_skipped"
    """Run when NO upstream task was skipped. Failed tasks are OK."""

    NONE_FAILED_MIN_ONE_SUCCESS = "none_failed_min_one_success"
    """Run when NO upstream task failed AND at least one succeeded."""

    NONE_FAILED_OR_SKIPPED = "none_failed_or_skipped"
    """Strictest. Run only when NO upstream task failed or was skipped
    — every upstream must have succeeded."""

    def __str__(self) -> str:
        return self.value


class _StateCount:
    """Aggregated counts of upstream task terminal states."""

    __slots__ = (
        "success",
        "failed",
        "skipped",
        "upstream_failed",
        "total",
        "done",
    )

    def __init__(
        self,
        states: Sequence[TaskState],
        *,
        total_upstreams: int | None = None,
    ) -> None:
        self.success: int = 0
        self.failed: int = 0
        self.skipped: int = 0
        self.upstream_failed: int = 0

        for state in states:
            if state is TaskState.SUCCESS:
                self.success += 1
            elif state is TaskState.FAILED:
                self.failed += 1
            elif state is TaskState.SKIPPED:
                self.skipped += 1
            elif state is TaskState.UPSTREAM_FAILED:
                self.upstream_failed += 1

        self.total = (
            total_upstreams if total_upstreams is not None else len(states)
        )
        self.done = (
            self.success + self.failed + self.skipped + self.upstream_failed
        )

    @property
    def all_done(self) -> bool:
        return self.done >= self.total


# NOTE:
#   Each handler receives a _StateCount and returns True if the rule is
#   satisfied.  Using a dispatch dict avoids a long if/elif chain and
#   makes it trivial to add new rules.


def _all_success(c: _StateCount) -> bool:
    return c.all_done and c.success == c.total


def _all_failed(c: _StateCount) -> bool:
    return c.all_done and c.failed == c.total


def _all_done(c: _StateCount) -> bool:
    return c.all_done


def _all_skipped(c: _StateCount) -> bool:
    return c.all_done and c.skipped == c.total


def _one_success(c: _StateCount) -> bool:
    # Fires immediately — no need to wait for all_done.
    return c.success >= 1


def _one_failed(c: _StateCount) -> bool:
    # Fires immediately — no need to wait for all_done.
    return c.failed >= 1


def _none_failed(c: _StateCount) -> bool:
    return c.all_done and c.failed == 0


def _none_skipped(c: _StateCount) -> bool:
    return c.all_done and c.skipped == 0


def _none_failed_min_one_success(c: _StateCount) -> bool:
    return c.all_done and c.failed == 0 and c.success >= 1


def _none_failed_or_skipped(c: _StateCount) -> bool:
    return c.all_done and c.failed == 0 and c.skipped == 0


_HANDLERS: dict[TriggerRule, Callable] = {
    TriggerRule.ALL_SUCCESS: _all_success,
    TriggerRule.ALL_FAILED: _all_failed,
    TriggerRule.ALL_DONE: _all_done,
    TriggerRule.ALL_SKIPPED: _all_skipped,
    TriggerRule.ONE_SUCCESS: _one_success,
    TriggerRule.ONE_FAILED: _one_failed,
    TriggerRule.NONE_FAILED: _none_failed,
    TriggerRule.NONE_SKIPPED: _none_skipped,
    TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS: _none_failed_min_one_success,
    TriggerRule.NONE_FAILED_OR_SKIPPED: _none_failed_or_skipped,
}


def is_trigger_satisfied(
    rule: TriggerRule | str,
    upstream_states: Sequence[TaskState | str],
    *,
    total_upstreams: int | None = None,
) -> bool:
    """Evaluate whether a trigger rule is satisfied.

    Args:
        rule:
            The trigger rule to evaluate.
        upstream_states:
            Terminal states reported so far by upstream tasks.
            For ``one_success`` / ``one_failed`` this may be a *partial*
            list (not all upstreams have finished yet).
        total_upstreams:
            Total number of upstream tasks.  Defaults to
            ``len(upstream_states)`` which assumes all have reported.

    Returns:
        ``True`` if the downstream task should run, ``False`` otherwise.

    Raises:
        ValueError: If ``rule`` or any state string is not recognised.

    Examples:
        >>> is_trigger_satisfied(
        ...     TriggerRule.ALL_SUCCESS,
        ...     [TaskState.SUCCESS, TaskState.SUCCESS],
        ... )
        True

        >>> is_trigger_satisfied(
        ...     "none_failed",
        ...     ["success", "skipped"],
        ... )
        True

        >>> is_trigger_satisfied(
        ...     TriggerRule.ONE_SUCCESS,
        ...     [TaskState.SUCCESS],
        ...     total_upstreams=5,
        ... )
        True
    """
    # Coerce strings to enums.
    if isinstance(rule, str):
        rule = TriggerRule(rule)

    states = [
        TaskState(s) if isinstance(s, str) else s for s in upstream_states
    ]

    # No upstreams → always run.
    if (total_upstreams or len(states)) == 0:
        return True

    counts = _StateCount(states, total_upstreams=total_upstreams)

    handler = _HANDLERS.get(rule)
    if handler is None:
        raise ValueError(f"Unknown trigger rule: {rule!r}")

    return handler(counts)


def evaluate_all_rules(
    upstream_states: Sequence[TaskState | str],
    *,
    total_upstreams: int | None = None,
) -> dict[TriggerRule, bool]:
    """Evaluate every trigger rule against the given upstream states.

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
    counts = _StateCount(states, total_upstreams=total_upstreams)

    return {rule: handler(counts) for rule, handler in _HANDLERS.items()}
