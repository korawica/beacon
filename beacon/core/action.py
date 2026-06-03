from __future__ import annotations

import logging
from dataclasses import dataclass, field as dc_field
from typing import Any, Type  # noqa: UP035  # see note in `uses` field below

from pydantic import BaseModel, Field

from ..callback import OnTaskEvent
from .plugin import PLUGINS_REGISTRY, BasePlugin
from .task_context import TaskContext
from .trigger_rule import TriggerRule


logger = logging.getLogger("beacon.action")


@dataclass
class DownstreamDirective:
    """What to do with downstream tasks after this action completes.

    Returned by :meth:`BaseAction.evaluate_downstream` and consumed by the
    scheduler to choose which downstream tasks to schedule vs. skip.
    """

    schedule: list[str] = dc_field(default_factory=list)
    """Task IDs to move to SCHEDULED → QUEUED."""

    skip: list[str] = dc_field(default_factory=list)
    """Task IDs to mark SKIPPED (and let their downstream cascade via trigger rules)."""


class BaseAction(BaseModel):
    """Base Action Model.

    Pure pydantic *definition* of one node in a DAG. Lifecycle/state-machine
    behavior lives in the scheduler — this class intentionally does NOT own
    state transitions or execution.
    """

    id: str = Field(description="A task ID")
    type: str = Field(description="The type of action")
    desc: str | None = Field(
        default=None, description="A description of the task"
    )
    uses: str | Type[BasePlugin] = Field(  # noqa: UP006
        # NOTE: Cannot use lowercase ``type[BasePlugin]`` here because this
        # model already defines a ``type`` field above which shadows the
        # builtin ``type`` during forward-ref resolution.
        description="A plugin name in the registry or a plugin model class",
    )
    upstream: list[str] = Field(
        default_factory=list,
        description="A list of upstream task ID(s)",
    )
    teardown: str | None = Field(
        default=None,
        description=(
            "If set, marks this task as a teardown for the referenced task ID. "
            "A teardown task always runs after all dependents of its setup task "
            "have reached terminal state, regardless of success or failure."
        ),
    )
    trigger_rule: str = Field(
        default=TriggerRule.ALL_SUCCESS,
        description="The trigger rule",
    )
    callbacks: list[OnTaskEvent] = Field(
        default_factory=list,
        description="A list of task-level callback object(s)",
    )
    inputs: dict[str, Any] = Field(
        default_factory=dict,
        description="A dict of inputs that will pass to its plugin model",
    )

    def plugin(self) -> Type[BasePlugin]:  # noqa: UP006
        """Resolve the plugin class for this action.

        Raises:
            LookupError: when ``uses`` is a string that is not registered.
        """
        if isinstance(self.uses, str):
            if self.uses not in PLUGINS_REGISTRY:
                raise LookupError(
                    f"Plugin {self.uses!r} not found in registry.",
                )
            return PLUGINS_REGISTRY[self.uses]
        return self.uses

    def plugin_name(self) -> str:
        """Return the canonical plugin name for this action."""
        if isinstance(self.uses, str):
            return self.uses
        return getattr(self.uses, "plugin_name", "unknown")

    def evaluate_downstream(
        self,
        task_ctx: TaskContext,
        all_downstream: list[str],
    ) -> DownstreamDirective:
        """Determine which downstream tasks to schedule vs skip.

        Override in subclasses (Branch, ShortCircuit) to provide custom
        routing logic based on plugin outputs.

        Default: schedule all downstream on success.
        """
        return DownstreamDirective(schedule=all_downstream, skip=[])

    def build_task_context(
        self,
        *,
        run_id: str,
        dag_id: str,
        dag_version: str,
        run_date: Any,
        logical_date: Any,
        data_interval_start: Any,
        data_interval_end: Any,
        params: dict[str, Any],
        rendered_inputs: dict[str, Any],
    ) -> TaskContext:
        """Build a TaskContext for this action. Called by the scheduler."""
        return TaskContext(
            run_id=run_id,
            dag_id=dag_id,
            task_id=self.id,
            dag_version=dag_version,
            run_date=run_date,
            logical_date=logical_date,
            data_interval_start=data_interval_start,
            data_interval_end=data_interval_end,
            params=params,
            inputs=rendered_inputs,
            plugin_name=self.plugin_name(),
            retries=getattr(self, "retries", 0),
            retry_delay=getattr(self, "retry_delay", 10),
            execution_timeout=getattr(self, "execution_timeout", None),
            exponential_backoff=getattr(self, "exponential_backoff", True),
        )
