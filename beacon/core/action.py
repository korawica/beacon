from __future__ import annotations

import logging
from dataclasses import dataclass, field as dc_field
from typing import Any, Type  # noqa

from pydantic import BaseModel, Field

from .executor import BaseExecutor, LocalExecutor
from .plugin import PLUGINS_REGISTRY, BasePlugin
from .state import TaskState
from .task_context import AttemptStatus, TaskContext
from .trigger_rule import TriggerRule

logger = logging.getLogger("beacon.action")


@dataclass
class DownstreamDirective:
    """What to do with downstream tasks after this action completes."""

    schedule: list[str] = dc_field(default_factory=list)
    """Task IDs to move to SCHEDULED → QUEUED."""

    skip: list[str] = dc_field(default_factory=list)
    """Task IDs to mark SKIPPED (and transitively skip their downstream)."""


class BaseAction(BaseModel):
    """Base Action Model.

    This base action model is used for all actions, Task, Branch, Sensor, or
    Group.
    """

    id: str = Field(description="A task ID")
    type: str = Field(description="The type of action")
    desc: str = Field(default=None, description="A description of the task")
    uses: str | Type[BasePlugin] = Field(  # noqa: UP007
        description="An unsing plugin name in registry or a plugin model class",
    )
    upstream: list[str] = Field(
        default_factory=list,
        description="A list of upstream task ID(s)",
    )
    trigger_rule: str = Field(
        default=TriggerRule.ALL_DONE,
        description="The trigger rule",
    )
    callbacks: list = Field(
        default_factory=list,
        description="A list of callback object(s)",
    )
    inputs: dict[str, str | int | float | bool] = Field(
        default_factory=dict,
        description="A dict of inputs that will passing to its plugin model",
    )

    def plugin(self) -> type[BaseModel]:
        """Get the plugin model."""
        if isinstance(self.uses, str):
            if self.uses not in PLUGINS_REGISTRY:
                raise NotImplementedError(
                    f"A plugin {self.uses!r} not implemented on the registry.",
                )
            return PLUGINS_REGISTRY[self.uses]
        return self.uses

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
        plugin_name = (
            self.uses
            if isinstance(self.uses, str)
            else getattr(self.uses, "plugin_name", "unknown")
        )
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
            plugin_name=plugin_name,
            retries=getattr(self, "retries", 0),
            retry_delay=getattr(self, "retry_delay", 10),
            execution_timeout=getattr(self, "execution_timeout", None),
            exponential_backoff=getattr(self, "exponential_backoff", True),
        )

    async def warp_execute(
        self,
        task_ctx: TaskContext,
        *,
        executor: BaseExecutor | None = None,
        set_state: Any = None,
        on_retry_enqueue: Any = None,
    ) -> TaskState:
        """Main orchestration method called by the worker.

        Handles the full lifecycle: state transitions, retries, callbacks.

        Args:
            task_ctx: The TaskContext from metadata store.
            executor: The executor to run the task. Defaults to LocalExecutor.
            set_state: Async callable(task_ctx, TaskState) to persist state.
            on_retry_enqueue: Async callable(task_ctx, delay) to re-enqueue.

        Returns:
            The final TaskState after execution.
        """
        executor = executor or LocalExecutor()

        # --- Transition: QUEUED → RUNNING ---
        await self._transition(task_ctx, TaskState.RUNNING, set_state)
        await self._fire_callbacks("start", task_ctx)

        # --- Execute via executor ---
        task_ctx = await executor.run_task(task_ctx)

        # --- Evaluate result ---
        last = task_ctx.last_attempt
        if last and last.state == AttemptStatus.SUCCESS:
            await self._transition(task_ctx, TaskState.SUCCESS, set_state)
            await self._fire_callbacks("success", task_ctx)
            return TaskState.SUCCESS

        # Failed or timed out — decide retry or terminal failure
        if task_ctx.has_retries_left:
            await self._transition(task_ctx, TaskState.UP_FOR_RETRY, set_state)
            await self._fire_callbacks("retry", task_ctx)
            delay = task_ctx.next_retry_delay
            if on_retry_enqueue:
                await on_retry_enqueue(task_ctx, delay)
            return TaskState.UP_FOR_RETRY

        # No retries left
        await self._transition(task_ctx, TaskState.FAILED, set_state)
        await self._fire_callbacks("failure", task_ctx)
        return TaskState.FAILED

    async def _transition(
        self,
        task_ctx: TaskContext,
        target: TaskState,
        set_state: Any,
    ) -> None:
        """Validate and persist a state transition."""
        # Note: current state is tracked externally by the metadata store.
        # The validate_transition is called by set_state implementation.
        if set_state:
            await set_state(task_ctx, target)
        logger.info(
            "Task %s/%s → %s (attempt %d)",
            task_ctx.dag_id,
            task_ctx.task_id,
            target,
            task_ctx.current_attempt,
        )

    async def _fire_callbacks(self, event: str, task_ctx: TaskContext) -> None:
        """Fire task-level callbacks for the given event."""
        for cb in self.callbacks:
            try:
                if hasattr(cb, "on_event") and cb.on_event == event:
                    if hasattr(cb, "notify"):
                        await cb.notify(task_ctx, event)
            except Exception as exc:
                logger.error(
                    "Callback error on %s for task %s: %s",
                    event,
                    task_ctx.task_id,
                    exc,
                )
