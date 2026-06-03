"""Callback and Hook system.

Hooks are triggered on task/DAG lifecycle events. They use the same
registry pattern as plugins — string resolution for YAML, class reference
for Python.
"""

import logging
import threading
from abc import ABC, abstractmethod
from typing import Any, ClassVar, Literal

from pydantic import BaseModel, Field

logger = logging.getLogger("beacon.callback")

HOOKS_REGISTRY: dict[str, type[BaseHook]] = {}
_lock = threading.Lock()


class BaseHook(ABC):
    """Base hook class. All hooks implement notify()."""

    hook_name: ClassVar[str] = "base"

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        name = getattr(cls, "hook_name", None)
        if name and name != "base":
            with _lock:
                HOOKS_REGISTRY[name] = cls

    @abstractmethod
    async def notify(self, event: str, data: dict[str, Any]) -> None:
        """Fire the hook with event data."""
        raise NotImplementedError


def _resolve_hook(
    hook: str | type[BaseHook] | BaseHook, inputs: dict
) -> BaseHook:
    """Resolve a hook from string name, class, or instance."""
    if isinstance(hook, BaseHook):
        return hook
    if isinstance(hook, str):
        if hook not in HOOKS_REGISTRY:
            raise ValueError(f"Hook {hook!r} not found in registry.")
        return HOOKS_REGISTRY[hook](**inputs)
    if isinstance(hook, type) and issubclass(hook, BaseHook):
        return hook(**inputs)
    raise TypeError(f"Invalid hook type: {type(hook)}")


class OnTaskEvent(BaseModel):
    """Task-level callback configuration."""

    model_config = {"arbitrary_types_allowed": True}

    on_event: Literal["start", "success", "failure", "retry"] = Field(
        description="Event that triggers this callback"
    )
    hook: str | Any = Field(
        description="Hook name (string) or hook class/instance"
    )
    inputs: dict[str, Any] = Field(
        default_factory=dict,
        description="Arguments passed to the hook constructor",
    )

    async def notify(self, task_ctx: Any, event: str) -> None:
        """Resolve hook and fire notification."""
        resolved = _resolve_hook(self.hook, self.inputs)
        data = {
            "run_id": task_ctx.run_id,
            "dag_id": task_ctx.dag_id,
            "task_id": task_ctx.task_id,
            "attempt": task_ctx.current_attempt,
            "params": task_ctx.params,
        }
        if task_ctx.last_attempt and task_ctx.last_attempt.error:
            data["error"] = task_ctx.last_attempt.error
        if task_ctx.outputs:
            data["outputs"] = task_ctx.outputs
        await resolved.notify(event, data)


class OnEvent(BaseModel):
    """DAG-level callback configuration."""

    model_config = {"arbitrary_types_allowed": True}

    on_event: Literal["start", "success", "failure", "complete"] = Field(
        description="Event that triggers this callback"
    )
    hook: str | Any = Field(
        description="Hook name (string) or hook class/instance"
    )
    inputs: dict[str, Any] = Field(
        default_factory=dict,
        description="Arguments passed to the hook constructor",
    )

    async def notify(self, data: dict[str, Any], event: str) -> None:
        """Resolve hook and fire notification."""
        resolved = _resolve_hook(self.hook, self.inputs)
        await resolved.notify(event, data)
