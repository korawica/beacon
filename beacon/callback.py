"""Callback and Hook system.

Callbacks are triggered on task/DAG lifecycle events. They use the same
registry pattern as plugins — string resolution for YAML, class reference
for Python.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, ClassVar, Literal

from pydantic import BaseModel, Field, PrivateAttr

logger = logging.getLogger("beacon.callback")

CALLBACKS_REGISTRY: dict[str, type[Callback]] = {}


class Callback(ABC):
    """Base callback class. All callbacks implement :meth:`notify`.

    Subclasses auto-register when they declare a non-base ``hook_name``.
    """

    hook_name: ClassVar[str] = "base"

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        name = cls.__dict__.get("hook_name")
        if isinstance(name, str) and name and name != "base":
            existing = CALLBACKS_REGISTRY.get(name)
            if existing is not None and existing is not cls:
                logger.warning(
                    "Callback %r is being overridden (existing=%s, new=%s).",
                    name,
                    existing.__qualname__,
                    cls.__qualname__,
                )
            CALLBACKS_REGISTRY[name] = cls

    @abstractmethod
    async def notify(self, event: str, data: dict[str, Any]) -> None:
        """Fire the callback with event data."""
        raise NotImplementedError


def _resolve_hook(
    hook: str | type[Callback] | Callback, inputs: dict
) -> Callback:
    """Resolve a callback from string name, class, or instance."""
    if isinstance(hook, Callback):
        return hook
    if isinstance(hook, str):
        if hook not in CALLBACKS_REGISTRY:
            raise ValueError(f"Callback {hook!r} not found in registry.")
        return CALLBACKS_REGISTRY[hook](**inputs)
    if isinstance(hook, type) and issubclass(hook, Callback):
        return hook(**inputs)
    raise TypeError(f"Invalid callback type: {type(hook)}")


class _CachedHookMixin(BaseModel):
    """Mixin: resolve the hook once and cache the instance."""

    model_config = {"arbitrary_types_allowed": True}

    _resolved: Callback | None = PrivateAttr(default=None)

    def _get_resolved(self) -> Callback:
        if self._resolved is None:
            # ``self.hook`` / ``self.inputs`` are provided by concrete subclasses
            self._resolved = _resolve_hook(self.hook, self.inputs)  # type: ignore[attr-defined]
        return self._resolved


class OnTaskEvent(_CachedHookMixin):
    """Task-level callback configuration."""

    on_event: Literal["start", "success", "failure", "retry", "skipped"] = (
        Field(description="Event that triggers this callback")
    )
    hook: str | Any = Field(
        description="Callback name (string) or callback class/instance"
    )
    inputs: dict[str, Any] = Field(
        default_factory=dict,
        description="Arguments passed to the callback constructor",
    )

    async def notify(self, task_ctx: Any, event: str) -> None:
        """Resolve callback (cached) and fire notification."""
        resolved = self._get_resolved()
        data: dict[str, Any] = {
            "run_id": task_ctx.run_id,
            "dag_id": task_ctx.dag_id,
            "task_id": task_ctx.task_id,
            "attempt": task_ctx.attempt_number,
            "variables": task_ctx.variables,
        }
        if task_ctx.last_attempt and task_ctx.last_attempt.error:
            data["error"] = task_ctx.last_attempt.error
        if task_ctx.outputs:
            data["outputs"] = task_ctx.outputs
        await resolved.notify(event, data)


class OnDagEvent(_CachedHookMixin):
    """DAG-level callback configuration."""

    on_event: Literal["start", "success", "failure", "finished"] = Field(
        description="Event that triggers this callback"
    )
    hook: str | Any = Field(
        description="Callback name (string) or callback class/instance"
    )
    inputs: dict[str, Any] = Field(
        default_factory=dict,
        description="Arguments passed to the callback constructor",
    )

    async def notify(self, data: dict[str, Any], event: str) -> None:
        """Resolve callback (cached) and fire notification."""
        resolved = self._get_resolved()
        await resolved.notify(event, data)
