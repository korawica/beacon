from __future__ import annotations

from typing import Type  # noqa
from pydantic import BaseModel, Field

from .plugin import PLUGINS_REGISTRY, BasePlugin
from .context import Context
from .trigger_rule import TriggerRule


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

    def warp_execute(self, context: Context): ...
