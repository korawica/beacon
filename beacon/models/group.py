from typing import Annotated, Union, Literal

from pydantic import BaseModel, Field

from ..core import TriggerRule
from .branch import Branch
from .short_circuit import ShortCircuit
from .task import Task
from .sensor import Sensor


class Group(BaseModel):
    id: str = Field(description="Group ID")
    type: Literal["group"] = Field(default="group")
    upstream: list[str] = Field(
        default_factory=list,
        description="A list of upstream task ID(s)",
    )
    trigger_rule: str = Field(
        default=TriggerRule.ALL_DONE,
        description="The trigger rule",
    )
    actions: list[ActionType] = Field(
        default_factory=list,
        description="A list of action(s) contained in this group",
    )


ActionType = Annotated[
    Union[
        Group,
        Task,
        Sensor,
        Branch,
        ShortCircuit,
    ],
    Field(
        discriminator="type",
        description="Any action model",
    ),
]
