from typing import Annotated, Union, Literal

from pydantic import BaseModel, Field

from ..core import TriggerRule
from .branch import Branch
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
    tasks: list[Action] = Field(
        default_factory=list,
        description="A list of task model(s)",
    )


Action = Annotated[
    Union[
        Group,
        Task,
        Sensor,
        Branch,
    ],
    Field(
        discriminator="type",
        description="An actions models",
    ),
]
