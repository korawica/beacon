from typing import Annotated, Union, Literal

from pydantic import BaseModel, Field

from .task import Task
from .sensor import Sensor


class Group(BaseModel):
    id: str = Field(description="Group ID")
    type: Literal["group"] = Field(default="group")
    upstreams: list[str] = Field(
        default_factory=list,
        description="A list of upstream task ID(s)",
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
    ],
    Field(
        discriminator="type",
        description="An actions models",
    ),
]
