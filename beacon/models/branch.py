from typing import Literal

from pydantic import Field

from ..core import BaseAction


class Branch(BaseAction):
    type: Literal["branch"] = Field(default="branch")
    success_downstream: list[str] = Field(
        default_factory=list,
        description="A list of success downstream tasks",
    )
    failure_downstream: list[str] = Field(
        default_factory=list,
        description="A list of failure downstream tasks",
    )
