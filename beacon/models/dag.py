from pydantic import BaseModel, Field

from .group import Action


class Dag(BaseModel):
    id: str = Field(description="A DAG ID")
    desc: str = Field(default=None, description="A description of the DAG")
    params: list = Field(
        default_factory=list, description="A list of parameters"
    )
    tasks: list[Action] = Field(
        default_factory=list,
        description="A list of task model(s)",
    )
    callbacks: list = Field(
        default_factory=list,
        description="A list of callback model(s)",
    )
