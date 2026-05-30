from pydantic import BaseModel, Field

from .group import Action


class Dag(BaseModel):
    """DAG Model."""

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
    default_inputs: dict[str, str | int | float | bool] = Field(
        default_factory=dict,
        description=(
            "A list of default inputs that will passing to each task's plugin "
            "model"
        ),
    )
