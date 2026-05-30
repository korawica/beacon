from pydantic import BaseModel, Field


class BaseAction(BaseModel):
    id: str = Field(description="A task ID")
    type: str = Field(description="The type of action")
    desc: str = Field(default=None, description="A description of the task")
    uses: str = Field(description="An unsing plugin name")
    upstreams: list[str] = Field(
        default_factory=list,
        description="A list of upstream task ID(s)",
    )
    trigger_rule: str = Field(
        default="all_done", description="The trigger rule"
    )
    callbacks: list = Field(
        default_factory=list,
        description="A list of callback object(s)",
    )
    inputs: dict[str, str | int | float | bool] = Field(
        default_factory=dict,
    )
