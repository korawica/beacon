from typing import Literal, Any

from pydantic import BaseModel, Field


class GlobalVariable(BaseModel):
    """Global Variable Model."""

    type: Literal["global_variable"] = Field(default="global_variable")
    stages: dict[str, dict[str, Any]] = Field(
        default_factory=dict,
        description="A mapping of environment name and its stages",
    )
