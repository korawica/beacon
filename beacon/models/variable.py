from typing import Any, Literal

from pydantic import BaseModel, Field


class Variable(BaseModel):
    """Variable Model."""

    type: Literal["variable"] = Field(default="variable")
    stages: dict[str, Any] = Field(
        default_factory=dict,
        description="A mapping of environment name and its variables",
    )
