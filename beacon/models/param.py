from pydantic import BaseModel, Field
from typing import Any, Literal


class Param(BaseModel):
    """Parameter Model."""

    name: str = Field(description="A parameter name")
    type: Literal[
        "str",
        "int",
        "float",
        "bool",
        "choice",
        "array",
        "object",
    ]
    default: Any = Field(description="A default value")
