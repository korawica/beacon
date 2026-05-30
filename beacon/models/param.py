from pydantic import BaseModel, Field
from typing import Any, Literal


class BaseParam(BaseModel): ...


class Param(BaseParam):
    name: str = Field(description="A parameter name")
    type: Literal[
        "str",
        "int",
        "float",
        "bool",
        "array",
        "object",
    ]
    default: Any = Field(description="A default value")
