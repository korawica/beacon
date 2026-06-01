from pydantic import BaseModel, Field
from typing import Any, Literal


class Param(BaseModel):
    """Parameter Model.

    str - default `None`
    float - default `None`
    int - default `None`
    bool - default `None`
    choice - default be the first value in the list
    array - default `[]`
    object - default `{}`
    """

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
