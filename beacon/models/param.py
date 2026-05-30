from pydantic import BaseModel
from typing import Literal


class BaseParam(BaseModel): ...


class Param(BaseParam):
    type: Literal[
        "str",
        "int",
        "float",
        "bool",
        "array",
        "object",
    ]
