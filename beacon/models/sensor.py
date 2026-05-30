from typing import Literal

from pydantic import Field

from ..core import BaseAction


class Sensor(BaseAction):
    type: Literal["sensor"] = Field(default="sensor")
