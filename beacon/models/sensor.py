from typing import Literal

from pydantic import Field

from ..core.action import BaseAction


class Sensor(BaseAction):
    type: Literal["sensor"] = Field(default="sensor")
