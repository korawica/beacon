from typing import Literal

from pydantic import Field

from ..core.action import BaseAction


class Task(BaseAction):
    type: Literal["task"] = Field(default="task")
    retries: int = Field(default=0, description="Number of retries")
    retry_delay: int = Field(default=10, description="Delay between retries")
    timeout: int | None = Field(default=None, description="Timeout in seconds")
