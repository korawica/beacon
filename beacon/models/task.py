from typing import Literal

from pydantic import Field

from ..core import BaseAction


class Task(BaseAction):
    """Task Action Model.

    !!! example

        ```yaml
        tasks:
            id: example
            type: task
            uses: "some-task"
            upstream: ["start"]
            retries: 1
            retry_delay: 5
            execution_timeout: 10
        ```
    """

    type: Literal["task"] = Field(default="task")
    retries: int = Field(default=0, description="Number of retries")
    retry_delay: int = Field(default=10, description="Delay between retries")
    execution_timeout: int | None = Field(
        default=None, description="Timeout in seconds"
    )
