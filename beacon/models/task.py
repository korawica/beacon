from typing import Literal

from pydantic import Field, BaseModel

from ..core import BaseAction


class TaskOutput(BaseModel):
    metadata: dict = Field(..., description="A task output metadata")


class Task(BaseAction):
    """Task Action Model.

    !!! example

        ```yaml
        tasks:
          - id: example
            type: task
            uses: "some-task"
            upstream: ["start"]
            retries: 1
            retry_delay: 5
            execution_timeout: 10
        ```
    """

    type: Literal["task"] = Field(
        default="task", description="A task action type"
    )
    retries: int = Field(default=0, description="A number of retries")
    retry_delay: int = Field(
        default=10, description="A delay second between retries"
    )
    execution_timeout: int | None = Field(
        default=None, description="An execution timeout in seconds"
    )
    exponential_backoff: bool = Field(
        default=True,
        description="Whether or not to exponentially backoff.",
    )

    def outputs(self) -> dict:
        """Return Task model outputs."""
        return {
            "metadata": {
                "inputs": self.inputs,
            },
            "outputs": {},
            "retries": self.retries,
        }
