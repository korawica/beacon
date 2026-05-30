from typing import Literal

from pydantic import Field

from ..core import BaseAction


class Branch(BaseAction):
    """Branch Action Model.

    !!! example

        ```yaml
        tasks:
          - id: example
            type: branch
            uses: "branch-plugin"
            success: ["task1", "task2"]
            failure: ["task3"]
        ```
    """

    type: Literal["branch"] = Field(default="branch")
    success: list[str] = Field(
        default_factory=list,
        description="A list of success downstream tasks",
    )
    failure: list[str] = Field(
        default_factory=list,
        description="A list of failure downstream tasks",
    )
