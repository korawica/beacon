from typing import Literal

from pydantic import Field

from ..core import BaseAction
from ..core.action import DownstreamDirective
from ..core.task_context import TaskContext


class Branch(BaseAction):
    """Branch Action Model.

    Executes a plugin that chooses which downstream path(s) to take.
    Plugin should return {"branch": ["task-id-1", "task-id-2"]}.
    Unchosen paths are SKIPPED.

    !!! example

        ```yaml
        tasks:
          - id: check-quality
            type: branch
            uses: py
            inputs:
              py_statement: ./check.py
            success: [process-good]
            failure: [quarantine]
        ```
    """

    type: Literal["branch"] = Field(default="branch")
    success: list[str] = Field(
        default_factory=list,
        description="Downstream tasks if branch resolves truthy",
    )
    failure: list[str] = Field(
        default_factory=list,
        description="Downstream tasks if branch resolves falsy",
    )

    def evaluate_downstream(
        self,
        task_ctx: TaskContext,
        all_downstream: list[str],
    ) -> DownstreamDirective:
        """Route downstream based on plugin output.

        Plugin returns {"branch": ["task-a", "task-b"]} to explicitly choose.
        Falls back to success/failure lists based on whether outputs exist.
        """
        chosen = task_ctx.outputs.get("branch")

        if isinstance(chosen, list):
            # Plugin explicitly chose task IDs
            known = set(self.success + self.failure)
            scheduled = [t for t in chosen if t in known or t in all_downstream]
            skipped = [t for t in all_downstream if t not in scheduled]
            return DownstreamDirective(schedule=scheduled, skip=skipped)

        # Fallback: success path if outputs are truthy, failure path otherwise
        if task_ctx.outputs:
            return DownstreamDirective(schedule=self.success, skip=self.failure)
        return DownstreamDirective(schedule=self.failure, skip=self.success)
