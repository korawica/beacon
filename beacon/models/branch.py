from typing import Any, Literal

from pydantic import Field

from ..core import BaseAction
from ..core.action import DownstreamDirective
from ..core.task_context import TaskContext


class Branch(BaseAction):
    """Branch Action Model.

    Executes a plugin that chooses which downstream path(s) to take.
    Unchosen paths are SKIPPED.

    Plugin return value interpretation (via ``extract_outputs``):
      - ``list[str]``  → those exact task IDs are scheduled.
      - ``str``        → that single task ID is scheduled.
      - ``True``       → ``success`` path is taken (default when no exception).
      - ``False``      → ``failure`` path is taken.
      - ``dict`` with ``"branch"`` key → used directly.
      - ``None`` / no return → defaults to ``success`` path.

    Plugins may also use the raise strategy:
      - ``raise TaskSkip()`` → task SKIPPED, all downstream SKIPPED.
      - ``raise TaskFail()`` → task FAILED, downstream UPSTREAM_FAILED.

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

    def extract_outputs(self, raw_outputs: dict[str, Any]) -> dict[str, Any]:
        """Convert the plugin's raw return value into a ``{"branch": [...]}`` dict.

        See class docstring for the full interpretation table.
        """
        # Unwrap non-dict return (executor stored it as {"_result": <value>})
        if "_result" in raw_outputs:
            result = raw_outputs["_result"]
            if isinstance(result, list):
                return {"branch": result}
            if isinstance(result, str):
                return {"branch": [result]}
            if isinstance(result, bool):
                # True → success path, False → failure path
                return {
                    "branch": list(self.success if result else self.failure)
                }
            # Any other non-None value → success path
            return {"branch": list(self.success)}

        # Plugin returned a dict explicitly
        if "branch" in raw_outputs:
            return raw_outputs  # already structured correctly

        # Empty dict (None return or bare return) → default to success path
        return {"branch": list(self.success)}

    def evaluate_downstream(
        self,
        task_ctx: TaskContext,
        all_downstream: list[str],
    ) -> DownstreamDirective:
        """Route downstream based on ``outputs["branch"]`` (set by extract_outputs)."""
        chosen = task_ctx.outputs.get("branch", [])
        if isinstance(chosen, list):
            known = set(self.success + self.failure)
            scheduled = [t for t in chosen if t in known or t in all_downstream]
            skipped = [t for t in all_downstream if t not in scheduled]
            return DownstreamDirective(schedule=scheduled, skip=skipped)
        return DownstreamDirective(schedule=self.success, skip=self.failure)
