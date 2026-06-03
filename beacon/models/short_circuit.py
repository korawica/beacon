from typing import Literal

from pydantic import Field

from ..core import BaseAction
from ..core.action import DownstreamDirective
from ..core.task_context import TaskContext


class ShortCircuit(BaseAction):
    """ShortCircuit Action Model.

    If plugin returns {"continue": False}, ALL downstream tasks are SKIPPED
    recursively. Used for "should this DAG continue?" gates.

    !!! example

        ```yaml
        tasks:
          - id: should-run
            type: short_circuit
            uses: py
            inputs:
              py_file: ./check_if_needed.py

          - id: expensive-etl
            upstream: [should-run]
            type: task
            uses: py
            inputs:
              py_file: ./etl.py
        ```
    """

    type: Literal["short_circuit"] = Field(default="short_circuit")

    def evaluate_downstream(
        self,
        task_ctx: TaskContext,
        all_downstream: list[str],
    ) -> DownstreamDirective:
        """Skip all downstream if plugin returns continue=False."""
        should_continue = task_ctx.outputs.get("continue", True)
        if should_continue:
            return DownstreamDirective(schedule=all_downstream, skip=[])
        return DownstreamDirective(schedule=[], skip=all_downstream)
