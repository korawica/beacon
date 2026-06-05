from typing import Any, Literal

from pydantic import Field

from ..core import BaseAction
from ..core.action import DownstreamDirective
from ..core.task_context import TaskContext


class ShortCircuit(BaseAction):
    """ShortCircuit Action Model.

    If the plugin signals ``False``, ALL downstream tasks are SKIPPED
    recursively. Used for "should this DAG continue?" gates.

    Plugin return value interpretation (via ``extract_outputs``):
      - ``False``      → skip all downstream.
      - ``True``       → continue (default when no exception raised).
      - ``dict`` with ``"continue"`` key → used directly.
      - ``None`` / no return → defaults to continuing.

    Plugins may also use the raise strategy:
      - ``raise TaskSkip()`` → this task SKIPPED, all downstream SKIPPED.
      - ``raise TaskFail()`` → permanent failure.

    !!! example

        ```yaml
        tasks:
          - id: should-run
            type: short_circuit
            uses: py
            inputs:
              py_statement: ./check_if_needed.py

          - id: expensive-etl
            upstream: [should-run]
            type: task
            uses: py
            inputs:
              py_statement: ./etl.py
        ```
    """

    type: Literal["short_circuit"] = Field(default="short_circuit")

    def extract_outputs(self, raw_outputs: dict[str, Any]) -> dict[str, Any]:
        """Convert the plugin's raw return value into a ``{"continue": bool}`` dict."""
        if "_result" in raw_outputs:
            result = raw_outputs["_result"]
            if isinstance(result, bool):
                return {"continue": result}
            # Any other non-None value → truthy check
            return {"continue": bool(result)}

        if "continue" in raw_outputs:
            return raw_outputs  # already structured

        # Empty dict (None return or bare return) → default to continue
        return {"continue": True}

    def evaluate_downstream(
        self,
        task_ctx: TaskContext,
        all_downstream: list[str],
    ) -> DownstreamDirective:
        """Skip all downstream if plugin returned False."""
        should_continue = task_ctx.outputs.get("continue", True)
        if should_continue:
            return DownstreamDirective(schedule=all_downstream, skip=[])
        return DownstreamDirective(schedule=[], skip=all_downstream)
