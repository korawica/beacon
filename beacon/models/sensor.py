from typing import Literal

from pydantic import Field

from ..core import BaseAction
from ..core.action import DownstreamDirective
from ..core.task_context import TaskContext


class Sensor(BaseAction):
    """Sensor Action Model.

    Waits for an external condition. The plugin contains an async poke loop
    that yields the event loop between checks. No separate "deferrable" concept.

    On timeout:
      - fail_mode="soft": task FAILS → downstream UPSTREAM_FAILED
      - fail_mode="silent": task SKIPPED → downstream SKIPPED

    !!! example

        ```yaml
        tasks:
          - id: wait-for-file
            type: sensor
            uses: gcs-sensor
            inputs:
              bucket: my-bucket
              prefix: raw/
            check_interval: 30
            execution_timeout: 3600
            fail_mode: silent
        ```
    """

    type: Literal["sensor"] = Field(
        default="sensor",
        description="A sensor action type.",
    )
    check_interval: int = Field(
        default=60,
        description="Seconds between condition checks (passed to plugin via context).",
    )
    execution_timeout: int | None = Field(
        default=None,
        description="Max wait time in seconds before failing.",
    )
    exponential_backoff: bool = Field(
        default=True,
        description="Whether to increase interval between checks.",
    )
    fail_mode: Literal["soft", "silent"] = Field(
        default="soft",
        description="soft=FAIL on timeout, silent=SKIP on timeout.",
    )
    retries: int = Field(default=0, description="Retries on failure")
    retry_delay: int = Field(default=10, description="Delay between retries")

    def evaluate_downstream(
        self,
        task_ctx: TaskContext,
        all_downstream: list[str],
    ) -> DownstreamDirective:
        """On success: schedule all downstream. Same as Task default."""
        return DownstreamDirective(schedule=all_downstream, skip=[])
