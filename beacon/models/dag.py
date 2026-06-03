import asyncio
import logging
import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

from .group import ActionType

logger = logging.getLogger("beacon.dag")


class Dag(BaseModel):
    """DAG Model."""

    id: str = Field(description="A DAG ID")
    type: Literal["dag"] = Field(default="dag", description="The DAG type")
    desc: str = Field(default=None, description="A description of the DAG")
    project: str = Field(default="default", description="A project name")
    owners: list[str] = Field(
        description="A list of owners",
    )
    labels: dict[str, str] = Field(
        default_factory=dict,
        description="A mapping of labels",
    )
    params: list = Field(
        default_factory=list, description="A list of parameters"
    )
    actions: list[ActionType] = Field(
        default_factory=list,
        description="A list of action(s) (Task, Branch, Sensor, Group, ...)",
    )
    callbacks: list = Field(
        default_factory=list,
        description="A list of callback model(s)",
    )
    default_inputs: dict[str, str | int | float | bool] = Field(
        default_factory=dict,
        description=(
            "A list of default inputs that will passing to each task's plugin "
            "model"
        ),
    )

    def run(
        self,
        *,
        params: dict[str, Any] | None = None,
        variables: dict[str, Any] | None = None,
        logical_date: datetime | None = None,
        metadata_path: str | None = None,
    ) -> dict[str, Any]:
        """Run the DAG locally end-to-end.

        Executes all tasks in topological order using the local executor,
        persisting state to a temporary (or specified) metadata store.

        Args:
            params: Runtime parameters for templating.
            variables: Variables for vars() resolution.
            logical_date: Simulated logical date.
            metadata_path: Path for metadata store. Uses temp dir if None.

        Returns:
            Dict with run_id, final states per task, and outputs per task.
        """
        from ..dryrun import (
            dryrun as _dryrun,
            _flatten_actions,
            _topological_sort,
        )
        from ..metadata.json_store import JsonMetadata
        from ..worker import Worker
        from ..core.task_context import TaskContext

        # Validate first
        dr = _dryrun(
            self, params=params, variables=variables, logical_date=logical_date
        )
        if not dr.is_valid:
            raise ValueError(f"DAG validation failed:\n{dr.print()}")

        params = params or {}
        now = logical_date or datetime.now()
        run_id = f"run-{uuid.uuid4().hex[:8]}"

        # Resolve metadata path
        if metadata_path is None:
            import tempfile

            metadata_path = tempfile.mkdtemp(prefix="beacon_run_")

        meta = JsonMetadata(metadata_path)

        # Build task map and execution order
        task_map: dict[str, Any] = {}
        _flatten_actions(self.actions, task_map)
        task_order = _topological_sort(task_map)

        # Execute tasks in order
        results: dict[str, Any] = {
            "run_id": run_id,
            "states": {},
            "outputs": {},
        }

        async def _execute():
            for task_id in task_order:
                action = task_map[task_id]
                plugin_name = (
                    action.uses
                    if isinstance(action.uses, str)
                    else getattr(action.uses, "plugin_name", "unknown")
                )

                # Merge default_inputs with action inputs
                merged_inputs = {**self.default_inputs, **action.inputs}

                task_ctx = TaskContext(
                    run_id=run_id,
                    dag_id=self.id,
                    task_id=task_id,
                    dag_version="local",
                    run_date=now,
                    logical_date=now,
                    data_interval_start=now,
                    data_interval_end=now,
                    params=params,
                    inputs=merged_inputs,
                    plugin_name=plugin_name,
                    retries=getattr(action, "retries", 0),
                    retry_delay=getattr(action, "retry_delay", 10),
                    execution_timeout=getattr(
                        action, "execution_timeout", None
                    ),
                    exponential_backoff=getattr(
                        action, "exponential_backoff", True
                    ),
                )

                upstream_ids = list(action.upstream)
                worker = Worker(meta, max_concurrent=1)
                await worker.submit(task_ctx, upstream_task_ids=upstream_ids)

                async def stop():
                    await asyncio.sleep(0.5)
                    await worker.shutdown()

                await asyncio.gather(worker.run(), stop())

                # Collect results
                from ..core.state import TaskState

                state = await meta.get_task_state(run_id, self.id, task_id)
                ctx = await meta.get_task_context(run_id, self.id, task_id)
                results["states"][task_id] = state
                results["outputs"][task_id] = ctx.outputs if ctx else {}

                if state == TaskState.FAILED:
                    logger.error("Task %s failed, stopping DAG run.", task_id)
                    break

        asyncio.run(_execute())
        return results

    def test(
        self,
        *,
        params: dict[str, Any] | None = None,
        variables: dict[str, Any] | None = None,
        logical_date: datetime | None = None,
    ) -> dict[str, Any]:
        """Test the DAG by executing all tasks and verifying plugin compatibility.

        Like run(), but focused on validation:
          - Verifies each plugin can be instantiated with its inputs.
          - Executes plugins and reports success/failure per task.
          - Does NOT persist state beyond the test.

        Args:
            params: Runtime parameters for templating.
            variables: Variables for vars() resolution.
            logical_date: Simulated logical date.

        Returns:
            Dict with pass/fail status per task and any errors.
        """
        import tempfile

        metadata_path = tempfile.mkdtemp(prefix="beacon_test_")
        results = {"dag_id": self.id, "tasks": {}, "passed": True}

        try:
            run_results = self.run(
                params=params,
                variables=variables,
                logical_date=logical_date,
                metadata_path=metadata_path,
            )
        except ValueError as e:
            results["passed"] = False
            results["error"] = str(e)
            return results

        from ..core.state import TaskState

        for task_id, state in run_results["states"].items():
            task_result = {
                "state": state.value if state else "unknown",
                "outputs": run_results["outputs"].get(task_id, {}),
            }
            if state != TaskState.SUCCESS:
                task_result["passed"] = False
                results["passed"] = False
            else:
                task_result["passed"] = True
            results["tasks"][task_id] = task_result

        return results

    def dryrun(
        self,
        *,
        params: dict[str, Any] | None = None,
        variables: dict[str, Any] | None = None,
        logical_date: datetime | None = None,
        cron: str | None = None,
    ):
        """Dry run the DAG for showing final Jinja template rendering.

        Validates the DAG structure and renders all templates without
        executing any plugins. Use this to verify your DAG definition
        before deploying.

        Args:
            params: Runtime parameters for templating.
            variables: Variables for vars() resolution.
            logical_date: Simulated logical date.
            cron: Cron expression to compute data_interval_start/end.

        Returns:
            DryRunResult with validation status and resolved inputs.
        """
        from ..dryrun import dryrun as _dryrun

        return _dryrun(
            self,
            params=params,
            variables=variables,
            logical_date=logical_date,
            cron=cron,
        )
