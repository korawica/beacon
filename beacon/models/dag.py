import asyncio
import logging
import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

from .group import ActionType
from .param import Param

logger = logging.getLogger("beacon.dag")


class Dag(BaseModel):
    """DAG Model."""

    id: str = Field(description="A DAG ID")
    type: Literal["dag"] = Field(default="dag", description="The DAG type")
    desc: str | None = Field(
        default=None, description="A description of the DAG"
    )
    project: str = Field(default="default", description="A project name")
    owners: list[str] = Field(
        default_factory=list,
        description="A list of owners",
    )
    labels: dict[str, str] = Field(
        default_factory=dict,
        description="A mapping of labels",
    )
    params: list[Param] = Field(
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
            "Default inputs merged into every task's inputs before execution"
        ),
    )

    def run(
        self,
        *,
        params: dict[str, Any] | None = None,
        variables: dict[str, Any] | None = None,
        logical_date: datetime | None = None,
        metadata_path: str | None = None,
        max_concurrent: int = 10,
    ) -> dict[str, Any]:
        """Run the DAG locally end-to-end via :class:`LocalScheduler`.

        Validates the DAG via :meth:`dryrun` first, then schedules with
        full lifecycle semantics: trigger rules, branch / short-circuit
        propagation, ``UPSTREAM_FAILED`` cascade, teardown, and
        DAG-level callbacks.

        Returns:
            ``{"run_id": ..., "state": ..., "states": {...}, "outputs": {...}}``.
        """
        from ..dryrun import dryrun as _dryrun
        from ..metadata.json_store import JsonMetadata
        from ..scheduler import LocalScheduler

        dr = _dryrun(
            self, params=params, variables=variables, logical_date=logical_date
        )
        if not dr.is_valid:
            raise ValueError(f"DAG validation failed:\n{dr.print()}")

        if metadata_path is None:
            import tempfile

            metadata_path = tempfile.mkdtemp(prefix="beacon_run_")

        meta = JsonMetadata(metadata_path)
        scheduler = LocalScheduler(
            self,
            meta=meta,
            max_concurrent=max_concurrent,
            variables=variables or {},
        )

        run_id = f"run-{uuid.uuid4().hex[:8]}"
        result = asyncio.run(
            scheduler.run(
                params=params or {},
                run_id=run_id,
                logical_date=logical_date,
            )
        )
        return {
            "run_id": result.run_id,
            "state": result.state,
            "states": dict(result.states),
            "outputs": dict(result.outputs),
        }

    def test(
        self,
        *,
        params: dict[str, Any] | None = None,
        variables: dict[str, Any] | None = None,
        logical_date: datetime | None = None,
    ) -> dict[str, Any]:
        """Test the DAG by running it in a tempdir-backed metadata store.

        Returns a per-task pass/fail summary.
        """
        try:
            run_results = self.run(
                params=params,
                variables=variables,
                logical_date=logical_date,
            )
        except ValueError as e:
            return {
                "dag_id": self.id,
                "passed": False,
                "error": str(e),
                "tasks": {},
            }

        from ..core.state import TaskState

        results = {
            "dag_id": self.id,
            "passed": run_results["state"] == "success",
            "tasks": {},
        }
        for task_id, state in run_results["states"].items():
            results["tasks"][task_id] = {
                "state": state.value
                if isinstance(state, TaskState)
                else str(state),
                "outputs": run_results["outputs"].get(task_id, {}),
                "passed": state == TaskState.SUCCESS,
            }
        return results

    def dryrun(
        self,
        *,
        params: dict[str, Any] | None = None,
        variables: dict[str, Any] | None = None,
        logical_date: datetime | None = None,
        cron: str | None = None,
    ):
        """Validate and render the DAG without executing any plugins."""
        from ..dryrun import dryrun as _dryrun

        return _dryrun(
            self,
            params=params,
            variables=variables,
            logical_date=logical_date,
            cron=cron,
        )
