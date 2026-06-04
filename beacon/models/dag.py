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
        """Run the DAG locally end-to-end via :class:`DagRunner`.

        Validates the DAG via :meth:`dryrun` first, then schedules with
        full lifecycle semantics: trigger rules, branch / short-circuit
        propagation, ``UPSTREAM_FAILED`` cascade, teardown, and
        DAG-level callbacks.

        Returns:
            ``{"run_id": ..., "state": ..., "states": {...}, "outputs": {...}}``.
        """
        from ..dryrun import dryrun as _dryrun
        from ..metadata.json_store import JsonMetadata
        from ..runner import DagRunner

        dr = _dryrun(
            self, params=params, variables=variables, logical_date=logical_date
        )
        if not dr.is_valid:
            raise ValueError(f"DAG validation failed:\n{dr.print()}")

        if metadata_path is None:
            import tempfile

            metadata_path = tempfile.mkdtemp(prefix="beacon_run_")

        meta = JsonMetadata(metadata_path)
        scheduler = DagRunner(
            self,
            meta=meta,
            max_concurrent=max_concurrent,
            variables=variables or {},
        )

        run_id = f"manual-{self.id}-{uuid.uuid4().hex[:8]}"
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

    def clear(
        self,
        *,
        run_id: str,
        task_id: str | list[str],
        downstream: bool = False,
        metadata_path: str,
        max_concurrent: int = 10,
    ) -> dict[str, Any]:
        """Clear one or more tasks in an existing run, then re-execute.

        Backfill / fix-and-rerun convenience. Resets the chosen task(s)
        to ``NONE`` state (wipes attempts + outputs in metadata) and
        immediately re-runs the DAG with ``resume=True`` so already-
        terminal upstream tasks are NOT re-executed.

        For surgical clear-without-rerun, drop down to the runner:
        ``await DagRunner(dag, meta).clear(run_id=..., task_ids=...)``.

        Args:
            run_id: The existing DagRun to operate on.
            task_id: Single id or list of ids to clear.
            downstream: Also clear every task transitively downstream.
                Use when the cleared task's outputs feed already-successful
                downstream tasks.
            metadata_path: Path to the metadata store (same path used to
                run the DAG originally — required for state to survive).
            max_concurrent: Worker concurrency for the rerun.

        Returns:
            Same shape as :meth:`run`, plus ``"cleared": [task_id, ...]``.

        Example::

            dag.run(metadata_path="./meta", ...)             # initial
            dag.clear(run_id="run-abc", task_id="task2",
                      downstream=True, metadata_path="./meta")  # fix + rerun
        """
        from ..metadata.json_store import JsonMetadata
        from ..runner import DagRunner

        meta = JsonMetadata(metadata_path)
        runner = DagRunner(self, meta=meta, max_concurrent=max_concurrent)

        async def _clear_and_run() -> tuple[list[str], Any]:
            cleared = await runner.clear(
                run_id=run_id, task_ids=task_id, downstream=downstream
            )
            result = await runner.run(run_id=run_id, resume=True)
            return cleared, result

        cleared, result = asyncio.run(_clear_and_run())
        return {
            "run_id": result.run_id,
            "state": result.state,
            "states": dict(result.states),
            "outputs": dict(result.outputs),
            "cleared": cleared,
        }

    def fail(
        self,
        *,
        run_id: str,
        task_id: str | list[str],
        metadata_path: str,
        max_concurrent: int = 10,
    ) -> dict[str, Any]:
        """Force-fail task(s) and re-fire affected teardowns.

        Use when a task is stuck or known-bad and you want the resource
        cleanup (teardown) to fire instead of retrying.

        Semantics:
            1. Mark each task as FAILED.
            2. Auto-clear any teardown whose dependency set includes the
               failed task.
            3. Resume the run → only the affected teardowns execute.

        Returns:
            Same shape as :meth:`run`, plus ``"failed"`` and
            ``"teardowns_fired"`` keys.

        Example::

            dag.run(metadata_path="./meta", ...)                 # running
            dag.fail(run_id="manual-spark-x", task_id="process",
                     metadata_path="./meta")
            # → teardown `stop` fires; `launch` stays SUCCESS.
        """
        from ..metadata.json_store import JsonMetadata
        from ..runner import DagRunner

        meta = JsonMetadata(metadata_path)
        runner = DagRunner(self, meta=meta, max_concurrent=max_concurrent)

        async def _fail_and_resume():
            info = await runner.fail(run_id=run_id, task_ids=task_id)
            result = await runner.run(run_id=run_id, resume=True)
            return info, result

        info, result = asyncio.run(_fail_and_resume())
        return {
            "run_id": result.run_id,
            "state": result.state,
            "states": dict(result.states),
            "outputs": dict(result.outputs),
            "failed": info["failed"],
            "teardowns_fired": info["teardowns_cleared"],
        }

    def backfill(
        self,
        *,
        start_date: datetime,
        end_date: datetime,
        cron: str,
        metadata_path: str,
        params: dict[str, Any] | None = None,
        variables: dict[str, Any] | None = None,
        reset_existing: bool = False,
        max_concurrent: int = 10,
    ) -> list[dict[str, Any]]:
        """Run the DAG once per cron tick in ``[start_date, end_date]``.

        One DagRun per logical date. ``run_id`` is deterministic from the
        logical date, so re-invoking backfill over the same range either
        skips existing runs (default) or clears + re-executes them
        (``reset_existing=True``).

        Args:
            start_date: Inclusive lower bound for logical dates.
            end_date: Inclusive upper bound for logical dates.
            cron: Cron expression that defines the schedule ticks within
                the range (e.g. ``"0 0 * * *"`` for daily at midnight).
            metadata_path: Persistent metadata store path.
            params: DAG params applied to every generated run.
            variables: Stage variables applied to every generated run.
            reset_existing: When a run with the deterministic id already
                exists for a logical date: ``False`` (default) skips it;
                ``True`` clears every task in that run and re-executes.
            max_concurrent: Worker concurrency within each run.

        Returns:
            One dict per logical date::

                {
                    "run_id": ...,
                    "logical_date": datetime,
                    "state": "success" | "failed" | "skipped",
                    "states": {task_id: TaskState},
                    "outputs": {task_id: {...}},
                }

            ``state == "skipped"`` means the run already existed and
            ``reset_existing=False``.

        Example::

            dag.backfill(
                start_date=datetime(2026, 1, 1),
                end_date=datetime(2026, 1, 7),
                cron="0 0 * * *",          # daily
                metadata_path="./meta",
                params={"source": "postgres"},
                reset_existing=True,       # re-run any existing days too
            )
        """
        from croniter import croniter

        from ..metadata.json_store import JsonMetadata
        from ..runner import DagRunner, _build_graph

        if end_date < start_date:
            raise ValueError(
                f"end_date {end_date} is before start_date {start_date}"
            )

        meta = JsonMetadata(metadata_path)
        runner = DagRunner(
            self,
            meta=meta,
            max_concurrent=max_concurrent,
            variables=variables or {},
        )
        graph = _build_graph(self)
        all_task_ids = list(graph.task_map)

        # Enumerate cron ticks in [start_date, end_date].
        from datetime import timedelta

        logical_dates: list[datetime] = []
        itr = croniter(cron, start_date - timedelta(microseconds=1))
        while True:
            nxt: datetime = itr.get_next(datetime)
            if nxt > end_date:
                break
            logical_dates.append(nxt)

        async def _backfill_all() -> list[dict[str, Any]]:
            out: list[dict[str, Any]] = []
            for logical_date in logical_dates:
                run_id = (
                    f"backfill-{self.id}-"
                    f"{logical_date.strftime('%Y%m%dT%H%M%S')}"
                )
                existing = await meta.get_dag_run(run_id, self.id)

                if existing is not None and not reset_existing:
                    out.append(
                        {
                            "run_id": run_id,
                            "logical_date": logical_date,
                            "state": "skipped",
                            "states": {},
                            "outputs": {},
                        }
                    )
                    continue

                if existing is not None:
                    # Reset: clear every task, then resume.
                    for tid in all_task_ids:
                        await meta.clear_task(run_id, self.id, tid)
                    result = await runner.run(run_id=run_id, resume=True)
                else:
                    result = await runner.run(
                        run_id=run_id,
                        params=params or {},
                        logical_date=logical_date,
                    )

                out.append(
                    {
                        "run_id": result.run_id,
                        "logical_date": logical_date,
                        "state": result.state,
                        "states": dict(result.states),
                        "outputs": dict(result.outputs),
                    }
                )
            return out

        return asyncio.run(_backfill_all())

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
