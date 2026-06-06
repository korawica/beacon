import asyncio
import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field, PrivateAttr

from .group import ActionType

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

    # --- bundle metadata (set by the loader; not serialised) ----------
    _source_file: Path | None = PrivateAttr(default=None)
    _bundle_root: Path | None = PrivateAttr(default=None)

    def run(
        self,
        *,
        variables: dict[str, Any] | None = None,
        logical_date: datetime | None = None,
        metadata_path: str | None = None,
        max_concurrent: int = 10,
    ) -> dict[str, Any]:
        """Run the DAG locally end-to-end via :class:`DagRunner`.

        Validates the DAG via :meth:`plan` first, then schedules with
        full lifecycle semantics: trigger rules, branch / short-circuit
        propagation, ``UPSTREAM_FAILED`` cascade, teardown, and
        DAG-level callbacks.

        Returns:
            ``{"run_id": ..., "state": ..., "states": {...}, "outputs": {...}}``.

        Variable resolution:
            If the DAG was loaded from a bundle (loader populated
            ``_source_file`` + ``_bundle_root``) and the caller does
            **not** pass ``variables=``, the bundle's scoped variable
            chain (dag → group → bundle ``global_variables.yml``) is
            auto-resolved. Explicit ``variables=`` always wins.
        """
        from ..plan import plan as _plan
        from ..metadata.json_store import LocalMetadata
        from ..runner import DagRunner

        # Auto-resolve scoped variables if available and not overridden.
        if variables is None and self._source_file and self._bundle_root:
            from ..core.bundle import LocalBundle

            try:
                scope = LocalBundle(
                    name=self._bundle_root.name, path=self._bundle_root
                ).variable_scope
                variables = scope.resolve_for(self._source_file)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Scoped variable resolution failed for dag %r: %s",
                    self.id,
                    exc,
                )
                variables = None

        dr = _plan(self, variables=variables, logical_date=logical_date)
        if not dr.is_valid:
            raise ValueError(f"DAG validation failed:\n{dr.print()}")

        if metadata_path is None:
            import tempfile

            metadata_path = tempfile.mkdtemp(prefix="beacon_run_")

        meta = LocalMetadata(metadata_path)
        scheduler = DagRunner(
            self,
            meta=meta,
            max_concurrent=max_concurrent,
            variables=variables or {},
        )

        run_id = f"manual-{self.id}-{uuid.uuid4().hex[:8]}"
        result = asyncio.run(
            scheduler.run(
                variables=variables,
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
        from ..metadata.json_store import LocalMetadata
        from ..runner import DagRunner

        meta = LocalMetadata(metadata_path)
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

        Shorthand for ``dag.mark(state="failed", ...)``.
        """
        return self.mark(
            run_id=run_id,
            task_id=task_id,
            state="failed",
            metadata_path=metadata_path,
            max_concurrent=max_concurrent,
        )

    def mark(
        self,
        *,
        run_id: str,
        task_id: str | list[str],
        state: str,
        metadata_path: str,
        max_concurrent: int = 10,
    ) -> dict[str, Any]:
        """Force a task to a terminal state and re-fire affected teardowns.

        Use when a task is stuck, or you know externally it's done:
            - ``state="failed"`` → kill and clean up
            - ``state="success"`` → mark done, unblock downstream

        In both cases, any task-level teardown whose dep set includes the
        marked task is auto-cleared and re-fires on resume.

        Returns:
            Same shape as :meth:`run`, plus ``"marked"`` and
            ``"teardowns_fired"`` keys.

        Example::

            dag.mark(run_id=..., task_id="process", state="failed",
                     metadata_path="./meta")
            dag.mark(run_id=..., task_id="sensor", state="success",
                     metadata_path="./meta")
        """
        from ..core.state import TaskState
        from ..metadata.json_store import LocalMetadata
        from ..runner import DagRunner

        valid_states = {"failed", "success", "skipped"}
        if state not in valid_states:
            raise ValueError(
                f"state must be one of {valid_states}, got {state!r}"
            )
        target_state = TaskState(state)

        meta = LocalMetadata(metadata_path)
        runner = DagRunner(self, meta=meta, max_concurrent=max_concurrent)

        async def _mark_and_resume():
            info = await runner.mark(
                run_id=run_id, task_ids=task_id, state=target_state
            )
            result = await runner.run(run_id=run_id, resume=True)
            return info, result

        info, result = asyncio.run(_mark_and_resume())
        return {
            "run_id": result.run_id,
            "state": result.state,
            "states": dict(result.states),
            "outputs": dict(result.outputs),
            "marked": info["marked"],
            "teardowns_fired": info["teardowns_cleared"],
        }

    def backfill(
        self,
        *,
        start_date: datetime,
        end_date: datetime,
        cron: str,
        metadata_path: str,
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
            variables: Variables applied to every generated run.
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
                variables={"source": "postgres"},
                reset_existing=True,       # re-run any existing days too
            )
        """
        from croniter import croniter

        from ..core.graph import build_graph
        from ..metadata.json_store import LocalMetadata
        from ..runner import DagRunner

        if end_date < start_date:
            raise ValueError(
                f"end_date {end_date} is before start_date {start_date}"
            )

        meta = LocalMetadata(metadata_path)
        runner = DagRunner(
            self,
            meta=meta,
            max_concurrent=max_concurrent,
            variables=variables or {},
        )
        graph = build_graph(self.actions)
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
                        variables=variables,
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
        variables: dict[str, Any] | None = None,
        logical_date: datetime | None = None,
    ) -> dict[str, Any]:
        """Test the DAG by running it in a tempdir-backed metadata store.

        Returns a per-task pass/fail summary.
        """
        try:
            run_results = self.run(
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

    def plan(
        self,
        *,
        variables: dict[str, Any] | None = None,
        logical_date: datetime | None = None,
        data_interval_start: datetime | None = None,
        data_interval_end: datetime | None = None,
        cron: str | None = None,
    ):
        """Validate and render the DAG without executing any plugins.

        Shows resolved inputs per task against real variables /
        logical_date before a single task runs.
        """
        from ..plan import plan as _plan

        return _plan(
            self,
            variables=variables,
            logical_date=logical_date,
            data_interval_start=data_interval_start,
            data_interval_end=data_interval_end,
            cron=cron,
        )

    def dryrun(
        self,
        *,
        variables: dict[str, Any] | None = None,
        logical_date: datetime | None = None,
        cron: str | None = None,
    ):
        """Deprecated — use ``dag.plan()`` instead."""
        return self.plan(
            variables=variables,
            logical_date=logical_date,
            cron=cron,
        )
