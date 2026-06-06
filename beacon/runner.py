"""DagRunner.

In-process async DAG orchestrator. Owns graph traversal, trigger-rule
evaluation, branch / short-circuit propagation, ``UPSTREAM_FAILED`` /
``SKIPPED`` cascading, teardown scheduling, and DAG-level callback firing.

The runner delegates per-task execution to :class:`beacon.worker.Worker`
via the ``on_terminal`` hook, so the worker only knows how to run one task
at a time and the runner owns the topology.

Used by :meth:`beacon.models.dag.Dag.run` and :meth:`Dag.test`.

Design notes
------------
* "Ready" tasks are those whose **non-teardown** upstreams are all in a
  terminal state. Once ready, the trigger rule decides between
  ``SCHEDULED`` and ``SKIPPED`` / ``UPSTREAM_FAILED``.
* Branch / ShortCircuit return a :class:`DownstreamDirective`. Listed
  ``skip`` task IDs are marked SKIPPED before they are evaluated, so the
  cascade falls out naturally from the trigger rule on their downstreams.
* Teardown tasks wait for the setup task **and** every transitive
  dependent of the setup task to reach a terminal state, then run with
  ``trigger_rule = ALL_DONE`` semantics regardless of dependent outcomes.
  Teardown failures are logged but do not change DAG state.
"""

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

from .callback import OnDagEvent
from .core.action import BaseAction, DownstreamDirective
from .core.context import build_runtime_dict
from .core.executor import BaseExecutor, LocalExecutor
from .core.graph import Graph, build_graph, collect_self_and_downstream
from .core.renderer import Renderer
from .core.state import TERMINAL_STATES, TaskState
from .core.task_context import TaskContext
from .core.trigger_rule import TriggerRule, evaluate_trigger_rule
from .metadata.json_store import LocalMetadata
from .worker import Worker

if TYPE_CHECKING:
    from .models.dag import Dag

logger = logging.getLogger("beacon.runner")


# ---------- Graph import (see core/graph.py) -------------------------------


# ---------- result ---------------------------------------------------------


@dataclass
class DagRunResult:
    """Outcome of a single :meth:`DagRunner.run` invocation."""

    run_id: str
    dag_id: str
    state: str = "running"
    """Overall DagRun state: ``success`` / ``failed`` / ``running``."""
    states: dict[str, TaskState] = field(default_factory=dict)
    """Per-task final state. Includes teardown tasks."""
    outputs: dict[str, dict[str, Any]] = field(default_factory=dict)
    """Per-task ``outputs`` written by plugins."""

    @property
    def passed(self) -> bool:
        return self.state == "success"


# ---------- runner ---------------------------------------------------------


class DagRunner:
    """In-process async DAG runner.

    Args:
        dag: The :class:`Dag` to execute.
        meta: Metadata store. A fresh tempdir-backed
            :class:`LocalMetadata` is created when ``None``.
        executor: Per-task executor. Defaults to :class:`LocalExecutor`.
        max_concurrent: Max concurrent in-flight tasks.
    """

    def __init__(
        self,
        dag: Dag,
        meta: LocalMetadata | None = None,
        executor: BaseExecutor | None = None,
        max_concurrent: int = 10,
        variables: dict[str, Any] | None = None,
        bundle_root: Any = None,
    ) -> None:
        self.dag = dag
        self.meta = meta or self._tempdir_meta()
        self.executor = executor or LocalExecutor()
        self.max_concurrent = max_concurrent
        self.variables = variables or {}
        # Bundle context used by plugins to resolve relative asset paths
        # (e.g. ``py_statement: transform.py``). Falls back to the dag's loader-set
        # ``_bundle_root`` so ``Dag.run()`` works without explicit plumbing.
        self.bundle_root = bundle_root or getattr(dag, "_bundle_root", None)

    @staticmethod
    def _tempdir_meta() -> LocalMetadata:
        import tempfile

        return LocalMetadata(tempfile.mkdtemp(prefix="beacon_sched_"))

    # --- public API ---

    async def run(
        self,
        *,
        variables: dict[str, Any] | None = None,
        run_id: str | None = None,
        logical_date: datetime | None = None,
        dag_version: str = "local",
        resume: bool = False,
    ) -> DagRunResult:
        """Execute the DAG end-to-end. See :meth:`_run_impl` for details.

        This wrapper pushes a :class:`~beacon.core.assets.BundleContext`
        so plugins can resolve relative asset paths (e.g.
        ``py_statement: transform.py``) before the impl body runs.
        """
        from .core.assets import (
            BundleContext,
            reset_bundle_context,
            set_bundle_context,
        )

        token = set_bundle_context(
            BundleContext(
                bundle_root=self.bundle_root,
                dag_source_file=getattr(self.dag, "_source_file", None),
            )
        )
        try:
            return await self._run_impl(
                variables=variables,
                run_id=run_id,
                logical_date=logical_date,
                dag_version=dag_version,
                resume=resume,
            )
        finally:
            reset_bundle_context(token)

    async def _run_impl(
        self,
        *,
        variables: dict[str, Any] | None = None,
        run_id: str | None = None,
        logical_date: datetime | None = None,
        dag_version: str = "local",
        resume: bool = False,
    ) -> DagRunResult:
        """Execute the DAG end-to-end. Returns a :class:`DagRunResult`.

        Args:
            variables: Run-time variable overrides (merged with DagRunner.variables).
            run_id: Reuse a run_id to resume. Required if ``resume=True``.
            logical_date: Initial logical date (ignored on resume).
            dag_version: Stamp on the DagRun + TaskContexts.
            resume: Continue an existing run. Tasks already in a terminal
                state are left alone; tasks in ``NONE`` state (typically
                because they were cleared via :meth:`clear`) are executed.
                Use this with :meth:`clear` for backfill / re-run flows.
        """
        run_id = run_id or f"manual-{self.dag.id}-{uuid.uuid4().hex[:8]}"
        # Merge runner's variables with run-time overrides
        effective_variables = {**self.variables, **(variables or {})}
        now = logical_date or datetime.now()
        graph = build_graph(self.dag.actions)
        result = DagRunResult(run_id=run_id, dag_id=self.dag.id)

        if resume:
            existing = await self.meta.get_dag_run(run_id, self.dag.id)
            if existing is None:
                raise ValueError(
                    f"Cannot resume: no DagRun {run_id!r} for "
                    f"{self.dag.id!r}. Did you forget to clear first?"
                )
            # Use the ORIGINAL logical_date for determinism.
            persisted_logical = existing.get("logical_date")
            if persisted_logical:
                try:
                    now = datetime.fromisoformat(str(persisted_logical))
                except ValueError:
                    pass  # leave `now` as the caller's argument
            # Re-open the run so terminal callbacks fire correctly.
            await self.meta.update_dag_run_state(run_id, self.dag.id, "running")
            # Seed local_states from metadata so terminal tasks are preserved.
            persisted_states = await self.meta.get_all_task_states(
                run_id, self.dag.id
            )
            local_states: dict[str, TaskState] = {
                tid: persisted_states.get(tid, TaskState.NONE)
                for tid in graph.task_map
            }
            # Re-hydrate result.outputs from terminal tasks so downstream
            # consumers (and the returned DagRunResult) see prior outputs.
            for tid, state in local_states.items():
                if state in TERMINAL_STATES:
                    result.states[tid] = state
                    outputs = await self.meta.get_task_outputs(
                        run_id, self.dag.id, tid
                    )
                    if outputs:
                        result.outputs[tid] = outputs
        else:
            await self.meta.create_dag_run(
                run_id=run_id,
                dag_id=self.dag.id,
                dag_version=dag_version,
                state="running",
                logical_date=now,
                variables=effective_variables,
            )
            local_states = {tid: TaskState.NONE for tid in graph.task_map}

        # Inputs decided by branch/short-circuit cascade.
        forced_skip: set[str] = set()

        await self._fire_dag_callbacks("start", result, dag_version)

        worker = Worker(
            self.meta,
            executor=self.executor,
            max_concurrent=self.max_concurrent,
        )

        # An event we use to wake the planning loop when a task completes.
        wake = asyncio.Event()
        # In-flight tracking.
        in_flight: set[str] = set()

        async def on_task_terminal(task_ctx: TaskContext, state: TaskState):
            local_states[task_ctx.task_id] = state
            result.states[task_ctx.task_id] = state
            if state == TaskState.SUCCESS:
                action = graph.task_map[task_ctx.task_id]
                # Let the action normalize the raw plugin output into final
                # structured outputs (e.g. branch list, continue flag) before
                # evaluate_downstream reads from task_ctx.outputs.
                task_ctx.outputs = action.extract_outputs(task_ctx.outputs)
                all_down = graph.downstream.get(task_ctx.task_id, [])
                directive = action.evaluate_downstream(task_ctx, all_down)
                self._apply_directive(directive, local_states, forced_skip)
            if task_ctx.outputs:
                result.outputs[task_ctx.task_id] = task_ctx.outputs
            in_flight.discard(task_ctx.task_id)
            wake.set()

        worker_task = asyncio.create_task(worker.run())

        try:
            await self._main_loop(
                graph=graph,
                worker=worker,
                local_states=local_states,
                forced_skip=forced_skip,
                in_flight=in_flight,
                result=result,
                variables=effective_variables,
                run_id=run_id,
                dag_version=dag_version,
                now=now,
                on_terminal=on_task_terminal,
                wake=wake,
                teardown_phase=False,
            )

            # Teardown phase
            await self._main_loop(
                graph=graph,
                worker=worker,
                local_states=local_states,
                forced_skip=forced_skip,
                in_flight=in_flight,
                result=result,
                variables=effective_variables,
                run_id=run_id,
                dag_version=dag_version,
                now=now,
                on_terminal=on_task_terminal,
                wake=wake,
                teardown_phase=True,
            )
        finally:
            await worker.shutdown()
            await worker_task

        # Compute DAG state ignoring teardown outcomes.
        result.state = self._compute_dag_state(graph, local_states)
        await self.meta.update_dag_run_state(run_id, self.dag.id, result.state)
        # Map terminal state -> callback event name.
        event_name = "success" if result.state == "success" else "failure"
        await self._fire_dag_callbacks(event_name, result, dag_version)
        await self._fire_dag_callbacks("finished", result, dag_version)
        self.meta.evict_run_from_cache(run_id)
        return result

    # --- main loop ---

    async def _main_loop(
        self,
        *,
        graph: Graph,
        worker: Worker,
        local_states: dict[str, TaskState],
        forced_skip: set[str],
        in_flight: set[str],
        result: DagRunResult,
        variables: dict[str, Any],
        run_id: str,
        dag_version: str,
        now: datetime,
        on_terminal: Any,
        wake: asyncio.Event,
        teardown_phase: bool,
    ) -> None:
        """Drive scheduling until no more progress is possible in this phase."""
        candidate_ids = (
            graph.teardown_ids if teardown_phase else graph.normal_ids
        )

        while True:
            progressed = await self._enqueue_ready(
                graph=graph,
                worker=worker,
                candidate_ids=candidate_ids,
                local_states=local_states,
                forced_skip=forced_skip,
                in_flight=in_flight,
                result=result,
                variables=variables,
                run_id=run_id,
                dag_version=dag_version,
                now=now,
                on_terminal=on_terminal,
                teardown_phase=teardown_phase,
            )

            if not in_flight:
                # No work in progress. If we couldn't enqueue anything new,
                # this phase is done.
                if not progressed:
                    return
                continue

            wake.clear()
            await wake.wait()

    async def _enqueue_ready(
        self,
        *,
        graph: Graph,
        worker: Worker,
        candidate_ids: set[str],
        local_states: dict[str, TaskState],
        forced_skip: set[str],
        in_flight: set[str],
        result: DagRunResult,
        variables: dict[str, Any],
        run_id: str,
        dag_version: str,
        now: datetime,
        on_terminal: Any,
        teardown_phase: bool,
    ) -> bool:
        """Iterate once over candidates; enqueue, skip, or fail as warranted.

        Returns True if any state transitioned (incl. skip/upstream_failed),
        which means we should re-check candidates immediately.
        """
        progressed = False
        for tid in sorted(candidate_ids):
            if local_states[tid] != TaskState.NONE:
                continue
            if tid in in_flight:
                continue

            action = graph.task_map[tid]
            if teardown_phase:
                deps = list(graph.teardown_deps[tid])
                trigger = TriggerRule.ALL_DONE
            else:
                deps = list(action.upstream)
                trigger = TriggerRule(action.trigger_rule)

            # Force-skip from branch / short-circuit directive
            if tid in forced_skip and not teardown_phase:
                await self._mark_terminal(
                    tid, TaskState.SKIPPED, local_states, result, run_id
                )
                progressed = True
                continue

            dep_states = [local_states[d] for d in deps if d in local_states]
            if any(s not in TERMINAL_STATES for s in dep_states):
                continue
            if len(dep_states) != len(deps):
                # An upstream is missing from local_states — treat as not ready.
                continue

            satisfied = evaluate_trigger_rule(trigger, dep_states)
            if not satisfied:
                has_failed = any(
                    s in (TaskState.FAILED, TaskState.UPSTREAM_FAILED)
                    for s in dep_states
                )
                terminal = (
                    TaskState.UPSTREAM_FAILED
                    if has_failed
                    else TaskState.SKIPPED
                )
                await self._mark_terminal(
                    tid, terminal, local_states, result, run_id
                )
                progressed = True
                continue

            # Ready to run
            in_flight.add(tid)
            await self._enqueue(
                tid=tid,
                action=action,
                worker=worker,
                graph=graph,
                local_states=local_states,
                variables=variables,
                run_id=run_id,
                dag_version=dag_version,
                now=now,
                on_terminal=on_terminal,
                teardown_phase=teardown_phase,
            )
            progressed = True
        return progressed

    async def _enqueue(
        self,
        *,
        tid: str,
        action: BaseAction,
        worker: Worker,
        graph: Graph,
        local_states: dict[str, TaskState],
        variables: dict[str, Any],
        run_id: str,
        dag_version: str,
        now: datetime,
        on_terminal: Any,
        teardown_phase: bool,
    ) -> None:
        """Build TaskContext and submit to the worker.

        Rendering happens here so that ``vars()`` / ``secrets()`` / ``runtime``
        are resolved at enqueue time. Upstream outputs are resolved by the
        worker right before execution.
        """
        from .core.renderer import make_vars_func, make_secrets_func

        merged_inputs = {**self.dag.default_inputs, **action.inputs}

        # Trigger-time render: bind vars, secrets, runtime. Upstream outputs
        # are bound to ``{}`` here; the worker fills them after dep lookup.
        vars_func = make_vars_func(variables)
        secrets_func = make_secrets_func()

        renderer = Renderer(
            {
                "vars": vars_func,
                "secrets": secrets_func,
                "runtime": build_runtime_dict(
                    run_id=run_id,
                    dag_id=self.dag.id,
                    task_id=tid,
                    run_date=now,
                    logical_date=now,
                    data_interval_start=now,
                    data_interval_end=now,
                    attempt_number=1,
                ),
                "outputs": {},
            }
        )
        try:
            rendered_inputs = renderer.render(merged_inputs)
        except Exception as exc:  # noqa: BLE001
            # Expected when inputs contain {{ outputs.X.Y }} — these are
            # resolved in the worker's second-pass render after upstream
            # outputs become available. Not an error.
            logger.debug(
                "First-pass render deferred for %s/%s: %s",
                self.dag.id,
                tid,
                exc,
            )
            rendered_inputs = merged_inputs

        task_ctx = action.build_task_context(
            run_id=run_id,
            dag_id=self.dag.id,
            dag_version=dag_version,
            run_date=now,
            logical_date=now,
            data_interval_start=now,
            data_interval_end=now,
            variables=variables,
            rendered_inputs=rendered_inputs,
        )
        # For teardowns we expose the setup task's outputs even though it's
        # not in action.upstream.
        upstream_ids = list(action.upstream)
        if teardown_phase and tid in graph.teardown_setup:
            setup_id = graph.teardown_setup[tid]
            if setup_id not in upstream_ids:
                upstream_ids.append(setup_id)

        await worker.submit(
            task_ctx,
            callbacks=list(action.callbacks),
            upstream_task_ids=upstream_ids,
            on_terminal=on_terminal,
        )

    async def _mark_terminal(
        self,
        tid: str,
        state: TaskState,
        local_states: dict[str, TaskState],
        result: DagRunResult,
        run_id: str,
    ) -> None:
        """Mark a non-executed task as terminal in metadata + local view."""
        local_states[tid] = state
        result.states[tid] = state
        await self.meta.set_task_state(run_id, self.dag.id, tid, state)
        logger.info("Task %s/%s → %s (not executed)", self.dag.id, tid, state)

    @staticmethod
    def _apply_directive(
        directive: DownstreamDirective,
        local_states: dict[str, TaskState],
        forced_skip: set[str],
    ) -> None:
        for tid in directive.skip:
            if tid in local_states and local_states[tid] == TaskState.NONE:
                forced_skip.add(tid)

    def _compute_dag_state(
        self,
        graph: Graph,
        local_states: dict[str, TaskState],
    ) -> str:
        """Compute DagRun terminal state ignoring teardown outcomes."""
        for tid in graph.normal_ids:
            s = local_states.get(tid, TaskState.NONE)
            if s in (TaskState.FAILED, TaskState.UPSTREAM_FAILED):
                return "failed"
        return "success"

    async def _fire_dag_callbacks(
        self,
        event: str,
        result: DagRunResult,
        dag_version: str,
    ) -> None:
        """Fire DAG-level callbacks matching ``event``."""
        callbacks = [
            cb
            for cb in self.dag.callbacks
            if isinstance(cb, OnDagEvent) and cb.on_event == event
        ]
        if not callbacks:
            return
        data: dict[str, Any] = {
            "dag_id": self.dag.id,
            "run_id": result.run_id,
            "dag_version": dag_version,
            "state": result.state if event != "start" else "running",
            "task_states": {tid: str(s) for tid, s in result.states.items()},
        }
        for cb in callbacks:
            try:
                await cb.notify(data, event)
            except Exception as exc:  # noqa: BLE001
                logger.error("DAG callback error on %s: %s", event, exc)

    # --- clear / backfill API -------------------------------------------

    async def clear(
        self,
        *,
        run_id: str,
        task_ids: str | list[str],
        downstream: bool = False,
    ) -> list[str]:
        """Clear one or more tasks in an existing DagRun so they can re-run.

        Resets each cleared task's state to ``NONE`` and wipes its
        ``attempts`` + ``outputs`` in the metadata store. Upstream
        outputs are untouched — when re-executed, the task reads the
        same upstream values it would on a fresh run.

        Args:
            run_id: The existing DagRun to operate on.
            task_ids: Single task id or list of task ids to clear.
            downstream: When ``True``, also clear every task transitively
                downstream of each ``task_ids`` entry. Required when the
                cleared task's outputs feed downstream tasks that have
                already succeeded — otherwise the downstream would not
                re-read the new outputs.

        Returns:
            The full list of task ids that were cleared (request +
            downstream expansion), in topological order.

        Example::

            runner = DagRunner(dag, meta=meta)
            await runner.clear(run_id="run-abc", task_ids="task2",
                               downstream=True)
            await runner.run(run_id="run-abc", resume=True)
        """
        if isinstance(task_ids, str):
            task_ids = [task_ids]

        graph = build_graph(self.dag.actions)
        for tid in task_ids:
            if tid not in graph.task_map:
                raise ValueError(
                    f"Task {tid!r} not found in DAG {self.dag.id!r}"
                )

        to_clear: list[str] = []
        seen: set[str] = set()
        for tid in task_ids:
            for resolved in collect_self_and_downstream(
                graph, tid, include_downstream=downstream
            ):
                if resolved not in seen:
                    seen.add(resolved)
                    to_clear.append(resolved)

        # Auto-include any teardown whose dependency set was disturbed.
        # Rationale: a teardown exists to clean up a resource created by
        # its setup. If we re-run any task that touches the resource,
        # the cleanup must re-fire too — otherwise the original teardown's
        # side-effects (e.g. "spark app stopped") are stale, and the
        # re-execution either fails or leaks the new resource.
        for teardown_id, deps in graph.teardown_deps.items():
            if teardown_id in seen:
                continue
            if deps & seen:
                seen.add(teardown_id)
                to_clear.append(teardown_id)

        for tid in to_clear:
            await self.meta.clear_task(run_id, self.dag.id, tid)
        logger.info(
            "Cleared %d task(s) in run %s/%s: %s",
            len(to_clear),
            self.dag.id,
            run_id,
            to_clear,
        )
        return to_clear

    async def fail(
        self,
        *,
        run_id: str,
        task_ids: str | list[str],
    ) -> dict[str, list[str]]:
        """Force-fail task(s). Shorthand for ``mark(state=TaskState.FAILED)``."""
        return await self.mark(
            run_id=run_id, task_ids=task_ids, state=TaskState.FAILED
        )

    async def mark(
        self,
        *,
        run_id: str,
        task_ids: str | list[str],
        state: TaskState,
    ) -> dict[str, list[str]]:
        """Force task(s) to a terminal state and re-fire affected teardowns.

        Works for ``FAILED``, ``SUCCESS``, or ``SKIPPED``.

        Returns:
            ``{"marked": [...], "teardowns_cleared": [...]}``.
        """
        if isinstance(task_ids, str):
            task_ids = [task_ids]

        graph = build_graph(self.dag.actions)
        for tid in task_ids:
            if tid not in graph.task_map:
                raise ValueError(
                    f"Task {tid!r} not found in DAG {self.dag.id!r}"
                )

        # 1. Set state for each task.
        for tid in task_ids:
            await self.meta.set_task_state(run_id, self.dag.id, tid, state)

        # 2. Auto-clear teardowns whose dep set intersects the marked tasks.
        # Skip any teardown that was itself explicitly marked (don't undo it).
        marked_set = set(task_ids)
        teardowns_cleared: list[str] = []
        for teardown_id, deps in graph.teardown_deps.items():
            if teardown_id in marked_set:
                continue
            if deps & marked_set:
                await self.meta.clear_task(run_id, self.dag.id, teardown_id)
                teardowns_cleared.append(teardown_id)

        logger.info(
            "Marked %s as %s in run %s/%s; teardowns cleared: %s",
            task_ids,
            state.value,
            self.dag.id,
            run_id,
            teardowns_cleared,
        )
        return {"marked": task_ids, "teardowns_cleared": teardowns_cleared}


# --- run_id trigger convention ---------------------------------------------
#
# By convention, run_id encodes how the run was triggered:
#
#   manual-{dag_id}-{uuid}         → Dag.run() / DagRunner.run() invocation
#   backfill-{dag_id}-{timestamp}  → Dag.backfill()
#   scheduled-{dag_id}-{timestamp} → Phase 2 DeploymentScheduler (cron)
#
# Anything else falls back to "unknown". The convention is enforced by the
# generators in beacon.models.dag.Dag and DagRunner.run; downstream tools
# (CLI listings, API filters, the future UI) read it via run_trigger().

_TRIGGER_PREFIXES = ("manual", "backfill", "scheduled")


def run_trigger(run_id: str) -> str:
    """Return the trigger type encoded in a ``run_id``.

    >>> run_trigger("manual-etl-a1b2c3d4")
    'manual'
    >>> run_trigger("backfill-etl-20260101T000000")
    'backfill'
    >>> run_trigger("legacy-id")
    'unknown'
    """
    head = run_id.split("-", 1)[0]
    return head if head in _TRIGGER_PREFIXES else "unknown"
