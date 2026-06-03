"""LocalScheduler.

In-process async DAG orchestrator. Owns graph traversal, trigger-rule
evaluation, branch / short-circuit propagation, ``UPSTREAM_FAILED`` /
``SKIPPED`` cascading, teardown scheduling, and DAG-level callback firing.

The scheduler delegates per-task execution to :class:`beacon.worker.Worker`
via the ``on_terminal`` hook, so the worker only knows how to run one task
at a time and the scheduler owns the topology.

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
from .core.executor import BaseExecutor, LocalExecutor
from .core.state import TERMINAL_STATES, TaskState
from .core.task_context import TaskContext
from .core.trigger_rule import TriggerRule, evaluate_trigger_rule
from .metadata.json_store import JsonMetadata
from .worker import Worker

if TYPE_CHECKING:
    from .models.dag import Dag

logger = logging.getLogger("beacon.scheduler")


# ---------- helpers --------------------------------------------------------


def _flatten_actions(actions: list[Any], out: dict[str, BaseAction]) -> None:
    """Flatten Groups; collect leaf actions into ``out`` keyed by id."""
    for action in actions:
        if hasattr(action, "actions") and action.actions:
            _flatten_actions(action.actions, out)
        else:
            out[action.id] = action


@dataclass
class _Graph:
    """Pre-computed DAG topology."""

    task_map: dict[str, BaseAction]
    downstream: dict[str, list[str]]
    teardown_for_setup: dict[str, str]
    """setup_id → teardown_id"""
    teardown_setup: dict[str, str]
    """teardown_id → setup_id"""
    teardown_deps: dict[str, set[str]]
    """teardown_id → {setup_id + all transitive non-teardown dependents}"""

    @property
    def teardown_ids(self) -> set[str]:
        return set(self.teardown_setup)

    @property
    def normal_ids(self) -> set[str]:
        return set(self.task_map) - self.teardown_ids


def _build_graph(dag: Dag) -> _Graph:
    task_map: dict[str, BaseAction] = {}
    _flatten_actions(dag.actions, task_map)

    downstream: dict[str, list[str]] = {tid: [] for tid in task_map}
    for tid, action in task_map.items():
        for up in action.upstream:
            if up in downstream:
                downstream[up].append(tid)

    teardown_for_setup: dict[str, str] = {}
    teardown_setup: dict[str, str] = {}
    for tid, action in task_map.items():
        td = getattr(action, "teardown", None)
        if td:
            teardown_for_setup[td] = tid
            teardown_setup[tid] = td

    teardown_ids = set(teardown_setup)

    def transitive_dependents(setup_id: str) -> set[str]:
        seen: set[str] = set()
        stack = [setup_id]
        while stack:
            n = stack.pop()
            for d in downstream.get(n, []):
                if d in teardown_ids or d in seen:
                    continue
                seen.add(d)
                stack.append(d)
        return seen

    teardown_deps: dict[str, set[str]] = {
        tdid: {setup_id} | transitive_dependents(setup_id)
        for setup_id, tdid in teardown_for_setup.items()
    }

    return _Graph(
        task_map=task_map,
        downstream=downstream,
        teardown_for_setup=teardown_for_setup,
        teardown_setup=teardown_setup,
        teardown_deps=teardown_deps,
    )


# ---------- result ---------------------------------------------------------


@dataclass
class DagRunResult:
    """Outcome of a single :meth:`LocalScheduler.run` invocation."""

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


# ---------- scheduler ------------------------------------------------------


class LocalScheduler:
    """In-process async DAG runner.

    Args:
        dag: The :class:`Dag` to execute.
        meta: Metadata store. A fresh tempdir-backed
            :class:`JsonMetadata` is created when ``None``.
        executor: Per-task executor. Defaults to :class:`LocalExecutor`.
        max_concurrent: Max concurrent in-flight tasks.
    """

    def __init__(
        self,
        dag: Dag,
        meta: JsonMetadata | None = None,
        executor: BaseExecutor | None = None,
        max_concurrent: int = 10,
    ) -> None:
        self.dag = dag
        self.meta = meta or self._tempdir_meta()
        self.executor = executor or LocalExecutor()
        self.max_concurrent = max_concurrent

    @staticmethod
    def _tempdir_meta() -> JsonMetadata:
        import tempfile

        return JsonMetadata(tempfile.mkdtemp(prefix="beacon_sched_"))

    # --- public API ---

    async def run(
        self,
        *,
        params: dict[str, Any] | None = None,
        run_id: str | None = None,
        logical_date: datetime | None = None,
        dag_version: str = "local",
    ) -> DagRunResult:
        """Execute the DAG end-to-end. Returns a :class:`DagRunResult`."""
        run_id = run_id or f"run-{uuid.uuid4().hex[:8]}"
        params = params or {}
        now = logical_date or datetime.now()
        graph = _build_graph(self.dag)
        result = DagRunResult(run_id=run_id, dag_id=self.dag.id)

        await self.meta.create_dag_run(
            run_id=run_id,
            dag_id=self.dag.id,
            dag_version=dag_version,
            state="running",
            logical_date=now,
            params=params,
        )

        # All tasks start NONE in the local view.
        local_states: dict[str, TaskState] = {
            tid: TaskState.NONE for tid in graph.task_map
        }
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
            if task_ctx.outputs:
                result.outputs[task_ctx.task_id] = task_ctx.outputs
            # Apply branch / short-circuit directive on success.
            if state == TaskState.SUCCESS:
                action = graph.task_map[task_ctx.task_id]
                all_down = graph.downstream.get(task_ctx.task_id, [])
                directive = action.evaluate_downstream(task_ctx, all_down)
                self._apply_directive(directive, local_states, forced_skip)
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
                params=params,
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
                params=params,
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
        graph: _Graph,
        worker: Worker,
        local_states: dict[str, TaskState],
        forced_skip: set[str],
        in_flight: set[str],
        result: DagRunResult,
        params: dict[str, Any],
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
                params=params,
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
        graph: _Graph,
        worker: Worker,
        candidate_ids: set[str],
        local_states: dict[str, TaskState],
        forced_skip: set[str],
        in_flight: set[str],
        result: DagRunResult,
        params: dict[str, Any],
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
                params=params,
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
        graph: _Graph,
        local_states: dict[str, TaskState],
        params: dict[str, Any],
        run_id: str,
        dag_version: str,
        now: datetime,
        on_terminal: Any,
        teardown_phase: bool,
    ) -> None:
        """Build TaskContext and submit to the worker."""
        merged_inputs = {**self.dag.default_inputs, **action.inputs}
        task_ctx = action.build_task_context(
            run_id=run_id,
            dag_id=self.dag.id,
            dag_version=dag_version,
            run_date=now,
            logical_date=now,
            data_interval_start=now,
            data_interval_end=now,
            params=params,
            rendered_inputs=merged_inputs,
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
        graph: _Graph,
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
