"""DAG Plan — pre-execution validation and template rendering.

Shows exactly what Beacon will do before you deploy: resolves every
Jinja template against real params / variables / logical_date so you can
catch misconfigured inputs, missing plugins, and graph errors before a
single task runs.

Usage:
    from beacon.plan import plan

    result = plan(
        dag=dag,
        params={"source_system": "orders"},
        variables={"bucket": "prod-bucket"},
        logical_date=datetime(2026, 6, 3, 2, 0, 0),
        cron="0 2 * * *",            # optional — computes data intervals
    )
    print(result)         # pretty-printed report
    result.is_valid       # True if no errors
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .core.context import build_runtime_dict
from .core.plugin import PLUGINS_REGISTRY
from .models.dag import Dag

logger = logging.getLogger("beacon.plan")


@dataclass
class PlanIssue:
    """A single validation issue found during planning."""

    task_id: str
    category: str  # "plugin" | "graph" | "template"
    message: str


@dataclass
class PlannedTask:
    """A task after plan-time resolution — shows what inputs the plugin will receive."""

    task_id: str
    type: str
    plugin_name: str
    inputs: dict[str, Any]
    upstream: list[str]


@dataclass
class PlanResult:
    """Result of a DAG plan."""

    dag_id: str
    errors: list[PlanIssue] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    planned_tasks: list[PlannedTask] = field(default_factory=list)
    task_order: list[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        """True if no errors found."""
        return len(self.errors) == 0

    def __str__(self) -> str:
        """Return a formatted plan report."""
        lines = [f"=== Plan: {self.dag_id} ===", ""]

        if self.errors:
            lines.append(f"❌ ERRORS ({len(self.errors)}):")
            for e in self.errors:
                lines.append(f"  [{e.category}] {e.task_id}: {e.message}")
            lines.append("")

        if self.warnings:
            lines.append(f"⚠️  WARNINGS ({len(self.warnings)}):")
            for w in self.warnings:
                lines.append(f"  {w}")
            lines.append("")

        if self.planned_tasks:
            lines.append(f"✅ TASKS ({len(self.planned_tasks)}):")
            lines.append(f"   Execution order: {' → '.join(self.task_order)}")
            lines.append("")
            for t in self.planned_tasks:
                lines.append(f"  [{t.type}] {t.task_id}")
                lines.append(f"    plugin: {t.plugin_name}")
                if t.upstream:
                    lines.append(f"    upstream: {t.upstream}")
                if t.inputs:
                    for k, v in t.inputs.items():
                        val_str = (
                            repr(v)
                            if len(repr(v)) < 60
                            else repr(v)[:57] + "..."
                        )
                        lines.append(f"    {k}: {val_str}")
                lines.append("")

        status = "PASS ✅" if self.is_valid else "FAIL ❌"
        lines.append(f"Result: {status}")
        return "\n".join(lines)

    def print(self) -> None:
        """Print the plan report to stdout."""
        print(str(self))


def plan(
    dag: Dag,
    *,
    params: dict[str, Any] | None = None,
    variables: dict[str, Any] | None = None,
    logical_date: datetime | None = None,
    data_interval_start: datetime | None = None,
    data_interval_end: datetime | None = None,
    cron: str | None = None,
) -> PlanResult:
    """Validate and plan a DAG definition — shows resolved inputs before execution.

    Checks:
      1. All plugins exist in registry
      2. DAG graph is acyclic (no cycles)
      3. All upstream and teardown references exist
      4. Jinja template rendering with provided params / variables / dates

    Args:
        dag: The DAG model to validate.
        params: Runtime parameters to simulate (merged over DAG-level defaults).
        variables: Variables (from variables.yml) to simulate.
        logical_date: Simulated logical_date for ``runtime.*`` rendering.
            Defaults to ``datetime.now()``.
        data_interval_start: Explicit data interval start. When provided,
            takes precedence over ``cron``-derived value.
        data_interval_end: Explicit data interval end. When provided,
            takes precedence over ``cron``-derived value.
        cron: Cron expression used to compute ``data_interval_start`` and
            ``data_interval_end`` from ``logical_date``.  Ignored when
            ``data_interval_start`` / ``data_interval_end`` are supplied
            directly.  If neither ``cron`` nor explicit intervals are given,
            both default to ``logical_date``.

    Returns:
        PlanResult with errors, warnings, and per-task resolved inputs.
    """
    params = params or {}
    variables = variables or {}
    logical_date = logical_date or datetime.now()
    result = PlanResult(dag_id=dag.id)

    # Resolve data intervals — explicit args beat cron-computed, cron beats default.
    if data_interval_start is not None and data_interval_end is not None:
        dis, die = data_interval_start, data_interval_end
    else:
        dis, die = _compute_data_interval(cron, logical_date)
        if data_interval_start is not None:
            dis = data_interval_start
        if data_interval_end is not None:
            die = data_interval_end

    # Merge DAG-level param defaults, then caller-supplied params win.
    effective_params: dict[str, Any] = {p.name: p.default for p in dag.params}
    effective_params.update(params)

    # Build task map
    task_map: dict[str, Any] = {}
    _flatten_actions(dag.actions, task_map)

    # --- Check 1: Plugin existence ---
    for task_id, action in task_map.items():
        if isinstance(action.uses, str) and action.uses not in PLUGINS_REGISTRY:
            result.errors.append(
                PlanIssue(
                    task_id=task_id,
                    category="plugin",
                    message=f"Plugin {action.uses!r} not found in registry.",
                )
            )

    # --- Check 2: Upstream references exist ---
    all_ids = set(task_map.keys())
    for task_id, action in task_map.items():
        for up in action.upstream:
            if up not in all_ids:
                result.errors.append(
                    PlanIssue(
                        task_id=task_id,
                        category="graph",
                        message=f"Upstream {up!r} does not exist in DAG.",
                    )
                )

    # --- Check 2b: Teardown references exist ---
    for task_id, action in task_map.items():
        teardown_ref = getattr(action, "teardown", None)
        if teardown_ref and teardown_ref not in all_ids:
            result.errors.append(
                PlanIssue(
                    task_id=task_id,
                    category="graph",
                    message=(
                        f"Teardown references task {teardown_ref!r} "
                        f"which does not exist in DAG."
                    ),
                )
            )
        if teardown_ref and teardown_ref == task_id:
            result.errors.append(
                PlanIssue(
                    task_id=task_id,
                    category="graph",
                    message="A task cannot be a teardown for itself.",
                )
            )

    # --- Check 3: Cycle detection ---
    cycle = _detect_cycle(task_map)
    if cycle:
        result.errors.append(
            PlanIssue(
                task_id=cycle[0],
                category="graph",
                message=f"Cycle detected: {' → '.join(cycle)}",
            )
        )

    # --- Check 4: Topological sort (execution order) ---
    if not cycle:
        result.task_order = _topological_sort(task_map)

    # --- Check 5: Resolve inputs with Jinja (best-effort) ---
    for task_id, action in task_map.items():
        plugin_name = (
            action.uses
            if isinstance(action.uses, str)
            else getattr(action.uses, "plugin_name", "?")
        )
        rendered_inputs, warnings = _render_inputs(
            action.inputs,
            effective_params,
            variables,
            logical_date,
            dis,
            die,
            dag.id,
            task_id,
        )
        result.warnings.extend(warnings)
        result.planned_tasks.append(
            PlannedTask(
                task_id=task_id,
                type=getattr(action, "type", "task"),
                plugin_name=plugin_name,
                inputs=rendered_inputs,
                upstream=list(action.upstream),
            )
        )

    return result


def _flatten_actions(actions: list, task_map: dict[str, Any]) -> None:
    """Flatten nested groups into a flat task map."""
    for action in actions:
        if hasattr(action, "actions") and action.actions:
            _flatten_actions(action.actions, task_map)
        else:
            task_map[action.id] = action


def _detect_cycle(task_map: dict[str, Any]) -> list[str] | None:
    """Detect cycle in DAG using DFS. Returns cycle path or None."""
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {tid: WHITE for tid in task_map}
    path: list[str] = []

    def dfs(node: str) -> list[str] | None:
        color[node] = GRAY
        path.append(node)
        for up in task_map[node].upstream:
            if up not in color:
                continue
            if color[up] == GRAY:
                idx = path.index(up)
                return path[idx:] + [up]
            if color[up] == WHITE:
                result = dfs(up)
                if result:
                    return result
        path.pop()
        color[node] = BLACK
        return None

    for tid in task_map:
        if color[tid] == WHITE:
            cycle = dfs(tid)
            if cycle:
                return cycle
    return None


def _topological_sort(task_map: dict[str, Any]) -> list[str]:
    """Compute execution order via topological sort (Kahn's algorithm).

    Uses a min-heap so ties resolve by lexical order.
    """
    import heapq

    in_degree = {tid: 0 for tid in task_map}
    downstream: dict[str, list[str]] = {tid: [] for tid in task_map}

    for tid, action in task_map.items():
        for up in action.upstream:
            if up in task_map:
                in_degree[tid] += 1
                downstream[up].append(tid)

    heap: list[str] = [tid for tid, deg in in_degree.items() if deg == 0]
    heapq.heapify(heap)
    order: list[str] = []

    while heap:
        node = heapq.heappop(heap)
        order.append(node)
        for child in downstream[node]:
            in_degree[child] -= 1
            if in_degree[child] == 0:
                heapq.heappush(heap, child)

    return order


def _compute_data_interval(
    cron: str | None, logical_date: datetime
) -> tuple[datetime, datetime]:
    """Compute data_interval_start and data_interval_end from cron.

    If cron is provided, uses croniter to find the previous and next
    boundaries. If not, both default to logical_date.
    """
    if cron is None:
        return logical_date, logical_date

    from croniter import croniter

    cron_iter = croniter(cron, logical_date)
    data_interval_end = cron_iter.get_next(datetime)
    return logical_date, data_interval_end


def _render_inputs(
    inputs: dict,
    params: dict,
    variables: dict,
    logical_date: datetime,
    data_interval_start: datetime,
    data_interval_end: datetime,
    dag_id: str,
    task_id: str,
) -> tuple[dict[str, Any], list[str]]:
    """Render task inputs with the Renderer. Returns (rendered_inputs, warnings).

    Unknown ``vars()`` keys resolve to a sentinel string so plan output
    is shown even when stage variables are incomplete. Other undefined
    names are collected as warnings (likely a typo).
    """
    from .core.renderer import Renderer

    def vars_func(name: str) -> str:
        return variables.get(name, f"<unresolved: vars('{name}')>")

    ctx = {
        "params": params,
        "vars": vars_func,
        "outputs": {},
        "runtime": build_runtime_dict(
            run_id=f"plan-{dag_id}",
            dag_id=dag_id,
            task_id=task_id,
            run_date=logical_date,
            logical_date=logical_date,
            data_interval_start=data_interval_start,
            data_interval_end=data_interval_end,
            attempt_number=1,
        ),
    }
    renderer = Renderer(ctx)
    rendered: dict[str, Any] = {}
    warnings: list[str] = []

    for key, value in inputs.items():
        try:
            rendered[key] = renderer.render(value)
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"[{task_id}] Failed to render '{key}': {exc}")
            rendered[key] = value

    return rendered, warnings
