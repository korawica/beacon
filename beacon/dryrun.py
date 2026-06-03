"""DAG Dry Run and Validation.

Validates a DAG definition before deployment:
  - Plugin existence in registry
  - Plugin ↔ action type compatibility
  - DAG graph structure (cycles, missing upstreams)
  - Jinja template rendering with provided params/variables
  - Output summary of resolved inputs per task

Usage:
    from beacon.dryrun import dryrun

    result = dryrun(
        dag=dag,
        params={"source_system": "orders"},
        variables={"bucket": "prod-bucket"},
        logical_date=datetime(2026, 6, 3, 2, 0, 0),
    )
    result.print()       # Pretty print results
    result.is_valid      # True if no errors
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .core.plugin import PLUGINS_REGISTRY
from .models.dag import Dag

logger = logging.getLogger("beacon.dryrun")


@dataclass
class ValidationError:
    """A single validation error."""

    task_id: str
    category: str  # "plugin", "compatibility", "graph", "template"
    message: str


@dataclass
class ResolvedTask:
    """A task after dry-run resolution."""

    task_id: str
    type: str
    plugin_name: str
    inputs: dict[str, Any]
    upstream: list[str]


@dataclass
class DryRunResult:
    """Result of a DAG dry run."""

    dag_id: str
    errors: list[ValidationError] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    resolved_tasks: list[ResolvedTask] = field(default_factory=list)
    task_order: list[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        """True if no errors found."""
        return len(self.errors) == 0

    def print(self) -> str:
        """Return formatted dry-run report."""
        lines = [f"=== DryRun: {self.dag_id} ===", ""]

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

        if self.resolved_tasks:
            lines.append(f"✅ TASKS ({len(self.resolved_tasks)}):")
            lines.append(f"   Execution order: {' → '.join(self.task_order)}")
            lines.append("")
            for t in self.resolved_tasks:
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


def dryrun(
    dag: Dag,
    *,
    params: dict[str, Any] | None = None,
    variables: dict[str, Any] | None = None,
    logical_date: datetime | None = None,
) -> DryRunResult:
    """Validate and dry-run a DAG definition.

    Checks:
      1. All plugins exist in registry
      2. Plugin ↔ action type compatibility
      3. DAG graph is acyclic (no cycles)
      4. All upstream references exist
      5. Jinja template rendering with provided params/variables

    Args:
        dag: The DAG model to validate.
        params: Runtime parameters to simulate.
        variables: Variables (from variables.yml) to simulate.
        logical_date: Simulated logical_date for rendering.

    Returns:
        DryRunResult with errors, warnings, and resolved task info.
    """
    params = params or {}
    variables = variables or {}
    logical_date = logical_date or datetime.now()
    result = DryRunResult(dag_id=dag.id)

    # Merge default params from DAG definition
    effective_params = {}
    for p in dag.params:
        if hasattr(p, "name") and hasattr(p, "default"):
            effective_params[p.name] = p.default
    effective_params.update(params)

    # Build task map
    task_map: dict[str, Any] = {}
    _flatten_tasks(dag.tasks, task_map)

    # --- Check 1: Plugin existence ---
    for task_id, action in task_map.items():
        plugin_name = (
            action.uses
            if isinstance(action.uses, str)
            else getattr(action.uses, "plugin_name", "?")
        )
        if isinstance(action.uses, str) and action.uses not in PLUGINS_REGISTRY:
            result.errors.append(
                ValidationError(
                    task_id=task_id,
                    category="plugin",
                    message=f"Plugin {action.uses!r} not found in registry.",
                )
            )

    # --- Check 2: Plugin ↔ action compatibility ---
    for task_id, action in task_map.items():
        plugin_name = (
            action.uses
            if isinstance(action.uses, str)
            else getattr(action.uses, "plugin_name", "?")
        )
        if isinstance(action.uses, str) and action.uses in PLUGINS_REGISTRY:
            plugin_cls = PLUGINS_REGISTRY[action.uses]
            compatible = getattr(plugin_cls, "compatible_actions", ())
            action_type = getattr(action, "type", "task")
            if compatible and action_type not in compatible:
                result.errors.append(
                    ValidationError(
                        task_id=task_id,
                        category="compatibility",
                        message=(
                            f"Plugin {plugin_name!r} is only compatible with "
                            f"{compatible}, but used with action type {action_type!r}."
                        ),
                    )
                )

    # --- Check 3: Upstream references exist ---
    all_ids = set(task_map.keys())
    for task_id, action in task_map.items():
        for up in action.upstream:
            if up not in all_ids:
                result.errors.append(
                    ValidationError(
                        task_id=task_id,
                        category="graph",
                        message=f"Upstream {up!r} does not exist in DAG.",
                    )
                )

    # --- Check 4: Cycle detection ---
    cycle = _detect_cycle(task_map)
    if cycle:
        result.errors.append(
            ValidationError(
                task_id=cycle[0],
                category="graph",
                message=f"Cycle detected: {' → '.join(cycle)}",
            )
        )

    # --- Check 5: Topological sort (execution order) ---
    if not cycle:
        result.task_order = _topological_sort(task_map)

    # --- Check 6: Resolve inputs with Jinja (best-effort) ---
    for task_id, action in task_map.items():
        plugin_name = (
            action.uses
            if isinstance(action.uses, str)
            else getattr(action.uses, "plugin_name", "?")
        )
        resolved_inputs = _render_inputs(
            action.inputs,
            effective_params,
            variables,
            logical_date,
            result,
            task_id,
        )
        result.resolved_tasks.append(
            ResolvedTask(
                task_id=task_id,
                type=getattr(action, "type", "task"),
                plugin_name=plugin_name,
                inputs=resolved_inputs,
                upstream=list(action.upstream),
            )
        )

    return result


def _flatten_tasks(tasks: list, task_map: dict[str, Any]) -> None:
    """Flatten nested groups into a flat task map."""
    for action in tasks:
        if hasattr(action, "tasks") and action.tasks:
            # Group — recurse
            _flatten_tasks(action.tasks, task_map)
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
                # Found cycle
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
    """Compute execution order via topological sort."""
    in_degree = {tid: 0 for tid in task_map}
    downstream: dict[str, list[str]] = {tid: [] for tid in task_map}

    for tid, action in task_map.items():
        for up in action.upstream:
            if up in task_map:
                in_degree[tid] += 1
                downstream[up].append(tid)

    queue = [tid for tid, deg in in_degree.items() if deg == 0]
    order: list[str] = []

    while queue:
        queue.sort()  # deterministic order
        node = queue.pop(0)
        order.append(node)
        for child in downstream[node]:
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    return order


def _render_inputs(
    inputs: dict,
    params: dict,
    variables: dict,
    logical_date: datetime,
    result: DryRunResult,
    task_id: str,
) -> dict[str, Any]:
    """Best-effort Jinja rendering of task inputs."""
    from jinja2 import Template

    rendered = {}

    def vars_func(name: str) -> str:
        return variables.get(name, f"<unresolved: vars('{name}')>")

    for key, value in inputs.items():
        if isinstance(value, str) and "{{" in value:
            try:
                tmpl = Template(value)
                rendered_val = tmpl.render(
                    params=params,
                    vars=vars_func,
                    outputs={},  # no upstream outputs at dry-run
                    runtime={"logical_date": logical_date},
                )
                rendered[key] = rendered_val
            except Exception as exc:
                result.warnings.append(
                    f"[{task_id}] Failed to render '{key}': {exc}"
                )
                rendered[key] = value
        elif isinstance(value, dict):
            rendered[key] = _render_inputs(
                value, params, variables, logical_date, result, task_id
            )
        else:
            rendered[key] = value

    return rendered
