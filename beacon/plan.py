"""DAG Plan — pre-execution validation and template rendering.

Shows exactly what Beacon will do before you deploy: resolves every
Jinja template against real variables / logical_date so you can
catch misconfigured inputs, missing plugins, and graph errors before a
single task runs.

Usage:
    from beacon.plan import plan

    result = plan(
        dag=dag,
        variables={"bucket": "prod-bucket"},
        logical_date=datetime(2026, 6, 3, 2, 0, 0),
        cron="0 2 * * *",            # optional — computes data intervals
    )
    print(result)         # pretty-printed report
    result.is_valid       # True if no errors
"""

import ast
import logging
import re
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
    category: str  # "plugin" | "graph" | "template" | "variable"
    message: str


@dataclass
class RequiredVariable:
    """A variable required by the DAG."""

    key: str
    has_default: bool
    found_in: str | None  # "variables" if provided, None otherwise
    default_value: Any = None


@dataclass
class RequiredSecret:
    """A secret (environment variable) required by the DAG."""

    key: str
    found_in: str | None  # "environment" if set, None otherwise


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
    required_variables: list[RequiredVariable] = field(default_factory=list)
    required_secrets: list[RequiredSecret] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        """True if no errors found."""
        return len(self.errors) == 0

    def __str__(self) -> str:
        """Return a formatted plan report."""
        lines = [f"=== Plan: {self.dag_id} ===", ""]

        # Variables section
        if self.required_variables:
            missing_vars = [
                v
                for v in self.required_variables
                if not v.has_default and v.found_in is None
            ]
            if missing_vars:
                lines.append(f"❌ MISSING VARIABLES ({len(missing_vars)}):")
                for v in missing_vars:
                    lines.append(f"  {v.key}")
                lines.append("")
            else:
                lines.append(f"✅ VARIABLES ({len(self.required_variables)}):")
                for v in self.required_variables:
                    status = (
                        "✓" if v.found_in else ("~" if v.has_default else "✗")
                    )
                    default_str = (
                        f" (default: {v.default_value!r})"
                        if v.has_default
                        else ""
                    )
                    lines.append(f"  [{status}] {v.key}{default_str}")
                lines.append("")

        # Secrets section
        if self.required_secrets:
            missing_secrets = [
                s for s in self.required_secrets if s.found_in is None
            ]
            if missing_secrets:
                lines.append(f"⚠️  MISSING SECRETS ({len(missing_secrets)}):")
                for s in missing_secrets:
                    lines.append(f"  {s.key} (set via environment variable)")
                lines.append("")
            else:
                lines.append(f"✅ SECRETS ({len(self.required_secrets)}):")
                for s in self.required_secrets:
                    lines.append(f"  [✓] {s.key}")
                lines.append("")

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
      3. All upstream and teardown references to exist
      4. Jinja template rendering with provided variables / dates
      5. Required variables and secrets are detected and validated

    Args:
        dag: The DAG model to validate.
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
        PlanResult with errors, warnings, required variables/secrets,
        and per-task resolved inputs.
    """
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

    from .core.remote_plugin import is_remote_ref, ref_to_plugin_name

    # Build task map — also detect duplicate IDs.
    task_map: dict[str, Any] = {}
    duplicate_ids = _flatten_actions(dag.actions, task_map)

    # --- Check 0: Duplicate task IDs ---
    for dup_id in sorted(duplicate_ids):
        result.errors.append(
            PlanIssue(
                task_id=dup_id,
                category="graph",
                message=(
                    f"Duplicate task ID {dup_id!r}. Every task ID must be unique "
                    f"within the DAG (including inside groups)."
                ),
            )
        )

    # --- Check 1: Plugin existence ---
    for task_id, action in task_map.items():
        if not isinstance(action.uses, str):
            continue
        ref = action.uses
        if is_remote_ref(ref):
            # Remote plugin — installed at runtime via uv. Not an error.
            result.warnings.append(
                f"[{task_id}] Remote plugin {ref!r} will be installed "
                f"via 'uv run --with' before this task runs."
            )
        elif ref_to_plugin_name(ref) in PLUGINS_REGISTRY:
            pass  # plain name that happens to match lookup key — fine
        elif ref not in PLUGINS_REGISTRY:
            result.errors.append(
                PlanIssue(
                    task_id=task_id,
                    category="plugin",
                    message=f"Plugin {ref!r} not found in registry.",
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

    # --- Check 5: Detect required variables and secrets ---
    all_inputs = _collect_all_inputs(task_map)
    result.required_variables = _detect_required_variables(
        all_inputs, variables
    )
    result.required_secrets = _detect_required_secrets(all_inputs)

    # --- Check 6: Resolve inputs with Jinja (best-effort) ---
    for task_id, action in task_map.items():
        plugin_name = (
            action.uses
            if isinstance(action.uses, str)
            else getattr(action.uses, "plugin_name", "?")
        )
        rendered_inputs, warnings = _render_inputs(
            action.inputs,
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


def _flatten_actions(
    actions: list,
    task_map: dict[str, Any],
    _seen: set[str] | None = None,
) -> list[str]:
    """Flatten nested groups into a flat task map.

    Returns a deduplicated list of task IDs that appear more than once.
    """
    if _seen is None:
        _seen = set()
    duplicates: set[str] = set()
    for action in actions:
        if hasattr(action, "actions") and action.actions:
            child_dups = _flatten_actions(action.actions, task_map, _seen)
            duplicates.update(child_dups)
        else:
            if action.id in _seen:
                duplicates.add(action.id)
            _seen.add(action.id)
            task_map[action.id] = action
    return sorted(duplicates)


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


def _collect_all_inputs(task_map: dict[str, Any]) -> list[tuple[str, Any]]:
    """Collect all input values from all tasks for analysis.

    Returns list of (task_id, value) pairs for all input values.
    """
    all_inputs: list[tuple[str, Any]] = []
    for task_id, action in task_map.items():
        for key, value in action.inputs.items():
            all_inputs.append((task_id, value))
    return all_inputs


def _detect_required_variables(
    all_inputs: list[tuple[str, Any]],
    variables: dict[str, Any],
) -> list[RequiredVariable]:
    """Extract required variables from Jinja templates.

    Parses ``{{ vars("key") }}`` and ``{{ vars("key", "default") }}`` patterns
    to determine what variables the DAG needs.
    """
    # Pattern matches: vars("key") or vars("key", "default")
    # Also matches: vars("nested.key")
    vars_pattern = re.compile(
        r'vars\s*\(\s*["\']([^"\']+)["\'](?:\s*,\s*([^)]+))?\s*\)'
    )

    required: dict[str, RequiredVariable] = {}

    for task_id, value in all_inputs:
        if not isinstance(value, str):
            continue

        for match in vars_pattern.finditer(value):
            key = match.group(1)
            default_arg = match.group(2)

            # Check if nested key exists in variables
            found_in = None
            if "." in key:
                parts = key.split(".")
                v = variables
                found = True
                for part in parts:
                    if isinstance(v, dict) and part in v:
                        v = v[part]
                    else:
                        found = False
                        break
                if found:
                    found_in = "variables"
            elif key in variables:
                found_in = "variables"

            if key not in required:
                required[key] = RequiredVariable(
                    key=key,
                    has_default=default_arg is not None,
                    found_in=found_in,
                    default_value=ast.literal_eval(default_arg.strip())
                    if default_arg and default_arg.strip()
                    else None,
                )
            else:
                # If any usage has no default, the variable is required
                if default_arg is None:
                    required[key].has_default = False
                if found_in and required[key].found_in is None:
                    required[key].found_in = found_in

    return sorted(required.values(), key=lambda v: v.key)


def _detect_required_secrets(
    all_inputs: list[tuple[str, Any]],
) -> list[RequiredSecret]:
    """Extract required secrets from Jinja templates.

    Parses ``{{ secrets("KEY") }}`` patterns to determine what
    environment variables the DAG needs.
    """
    import os

    # Pattern matches: secrets("KEY")
    secrets_pattern = re.compile(r'secrets\s*\(\s*["\']([^"\']+)["\']\s*\)')

    required: dict[str, RequiredSecret] = {}

    for task_id, value in all_inputs:
        if not isinstance(value, str):
            continue

        for match in secrets_pattern.finditer(value):
            key = match.group(1)
            found_in = "environment" if key in os.environ else None

            if key not in required:
                required[key] = RequiredSecret(key=key, found_in=found_in)

    return sorted(required.values(), key=lambda s: s.key)


def _render_inputs(
    inputs: dict,
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
    from .core.renderer import Renderer, make_vars_func, make_secrets_func

    vars_func = make_vars_func(variables)
    secrets_func = make_secrets_func()

    ctx = {
        "vars": vars_func,
        "secrets": secrets_func,
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
