"""DAG Graph Operations.

This module provides graph topology utilities for DAG validation and execution.
All graph-related logic lives here so it can be shared between:

- `beacon.runner.DagRunner` - execution orchestration
- `beacon.plan.plan` - validation
- `beacon.models.dag.Dag` - user API (backfill)

The module is intentionally small and focused on graph operations only.
State transitions and execution logic remain in their respective modules.
"""

from dataclasses import dataclass
from typing import Any

from .action import BaseAction

__all__ = (
    "Graph",
    "build_graph",
    "flatten_actions",
    "collect_self_and_downstream",
    "detect_cycle",
    "topological_sort",
)


@dataclass
class Graph:
    """Pre-computed DAG topology.

    Attributes:
        task_map: Flat map of task_id → BaseAction (Groups flattened).
        downstream: Map of task_id → list of downstream task_ids.
        teardown_for_setup: Map of setup_id → teardown_id.
        teardown_setup: Map of teardown_id → setup_id.
        teardown_deps: Map of teardown_id → {setup_id + all transitive non-teardown dependents}.
    """

    task_map: dict[str, BaseAction]
    downstream: dict[str, list[str]]
    teardown_for_setup: dict[str, str]
    teardown_setup: dict[str, str]
    teardown_deps: dict[str, set[str]]

    @property
    def teardown_ids(self) -> set[str]:
        """Set of all teardown task IDs."""
        return set(self.teardown_setup)

    @property
    def normal_ids(self) -> set[str]:
        """Set of all non-teardown task IDs."""
        return set(self.task_map) - self.teardown_ids


def flatten_actions(
    actions: list[Any], out: dict[str, BaseAction]
) -> list[str]:
    """Flatten Groups; collect leaf actions into ``out`` keyed by id.

    Args:
        actions: List of actions (may contain Groups with nested actions).
        out: Output dict to populate with task_id → BaseAction.

    Returns:
        List of duplicate task IDs found (empty if no duplicates).
    """
    seen: set[str] = set()
    duplicates: list[str] = []

    def _flatten(action_list: list[Any]) -> None:
        for action in action_list:
            if hasattr(action, "actions") and action.actions:
                _flatten(action.actions)
            else:
                if action.id in seen:
                    duplicates.append(action.id)
                seen.add(action.id)
                out[action.id] = action

    _flatten(actions)
    return duplicates


def build_graph(actions: list[Any]) -> Graph:
    """Build a Graph from a list of actions.

    Args:
        actions: List of actions (may contain Groups with nested actions).

    Returns:
        Graph with pre-computed topology.
    """
    task_map: dict[str, BaseAction] = {}
    flatten_actions(actions, task_map)

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

    return Graph(
        task_map=task_map,
        downstream=downstream,
        teardown_for_setup=teardown_for_setup,
        teardown_setup=teardown_setup,
        teardown_deps=teardown_deps,
    )


def collect_self_and_downstream(
    graph: Graph, root: str, *, include_downstream: bool
) -> list[str]:
    """Return ``[root]`` plus, optionally, every transitive downstream.

    Args:
        graph: The DAG graph.
        root: Starting task ID.
        include_downstream: If True, include all transitive downstream tasks.

    Returns:
        List of task IDs in BFS order (root first).
    """
    if not include_downstream:
        return [root]
    order: list[str] = [root]
    seen: set[str] = {root}
    queue: list[str] = [root]
    while queue:
        current = queue.pop(0)
        for child in graph.downstream.get(current, []):
            if child in seen:
                continue
            seen.add(child)
            order.append(child)
            queue.append(child)
    return order


def detect_cycle(task_map: dict[str, BaseAction]) -> list[str] | None:
    """Detect cycle in DAG using DFS.

    Args:
        task_map: Map of task_id → BaseAction.

    Returns:
        Cycle path if found, None otherwise.
    """
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


def topological_sort(task_map: dict[str, BaseAction]) -> list[str]:
    """Compute execution order via topological sort (Kahn's algorithm).

    Uses a min-heap so ties resolve by lexical order.

    Args:
        task_map: Map of task_id → BaseAction.

    Returns:
        List of task IDs in topological order.
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
