"""Protocol definitions for Beacon.

This module defines the protocols that enable loose coupling between
components. Any class implementing these protocols can be used as the
corresponding component without inheritance.

The main protocol is `MetadataProtocol` - the interface that all metadata
stores must satisfy. This allows the Worker, Scheduler, and API Server
to work with any storage backend (JSON files, SQLite, PostgreSQL, etc.)
without knowing the implementation details.

Design Pattern
--------------
Beacon uses **Protocol + Mixin** pattern:

1. `MetadataProtocol` - Defines the interface (structural typing)
2. `BaseMetadata` - Optional abstract base class with shared logic

Why both?
- Protocol enables duck typing: any class with matching methods works
- Base class reduces boilerplate: common operations (LRU cache, etc.)

Implementing a New Backend
--------------------------
```python
class PostgresMetadata(BaseMetadata):
    '''PostgreSQL-backed metadata store for multi-node deployments.'''

    def __init__(self, dsn: str):
        self.pool = asyncpg.create_pool(dsn)

    async def create_dag_run(self, run_id, dag_id, dag_version, ...):
        async with self.pool.acquire() as conn:
            await conn.execute(
                \"\"\"INSERT INTO dag_runs (run_id, dag_id, dag_version, state, ...)
                   VALUES ($1, $2, $3, $4, ...)\"\"\",
                run_id, dag_id, dag_version, state, ...
            )

    # ... implement all abstract methods
```
"""

from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from .state import TaskState
    from .task_context import TaskContext


@runtime_checkable
class MetadataProtocol(Protocol):
    """Protocol that all metadata stores must satisfy.

    Any class implementing these methods can be used as the metadata store
    for the Worker, Scheduler, and API Server — without inheritance.

    Implementations:
        - `beacon.metadata.local_store.LocalMetadata` - JSON file storage (default)
        - Future: SqliteMetadata, PostgresMetadata
    """

    # =========================================================================
    # DagRun Operations
    # =========================================================================

    async def create_dag_run(
        self,
        run_id: str,
        dag_id: str,
        dag_version: str,
        state: str = "running",
        logical_date: datetime | None = None,
        variables: dict[str, Any] | None = None,
    ) -> None:
        """Create a new DagRun record.

        Args:
            run_id: Unique run identifier (e.g., "manual-{dag_id}-{uuid}")
            dag_id: DAG identifier
            dag_version: DAG bundle version at trigger time
            state: Initial state (default: "running")
            logical_date: Schedule logical date (= data_interval_start)
            variables: Resolved variables for this run
        """
        ...

    async def get_dag_run(
        self, run_id: str, dag_id: str
    ) -> dict[str, Any] | None:
        """Get a DagRun record by run_id and dag_id.

        Returns:
            Dict with run metadata, or None if not found.
        """
        ...

    async def update_dag_run_state(
        self, run_id: str, dag_id: str, state: str
    ) -> None:
        """Update DagRun state (running → success/failed)."""
        ...

    async def list_dag_runs(
        self,
        dag_id: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """List recent DagRuns, newest first.

        Args:
            dag_id: Filter by DAG, or None for all DAGs
            limit: Max number of runs to return
        """
        ...

    async def list_active_runs(
        self, dag_id: str | None = None
    ) -> list[dict[str, Any]]:
        """List active (non-terminal) DagRuns.

        Used by crash recovery to find orphaned runs.
        """
        ...

    # =========================================================================
    # TaskContext Operations
    # =========================================================================

    async def put_task_context(
        self, run_id: str, dag_id: str, task_id: str, task_ctx: TaskContext
    ) -> None:
        """Store a TaskContext (serialized)."""
        ...

    async def get_task_context(
        self, run_id: str, dag_id: str, task_id: str
    ) -> TaskContext | None:
        """Get a TaskContext by run_id, dag_id, task_id."""
        ...

    async def get_all_task_contexts(
        self, run_id: str, dag_id: str
    ) -> dict[str, TaskContext]:
        """Get all TaskContexts for a run (parallel reads)."""
        ...

    async def get_task_outputs(
        self, run_id: str, dag_id: str, task_id: str
    ) -> dict[str, Any]:
        """Fast path: read only the outputs field from a TaskContext.

        Skips full Pydantic validation for performance.
        """
        ...

    async def clear_task(self, run_id: str, dag_id: str, task_id: str) -> None:
        """Reset a task for re-execution (used by Dag.clear)."""
        ...

    # =========================================================================
    # TaskState Operations
    # =========================================================================

    async def set_task_state(
        self, run_id: str, dag_id: str, task_id: str, state: TaskState
    ) -> None:
        """Set task state (NONE → SCHEDULED → QUEUED → RUNNING → SUCCESS/FAILED)."""
        ...

    async def get_task_state(
        self, run_id: str, dag_id: str, task_id: str
    ) -> TaskState | None:
        """Get task state."""
        ...

    async def get_all_task_states(
        self, run_id: str, dag_id: str
    ) -> dict[str, TaskState]:
        """Get all task states for a run."""
        ...

    # =========================================================================
    # Heartbeat (Crash Recovery)
    # =========================================================================

    async def get_task_state_with_heartbeat(
        self, run_id: str, dag_id: str, task_id: str
    ) -> dict[str, Any] | None:
        """Get full task state dict including heartbeat_at field.

        Used by crash recovery to detect zombie tasks.
        """
        ...

    async def update_task_heartbeat(
        self, run_id: str, dag_id: str, task_id: str
    ) -> None:
        """Update heartbeat_at timestamp for a RUNNING task.

        Called periodically by the worker while a task executes.
        """
        ...

    # =========================================================================
    # Deployment Operations
    # =========================================================================

    async def upsert_deployment(self, deployment: dict[str, Any]) -> None:
        """Create or replace a deployment record."""
        ...

    async def get_deployment(self, deployment_id: str) -> dict[str, Any] | None:
        """Get a deployment by ID."""
        ...

    async def list_deployments(self) -> list[dict[str, Any]]:
        """List all deployments."""
        ...

    async def delete_deployment(self, deployment_id: str) -> bool:
        """Delete a deployment. Returns True if deleted."""
        ...

    async def update_deployment_scheduler_state(
        self,
        deployment_id: str,
        *,
        last_scheduled_at: datetime,
    ) -> None:
        """Record the most recent scheduled logical_date."""
        ...

    # =========================================================================
    # Manual Trigger Queue
    # =========================================================================

    async def enqueue_trigger(
        self,
        deployment_id: str,
        variables: dict[str, Any] | None = None,
    ) -> str:
        """Write a pending manual-trigger request.

        Returns:
            trigger_id
        """
        ...

    async def drain_triggers(
        self, deployment_id: str | None = None
    ) -> list[dict[str, Any]]:
        """Atomically pop every pending trigger.

        Args:
            deployment_id: Filter by deployment, or None for all

        Returns:
            List of trigger dicts
        """
        ...

    # =========================================================================
    # Coordination (Multi-Instance Support)
    # =========================================================================

    async def try_create_scheduled_run(
        self,
        run_id: str,
        dag_id: str,
        dag_version: str,
        logical_date: datetime,
        deployment_id: str,
        state: str = "running",
        variables: dict[str, Any] | None = None,
    ) -> tuple[bool, str]:
        """Atomically create a scheduled DagRun only if not already exists.

        This is the coordination primitive for multi-instance schedulers.
        Only one instance will succeed in creating the run for a given
        (dag_id, logical_date) combination.

        Args:
            run_id: Proposed run identifier
            dag_id: DAG identifier
            dag_version: DAG bundle version at trigger time
            logical_date: Schedule logical date (used for deduplication)
            deployment_id: Deployment that triggered this run
            state: Initial state (default: "running")
            variables: Resolved variables for this run

        Returns:
            Tuple of (created, run_id):
            - created=True if this instance won the race
            - created=False if another instance already created the run
            - run_id is the actual run_id (may differ if another instance won)
        """
        ...

    async def try_claim_trigger(
        self,
        trigger_id: str,
        deployment_id: str,
        instance_id: str,
    ) -> bool:
        """Atomically claim a trigger for processing.

        Prevents multiple scheduler instances from processing the same trigger.

        Args:
            trigger_id: The trigger to claim
            deployment_id: Deployment the trigger belongs to
            instance_id: Unique identifier of the claiming instance

        Returns:
            True if claim succeeded, False if already claimed by another instance
        """
        ...

    async def try_update_scheduler_state(
        self,
        deployment_id: str,
        last_scheduled_at: datetime,
    ) -> bool:
        """Atomically update last_scheduled_at if newer than current value.

        This is used to coordinate which scheduler instance "owns" the next
        scheduled tick for a deployment. The instance that successfully updates
        the state gets to fire the run.

        Args:
            deployment_id: Deployment to update
            last_scheduled_at: The new last_scheduled_at value

        Returns:
            True if update succeeded (this instance won the tick),
            False if another instance already updated to an equal or later time
        """
        ...

    # =========================================================================
    # Cache Management
    # =========================================================================

    def evict_run_from_cache(self, run_id: str) -> None:
        """Remove all cached state for a completed run."""
        ...


# =============================================================================
# Optional Base Class (for reducing boilerplate)
# =============================================================================


class BaseMetadata:
    """Optional abstract base class for metadata stores.

    Provides:
    - LRU cache for task states
    - Active run tracking
    - Common helper methods

    Subclasses must implement all async methods from MetadataProtocol.
    This base class is NOT required - any class implementing MetadataProtocol
    works. Use this only if you want the built-in caching utilities.
    """

    def __init__(self) -> None:
        from collections import OrderedDict

        self._state_cache: OrderedDict[str, TaskState] = OrderedDict()
        self._active_runs: dict[str, set[str]] = {}
        self._cache_size = 4096

    # --- Cache helpers (LRU) ---

    def _cache_get(self, key: str) -> TaskState | None:
        """Get from LRU cache, moving to end on hit."""
        if key in self._state_cache:
            self._state_cache.move_to_end(key)
            return self._state_cache[key]
        return None

    def _cache_put(self, key: str, state: TaskState) -> None:
        """Put in LRU cache, evicting oldest if full."""
        if key in self._state_cache:
            self._state_cache.move_to_end(key)
            self._state_cache[key] = state
            return
        if len(self._state_cache) >= self._cache_size:
            self._state_cache.popitem(last=False)
        self._state_cache[key] = state

    def evict_run_from_cache(self, run_id: str) -> None:
        """Remove all cached state for a completed run."""
        keys_to_remove = [
            k for k in self._state_cache if k.startswith(f"{run_id}:")
        ]
        for k in keys_to_remove:
            del self._state_cache[k]

    # --- Active run tracking ---

    def _track_active_run(self, dag_id: str, run_id: str) -> None:
        """Track an active run for list_active_runs()."""
        self._active_runs.setdefault(dag_id, set()).add(run_id)

    def _untrack_active_run(self, dag_id: str, run_id: str) -> None:
        """Remove run from active tracking."""
        if dag_id in self._active_runs:
            self._active_runs[dag_id].discard(run_id)
            if not self._active_runs[dag_id]:
                del self._active_runs[dag_id]

    def _get_active_runs(
        self, dag_id: str | None = None
    ) -> list[tuple[str, str]]:
        """Get list of (dag_id, run_id) pairs for active runs."""
        if dag_id:
            return [
                (dag_id, rid) for rid in self._active_runs.get(dag_id, set())
            ]
        return [
            (did, rid)
            for did, rids in self._active_runs.items()
            for rid in rids
        ]
