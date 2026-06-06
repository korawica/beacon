"""JSON file-based metadata store (optimized for 1000+ DAG workloads).

Structure::

    {base_path}/
    ├── dag_runs/dag_id={dag_id}/{run_id}.json
    ├── task_contexts/dag_id={dag_id}/run_id={run_id}/{task_id}.json
    ├── task_states/dag_id={dag_id}/run_id={run_id}/{task_id}.json
    ├── deployments/{deployment_id}.json
    └── triggers/deployment_id={deployment_id}/{trigger_id}.json

Hive-style partitioning benefits:
- Explicit partition keys: ``ls {path}/dag_runs/dag_id=my-dag/``
- Query engine compatible (DuckDB, Spark, Trino)
- Fast filtering: ``find . -type d -name "dag_id=etl*"``
"""

import asyncio
import json
import logging
import os
import tempfile
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from ..core.protocols import BaseMetadata
from ..core.state import TaskState
from ..core.task_context import TaskContext

logger = logging.getLogger("beacon.metadata")

_CACHE_SIZE = 4096


class LocalMetadata(BaseMetadata):
    """JSON file metadata store optimized for 1000+ DAG workloads.

    Implements MetadataProtocol via BaseMetadata inheritance.
    Suitable for single-node deployments (dev → 1000 DAGs).

    For multi-node production, use SqliteMetadata (Phase 2) or
    PostgresMetadata (Phase 3).

    Coordination (Multi-Instance Support)
    -------------------------------------
    This class supports coordination between multiple scheduler instances
    using file-based locks. The coordination primitives are:

    - `try_create_scheduled_run`: Create a run only if not already exists
    - `try_claim_trigger`: Claim a trigger for processing
    - `try_update_scheduler_state`: Update scheduler state atomically

    Lock files are stored in `{base_path}/.locks/` and use `fcntl.flock`
    for atomic cross-process locking on Unix systems.
    """

    def __init__(self, base_path: str | Path = "./metadata.db") -> None:
        super().__init__()  # Initialize LRU cache from BaseMetadata
        self.base_path = Path(base_path)
        self._dag_runs_dir = self.base_path / "dag_runs"
        self._task_contexts_dir = self.base_path / "task_contexts"
        self._task_states_dir = self.base_path / "task_states"
        self._deployments_dir = self.base_path / "deployments"
        self._triggers_dir = self.base_path / "triggers"
        self._locks_dir = self.base_path / ".locks"
        for d in (
            self._dag_runs_dir,
            self._task_contexts_dir,
            self._task_states_dir,
            self._deployments_dir,
            self._triggers_dir,
            self._locks_dir,
        ):
            d.mkdir(parents=True, exist_ok=True)

    # --- DagRun ---

    async def create_dag_run(
        self,
        run_id: str,
        dag_id: str,
        dag_version: str,
        state: str = "running",
        logical_date: datetime | None = None,
        variables: dict[str, Any] | None = None,
    ) -> None:
        data = {
            "run_id": run_id,
            "dag_id": dag_id,
            "dag_version": dag_version,
            "state": state,
            "logical_date": str(logical_date) if logical_date else None,
            "variables": variables or {},
            "created_at": str(datetime.now()),
            "ended_at": None,
        }
        path = self._dag_run_path(dag_id, run_id)
        await _async_write(path, data)
        self._active_runs.setdefault(dag_id, set()).add(run_id)

    async def get_dag_run(
        self, run_id: str, dag_id: str
    ) -> dict[str, Any] | None:
        return await _async_read(self._dag_run_path(dag_id, run_id))

    async def update_dag_run_state(
        self, run_id: str, dag_id: str, state: str
    ) -> None:
        path = self._dag_run_path(dag_id, run_id)
        data = await _async_read(path)
        if not data:
            return
        data["state"] = state
        if state in ("success", "failed"):
            data["ended_at"] = str(datetime.now())
            if dag_id in self._active_runs:
                self._active_runs[dag_id].discard(run_id)
        await _async_write(path, data)

    async def list_active_runs(
        self, dag_id: str | None = None
    ) -> list[dict[str, Any]]:
        """List active (non-terminal) DAG runs via the in-memory index."""
        if dag_id:
            pairs = [
                (dag_id, rid) for rid in self._active_runs.get(dag_id, set())
            ]
        else:
            pairs = [
                (did, rid)
                for did, rids in self._active_runs.items()
                for rid in rids
            ]
        if not pairs:
            return []
        datas = await asyncio.gather(
            *(_async_read(self._dag_run_path(d, r)) for d, r in pairs)
        )
        return [d for d in datas if d]

    # --- TaskContext ---

    async def put_task_context(
        self, run_id: str, dag_id: str, task_id: str, task_ctx: TaskContext
    ) -> None:
        path = self._task_context_path(dag_id, run_id, task_id)
        await _async_write_raw(path, task_ctx.model_dump_json(indent=2))

    async def get_task_context(
        self, run_id: str, dag_id: str, task_id: str
    ) -> TaskContext | None:
        path = self._task_context_path(dag_id, run_id, task_id)
        data = await _async_read(path)
        if data is None:
            return None
        return TaskContext.model_validate(data)

    async def get_all_task_contexts(
        self, run_id: str, dag_id: str
    ) -> dict[str, TaskContext]:
        """Get all TaskContexts for a run (parallel reads)."""
        run_dir = (
            self._task_contexts_dir / f"dag_id={dag_id}" / f"run_id={run_id}"
        )
        if not run_dir.exists():
            return {}
        files = list(run_dir.glob("*.json"))
        if not files:
            return {}
        datas = await asyncio.gather(*(_async_read(f) for f in files))
        return {
            f.stem: TaskContext.model_validate(d)
            for f, d in zip(files, datas)
            if d
        }

    async def get_task_outputs(
        self, run_id: str, dag_id: str, task_id: str
    ) -> dict[str, Any]:
        """Fast path: read only the ``outputs`` field from a task context.

        Skips the full ``TaskContext`` Pydantic validation, which is
        significant when downstream tasks resolve many upstream outputs.
        Returns ``{}`` if the task context or outputs are missing.
        """
        path = self._task_context_path(dag_id, run_id, task_id)
        data = await _async_read(path)
        if not data:
            return {}
        outputs = data.get("outputs") or {}
        return outputs if isinstance(outputs, dict) else {}

    async def clear_task(self, run_id: str, dag_id: str, task_id: str) -> None:
        """Reset a task so it will be re-executed on the next run.

        Wipes attempts + outputs in the stored TaskContext and forces
        state back to ``NONE``. Upstream outputs are untouched, so the
        re-run reads the same upstreams it would on a fresh run.

        Used by ``DagRunner.clear`` / ``Dag.clear`` for backfill semantics.
        """
        ctx = await self.get_task_context(run_id, dag_id, task_id)
        if ctx is not None:
            ctx.attempts = []
            ctx.outputs = {}
            await self.put_task_context(run_id, dag_id, task_id, ctx)
        # State file: write NONE (or delete; we choose write for determinism)
        await _async_write(
            self._task_state_path(dag_id, run_id, task_id),
            {
                "run_id": run_id,
                "dag_id": dag_id,
                "task_id": task_id,
                "state": str(TaskState.NONE),
                "updated_at": str(datetime.now()),
            },
        )
        self._cache_put(f"{run_id}:{task_id}", TaskState.NONE)

    # --- TaskState ---

    async def set_task_state(
        self, run_id: str, dag_id: str, task_id: str, state: TaskState
    ) -> None:
        path = self._task_state_path(dag_id, run_id, task_id)
        await _async_write(
            path,
            {
                "run_id": run_id,
                "dag_id": dag_id,
                "task_id": task_id,
                "state": str(state),
                "updated_at": str(datetime.now()),
            },
        )
        self._cache_put(f"{run_id}:{task_id}", state)

    async def get_task_state(
        self, run_id: str, dag_id: str, task_id: str
    ) -> TaskState | None:
        cache_key = f"{run_id}:{task_id}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached
        path = self._task_state_path(dag_id, run_id, task_id)
        data = await _async_read(path)
        if data is None:
            return None
        state = TaskState(data["state"])
        self._cache_put(cache_key, state)
        return state

    async def get_task_state_with_heartbeat(
        self, run_id: str, dag_id: str, task_id: str
    ) -> dict[str, Any] | None:
        """Get full task state dict including heartbeat_at field.

        Used by crash recovery to detect zombie tasks.
        """
        path = self._task_state_path(dag_id, run_id, task_id)
        return await _async_read(path)

    async def update_task_heartbeat(
        self, run_id: str, dag_id: str, task_id: str
    ) -> None:
        """Update heartbeat_at timestamp for a RUNNING task.

        Called periodically by the worker while a task executes.
        Enables zombie detection on scheduler restart.
        """
        path = self._task_state_path(dag_id, run_id, task_id)
        data = await _async_read(path)
        if data is None:
            data = {
                "run_id": run_id,
                "dag_id": dag_id,
                "task_id": task_id,
                "state": str(TaskState.RUNNING),
            }
        data["heartbeat_at"] = str(datetime.now())
        data["updated_at"] = str(datetime.now())
        await _async_write(path, data)

    async def get_all_task_states(
        self, run_id: str, dag_id: str
    ) -> dict[str, TaskState]:
        """Get all task states for a run (parallel reads, cache-aware)."""
        run_dir = (
            self._task_states_dir / f"dag_id={dag_id}" / f"run_id={run_id}"
        )
        if not run_dir.exists():
            return {}

        results: dict[str, TaskState] = {}
        files_to_read: list[Path] = []
        for file in run_dir.glob("*.json"):
            task_id = file.stem
            cached = self._cache_get(f"{run_id}:{task_id}")
            if cached is not None:
                results[task_id] = cached
            else:
                files_to_read.append(file)

        if files_to_read:
            datas = await asyncio.gather(
                *(_async_read(f) for f in files_to_read)
            )
            for f, d in zip(files_to_read, datas):
                if d:
                    state = TaskState(d["state"])
                    results[f.stem] = state
                    self._cache_put(f"{run_id}:{f.stem}", state)
        return results

    # --- Cache helpers (LRU) ---

    def _cache_get(self, key: str) -> TaskState | None:
        if key in self._state_cache:
            self._state_cache.move_to_end(key)
            return self._state_cache[key]
        return None

    def _cache_put(self, key: str, state: TaskState) -> None:
        if key in self._state_cache:
            self._state_cache.move_to_end(key)
            self._state_cache[key] = state
            return
        if len(self._state_cache) >= _CACHE_SIZE:
            self._state_cache.popitem(last=False)
        self._state_cache[key] = state

    def evict_run_from_cache(self, run_id: str) -> None:
        """Remove all cached state for a completed run to free memory."""
        keys_to_remove = [
            k for k in self._state_cache if k.startswith(f"{run_id}:")
        ]
        for k in keys_to_remove:
            del self._state_cache[k]

    # --- Path helpers (hive-style partitioning) ---

    def _dag_run_path(self, dag_id: str, run_id: str) -> Path:
        return self._dag_runs_dir / f"dag_id={dag_id}" / f"{run_id}.json"

    def _task_context_path(
        self, dag_id: str, run_id: str, task_id: str
    ) -> Path:
        return (
            self._task_contexts_dir
            / f"dag_id={dag_id}"
            / f"run_id={run_id}"
            / f"{task_id}.json"
        )

    def _task_state_path(self, dag_id: str, run_id: str, task_id: str) -> Path:
        return (
            self._task_states_dir
            / f"dag_id={dag_id}"
            / f"run_id={run_id}"
            / f"{task_id}.json"
        )

    def _deployment_path(self, deployment_id: str) -> Path:
        return self._deployments_dir / f"{deployment_id}.json"

    def _trigger_path(self, deployment_id: str, trigger_id: str) -> Path:
        return (
            self._triggers_dir
            / f"deployment_id={deployment_id}"
            / f"{trigger_id}.json"
        )

    # --- Deployments ---
    # Stored as plain dicts (Deployment.model_dump) plus scheduler bookkeeping
    # under a top-level ``_scheduler`` key (``last_scheduled_at``).

    async def upsert_deployment(self, deployment: dict[str, Any]) -> None:
        """Create or replace a deployment record (keyed by ``id``)."""
        did = deployment["id"]
        path = self._deployment_path(did)
        existing = await _async_read(path) or {}
        # Preserve scheduler bookkeeping across updates.
        scheduler = existing.get("_scheduler", {})
        record = {**deployment, "_scheduler": scheduler}
        await _async_write(path, record)

    async def get_deployment(self, deployment_id: str) -> dict[str, Any] | None:
        return await _async_read(self._deployment_path(deployment_id))

    async def list_deployments(self) -> list[dict[str, Any]]:
        files = sorted(self._deployments_dir.glob("*.json"))
        if not files:
            return []
        datas = await asyncio.gather(*(_async_read(f) for f in files))
        return [d for d in datas if d]

    async def delete_deployment(self, deployment_id: str) -> bool:
        path = self._deployment_path(deployment_id)
        try:
            path.unlink()
            return True
        except FileNotFoundError:
            return False

    async def update_deployment_scheduler_state(
        self,
        deployment_id: str,
        *,
        last_scheduled_at: datetime,
    ) -> None:
        """Record the most recent logical_date the scheduler fired for this
        deployment so we don't double-schedule the same tick."""
        path = self._deployment_path(deployment_id)
        data = await _async_read(path)
        if not data:
            return
        scheduler = data.setdefault("_scheduler", {})
        scheduler["last_scheduled_at"] = last_scheduled_at.isoformat()
        await _async_write(path, data)

    # --- Manual-trigger queue ---
    # A trigger is a JSON file under triggers/{deployment_id}/{uuid}.json.
    # The scheduler consumes (= deletes) each file when it spawns the run.

    async def enqueue_trigger(
        self,
        deployment_id: str,
        variables: dict[str, Any] | None = None,
    ) -> str:
        """Write a pending manual-trigger request. Returns the trigger id."""
        trigger_id = uuid.uuid4().hex[:12]
        path = self._trigger_path(deployment_id, trigger_id)
        await _async_write(
            path,
            {
                "trigger_id": trigger_id,
                "deployment_id": deployment_id,
                "variables": variables or {},
                "created_at": datetime.now().isoformat(),
            },
        )
        return trigger_id

    async def drain_triggers(
        self, deployment_id: str | None = None
    ) -> list[dict[str, Any]]:
        """Atomically pop every pending trigger.

        If ``deployment_id`` is given, only that deployment's triggers are
        drained. Files are deleted after read; partial failure leaves the
        un-read files in place for the next tick.
        """
        if deployment_id is not None:
            dirs = [self._triggers_dir / f"deployment_id={deployment_id}"]
        else:
            dirs = [d for d in self._triggers_dir.iterdir() if d.is_dir()]
        out: list[dict[str, Any]] = []
        for d in dirs:
            if not d.exists():
                continue
            for f in sorted(d.glob("*.json")):
                data = await _async_read(f)
                if data:
                    out.append(data)
                try:
                    f.unlink()
                except FileNotFoundError:
                    pass
        return out

    # --- Listing ---

    async def list_dag_runs(
        self,
        dag_id: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """List recent DAG runs (active + terminal), newest first."""
        if dag_id is not None:
            shard_dirs = [self._dag_runs_dir / f"dag_id={dag_id}"]
        else:
            shard_dirs = [d for d in self._dag_runs_dir.iterdir() if d.is_dir()]
        files: list[Path] = []
        for d in shard_dirs:
            if d.exists():
                files.extend(d.glob("*.json"))
        files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        files = files[:limit]
        if not files:
            return []
        datas = await asyncio.gather(*(_async_read(f) for f in files))
        return [d for d in datas if d]

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

        Uses file-based locking to coordinate between multiple scheduler
        instances. Only one instance will succeed in creating the run.

        The deduplication key is (dag_id, logical_date) for scheduled runs.
        Manual runs (no logical_date) are not deduplicated.

        Returns:
            Tuple of (created, run_id):
            - created=True if this instance won the race
            - created=False if another instance already created the run
        """
        # For scheduled runs, use logical_date as the dedup key
        lock_key = f"{dag_id}_{logical_date.strftime('%Y%m%dT%H%M%S')}"

        result = await asyncio.to_thread(
            self._try_create_scheduled_run_sync,
            run_id,
            dag_id,
            dag_version,
            logical_date,
            deployment_id,
            state,
            variables,
            lock_key,
        )
        return result

    def _try_create_scheduled_run_sync(
        self,
        run_id: str,
        dag_id: str,
        dag_version: str,
        logical_date: datetime,
        deployment_id: str,
        state: str,
        variables: dict[str, Any] | None,
        lock_key: str,
    ) -> tuple[bool, str]:
        """Synchronous implementation of try_create_scheduled_run."""
        import fcntl

        lock_path = self._locks_dir / f"scheduled_{lock_key}.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            # Try to acquire exclusive lock (non-blocking)
            fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

                # Check if run already exists for this logical_date
                existing = self._find_run_by_logical_date_sync(
                    dag_id, logical_date
                )
                if existing:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                    os.close(fd)
                    return (False, existing["run_id"])

                # Create the run
                data = {
                    "run_id": run_id,
                    "dag_id": dag_id,
                    "dag_version": dag_version,
                    "state": state,
                    "logical_date": str(logical_date),
                    "deployment_id": deployment_id,
                    "variables": variables or {},
                    "created_at": str(datetime.now()),
                    "ended_at": None,
                }
                path = self._dag_run_path(dag_id, run_id)
                _sync_write(path, data)
                self._active_runs.setdefault(dag_id, set()).add(run_id)

                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)
                return (True, run_id)
            except BlockingIOError:
                # Another instance has the lock - wait and check
                os.close(fd)
                # Wait a bit for the other instance to finish
                import time

                time.sleep(0.1)
                existing = self._find_run_by_logical_date_sync(
                    dag_id, logical_date
                )
                if existing:
                    return (False, existing["run_id"])
                # Lock was released but no run created - retry would be complex
                # Just return False to let the next tick try again
                return (False, "")
        except OSError:
            # Lock acquisition failed
            return (False, "")

    def _find_run_by_logical_date_sync(
        self, dag_id: str, logical_date: datetime
    ) -> dict[str, Any] | None:
        """Find an existing run by dag_id and logical_date (sync version)."""
        dag_dir = self._dag_runs_dir / f"dag_id={dag_id}"
        if not dag_dir.exists():
            return None

        logical_date_str = logical_date.strftime("%Y%m%dT%H%M%S")
        expected_run_prefix = f"scheduled-{dag_id}-{logical_date_str}"

        for run_file in dag_dir.glob("*.json"):
            if run_file.stem == expected_run_prefix:
                data = _sync_read(run_file)
                if data:
                    return data

        return None

    async def try_claim_trigger(
        self,
        trigger_id: str,
        deployment_id: str,
        instance_id: str,
    ) -> bool:
        """Atomically claim a trigger for processing.

        Updates the trigger file with a `claimed_by` field. Only one
        instance can claim a trigger.

        Returns:
            True if claim succeeded, False if already claimed
        """
        return await asyncio.to_thread(
            self._try_claim_trigger_sync,
            trigger_id,
            deployment_id,
            instance_id,
        )

    def _try_claim_trigger_sync(
        self, trigger_id: str, deployment_id: str, instance_id: str
    ) -> bool:
        """Synchronous implementation of try_claim_trigger."""
        import fcntl

        trigger_path = self._trigger_path(deployment_id, trigger_id)
        if not trigger_path.exists():
            return False

        lock_path = self._locks_dir / f"trigger_{trigger_id}.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

                # Read trigger
                data = _sync_read(trigger_path)
                if not data:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                    os.close(fd)
                    return False

                # Check if already claimed
                if data.get("claimed_by"):
                    fcntl.flock(fd, fcntl.LOCK_UN)
                    os.close(fd)
                    return False

                # Claim it
                data["claimed_by"] = instance_id
                data["claimed_at"] = datetime.now().isoformat()
                _sync_write(trigger_path, data)

                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)
                return True
            except BlockingIOError:
                os.close(fd)
                return False
        except OSError:
            return False

    async def try_update_scheduler_state(
        self,
        deployment_id: str,
        last_scheduled_at: datetime,
    ) -> bool:
        """Atomically update last_scheduled_at if newer than current value.

        Uses file-based locking to ensure only one instance updates the
        scheduler state for a deployment at a time.

        Returns:
            True if update succeeded, False if another instance already
            updated to an equal or later time
        """
        return await asyncio.to_thread(
            self._try_update_scheduler_state_sync,
            deployment_id,
            last_scheduled_at,
        )

    def _try_update_scheduler_state_sync(
        self, deployment_id: str, last_scheduled_at: datetime
    ) -> bool:
        """Synchronous implementation of try_update_scheduler_state."""
        import fcntl

        lock_path = self._locks_dir / f"deployment_{deployment_id}.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

                # Read deployment
                dep_path = self._deployment_path(deployment_id)
                data = _sync_read(dep_path)
                if not data:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                    os.close(fd)
                    return False

                # Check current last_scheduled_at
                scheduler = data.setdefault("_scheduler", {})
                current_str = scheduler.get("last_scheduled_at")
                if current_str:
                    try:
                        current = datetime.fromisoformat(current_str)
                        if current >= last_scheduled_at:
                            # Another instance already scheduled this or later
                            fcntl.flock(fd, fcntl.LOCK_UN)
                            os.close(fd)
                            return False
                    except ValueError:
                        pass  # Invalid format, proceed with update

                # Update
                scheduler["last_scheduled_at"] = last_scheduled_at.isoformat()
                _sync_write(dep_path, data)

                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)
                return True
            except BlockingIOError:
                os.close(fd)
                return False
        except OSError:
            return False

    async def drain_triggers_with_claim(
        self, instance_id: str, deployment_id: str | None = None
    ) -> list[dict[str, Any]]:
        """Drain triggers that have been claimed by this instance.

        This is the coordinated version of drain_triggers. It:
        1. Finds all pending triggers
        2. Tries to claim each one
        3. Returns only triggers claimed by this instance

        Args:
            instance_id: Unique identifier for this scheduler instance
            deployment_id: Filter by deployment, or None for all

        Returns:
            List of trigger dicts claimed by this instance
        """
        if deployment_id is not None:
            dirs = [self._triggers_dir / f"deployment_id={deployment_id}"]
        else:
            dirs = [d for d in self._triggers_dir.iterdir() if d.is_dir()]

        out: list[dict[str, Any]] = []
        for d in dirs:
            if not d.exists():
                continue
            for f in sorted(d.glob("*.json")):
                data = await _async_read(f)
                if not data:
                    continue

                trigger_id = data.get("trigger_id")
                dep_id = data.get("deployment_id")

                if not trigger_id or not dep_id:
                    continue

                # Try to claim this trigger
                claimed = await self.try_claim_trigger(
                    trigger_id, dep_id, instance_id
                )
                if claimed:
                    out.append(data)
                    # Delete the trigger file
                    try:
                        f.unlink()
                    except FileNotFoundError:
                        pass

        return out


# --- Async I/O helpers ---


async def _async_write(path: Path, data: Any) -> None:
    """Atomic JSON write via temp file + rename."""

    def _do_write():
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(data, f, default=str)
            os.replace(tmp_path, path)
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    await asyncio.to_thread(_do_write)


async def _async_write_raw(path: Path, content: str) -> None:
    """Atomic raw-string write."""

    def _do_write():
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                f.write(content)
            os.replace(tmp_path, path)
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    await asyncio.to_thread(_do_write)


async def _async_read(path: Path) -> dict[str, Any] | None:
    """Non-blocking file read via thread pool. Handles missing file."""

    def _do_read():
        try:
            return json.loads(path.read_text())
        except FileNotFoundError:
            return None

    return await asyncio.to_thread(_do_read)


# --- Sync I/O helpers (for use within locked sections) ---


def _sync_write(path: Path, data: Any) -> None:
    """Synchronous atomic JSON write via temp file + rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, default=str)
        os.replace(tmp_path, path)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _sync_read(path: Path) -> dict[str, Any] | None:
    """Synchronous file read. Returns None if file doesn't exist."""
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        return None
