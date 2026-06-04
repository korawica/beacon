"""JSON file-based metadata store (optimized for 1000+ DAG workloads).

Structure::

    {base_path}/
    ├── dag_runs/{dag_id}/{run_id}.json
    ├── task_contexts/{dag_id}/{run_id}/{task_id}.json
    ├── task_states/{dag_id}/{run_id}/{task_id}.json
    ├── deployments/{deployment_id}.json
    └── triggers/{deployment_id}/{trigger_id}.json   # pending manual triggers
"""

import asyncio
import json
import logging
import os
import tempfile
import uuid
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import Any

from ..core.state import TaskState
from ..core.task_context import TaskContext

logger = logging.getLogger("beacon.metadata")

_CACHE_SIZE = 4096


class LocalMetadata:
    """JSON file metadata store optimized for 1000+ DAG workloads."""

    def __init__(self, base_path: str | Path = "./metadata.db") -> None:
        self.base_path = Path(base_path)
        self._dag_runs_dir = self.base_path / "dag_runs"
        self._task_contexts_dir = self.base_path / "task_contexts"
        self._task_states_dir = self.base_path / "task_states"
        self._deployments_dir = self.base_path / "deployments"
        self._triggers_dir = self.base_path / "triggers"
        for d in (
            self._dag_runs_dir,
            self._task_contexts_dir,
            self._task_states_dir,
            self._deployments_dir,
            self._triggers_dir,
        ):
            d.mkdir(parents=True, exist_ok=True)

        # LRU cache for task states. We're async-single-thread so no lock.
        self._state_cache: OrderedDict[str, TaskState] = OrderedDict()
        # Active run index: dag_id -> set of run_ids
        self._active_runs: dict[str, set[str]] = {}

    # --- DagRun ---

    async def create_dag_run(
        self,
        run_id: str,
        dag_id: str,
        dag_version: str,
        state: str = "running",
        logical_date: datetime | None = None,
        params: dict[str, Any] | None = None,
    ) -> None:
        data = {
            "run_id": run_id,
            "dag_id": dag_id,
            "dag_version": dag_version,
            "state": state,
            "logical_date": str(logical_date) if logical_date else None,
            "params": params or {},
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
        run_dir = self._task_contexts_dir / dag_id / run_id
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

    async def get_all_task_states(
        self, run_id: str, dag_id: str
    ) -> dict[str, TaskState]:
        """Get all task states for a run (parallel reads, cache-aware)."""
        run_dir = self._task_states_dir / dag_id / run_id
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

    # --- Path helpers (sharded by dag_id) ---

    def _dag_run_path(self, dag_id: str, run_id: str) -> Path:
        return self._dag_runs_dir / dag_id / f"{run_id}.json"

    def _task_context_path(
        self, dag_id: str, run_id: str, task_id: str
    ) -> Path:
        return self._task_contexts_dir / dag_id / run_id / f"{task_id}.json"

    def _task_state_path(self, dag_id: str, run_id: str, task_id: str) -> Path:
        return self._task_states_dir / dag_id / run_id / f"{task_id}.json"

    def _deployment_path(self, deployment_id: str) -> Path:
        return self._deployments_dir / f"{deployment_id}.json"

    def _trigger_path(self, deployment_id: str, trigger_id: str) -> Path:
        return self._triggers_dir / deployment_id / f"{trigger_id}.json"

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
        params: dict[str, Any] | None = None,
    ) -> str:
        """Write a pending manual-trigger request. Returns the trigger id."""
        trigger_id = uuid.uuid4().hex[:12]
        path = self._trigger_path(deployment_id, trigger_id)
        await _async_write(
            path,
            {
                "trigger_id": trigger_id,
                "deployment_id": deployment_id,
                "params": params or {},
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
            dirs = [self._triggers_dir / deployment_id]
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
            shard_dirs = [self._dag_runs_dir / dag_id]
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
