"""JSON file-based metadata store.

Simple, production-capable metadata persistence using JSON files.
Structure:
    {base_path}/
    ├── dag_runs/{run_id}.json
    ├── task_contexts/{run_id}_{task_id}.json
    └── task_states/{run_id}_{task_id}.json
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from ..core.state import TaskState
from ..core.task_context import TaskContext

logger = logging.getLogger("beacon.metadata")


class JsonMetadata:
    """JSON file metadata store."""

    def __init__(self, base_path: str | Path = "./metadata.db") -> None:
        self.base = Path(base_path)
        self._dag_runs = self.base / "dag_runs"
        self._task_contexts = self.base / "task_contexts"
        self._task_states = self.base / "task_states"
        # Create directories
        for d in (self._dag_runs, self._task_contexts, self._task_states):
            d.mkdir(parents=True, exist_ok=True)

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
        self._write(self._dag_runs / f"{run_id}.json", data)

    async def get_dag_run(self, run_id: str) -> dict[str, Any] | None:
        path = self._dag_runs / f"{run_id}.json"
        return self._read(path)

    async def update_dag_run_state(self, run_id: str, state: str) -> None:
        path = self._dag_runs / f"{run_id}.json"
        data = self._read(path)
        if data:
            data["state"] = state
            if state in ("success", "failed"):
                data["ended_at"] = str(datetime.now())
            self._write(path, data)

    # --- TaskContext ---

    async def put_task_context(
        self, run_id: str, task_id: str, task_ctx: TaskContext
    ) -> None:
        path = self._task_contexts / f"{run_id}_{task_id}.json"
        # Write directly from Pydantic JSON — avoids double serialization
        path.write_text(task_ctx.model_dump_json(indent=2))

    async def get_task_context(
        self, run_id: str, task_id: str
    ) -> TaskContext | None:
        path = self._task_contexts / f"{run_id}_{task_id}.json"
        data = self._read(path)
        if data is None:
            return None
        return TaskContext.model_validate(data)

    # --- TaskState ---

    async def set_task_state(
        self, run_id: str, task_id: str, state: TaskState
    ) -> None:
        path = self._task_states / f"{run_id}_{task_id}.json"
        self._write(
            path,
            {
                "run_id": run_id,
                "task_id": task_id,
                "state": str(state),
                "updated_at": str(datetime.now()),
            },
        )

    async def get_task_state(
        self, run_id: str, task_id: str
    ) -> TaskState | None:
        path = self._task_states / f"{run_id}_{task_id}.json"
        data = self._read(path)
        if data is None:
            return None
        return TaskState(data["state"])

    # --- Internal ---

    @staticmethod
    def _write(path: Path, data: Any) -> None:
        path.write_text(json.dumps(data, indent=2, default=str))

    @staticmethod
    def _read(path: Path) -> dict[str, Any] | None:
        if not path.exists():
            return None
        return json.loads(path.read_text())
