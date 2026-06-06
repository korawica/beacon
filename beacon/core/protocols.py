"""Protocol definitions for Beacon.

This module defines the protocols that enable loose coupling between
components. Any class implementing these protocols can be used as the
corresponding component without inheritance.

The main protocol is `MetadataProtocol` - the interface that all metadata
stores must satisfy. This allows the Worker, Scheduler, and API Server
to work with any storage backend (JSON files, SQLite, PostgreSQL, etc.)
without knowing the implementation details.
"""

from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from .state import TaskState
    from .task_context import TaskContext


class MetadataProtocol(Protocol):
    """Protocol that all metadata stores must satisfy.

    Any class implementing these methods can be used as the metadata store
    for the Worker, Scheduler, and API Server — without inheritance.

    Implementations:
        - `beacon.metadata.json_store.LocalMetadata` - JSON file storage
        - Future: SqliteMetadata, PostgresMetadata
    """

    # --- DagRun ---

    async def create_dag_run(
        self,
        run_id: str,
        dag_id: str,
        dag_version: str,
        state: str = "running",
        logical_date: datetime | None = None,
        variables: dict[str, Any] | None = None,
    ) -> None: ...

    async def get_dag_run(
        self, run_id: str, dag_id: str
    ) -> dict[str, Any] | None: ...

    async def update_dag_run_state(
        self, run_id: str, dag_id: str, state: str
    ) -> None: ...

    # --- TaskContext ---

    async def put_task_context(
        self, run_id: str, dag_id: str, task_id: str, task_ctx: TaskContext
    ) -> None: ...

    async def get_task_context(
        self, run_id: str, dag_id: str, task_id: str
    ) -> TaskContext | None: ...

    # --- TaskState ---

    async def set_task_state(
        self, run_id: str, dag_id: str, task_id: str, state: TaskState
    ) -> None: ...

    async def get_task_state(
        self, run_id: str, dag_id: str, task_id: str
    ) -> TaskState | None: ...
