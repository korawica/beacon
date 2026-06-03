"""Context.

The Context is a lightweight typed dict passed to plugin.execute().
It provides runtime information without coupling plugins to internal models.

For persistent task state across retries and remote executors, see
`beacon.core.task_context.TaskContext`.
"""

from datetime import datetime
from typing import Any, Protocol, TypedDict

from .state import TaskState
from .task_context import TaskContext


class MetadataProtocol(Protocol):
    """Protocol that all metadata stores must satisfy.

    Any class implementing these methods can be used as the metadata store
    for the Worker, Scheduler, and API Server — without inheritance.
    """

    async def create_dag_run(
        self,
        run_id: str,
        dag_id: str,
        dag_version: str,
        state: str = "running",
        logical_date: datetime | None = None,
        params: dict[str, Any] | None = None,
    ) -> None: ...

    async def get_dag_run(
        self, run_id: str, dag_id: str
    ) -> dict[str, Any] | None: ...

    async def update_dag_run_state(
        self, run_id: str, dag_id: str, state: str
    ) -> None: ...

    async def put_task_context(
        self, run_id: str, dag_id: str, task_id: str, task_ctx: TaskContext
    ) -> None: ...

    async def get_task_context(
        self, run_id: str, dag_id: str, task_id: str
    ) -> TaskContext | None: ...

    async def set_task_state(
        self, run_id: str, dag_id: str, task_id: str, state: TaskState
    ) -> None: ...

    async def get_task_state(
        self, run_id: str, dag_id: str, task_id: str
    ) -> TaskState | None: ...


class LoggerProtocol(Protocol):
    """Protocol for task logger that writes to the Logging Store."""

    def info(self, msg: str, *args: Any) -> None: ...
    def error(self, msg: str, *args: Any) -> None: ...
    def warning(self, msg: str, *args: Any) -> None: ...
    def debug(self, msg: str, *args: Any) -> None: ...


class Context(TypedDict, total=False):
    """Context passed to plugin.execute().

    Built by the executor from TaskContext before calling the plugin.
    Plugins receive this — they never see TaskContext directly.
    """

    # Identity
    run_id: str
    """DagRun ID."""

    dag_id: str
    """DAG ID."""

    task_id: str
    """Task ID within the DAG."""

    # Time
    run_date: datetime
    """Wall-clock when the DagRun was created."""

    logical_date: datetime
    """Logical Date that should equal to ``data_interval_start``."""

    data_interval_start: datetime
    """Data Interval Start."""

    data_interval_end: datetime
    """Data Interval End."""

    # Data
    params: dict[str, Any]
    """DAG params (Jinja-rendered with vars at trigger time)."""

    # Attempt info
    attempt_number: int
    """Current attempt number (1-based)."""

    # Upstream outputs
    upstream_outputs: dict[str, dict[str, Any]]
    """Outputs from upstream tasks: {task_id: {key: value}}."""

    # Services (injected by executor)
    metadata: MetadataProtocol
    """Access to metadata store (read/write task context)."""

    logger: LoggerProtocol
    """Structured logger → Logging Store."""
