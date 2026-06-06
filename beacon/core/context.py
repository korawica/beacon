"""Context.

The Context is a lightweight typed dict passed to plugin.execute().
It provides runtime information without coupling plugins to internal models.

For persistent task state across retries and remote executors, see
`beacon.core.task_context.TaskContext`.

For the metadata store protocol, see `beacon.core.protocols.MetadataProtocol`.
"""

import logging
from datetime import datetime
from typing import Any, TypedDict


# =============================================================================
# Helper Functions
# =============================================================================


def build_runtime_dict(
    run_id: str,
    dag_id: str,
    task_id: str,
    run_date: datetime,
    logical_date: datetime,
    data_interval_start: datetime,
    data_interval_end: datetime,
    attempt_number: int,
) -> dict[str, Any]:
    """Build the runtime dict for Jinja template context.

    This helper ensures consistent runtime dict structure across:
    - runner.py (first-pass render)
    - worker.py (second-pass render)
    - plan.py (validation)
    - python.py (Jinja templates)

    Args:
        run_id: DagRun ID
        dag_id: DAG ID
        task_id: Task ID within the DAG
        run_date: Wall-clock when DagRun was created
        logical_date: Logical date (= data_interval_start)
        data_interval_start: Data interval start
        data_interval_end: Data interval end
        attempt_number: Current attempt number (1-based)

    Returns:
        Dict with runtime info for Jinja templates.
    """
    return {
        "run_id": run_id,
        "dag_id": dag_id,
        "task_id": task_id,
        "run_date": run_date,
        "logical_date": logical_date,
        "data_interval_start": data_interval_start,
        "data_interval_end": data_interval_end,
        "attempt_number": attempt_number,
    }


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
    variables: dict[str, Any]
    """Variables from scoped chain + run-time overrides."""

    # Attempt info
    attempt_number: int
    """Current attempt number (1-based)."""

    # Upstream outputs
    upstream_outputs: dict[str, dict[str, Any]]
    """Outputs from upstream tasks: {task_id: {key: value}}."""

    # Services (injected by executor)
    logger: logging.Logger
    """Structured logger → Logging Store."""

    # Future: metadata access (Phase 2+)
    # metadata: MetadataProtocol
    # """Access to metadata store (read/write task context)."""
