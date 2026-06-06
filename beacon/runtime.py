"""Beacon runtime context for user Python files.

Public API:
    from beacon import load_context

    def main():
        ctx = load_context()
        ctx.logger.info("hello from %s", ctx.task_id)
"""

import logging
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

_current: ContextVar[RuntimeContext | None] = ContextVar(
    "beacon_runtime", default=None
)


@dataclass
class RuntimeContext:
    """Runtime context available inside user code during task execution."""

    run_id: str = ""
    dag_id: str = ""
    task_id: str = ""
    attempt_number: int = 1
    variables: dict[str, Any] = field(default_factory=dict)
    upstream_outputs: dict[str, dict[str, Any]] = field(default_factory=dict)
    run_date: datetime | None = None
    logical_date: datetime | None = None
    data_interval_start: datetime | None = None
    data_interval_end: datetime | None = None
    logger: logging.Logger = field(
        default_factory=lambda: logging.getLogger("beacon.task")
    )


def load_context() -> RuntimeContext:
    """Load the current beacon runtime context.

    Only available inside a function executed by the `py` plugin.

    Example:
        from beacon import load_context

        def main(source_system: str):
            ctx = load_context()
            ctx.logger.info("Processing %s", source_system)
    """
    ctx = _current.get()
    if ctx is None:
        raise RuntimeError(
            "load_context() called outside of beacon task execution."
        )
    return ctx


# --- internal helpers (used only by the standard `py` plugin) -------------


def _set_runtime_context(ctx: RuntimeContext) -> None:
    _current.set(ctx)


def _clear_runtime_context() -> None:
    _current.set(None)
