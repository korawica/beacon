"""Beacon — an everyday workflow orchestrator.

Top-level package = the *DAG authoring* surface. Write DAGs, attach
deployments per environment, plug in callbacks and custom plugins.

For extension authoring (custom plugins, executors, metadata stores) see
:mod:`beacon.core`. For observability hooks see
:func:`configure_logging`.
"""

# --- DAG authoring ---------------------------------------------------------
from .models.branch import Branch
from .models.dag import Dag
from .models.deployment import Deployment
from .models.group import Group
from .models.sensor import Sensor
from .models.short_circuit import ShortCircuit
from .models.task import Task

# --- Callbacks -------------------------------------------------------------
from .callback import Callback, OnDagEvent, OnTaskEvent

# --- Plugin extension (Context is what plugin.execute receives) ------------
from .core.context import Context
from .core.plugin import PLUGINS_REGISTRY, BasePlugin, register_plugin
from .core.trigger_rule import TriggerRule

# --- Raise strategy (control flow from inside a plugin) --------------------
from .errors import TaskFailed, TaskRetry, TaskSkipped

# --- Runtime / user task code ----------------------------------------------
from .runtime import load_context

# --- Running locally / advanced orchestration ------------------------------
from .runner import DagRunResult, DagRunner

# --- Observability ---------------------------------------------------------
from .logging import configure_logging, get_dispatcher, shutdown_logging

# --- Standard plugin auto-registration -------------------------------------
# Importing the standard provider package is what wires built-in plugin
# names (``empty``, ``py``, ``by_hour``, ...) into PLUGINS_REGISTRY so they
# resolve out-of-the-box. The name is not re-exported.
from .providers import standard as _standard  # noqa: F401

__all__ = (
    # DAG authoring
    "Branch",
    "Dag",
    "Deployment",
    "Group",
    "Sensor",
    "ShortCircuit",
    "Task",
    # Callbacks
    "Callback",
    "OnDagEvent",
    "OnTaskEvent",
    # Plugin extension
    "BasePlugin",
    "Context",
    "PLUGINS_REGISTRY",
    "TriggerRule",
    "register_plugin",
    # Raise strategy
    "TaskFailed",
    "TaskRetry",
    "TaskSkipped",
    # Runtime
    "load_context",
    # Orchestration
    "DagRunResult",
    "DagRunner",
    # Observability
    "configure_logging",
    "get_dispatcher",
    "shutdown_logging",
)
