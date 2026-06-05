"""Beacon extension API.

The everyday DAG-authoring API lives at the top level (``from beacon
import Dag, Task, ...``). This package is the *extension surface* for
authors of custom plugins, executors, and metadata stores. Everything
re-exported here is part of the supported public API.

Strictly-internal primitives (``DownstreamDirective``, ``Attempt``,
``AttemptStatus``, ``TERMINAL_STATES``) live in their submodules and are
not re-exported here — import them from their submodule if you really
need them.
"""

from .action import BaseAction
from .context import Context, MetadataProtocol
from .executor import BaseExecutor, LocalExecutor
from .plugin import (
    PLUGINS_REGISTRY,
    BasePlugin,
    register_plugin,
)
from .renderer import Renderer
from .state import TaskState
from .task_context import TaskContext
from .trigger_rule import TriggerRule

__all__ = (
    "BaseAction",
    "BaseExecutor",
    "BasePlugin",
    "Context",
    "LocalExecutor",
    "MetadataProtocol",
    "PLUGINS_REGISTRY",
    "Renderer",
    "TaskContext",
    "TaskState",
    "TriggerRule",
    "register_plugin",
)
