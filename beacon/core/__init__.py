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
from .context import Context
from .executor import BaseExecutor, LocalExecutor
from .graph import Graph, build_graph, flatten_actions
from .plugin import (
    PLUGINS_REGISTRY,
    BasePlugin,
    register_plugin,
)
from .protocols import MetadataProtocol
from .renderer import Renderer
from .state import TaskState, is_terminal, can_transition
from .task_context import TaskContext
from .trigger_rule import TriggerRule

__all__ = (
    "BaseAction",
    "BaseExecutor",
    "BasePlugin",
    "build_graph",
    "can_transition",
    "Context",
    "flatten_actions",
    "Graph",
    "is_terminal",
    "LocalExecutor",
    "MetadataProtocol",
    "PLUGINS_REGISTRY",
    "Renderer",
    "TaskContext",
    "TaskState",
    "TriggerRule",
    "register_plugin",
)
