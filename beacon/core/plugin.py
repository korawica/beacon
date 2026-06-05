"""Beacon plugin base and registry.

A plugin is a Pydantic-validated class implementing one ``async execute``
method. Plugins are looked up by string name (``uses: "py"``) from the
global registry.

Plugins are **plain Pydantic models** — they do not own templating,
state, or lifecycle logic. The scheduler resolves all Jinja templates
before instantiating a plugin, so plugins always receive concrete values.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, ClassVar, Final, Self, cast

from pydantic import BaseModel

from .context import Context

__all__ = (
    "BASE_PLUGIN_NAME",
    "PLUGINS_REGISTRY",
    "BasePlugin",
    "BaseTaskPlugin",
    "BaseSensorPlugin",
    "BaseBranchPlugin",
    "BaseShortCircuitPlugin",
    "register_plugin",
)

logger = logging.getLogger("beacon.core.plugin")


BASE_PLUGIN_NAME: Final[str] = "base"
PLUGINS_REGISTRY: dict[str, type] = {}


def register_plugin(
    cls: type,
    name: str | None = None,
    *,
    allow_override: bool = False,
) -> None:
    """Register a plugin class in the global registry.

    Args:
        cls: The plugin class. Must be a :class:`BasePlugin` subclass.
        name: Optional explicit name. Falls back to ``cls.plugin_name``.
        allow_override: When False (default), overriding an existing entry
            logs a warning. Use ``True`` for bundle-level intentional override.
    """
    plugin_name: str | None = name or getattr(cls, "plugin_name", None)
    if not plugin_name or plugin_name == BASE_PLUGIN_NAME:
        return

    existing = PLUGINS_REGISTRY.get(plugin_name)
    if existing is not None and existing is not cls:
        if allow_override:
            logger.debug(
                "Overriding plugin %r: %s -> %s",
                plugin_name,
                existing.__qualname__,
                cls.__qualname__,
            )
        else:
            logger.warning(
                "Plugin %r is being overridden (existing=%s, new=%s). "
                "Pass allow_override=True to silence this warning.",
                plugin_name,
                existing.__qualname__,
                cls.__qualname__,
            )

    PLUGINS_REGISTRY[plugin_name] = cls


class PluginMeta(type(BaseModel)):
    """Plugin Metaclass.

    Auto-registers a subclass using the plugin_name from either:
    1. Class keyword argument: ``class MyPlugin(BasePlugin, plugin_name="my-plugin"):``
    2. Explicit class variable: ``plugin_name: ClassVar[str] = "my-plugin"``
    3. Snakecase of class name as fallback: ``MyPlugin`` → ``my_plugin``

    Abstract classes (with unimplemented abstract methods) are NOT registered,
    allowing them to serve as intermediate bases for plugin families.
    """

    def __new__(
        mcs: type[Self],
        name: str,
        bases: tuple[type, ...],
        attrs: dict[str, Any],
        plugin_name: str | None = None,
        **kwargs: Any,
    ) -> type[Self]:
        pydantic_cls = cast(
            type[Self], super().__new__(mcs, name, bases, attrs, **kwargs)
        )

        # Skip registration for abstract classes (have unimplemented abstract methods)
        abstract_methods = getattr(
            pydantic_cls, "__abstractmethods__", frozenset()
        )
        if abstract_methods:
            return pydantic_cls

        # Determine plugin_name priority:
        # 1. Class keyword argument (plugin_name="...")
        # 2. Explicit class variable in attrs
        # 3. Fallback to snakecase of class name
        final_name: str | None = plugin_name

        if not final_name:
            explicit_name = attrs.get("plugin_name")
            if (
                isinstance(explicit_name, str)
                and explicit_name
                and explicit_name != BASE_PLUGIN_NAME
            ):
                final_name = explicit_name

        if not final_name:
            # Fallback: convert class name to snake_case
            # e.g., ByHourBranchPlugin -> by_hour_branch_plugin
            import re

            s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
            final_name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()

        if final_name and final_name != BASE_PLUGIN_NAME:
            # Set plugin_name on the class
            pydantic_cls.plugin_name = final_name
            register_plugin(pydantic_cls, final_name)

        return pydantic_cls


class BasePlugin(BaseModel, ABC, metaclass=PluginMeta):
    """Base Plugin Model.

    Subclasses register themselves automatically. The plugin name can be set
    via class keyword argument (preferred):

        class MyPlugin(BasePlugin, plugin_name="my-plugin"):
            ...

    Or via explicit class variable (deprecated but still supported):

        class MyPlugin(BasePlugin):
            plugin_name: ClassVar[str] = "my-plugin"

    If neither is provided, the snakecase of the class name is used as fallback
    (e.g., ``MyPlugin`` → ``my_plugin``).

    Plugins are plain Pydantic models — Jinja rendering is performed by the
    scheduler **before** the plugin is instantiated.
    """

    plugin_name: ClassVar[str] = BASE_PLUGIN_NAME
    compatible_actions: ClassVar[tuple[str, ...]] = ()
    """Action types this plugin is compatible with.

    Empty tuple means compatible with all action types. Set e.g.
    ``("branch",)`` to restrict to branch actions only. Enforced by
    :func:`beacon.dryrun.dryrun`.
    """

    template_ext: ClassVar[tuple[str, ...]] = ()
    """File extensions that trigger file-loading + Jinja rendering.

    When a plugin input value ends with one of these extensions, the plugin
    should load the file and render its contents with Jinja before use.
    Empty tuple (default) means no file-extension-based rendering.

    Example: ``template_ext = (".py",)`` means any input ending in ``.py``
    will be treated as a file path to load and render.
    """

    @abstractmethod
    async def execute(self, context: Context):
        """Plugin execution method.

        Args:
            context (Context): Runtime context built by the executor.
        """
        raise NotImplementedError(
            "The execute method of BasePlugin is not implemented."
        )

    async def teardown(self, context: Context) -> None:
        """Plugin teardown — runs ALWAYS after execute (success or failure).

        Override to clean up resources the plugin acquired during execute.
        Called by the executor in a finally block. Errors here are logged
        but do not change the task's outcome.

        Default: no-op.
        """


# =============================================================================
# Specialized Plugin Base Classes
# =============================================================================


class BaseTaskPlugin(BasePlugin, ABC):
    """Base class for TASK plugins.

    Task plugins perform work and return results. The output dict is stored
    as task outputs and accessible to downstream tasks via
    ``{{ outputs.task_id.key }}`` or ``load_context().upstream_outputs``.

    Output contract: ``dict[str, Any] | None`` — any dictionary or None.

    Downstream behavior: All downstream tasks are scheduled on SUCCESS.

    Example:
        ```python
        class MyTask(BaseTaskPlugin, plugin_name="my-task"):
            source: str
            target: str

            async def execute(self, context: Context) -> dict[str, Any]:
                # Do work
                rows = await process(self.source, self.target)
                return {"rows_processed": rows}
        ```
    """

    compatible_actions: ClassVar[tuple[str, ...]] = ("task",)


class BaseSensorPlugin(BasePlugin, ABC):
    """Base class for SENSOR plugins.

    Sensor plugins wait for an external condition to be met before proceeding.
    They run in a polling loop managed by the executor.

    Output contract: ``dict[str, Any]`` — typically ``{"condition_met": True, ...}``.

    Context includes:
        - ``check_interval``: Seconds between condition checks (default: 60)
        - ``execution_timeout``: Max wait time before failing

    Downstream behavior: All downstream tasks are scheduled when condition is met.

    Example:
        ```python
        class GcsSensor(BaseSensorPlugin, plugin_name="gcs-sensor"):
            bucket: str
            prefix: str

            async def execute(self, context: Context) -> dict[str, Any]:
                import asyncio
                from google.cloud import storage

                client = storage.Client()
                check_interval = context.get("check_interval", 60)

                while True:
                    blobs = list(client.list_blobs(self.bucket, prefix=self.prefix))
                    if blobs:
                        return {"condition_met": True, "files_found": len(blobs)}
                    await asyncio.sleep(check_interval)
        ```
    """

    compatible_actions: ClassVar[tuple[str, ...]] = ("sensor",)


class BaseBranchPlugin(BasePlugin, ABC):
    """Base class for BRANCH plugins.

    Branch plugins choose which downstream path(s) to schedule. Unchosen
    paths are marked as SKIPPED.

    Output contract: ``{"branch": ["task-id-1", "task-id-2", ...]}`` — a list
    of task IDs to run. Everything else in the action's ``success``/``failure``
    lists is SKIPPED.

    The action's ``success`` and ``failure`` fields define the candidate tasks.
    The plugin returns which subset to actually run.

    Downstream behavior: Only tasks in ``outputs["branch"]`` are scheduled.

    Example:
        ```python
        class QualityBranch(BaseBranchPlugin, plugin_name="quality-branch"):
            threshold: float

            async def execute(self, context: Context) -> dict[str, Any]:
                score = await get_quality_score()
                if score >= self.threshold:
                    return {"branch": context.get("success", [])}
                return {"branch": context.get("failure", [])}
        ```
    """

    compatible_actions: ClassVar[tuple[str, ...]] = ("branch",)


class BaseShortCircuitPlugin(BasePlugin, ABC):
    """Base class for SHORT_CIRCUIT plugins.

    Short-circuit plugins decide whether to continue the DAG or skip ALL
    downstream tasks recursively. Use for conditional execution based on
    runtime conditions.

    Output contract: ``{"continue": True | False}`` — whether to proceed.

    Downstream behavior:
        - ``{"continue": True}`` → all downstream scheduled normally
        - ``{"continue": False}`` → ALL downstream tasks SKIPPED recursively

    Example:
        ```python
        class RunToday(BaseShortCircuitPlugin, plugin_name="run-today"):
            allowed_days: list[str] = ["monday", "wednesday", "friday"]

            async def execute(self, context: Context) -> dict[str, Any]:
                from datetime import datetime
                today = datetime.now().strftime("%A").lower()
                return {"continue": today in self.allowed_days}
        ```
    """

    compatible_actions: ClassVar[tuple[str, ...]] = ("short_circuit",)
