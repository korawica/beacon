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

    Auto-registers a subclass only when it **explicitly declares** a
    ``plugin_name`` class variable in its own body. Intermediate / abstract
    bases without ``plugin_name`` are NOT auto-registered.
    """

    def __new__(
        mcs: type[Self],
        name: str,
        bases: tuple[type, ...],
        attrs: dict[str, Any],
        **kwargs: Any,
    ) -> type[Self]:
        pydantic_cls = cast(
            type[Self], super().__new__(mcs, name, bases, attrs, **kwargs)
        )
        explicit_name = attrs.get("plugin_name")
        if (
            isinstance(explicit_name, str)
            and explicit_name
            and explicit_name != BASE_PLUGIN_NAME
        ):
            register_plugin(pydantic_cls, explicit_name)
        return pydantic_cls


class BasePlugin(BaseModel, ABC, metaclass=PluginMeta):
    """Base Plugin Model.

    Subclasses register themselves automatically when they declare a
    ``plugin_name`` ClassVar. Plugins are plain Pydantic models — Jinja
    rendering is performed by the scheduler **before** the plugin is
    instantiated.
    """

    plugin_name: ClassVar[str] = BASE_PLUGIN_NAME
    compatible_actions: ClassVar[tuple[str, ...]] = ()
    """Action types this plugin is compatible with.

    Empty tuple means compatible with all action types. Set e.g.
    ``("branch",)`` to restrict to branch actions only. Enforced by
    :func:`beacon.dryrun.dryrun`.
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
