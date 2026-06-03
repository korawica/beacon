import logging
from abc import ABC, abstractmethod
from typing import Any, ClassVar, Final, Self, cast

from pydantic import BaseModel

from ..utils import to_snake_case
from .context import Context
from .templater import Templater

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

    Auto-registers a subclass to :data:`PLUGINS_REGISTRY` **only when the
    subclass explicitly declares** a ``plugin_name`` class variable in its
    own body. This avoids accidentally registering intermediate / abstract
    bases under an auto-generated snake_case name.
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


class BasePlugin(Templater, ABC, metaclass=PluginMeta):
    """Base Plugin Model.

    Subclasses register themselves automatically when they declare a
    ``plugin_name`` ClassVar. Subclasses without an explicit ``plugin_name``
    are treated as intermediate/abstract and are NOT auto-registered.
    """

    plugin_name: ClassVar[str] = BASE_PLUGIN_NAME
    compatible_actions: ClassVar[tuple[str, ...]] = ()
    """Action types this plugin is compatible with.

    Empty tuple means compatible with all action types.
    Set to e.g. ``("branch",)`` to restrict to branch actions only.
    """

    @abstractmethod
    async def execute(self, context: Context):
        """Plugin execution method.

        Args:
            context (Context):
                A DAG runned context that was generated after queue DAG.
        """
        raise NotImplementedError(
            "The execute method of BasePlugin is not implemented."
        )


# Silence ruff unused-import for the helper kept for callers who want manual
# snake_case names (rare, but part of the public surface).
_ = to_snake_case
