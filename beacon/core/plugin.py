import threading
import logging
from abc import ABC, abstractmethod
from typing import ClassVar, Final, Self, Any, cast

from pydantic import BaseModel

from .context import Context
from .templater import Templater
from ..utils import to_snake_case

__all__ = (
    "BASE_PLUGIN_NAME",
    "PLUGINS_REGISTRY",
    "BasePlugin",
    "register_plugin",
)

logger = logging.getLogger("beacon.core")


BASE_PLUGIN_NAME: Final[str] = "base"
PLUGINS_REGISTRY: dict[str, type] = {}
_lock = threading.Lock()


def register_plugin(cls: type, name: str | None = None) -> None:
    """Register a plugin class.

    Args:
        cls (type):
            A subclass of Plugin model
        name (str | None, optional):
            A Plugin name that want to use instead of its class attribute name,
            ``plugin_name``.

    !!! example

        ```python
        from beacon.core import BasePlugin, register_plugin

        register_plugin(BasePlugin)
        ```
    """
    plugin_name: str = name or getattr(cls, "plugin_name", BASE_PLUGIN_NAME)
    if (
        plugin_name and plugin_name != BASE_PLUGIN_NAME
        # NOTE: Disallow override the plugins.
        # and plugin_name not in PLUGINS_REGISTRY
    ):
        if plugin_name in PLUGINS_REGISTRY:
            logger.debug("Overriding plugin registry with %s", plugin_name)

        # NOTE: Start update plugin to the registry.
        with _lock:
            PLUGINS_REGISTRY[plugin_name] = cls


class PluginMeta(type(BaseModel)):
    """Plugin Metaclass.

    This metaclass auto-registers every BasePlugin subclass to the ``PLUGINS_REGISTRY``
    for using from any Action model by ``uses`` field.
    """

    def __new__(
        cls: type[Self],
        name: str,
        bases: tuple[type, ...],
        attrs: dict[str, Any],
        **kwargs,
    ) -> type[Self]:
        """Register its subclass to the ``PLUGINS_REGISTRY`` with the
        ``plugin_name`` class variable.
        """
        new_cls = cast(
            type[Self], super().__new__(cls, name, bases, attrs, **kwargs)
        )
        register_plugin(
            new_cls,
            attrs.get("plugin_name", to_snake_case(new_cls.__name__)),
        )
        return new_cls


class BasePlugin(Templater, ABC, metaclass=PluginMeta):
    """Base Plugin Model.

    This class auto-registers every BasePlugin subclass with the ``plugin_name``
    class variable to the ``PLUGINS_REGISTRY`` for using from any Action model by
    ``uses`` field.
    """

    plugin_name: ClassVar[str] = BASE_PLUGIN_NAME

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
