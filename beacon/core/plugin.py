import threading
from abc import ABC, abstractmethod
from typing import ClassVar, Final, Self, Any

from pydantic import BaseModel

from .context import Context

BASE_PLUGIN_NAME: Final[str] = "base"
PLUGINS_REGISTRY: dict[str, type] = {}
_lock = threading.Lock()


class PluginMeta(type(BaseModel)):
    """Plugin Metaclass.

    This metaclass auto-registers every BasePlugin subclass.
    """

    def __new__(
        cls: type[Self],
        name: str,
        bases: tuple[type, ...],
        attrs: dict[str, Any],
        **kwargs,
    ) -> type:
        """Override the __new__ method."""
        new_cls = super().__new__(cls, name, bases, attrs, **kwargs)
        plugin_name: str = attrs.get("plugin_name", BASE_PLUGIN_NAME)
        if (
            plugin_name and plugin_name != BASE_PLUGIN_NAME
            # NOTE: Disallow override the plugins.
            # and plugin_name not in PLUGINS_REGISTRY
        ):
            with _lock:
                PLUGINS_REGISTRY[plugin_name] = new_cls
        return new_cls


class BasePlugin(BaseModel, ABC, metaclass=PluginMeta):
    """Base Plugin Model.

    This class auto-registers every BasePlugin subclass.
    """

    plugin_name: ClassVar[str] = BASE_PLUGIN_NAME

    @abstractmethod
    def execute(self, context: Context):
        raise NotImplementedError(
            "The execute method of BasePlugin is not implemented."
        )
