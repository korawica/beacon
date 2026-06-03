from typing import ClassVar

from pydantic import BaseModel

PLUGINS_REGISTRY = {}


class PluginMeta(type(BaseModel)):
    """Metaclass that auto-registers every BasePlugin subclass."""

    def __new__(cls, name, bases, attrs):
        new_cls = super().__new__(cls, name, bases, attrs)
        plugin_name = attrs.get("plugin_name")
        if plugin_name and plugin_name != "base":
            PLUGINS_REGISTRY[plugin_name] = new_cls
        return new_cls


class BasePlugin(BaseModel, metaclass=PluginMeta):
    plugin_name: ClassVar[str] = "base"


class PluginName(BasePlugin):
    plugin_name: ClassVar[str] = "plugin_name"

    source_system: str
    bucket: str
    prefix: str

    def execute(self, context):
        # plugin logic here
        pass


def test_check_registry():
    plugin = PluginName(
        source_system="source", bucket="bucket", prefix="/test/prefix"
    )
    print(plugin)
    print(PLUGINS_REGISTRY)
