from typing import ClassVar, TYPE_CHECKING

from .....core import BasePlugin

if TYPE_CHECKING:
    from .....core import Context


class EmptyPlugin(BasePlugin):
    """Empty Plugin.

    This plugin do not action anything. It will use for reserve tasks or test
    the DAG workflow and its dependencies.
    """

    plugin_name: ClassVar[str] = "empty"

    async def execute(self, context: Context) -> None:
        pass
