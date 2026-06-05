from typing import TYPE_CHECKING

from .....core import BasePlugin

if TYPE_CHECKING:
    from .....core import Context


class EmptyPlugin(BasePlugin, plugin_name="empty"):
    async def execute(self, context: Context) -> None:
        pass
