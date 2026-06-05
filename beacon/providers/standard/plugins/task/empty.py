from typing import TYPE_CHECKING

from .....core import BaseTaskPlugin

if TYPE_CHECKING:
    from .....core import Context


class EmptyPlugin(BaseTaskPlugin, plugin_name="empty"):
    async def execute(self, context: Context) -> None:
        pass
