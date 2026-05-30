from typing import ClassVar, TYPE_CHECKING

from .....core import BasePlugin

if TYPE_CHECKING:
    from .....core import Context


class EmptyPlugin(BasePlugin):
    """Empty Plugin."""

    plugin_name: ClassVar[str] = "empty"

    def execute(self, context: Context):
        pass
