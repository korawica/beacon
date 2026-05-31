from typing import ClassVar, TYPE_CHECKING

from pydantic import Field

from .....core import BasePlugin

if TYPE_CHECKING:
    from .....core import Context


class PythonPlugin(BasePlugin):
    """Python Plugin."""

    plugin_name: ClassVar[str] = "empty"

    py_file: str = Field(description="Python file")

    def execute(self, context: Context) -> None:
        pass
