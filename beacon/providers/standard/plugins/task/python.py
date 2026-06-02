from typing import ClassVar, TYPE_CHECKING

from pydantic import Field

from .....core import BasePlugin

if TYPE_CHECKING:
    from .....core import Context


class PythonPlugin(BasePlugin):
    """Python Plugin.

    !!! example

        ```yaml
        tasks:
          - id: example
            type: task
            uses: py
            py_file: ./example.py
            env:
              ENV_VAR: foo
        ```
    """

    plugin_name: ClassVar[str] = "py"

    py_file: str = Field(description="Python file")
    env: dict[str, str] = Field(
        default_factory=dict,
        description="A mapping of environment variables",
    )

    async def execute(self, context: Context) -> None:
        pass
