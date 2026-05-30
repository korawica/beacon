from typing import ClassVar, TYPE_CHECKING

from pydantic import Field

from .....core import BasePlugin

if TYPE_CHECKING:
    from .....core import Context


class ByHourBranchPlugin(BasePlugin):
    plugin_name: ClassVar[str] = "by_hours"

    hours: list[int] = Field(
        default_factory=list,
        description="A list of hour (0-23) to determine the downstream path.",
    )

    def execute(self, context: Context) -> bool:
        return True
