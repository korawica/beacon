from datetime import datetime
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
        """Execute the plugin."""
        logical_date: datetime = context["logical_date"]
        if logical_date.hour in self.hours:
            return True
        return False
