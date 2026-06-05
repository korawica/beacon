from datetime import datetime
from typing import TYPE_CHECKING

from pydantic import Field

from .....core import BasePlugin

if TYPE_CHECKING:
    from .....core import Context


class ByHourBranchPlugin(BasePlugin, plugin_name="by_hours"):
    hours: list[int] = Field(
        default_factory=list,
        description="Hours (0-23) that should take the success path.",
    )

    async def execute(self, context: Context) -> bool:
        """Return True (success path) if the logical_date hour is in ``hours``.

        The Branch action's ``extract_outputs`` maps ``True`` → ``success``
        and ``False`` → ``failure``, so the plugin no longer needs to know
        about the action's task ID lists.
        """
        logical_date: datetime = context["logical_date"]
        return logical_date.hour in self.hours
