from datetime import datetime
from typing import Any, TYPE_CHECKING

from pydantic import Field

from .....core import BaseBranchPlugin

if TYPE_CHECKING:
    from .....core import Context


class ByHourBranchPlugin(BaseBranchPlugin, plugin_name="by_hours"):
    hours: list[int] = Field(
        default_factory=list,
        description="Hours (0-23) that should take the success path.",
    )

    async def execute(self, context: Context) -> dict[str, Any]:
        """Return branch decision based on logical_date hour.

        Returns {"branch": [...]} with task IDs from success or failure list.
        """
        logical_date: datetime = context["logical_date"]
        if logical_date.hour in self.hours:
            return {"branch": context.get("success", [])}
        return {"branch": context.get("failure", [])}
