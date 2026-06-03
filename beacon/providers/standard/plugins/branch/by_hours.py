from datetime import datetime
from typing import Any, ClassVar, TYPE_CHECKING

from pydantic import Field

from .....core import BasePlugin

if TYPE_CHECKING:
    from .....core import Context


class ByHourBranchPlugin(BasePlugin):
    """Branch plugin that routes based on the logical_date hour.

    Returns {"branch": success_path} if the hour matches, otherwise
    {"branch": failure_path}. The success/failure paths are defined
    on the Branch action, not here — this plugin just signals which
    direction to go via the standard {"branch": [...]} contract.

    Usage:
        ```yaml
        tasks:
          - id: hour-gate
            type: branch
            uses: by_hours
            inputs:
              hours: [2, 3, 4]
            success: [full-load]
            failure: [incremental-load]
        ```
    """

    plugin_name: ClassVar[str] = "by_hours"

    hours: list[int] = Field(
        default_factory=list,
        description="Hours (0-23) that should take the success path.",
    )

    async def execute(self, context: Context) -> dict[str, Any]:
        """Return branch decision based on logical_date hour."""
        logical_date: datetime = context["logical_date"]
        if logical_date.hour in self.hours:
            return {"matched": True}
        return {}
