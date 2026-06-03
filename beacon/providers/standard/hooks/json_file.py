"""JSON File Hook.

Writes alert JSON files to a local directory on task lifecycle events.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, ClassVar

from ....callback import BaseHook

logger = logging.getLogger("beacon.hook.json_file")


class JsonFileHook(BaseHook):
    """Writes alert JSON files to a local directory.

    Output: {alert_dir}/{dag_id}_{task_id}_{event}_{timestamp}.json
    """

    hook_name: ClassVar[str] = "json-file"

    def __init__(self, alert_dir: str = "./alerts") -> None:
        self.alert_dir = Path(alert_dir)

    async def notify(self, event: str, data: dict[str, Any]) -> None:
        self.alert_dir.mkdir(parents=True, exist_ok=True)
        dag_id = data.get("dag_id", "unknown")
        task_id = data.get("task_id", "unknown")
        ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        filename = f"{dag_id}_{task_id}_{event}_{ts}.json"
        payload = {"event": event, "timestamp": ts, **data}
        (self.alert_dir / filename).write_text(
            json.dumps(payload, indent=2, default=str)
        )
        logger.info("Alert written: %s", filename)
