from datetime import datetime
from typing import TypedDict


class Context(TypedDict):
    """Context Typed Dict."""

    run_id: str
    run_date: datetime
    logical_date: datetime
    data_interval_start: datetime
    data_interval_end: datetime
