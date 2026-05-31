from datetime import datetime
from typing import TypedDict


class Context(TypedDict):
    """Context Typed Dict."""

    run_id: str
    """Run ID."""

    run_date: datetime
    """Run Date."""

    logical_date: datetime
    """Logical Date that should equal to ``data_interval_start``."""

    data_interval_start: datetime
    """Data Interval Start."""

    data_interval_end: datetime
    """Data Interval End."""
