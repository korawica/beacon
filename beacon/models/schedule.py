from typing import Literal

from pydantic import BaseModel, Field

from .dag import Dag


class Schedule(BaseModel):
    """Schedule Model."""

    id: str = Field(description="A schedule ID")
    type: Literal["schedule"] = Field(
        default="schedule", description="The schedule type"
    )
    cron: str = Field(description="A crontab of its schedule")
    timezone: str = Field(description="A timezone of its schedule")
    start_date: str = Field()
    end_date: str = Field()
    catch_up: bool = Field(
        default=False, description="Whether or not to catch up"
    )
    dag: Dag = Field(description="A DAG of its schedule")
