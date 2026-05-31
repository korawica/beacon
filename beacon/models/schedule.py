from pydantic import BaseModel, Field

from .dag import Dag


class Schedule(BaseModel):
    """Schedule Model."""

    cron: str = Field(description="A crontab of its schedule")
    timezone: str = Field(description="A timezone of its schedule")
    start_date: str
    end_date: str
    catch_up: bool = Field(
        default=False, description="Whether or not to catch up"
    )
    dag: Dag = Field(description="A DAG of its schedule")
