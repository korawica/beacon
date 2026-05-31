from pydantic import BaseModel, Field


class Schedule(BaseModel):
    """Schedule Model."""

    schedule: str = Field(description="A crontab of its schedule")
