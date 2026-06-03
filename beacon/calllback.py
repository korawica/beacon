from collections.abc import Callable
from typing import Literal

from pydantic import BaseModel, Field


class OnEvent(BaseModel):
    """On DAG Event model."""

    on_event: Literal[
        "start",
        "complete",
        "success",
        "failure",
    ] = Field(description="An event triggered by DAG execution")
    hook: Callable = Field(
        description="A hook function to trigger DAG execution",
    )
    inputs: dict


class OnTaskEvent(BaseModel):
    """On Task Event model."""

    on_event: Literal[
        "start",
        "complete",
        "success",
        "failure",
        "retry",
    ] = Field(description="An event triggered by task execution")
    hook: Callable = Field(
        description="A hook function to trigger task execution",
    )
    inputs: dict
