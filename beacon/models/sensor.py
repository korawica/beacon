from typing import Literal

from pydantic import Field

from ..core import BaseAction


class Sensor(BaseAction):
    """Sensor Action Model.

    !!! example

        ```yaml
        tasks:
          - id: sensor
            mode: poke
            check_interval: 5
            execution_timeout: 60
            exponential_backoff: true
            fail_mode: soft
        ```
    """

    type: Literal["sensor"] = Field(
        default="sensor",
        description="A sensor action type.",
    )
    mode: Literal["poke", "reschedule"] = Field(
        default="poke",
        description="A mode of the sensor.",
    )
    check_interval: int = Field(
        default=60,
        description="An interval in seconds that the sensor will poke.",
    )
    execution_timeout: int | None = Field(
        default=None,
        description="A timeout in seconds for the sensor checking.",
    )
    exponential_backoff: bool = Field(
        default=True,
        description="Whether or not to exponentially backoff.",
    )
    fail_mode: Literal["soft", "silent"] = Field(
        default="soft",
        description="A mode of fail event if it handle from a plugin.",
    )
