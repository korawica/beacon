from typing import Literal

from pydantic import Field

from ..core import BaseAction


class ShortCircuit(BaseAction):
    """Branch Action Model.

    !!! example

        ```yaml
        tasks:
          - id: example
            type: short_circuit
            uses: "some-plugin"
        ```
    """

    type: Literal["short_circuit"] = Field(default="short_circuit")
