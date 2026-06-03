# Example: Python with Standard

## Define Dag

First, you must define your Dag object.

```python title="dag.py"
from beacon import Dag, Group, Param, Sensor, Task
from beacon.providers.standard.plugins import EmptyPlugin
from beacon.calllback import OnEvent, OnTaskEvent
from beacon.metadata import SqliteMetadata
from beacon.providers.msteam import MsTeamCallback
from beacon.providers.smtp import SendmailCallback

dag = Dag(
    id="hello-world",
    owners=["de"],
    desc="An example workflow description",
    params=[
        Param(name="source_system", type="str", default="example"),
    ],
    callbacks=[
        OnEvent(
            on_event="success",
            hook=MsTeamCallback,
            inputs={"webhook_url": "https://my-webhook-url.com"},
        ),
        OnEvent(
            on_event="failure",
            hook=SendmailCallback,
            inputs={"to": "on-call@email.com"},
        ),
    ],
    tasks=[
        Task(
            id="start",
            uses="empty",
        ),
        Task(
            id="process",
            upstream=["start"],
            uses="py",
            inputs={
                "py_file": "./example_py_file.py",
                "py_function": "main",
                "params": {
                    "source_system": "{{ params.source_system }}",
                },
            },
            retries=3,
            execution_timeout=3600,
        ),
    ],
)
```

## Defining Python File

```python title="example_py_file.py"
from google.storage import Client

def main(source_system: str):
    client = Client()
    # Do something with the client
```
