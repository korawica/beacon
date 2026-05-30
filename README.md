# Beacon

An everyday workflow orchestrator that simple and easy to customize.

---

## Examples

This is the full example of a workflow that this beacon package support.

```python
from beacon import Runner, Dag, Group, Param, Sensor, Task
from beacon.callbacks import OnStart, OnFailure, OnTaskFailure
from beacon.metadata import SqliteMetadata
from beacon.providers.msteam import msteam_adaptive_card
from beacon.providers.smtp import send_mail


dag = Dag(
    id="hello-world",
    desc="An example workflow description",
    params=[
        Param(name="source_system", type="str", default="example"),
    ],
    callbacks=[
        OnStart(hook=msteam_adaptive_card("https://my-webhook-url.com")),
        OnFailure(hook=send_mail("on-call@email.com")),
    ],
    tasks=[
        Sensor(
            id="start",
            uses="plugins/cloud_storage_with_prefix",
            inputs={
                "source_system": "{{ params.source_system }}",
                "bucket": "my-bucket",
                "prefix": "my-prefix/{{ data_interval_start | utc | fmt('year=%Y/month=%m/day=%d/hour=%H') }}",
            }
        ),
        Group(
            id="extract",
            upstream=["start"],
            tasks=[
                Task(
                    id="extract-1",
                    uses="bigquery_count",
                    inputs={
                        "source_system": "{{ params.source_system }}",
                        "bucket": "my-bucket",
                        "prefix": "my-prefix/{{ data_interval_start | utc | fmt('year=%Y/month=%m/day=%d/hour=%H') }}",
                    },
                    callbacks=[
                        OnTaskFailure(hook=send_mail("owner@email.com")),
                    ],
                ),
                Task(
                    id="extract-2",
                    upstream=["extract-1"],
                    uses="bigquery_count",
                    inputs={
                        "source_system": "{{ params.source_system }}",
                        "bucket": "my-bucket",
                        "prefix": "my-prefix/{{ data_interval_start | utc | fmt('year=%Y/month=%m/day=%d/hour=%H') }}",
                    },
                    callbacks=[
                        OnTaskFailure(hook=send_mail("owner@email.com")),
                    ],
                ),
            ],
        ),
        Task(
            id="end",
            upstream=["extract", "start"],
            trigger_rule="all_done",
            uses="send_logs",
            inputs={
                "logs": [
                    {
                        "task_id": "extract-1",
                        "status": "{{ tasks.extract-1.status }}",
                        "records": "{{ tasks.extract-1.outputs.records }}"},
                    {
                        "task_id": "extract-2",
                        "status": "{{ tasks.extract-2.status }}",
                        "records": "{{ tasks.extract-2.outputs.records }}",
                    },
                ]
            },
            retries=3,
            retry_delay=60,
            timeout=3600,
        ),
    ],
)
runner = Runner(
    dag=dag,
    data_start_interval="2026-01-01T00:00:00Z",
    data_end_interval="2026-01-02T00:00:00Z",
    metadata=SqliteMetadata(path="metadata.db"),
)
```

## Defining Plugins

```python
from typing import ClassVar

from beacon import BasePlugin, Context


class BigQueryCount(BasePlugin):
    """BigQuery Count Task."""

    plugin_name: ClassVar["str"] = "bigquery_count"

    source_system: str
    bucket: str
    prefix: str

    def execute(self, context: Context):
        print("Start counting bigquery")
        print(context["params"]["source_system"])  # type: ignore


class CloudStorageWithPrefix(BasePlugin):
    """Cloud Storage With Prefix Sensor."""

    plugin_name: ClassVar["str"] = "cloud_storage_with_prefix"

    source_system: str
    bucket: str
    prefix: str

    def execute(self, context: Context):
        print("Start checking cloud storage with prefix")
        print(context["params"]["source_system"])  # type: ignore
```
