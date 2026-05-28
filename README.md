# Beacon

An everyday workflow orchestrator.

---

## Examples

This is the full example of a workflow that this beacon package support.

```python
from beacon import Runner, Workflow, Task, Group, Param, Sensor
from beacon.callbacks import OnStart, OnFailure, OnTaskFailure
from beacon.providers.msteam import msteam_adaptive_card
from beacon.providers.smtp import send_mail


wf = Workflow(
    id="hello-world",
    desc="An example workflow description",
    params=[
        Param(name="source_system", type="str", default="example"),
    ],
    callbacks={
        OnStart(hook=msteam_adaptive_card("https://my-webhook-url.com")),
        OnFailure(hook=send_mail("on-call@email.com")),
    },
    tasks=[
        Sensor(
            id="start",
            uses="bigquery_check",
            inputs={
                "source_system": "{{ params.source_system }}",
                "bucket": "my-bucket",
                "prefix": "my-prefix/{{ data_interval_start | utc | fmt('year=%Y/month=%m/day=%d/hour=%H') }}",
            }
        ),
        Group(
            id="extract",
            upstreams=["start"],
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
    workflow=wf,
    data_start_interval="2026-01-01T00:00:00Z",
    data_end_interval="2026-01-02T00:00:00Z",
    metadata=MetadataSQLite(path="metadata.db"),
)
```

!!! note "YAML Template"

    You can define workflow via YAML template:

    ```yaml
    id: hello-world
    desc: An example workflow description
    params:
      - name: source_system
        type: str
        default: example
    callbacks:
      - on_start:
          hook: msteam_adaptive_card
          args:
            - https://my-webhook-url.com
      - on_failure:
          hook: send_mail
          args:
            - "on-call@email.com"
    tasks:
      - id: start
        uses: empty
      - id: extract
        upstreams: [start]
        tasks:
          - id: extract-1
            uses: bigquery_count
            inputs:
              source_system: "{{ params.source_system }}"
              bucket: my-bucket
              prefix: "my-prefix/{{ data_interval_start | utc | fmt('year=%Y/month=%m/day=%d/hour=%H') }}"
            callbacks:
              - on_task_failure:
                  hook: send_mail
                  args:
                    - "owner@email.com"
            - id: extract-2
              upstream: [extract-1]
              uses: bigquery_count
              inputs:
                source_system: "{{ params.source_system }}"
                bucket: my-bucket
                prefix: "my-prefix/{{ data_interval_start | utc | fmt('year=%Y/month=%m/day=%d/hour=%H') }}"
              callbacks:
                - on_task_failure:
                    hook: send_mail
                    args:
                      - "owner@email.com"
        - id: end
          upstream: [extract, start]
          trigger_rule: all_done
          uses: send_logs
          inputs:
            logs:
              - task_id: extract-1
                status: "{{ tasks.extract-1.status }}"
                records: "{{ tasks.extract-1.outputs.records }}"
              - task_id: extract-2
                status: "{{ tasks.extract-2.status }}"
                records: "{{ tasks.extract-2.outputs.records }}"
    ```

## Custom Using Task

```python
from typing import Literal

from pydantic import BaseModel

from beacon import BaseBuilder
from beacon.models.context import Context


class BigQueryCountInput(BaseModel):
    source_system: str
    bucket: str
    prefix: str


class BigQueryCount(BaseBuilder):
    """BigQuery Count Task."""

    using: Literal["str"] = "empty"
    inputs: BigQueryCountInput

    def execute(self, context: Context):
        print(self.inputs)  # type: ignore
        print(context["params"]["source_system"])  # type: ignore
```
