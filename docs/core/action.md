# Action

An action is the core unit of work in a beacon DAG. Each action references a
plugin via `uses` and declares its dependencies via `upstream`.

=== "YAML Example"

    ```yaml
    actions:
      - id: extract
        type: task
        uses: py
        inputs:
          py_file: ./scripts/extract.py

      - id: transform
        type: task
        upstream: [extract]
        uses: py
        inputs:
          py_file: ./scripts/transform.py
        retries: 2
        callbacks:
          - on_event: failure
            hook: json-file
            inputs:
              alert_dir: ./alerts
    ```

=== "Python Example"

    ```python
    from beacon import Task, OnTaskEvent

    task = Task(
        id="transform",
        upstream=["extract"],
        uses="py",
        inputs={"py_file": "./scripts/transform.py"},
        retries=2,
        callbacks=[
            OnTaskEvent(on_event="failure", hook="json-file", inputs={"alert_dir": "./alerts"}),
        ],
    )
    ```

---

## Core Action Fields

Common to all action types:

| Field       | Type   | Description                                                          |
|-------------|--------|----------------------------------------------------------------------|
| id          | str    | Unique identifier within the DAG                                     |
| type        | str    | `task`, `sensor`, `branch`, `short_circuit`, `group`                 |
| uses        | str    | Plugin name to execute                                               |
| inputs      | dict   | Parameters passed to the plugin (supports Jinja)                     |
| upstream    | list   | Task IDs that must complete before this action runs                  |
| callbacks   | list[OnTaskEvent] | Callbacks fired on events (`start`, `success`, `failure`, `retry`, `skipped`) |

---

## Action Types

### Task

The standard action. Runs a plugin, stores outputs, schedules all downstream.

| Field               | Type | Default | Description                                |
|---------------------|------|---------|--------------------------------------------|
| retries             | int  | 0       | Max retry attempts on failure              |
| retry_delay         | int  | 10      | Base delay (seconds) between retries       |
| execution_timeout   | int  | None    | Max seconds per attempt                    |
| exponential_backoff | bool | true    | Double delay on each retry                 |

**Plugin contract:** Return `dict` (stored as outputs) or `None`.

**Downstream behavior:** On SUCCESS → schedule all downstream. On FAILED → mark downstream UPSTREAM_FAILED. On SKIPPED → mark downstream SKIPPED.

```yaml
- id: process
  type: task
  uses: py
  inputs:
    py_file: ./process.py
  retries: 3
```

#### Controlling Retry from Inside a Plugin

Plugins decide retry behavior by which exception they raise:

| Raises | Behavior |
|---|---|
| Any `Exception` | Retry up to `retries`, then `FAILED` |
| `TaskFailed` | Immediately `FAILED` — skip remaining retries |
| `TaskSkipped` | Mark `SKIPPED` — skip remaining retries |

```python
from beacon.errors import TaskFailed, TaskSkipped

async def execute(self, context: Context) -> dict:
    try:
        rows = await fetch(self.source)
    except ConnectionTimeout:
        raise  # Generic exception → beacon retries

    if not await schema_exists(self.target):
        raise TaskFailed("Target schema missing — no point retrying")

    if not rows:
        raise TaskSkipped("No new data this run")

    return {"rows": len(rows)}
```

---

### Sensor

Waits for an external condition. The plugin contains an async poke loop.

| Field               | Type   | Default | Description                                    |
|---------------------|--------|---------|------------------------------------------------|
| check_interval      | int    | 60      | Seconds between condition checks               |
| execution_timeout   | int    | None    | Max wait time before failing                   |
| exponential_backoff | bool   | true    | Increase interval between checks               |
| fail_mode           | str    | soft    | `soft` = fail task, `silent` = skip downstream |

**Plugin contract:** Loop with `await asyncio.sleep()` until condition met, then return.

**Downstream behavior:**
- Condition met → SUCCESS → schedule all downstream
- Timeout → FAILED (or SKIPPED if `fail_mode: silent`)

```yaml
- id: wait-for-file
  type: sensor
  uses: gcs-sensor
  inputs:
    bucket: my-bucket
    prefix: raw/2026-06-03/
  check_interval: 30
  execution_timeout: 3600
  fail_mode: silent
```

**How the sensor plugin works (async-first, no separate poke mode):**

```python
class GcsSensor(BasePlugin):
    plugin_name: ClassVar[str] = "gcs-sensor"
    bucket: str
    prefix: str

    async def execute(self, context: Context) -> dict:
        from google.cloud import storage
        client = storage.Client()
        while True:
            blobs = list(client.list_blobs(self.bucket, prefix=self.prefix))
            if blobs:
                return {"files_found": len(blobs)}
            # Yield event loop — no worker slot wasted
            await asyncio.sleep(context.get("check_interval", 60))
```

The executor wraps this in `asyncio.timeout(execution_timeout)`. If it times out:
- `fail_mode: soft` → FAILED → downstream gets UPSTREAM_FAILED
- `fail_mode: silent` → SKIPPED → downstream gets SKIPPED

---

### Branch

Executes a plugin that **chooses which downstream path(s) to take**.
Unchosen paths are SKIPPED.

| Field   | Type | Description                              |
|---------|------|------------------------------------------|
| success | list | Default downstream if plugin returns truthy |
| failure | list | Default downstream if plugin returns falsy  |

**Plugin contract:** Return `{"branch": ["task-id-1", "task-id-2"]}` — the list of
downstream task IDs to schedule. All others in success + failure are SKIPPED.

**Downstream behavior:**
- Tasks in returned list → SCHEDULED
- Tasks NOT in returned list → SKIPPED

```yaml
- id: check-quality
  type: branch
  uses: py
  inputs:
    py_file: ./scripts/check_quality.py
  success: [process-good]
  failure: [quarantine, alert]

- id: process-good
  type: task
  upstream: [check-quality]
  uses: py
  inputs:
    py_file: ./process_good.py

- id: quarantine
  type: task
  upstream: [check-quality]
  uses: py
  inputs:
    py_file: ./quarantine.py
```

```python
# scripts/check_quality.py
def main():
    if data_passes_validation():
        return {"branch": ["process-good"]}
    else:
        return {"branch": ["quarantine", "alert"]}
```

---

### ShortCircuit

Runs a plugin that returns `True` (continue) or `False` (skip all downstream).
Used for "should this DAG continue?" checks.

**Plugin contract:** Return `{"continue": True}` or `{"continue": False}`.

**Downstream behavior:**
- `continue: True` → schedule all downstream (same as Task)
- `continue: False` → SKIP all downstream tasks **recursively**

```yaml
- id: should-run-today
  type: short_circuit
  uses: py
  inputs:
    py_file: ./scripts/check_if_needed.py

- id: expensive-etl
  type: task
  upstream: [should-run-today]
  uses: py
  inputs:
    py_file: ./scripts/etl.py
```

```python
# scripts/check_if_needed.py
def main():
    if already_processed_today():
        return {"continue": False}  # skip entire downstream chain
    return {"continue": True}
```

---

### Group

Container for nested actions. Groups have no runtime behavior of their own —
they are flattened by the scheduler. Use groups to organize related actions
visually and to apply shared upstream dependencies.

```yaml
- id: ingest-stage
  type: group
  upstream: [start]
  actions:
    - id: extract-customers
      type: task
      uses: py
      inputs: { py_file: ./extract_customers.py }
    - id: extract-orders
      type: task
      uses: py
      inputs: { py_file: ./extract_orders.py }
```

---

## How Action Types Affect Scheduling

After a task completes, the scheduler calls `action.evaluate_downstream()`:

```text
┌────────────────────────────────────────────────────────────────────┐
│ Task completes → SUCCESS                                            │
│                                                                    │
│ Scheduler:                                                          │
│   1. Get action definition from DAG                                 │
│   2. Call action.evaluate_downstream(task_ctx, downstream_ids)      │
│   3. Returns DownstreamDirective(schedule=[...], skip=[...])        │
│   4. For each in `schedule` → set SCHEDULED → QUEUED               │
│   5. For each in `skip` → set SKIPPED                               │
│   6. For each downstream of SKIPPED → set SKIPPED (transitive)      │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

| Action Type    | `evaluate_downstream()` Logic                           |
|----------------|--------------------------------------------------------|
| Task           | Schedule all downstream (default)                       |
| Sensor         | Schedule all downstream (condition was met)             |
| Branch         | Schedule only `outputs["branch"]` list, skip rest       |
| ShortCircuit   | If `outputs["continue"]` is False, skip ALL downstream  |
| Group          | (not invoked — group is flattened, not executed)        |

---

## Output Conventions Per Action Type

| Action Type  | Expected Plugin Output                    | Used For                    |
|--------------|-------------------------------------------|-----------------------------|
| Task         | `{"key": "value", ...}` (any dict)        | Upstream outputs for downstream |
| Sensor       | `{"condition_met": True, ...}`            | Proof that condition was met |
| Branch       | `{"branch": ["task-a", "task-b"]}`        | Which path to take          |
| ShortCircuit | `{"continue": True/False}`                | Whether to proceed          |

All outputs are stored in `TaskContext.outputs` and available to downstream via
`{{ outputs.task_id.key }}` or `load_context().upstream_outputs["task_id"]`.
