# Beacon Reference

Single technical reference for Beacon. Covers architecture, models,
action types, templating, executor, metadata, and configuration.
Companion docs: [`deploy.md`](./deploy.md) (bundle / sync / variable
scoping) and [`roadmap.md`](./roadmap.md) (status + production plan).

---

## 1. Positioning & Principles

_Beacon_ is a lean, async-first workflow orchestrator. Target: teams that
need production-grade orchestration without Airflow's operational
complexity.

1. **Async-first.** Every action executes via `async def execute()`.
   No deferrable/triggerer split. One execution model.
2. **Plugin is the unit of logic.** DAGs reference plugins via `uses`;
   DAG files are pure configuration.
3. **Simple path is trivial.** Write a Python function, reference it
   with `uses: py`. No operator inheritance, no provider install.
4. **Reusable DAGs.** A `Dag` is *what to do*; a `Deployment` is
   *how/when* (cron, variable overrides). 1 DAG → N Deployments.
5. **Executor-agnostic.** `TaskContext` is serializable; the same task
   runs on local / Docker / K8s / Batch unchanged.
6. **Stateless runtime.** All state in the metadata store. Restart-safe.
7. **Horizontally scalable.** API server + scheduler can scale out with
   coordination via metadata store.

### Diff vs Airflow

| Aspect           | Airflow                              | Beacon                                |
|------------------|--------------------------------------|---------------------------------------|
| Execution        | Sync + Deferrable (two paths)        | Async-only                            |
| Plugin install   | `pip install` into runtime           | Registry lookup or `./plugins/` file  |
| DAG parsing      | Every scheduler heartbeat            | Parse once, version-tag, cache        |
| Sensor           | Holds a slot OR deferred             | `await asyncio.sleep()` — no slot use |
| Cross-task data  | XCom (unbounded shuttle)             | TaskContext (per-task, bounded)       |
| Remote execution | KubernetesExecutor + sidecar         | Executor reads TaskContext from store |
| DAG reuse        | 1 file = 1 DAG = 1 schedule          | 1 DAG, N Deployments                  |
| Catch-up         | All missed runs fired immediately    | Batched with `max_active_runs`        |
| Scaling          | Multi-scheduler + DB tuning          | Merged API + Scheduler + coordination |

---

## 2. System Architecture

### Phase 1 — Local (current, shipped)

```text
┌─────────────────────────────────────────────────────────┐
│                    USER API (Dag)                       │
├─────────────────────────────────────────────────────────┤
│  dag.plan()       validate templates + graph            │
│  dag.run()        one-shot execution                    │
│  dag.test()       tempdir one-shot + pass/fail          │
│  dag.clear()      fix bug → rerun task(s)               │
│  dag.mark()       force state → fire teardowns          │
│  dag.fail()       shorthand for mark(state="failed")    │
│  dag.backfill()   run N dates (skip/reset existing)     │
└──────────────────────────┬──────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────┐
│                  DagRunner (async)                      │
│  run(resume=False|True), clear(...), mark(...)          │
└──────────────────────────┬──────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────┐
│                Worker + Executor                        │
│  Worker: queue → resolve upstream outputs → dispatch    │
│  LocalExecutor.run_task():                              │
│    try:    plugin.execute(context)                      │
│    finally:plugin.teardown(context)  ← always fires     │
└──────────────────────────┬──────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────┐
│             Plugin (BasePlugin)                         │
│  execute(context) → dict     abstractmethod             │
│  teardown(context) → None    default no-op              │
└──────────────────────────┬──────────────────────────────┘
                           ▼
┌────────────────────────────────────────────────────────────────────────┐
│             Metadata Store (LocalMetadata)                             │
│  Sharded JSON, async I/O, LRU cache, atomic writes                     │
│    {path}/dag_runs/dag_id={dag_id}/{run_id}.json                       │
│    {path}/task_contexts/dag_id={dag_id}/run_id={run_id}/{task_id}.json │
│    {path}/task_states/dag_id={dag_id}/run_id={run_id}/{task_id}.json   │
│    {path}/.locks/                         ← coordination locks         │
└────────────────────────────────────────────────────────────────────────┘
```

### Phase 2 — Production Service (current)

Beacon now ships with a merged **API Server + Scheduler** that can scale
horizontally with coordination via the metadata store.

```text
┌─────────────────────────────────────────────────────────────────────────┐
│                 Merged API Server + Scheduler (N instances)             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                   │
│  │ Instance 1   │  │ Instance 2   │  │ Instance 3   │                   │
│  │ (inst-a1b2)  │  │ (inst-c3d4)  │  │ (inst-e5f6)  │                   │
│  │              │  │              │  │              │                   │
│  │ - REST API   │  │ - REST API   │  │ - REST API   │                   │
│  │ - Scheduler  │  │ - Scheduler  │  │ - Scheduler  │                   │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘                   │
│         │                 │                 │                            │
│         └─────────────────┼─────────────────┘                            │
│                           │                                              │
│                           ▼                                              │
│                  ┌─────────────────┐                                     │
│                  │ LocalMetadata   │                                     │
│                  │ (file locks)    │                                     │
│                  │                 │                                     │
│                  │ .locks/         │                                     │
│                  │  scheduled_*    │ → Run deduplication                 │
│                  │  deployment_*   │ → Scheduler state coordination      │
│                  │  trigger_*      │ → Trigger claiming                  │
│                  └─────────────────┘                                     │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Multi-Instance Coordination

When multiple API server instances run concurrently, they coordinate via
the metadata store to prevent duplicate runs:

| Coordination Method            | Purpose                          | How It Works                                               |
|--------------------------------|----------------------------------|------------------------------------------------------------|
| `try_create_scheduled_run()`   | Prevent duplicate scheduled runs | Atomic create with (dag_id, logical_date) uniqueness check |
| `try_update_scheduler_state()` | Claim scheduler tick             | Atomic update of last_scheduled_at only if newer           |
| `try_claim_trigger()`          | Claim manual trigger             | Atomic claim with instance_id tracking                     |
| `drain_triggers_with_claim()`  | Drain claimed triggers           | Returns only triggers claimed by this instance             |

**LocalMetadata** implements coordination using file-based locks (`fcntl.flock`):

```text
metadata.db/
├── .locks/
│   ├── scheduled_{dag_id}_{logical_date}.lock  ← Run creation lock
│   ├── deployment_{deployment_id}.lock          ← Scheduler state lock
│   └── trigger_{trigger_id}.lock                ← Trigger claim lock
```

For **SqliteMetadata** or **PostgresMetadata**, coordination uses database
transactions with `UNIQUE` constraints and `ON CONFLICT DO NOTHING`.

### Component Matrix

| Component        | Phase 1                | Phase 2 (current)               |
|------------------|------------------------|---------------------------------|
| Metadata         | `LocalMetadata`        | `LocalMetadata` (with locks)    |
| Executor         | `LocalExecutor`        | `LocalExecutor`                 |
| Queue            | `asyncio.Queue`        | `asyncio.Queue`                 |
| Scheduling       | `DagRunner` only       | `DeploymentScheduler` + API     |
| Process          | `dag.run()` in-process | `beacon api` (merged)           |
| Coordination     | N/A                    | File locks / DB constraints     |

---

## 3. Model Hierarchy

```text
Plugin ──── execution logic (execute + teardown)
   │
   ▼
Action ──── references a plugin via `uses`, provides `inputs`
   │
   ▼         types: Task, Sensor, Branch, ShortCircuit, Group
Dag ──────── reusable template: actions + deps + callbacks
   │
   ▼
Deployment ─ binds a Dag to: cron + tz + variable_overrides
             N Deployments → 1 Dag
```

### Dag vs Deployment

```text
Dag(id="extract-load-table")
  │
  ├── Deployment(id="daily-customers-from-postgres",
  │              cron="0 2 * * *", variable_overrides={source: postgres}, owners=["tom"])
  └── Deployment(id="hourly-orders-from-mysql",
                 cron="0 * * * *", variable_overrides={source: mysql}, owners=["sara"])
```

A `Deployment` carries:
- Identity (`Deployment.id` shown in UI)
- Schedule (`cron`, `start_date`, `end_date`, `timezone`, `catch_up`)
- Concurrency (`max_active_runs` — limit concurrent runs per deployment)
- Variable overrides (`variable_overrides` — layered on top of the
  bundle's scoped `variables.yml` / `global_variables.yml` chain)
- Variable requirements (`variable_requirements` — extracted from DAG
  templates at deploy time via `--bundle`; used to validate triggers)
- Version pin (`dag_version`, optional)
- Owners, labels, `enabled` flag

### Catch-up Scheduling

When a deployment is created with `catch_up=True` and a past `start_date`:

```yaml
id: daily-etl
dag_id: etl-pipeline
cron: "0 0 * * *"
start_date: "2026-03-01T00:00:00"  # 3 months ago
catch_up: true
max_active_runs: 2  # limit concurrent backfill runs
```

On first scheduler start:

```text
Catch-up for daily-etl: 97 missed run(s) from 2026-03-01 to 2026-06-05
RUN daily-etl (scheduled) → dag=etl-pipeline run_id=scheduled-etl-pipeline-20260301T000000
RUN daily-etl (scheduled) → dag=etl-pipeline run_id=scheduled-etl-pipeline-20260302T000000
Catch-up paused for daily-etl: max_active_runs (2) reached, 95 run(s) remaining
```

As runs complete, remaining catch-up runs are scheduled.

**Difference from Airflow:**

| Airflow                             | Beacon                            |
|-------------------------------------|-----------------------------------|
| All missed runs queued immediately  | Batching via `max_active_runs`    |
| No per-deployment concurrency limit | `max_active_runs` per deployment  |
| Run order depends on DB query       | Explicit ASC order (oldest first) |

### Dag user API (Phase 1)

| Method                           | Purpose                              | When to use               |
|----------------------------------|--------------------------------------|---------------------------|
| `dag.plan()`                     | Validate graph + templates           | Before deploy / in CI     |
| `dag.run()`                      | Execute once, persist state          | Manual trigger            |
| `dag.test()`                     | Execute in tempdir, report pass/fail | Development               |
| `dag.clear(task_id=...)`         | Reset task + downstream → rerun      | Fix bug, rerun            |
| `dag.mark(state=...)`            | Force task to terminal state         | Kill stuck task / unblock |
| `dag.fail(task_id=...)`          | Shorthand for `mark(state="failed")` | Kill + clean up           |
| `dag.backfill(start, end, cron)` | Run N dates in range                 | Historical reprocessing   |

`DagRunner` is the async engine behind these — graph traversal, trigger
rule evaluation, branch / short-circuit propagation, teardown scheduling,
DAG-level callbacks, `resume=True` for re-execution of cleared tasks.

---

## 4. Plugin System

### Contract

Every plugin is a plain Pydantic model that inherits from a single base class:

```python
from beacon import BasePlugin, Context

class MyTask(BasePlugin, plugin_name="my-task"):
    # Typed inputs — validated by Pydantic, Jinja-rendered before instantiation
    source: str
    target: str

    async def execute(self, context: Context):
        """Main logic. Return a dict of outputs, any other value, or None."""
        ...

    async def teardown(self, context: Context) -> None:
        """Cleanup. ALWAYS fires after execute (success or failure).
        Default: no-op. Override for resource cleanup."""
        ...
```

If `plugin_name` is not provided, the snake_case of the class name is used
(e.g., `MyTask` → `my_task`).

A plugin can be used with **any** action type (`task`, `sensor`, `branch`,
`short_circuit`). The action model owns the interpretation of the plugin's
return value — see §5 and `action.extract_outputs`.

### Raise Strategy (controls task flow)

Plugins express control flow by raising exceptions, not by returning specific
dict shapes. This keeps the plugin's logic clean and independent of the
surrounding action type.

| Raises          | Behavior                                      | Use case                          |
|-----------------|-----------------------------------------------|-----------------------------------|
| Any `Exception` | Retry up to `retries`, then `FAILED`          | Transient error (network timeout) |
| `TaskRetry`     | Explicitly consume one retry slot, then retry | Intentional retry with a message  |
| `TaskFailed`    | Immediately `FAILED`, exhaust retries         | Permanent failure (table missing) |
| `TaskSkipped`   | Mark `SKIPPED`, no retries                    | Nothing to do (empty partition)   |

Default when `execute` returns without raising: **success**. For branch and
short_circuit actions this also means "take the success / continue path".

```python
from beacon import BasePlugin, TaskFailed, TaskRetry, TaskSkipped, Context

class MyPlugin(BasePlugin, plugin_name="my-plugin"):
    threshold: float

    async def execute(self, context: Context):
        score = await get_score()

        if score < 0:
            raise TaskFailed("invalid score — no point retrying")
        if score == 0:
            raise TaskSkipped("nothing to process this run")
        if score < self.threshold:
            raise TaskRetry(f"score {score} below threshold, will retry")

        return {"score": score}   # dict → stored as task outputs
```

### Plugin Families

Use an abstract base class for shared configuration. All concrete subclasses
inherit from `BasePlugin` directly — no action-type-specific intermediaries:

```python
from abc import ABC, abstractmethod
from beacon import BasePlugin

class GcsBase(BasePlugin, ABC):
    """Shared config — NOT registered (abstract execute keeps it out of registry)."""
    bucket: str
    prefix: str = ""

    @abstractmethod
    async def execute(self, context): ...

    async def _list_files(self) -> list[str]:
        ...  # Shared GCS logic

class GcsCopy(GcsBase, plugin_name="gcs-copy"):
    """Works with a task action."""
    dest_bucket: str

    async def execute(self, context):
        count = await self._copy_files(self.dest_bucket)
        return {"files_copied": count}

class GcsSensor(GcsBase, plugin_name="gcs-sensor"):
    """Works with a sensor action (or any other)."""

    async def execute(self, context):
        import asyncio
        check_interval = context.get("check_interval", 60)
        while True:
            files = await self._list_files()
            if files:
                return {"files_found": len(files)}
            await asyncio.sleep(check_interval)
```

No inheritance chains beyond the abstract base. No mixins. No
`template_fields` — every string input is Jinja-rendered.

### Resolution

The `uses` field resolves a plugin from the registry, in order:

1. Built-in `PLUGINS_REGISTRY` (standard provider)
2. Entry-point discovered plugins (`beacon.plugins` group in `pyproject.toml`)
3. Local `./plugins/` directory (auto-discovered from bundle path)
4. Remote plugins (`org/repo@version` or `package@version`)

```text
uses: "py"                       → built-in
uses: "gcs-extract"              → ./plugins/gcs_extract.py
uses: "my-org/etl@1.2.0"         → remote (GitHub)
uses: "beacon-gcs@2.0.0"         → remote (PyPI)
```

### Two ways to add logic

| Pattern        | When                       | How                                   |
|----------------|----------------------------|---------------------------------------|
| `uses: py`     | One-off function, no reuse | Write `main()` in a `.py` file        |
| Custom plugin  | Reusable across DAGs       | Subclass `BasePlugin` in `./plugins/` |

Example `uses: py` flow:

```yaml title="dag.yml"
- id: transform
  type: task
  uses: py
  inputs:
    py_statement: ../scripts/transform.py
    py_function: main
    params: { source: "{{ params.source_system }}" }
```

```python title="scripts/transform.py"
from beacon import load_context

def main(source: str):
    ctx = load_context()
    ctx.logger.info("Processing %s (attempt %d)", source, ctx.attempt_number)
    return {"rows_processed": 1000}
```

---

## 5. Actions

An action is the unit of work in a DAG. Common fields:

| Field     | Type              | Description                                              |
|-----------|-------------------|----------------------------------------------------------|
| id        | str               | Unique within the DAG                                    |
| type      | str               | `task` / `sensor` / `branch` / `short_circuit` / `group` |
| uses      | str               | Plugin name to execute                                   |
| inputs    | dict              | Parameters passed to the plugin (Jinja-templated)        |
| upstream  | list[str]         | Task IDs that must complete first                        |
| callbacks | list[OnTaskEvent] | Fire on `start`/`success`/`failure`/`retry`/`skipped`    |
| teardown  | str               | Marks this task as teardown for `<task_id>` (see §8)     |

### 5.1 Task

| Field               | Type | Default | Description                          |
|---------------------|------|---------|--------------------------------------|
| retries             | int  | 0       | Max retry attempts on failure        |
| retry_delay         | int  | 10      | Base delay (seconds) between retries |
| execution_timeout   | int  | None    | Max seconds per attempt              |
| exponential_backoff | bool | true    | Double delay on each retry           |

**Plugin output:** `dict` (stored as outputs) or `None`.

**Downstream:** SUCCESS → schedule all. FAILED → downstream
UPSTREAM_FAILED. SKIPPED → downstream SKIPPED.

### 5.2 Sensor

Waits for an external condition. Plugin runs an async poke loop.

| Field               | Type | Default | Description                                                   |
|---------------------|------|---------|---------------------------------------------------------------|
| check_interval      | int  | 60      | Seconds between condition checks                              |
| execution_timeout   | int  | None    | Max wait time before failing                                  |
| exponential_backoff | bool | true    | Increase interval between checks                              |
| fail_mode           | str  | soft    | `soft` = SKIPPED on timeout; `silent` = SKIPPED on any errors |

```python
from beacon import BasePlugin, Context

class GcsSensor(BasePlugin, plugin_name="gcs-sensor"):
    bucket: str
    prefix: str

    async def execute(self, context: Context):
        from google.cloud import storage
        client = storage.Client()
        while True:
            blobs = list(client.list_blobs(self.bucket, prefix=self.prefix))
            if blobs:
                return {"files_found": len(blobs)}
            await asyncio.sleep(context.get("check_interval", 60))
```

Executor wraps in `asyncio.timeout(execution_timeout)`. The `await
asyncio.sleep()` yields the event loop — no worker slot wasted, no
separate triggerer.

### 5.3 Branch

Chooses which downstream path(s) to schedule. Unchosen → SKIPPED.

| Field   | Type      | Description                                         |
|---------|-----------|-----------------------------------------------------|
| success | list[str] | Tasks scheduled when plugin resolves truthy/default |
| failure | list[str] | Tasks scheduled when plugin resolves falsy          |

**Plugin return value** is interpreted by `Branch.extract_outputs` before
`evaluate_downstream` sees it:

| Plugin returns      | Result                                        |
|---------------------|-----------------------------------------------|
| `None` / no return  | `success` path (default)                      |
| `True`              | `success` path                                |
| `False`             | `failure` path                                |
| `list[str]`         | Those exact task IDs are scheduled            |
| `str`               | That single task ID is scheduled              |
| `{"branch": [...]}` | Used directly (explicit, backward-compatible) |

The plugin does **not** need to know the action's task ID lists. Use the
raise strategy to deviate from the default success path:

```yaml
- id: check-quality
  type: branch
  uses: py
  inputs: { py_statement: ./scripts/check_quality.py }
  success: [process-good]
  failure: [quarantine, alert]
```

```python title="scripts/check_quality.py"
# Return True/False — Branch handles routing
def main():
    score = run_quality_check()
    return score >= 0.9       # True → process-good, False → quarantine+alert

# Or return a list to choose specific paths
def main():
    if score >= 0.95:
        return ["process-good"]           # skip quarantine, skip alert
    if score >= 0.7:
        return ["process-good", "alert"]  # process but also alert
    return ["quarantine", "alert"]

# Or raise to skip entirely
def main():
    from beacon import TaskSkipped
    if not data_available():
        raise TaskSkipped("no data — skip branching entirely")
    return True
```

### 5.4 ShortCircuit

Conditionally skips all downstream tasks. `False` → SKIP ALL downstream
**recursively**.

**Plugin return value** is interpreted by `ShortCircuit.extract_outputs`:

| Plugin returns       | Result                               |
|----------------------|--------------------------------------|
| `None` / no return   | Continue (default)                   |
| `True`               | Continue                             |
| `False`              | Skip all downstream                  |
| `{"continue": bool}` | Used directly (backward-compatible)  |

```yaml
- id: should-run-today
  type: short_circuit
  uses: py
  inputs: { py_statement: ./scripts/check_if_needed.py }
```

```python title="scripts/check_if_needed.py"
def main():
    from datetime import datetime
    # False → skip all downstream; True (or no return) → continue
    return datetime.now().weekday() < 5   # only run on weekdays
```

### 5.5 Group

Container for nested actions. No runtime behavior — flattened by the
runner. Use to organize visually and apply shared upstream deps.

```yaml
- id: ingest-stage
  type: group
  upstream: [start]
  actions:
    - { id: extract-customers, type: task, uses: py, inputs: { py_statement: ./extract_customers.py } }
    - { id: extract-orders,    type: task, uses: py, inputs: { py_statement: ./extract_orders.py } }
```

### Downstream scheduling per action type

After a task succeeds the runner calls:
1. `action.extract_outputs(raw_outputs)` — normalizes the plugin's raw return
   value into a structured dict stored on `task_ctx.outputs`.
2. `action.evaluate_downstream(task_ctx, downstream_ids)` — reads from the
   normalized outputs and returns `DownstreamDirective(schedule=[...], skip=[...])`.

| Action Type    | `extract_outputs` does                                     | `evaluate_downstream` logic                     |
|----------------|------------------------------------------------------------|-------------------------------------------------|
| Task           | Strips `_result` wrapper; passes dict through unchanged    | Schedule all downstream                         |
| Sensor         | Same as Task                                               | Schedule all downstream (condition met)         |
| Branch         | Maps return value → `{"branch": [...]}`; default → success | Schedule `outputs["branch"]`, skip the rest     |
| ShortCircuit   | Maps return value → `{"continue": bool}`; default → `True` | `continue=False` → skip ALL downstream          |
| Group          | Not invoked — flattened at parse time                      | —                                               |

### Plugin outputs accessible to downstream tasks

| Action Type  | What is stored in `TaskContext.outputs`   | Accessible via                            |
|--------------|-------------------------------------------|-------------------------------------------|
| Task         | Whatever `dict` the plugin returned       | `{{ outputs.task_id.key }}`               |
| Sensor       | Whatever `dict` the plugin returned       | `{{ outputs.task_id.key }}`               |
| Branch       | `{"branch": ["chosen-task-id", ...]}`     | Routing only — not typically used further |
| ShortCircuit | `{"continue": True \| False}`             | Routing only — not typically used further |

All outputs are stored in `TaskContext.outputs` and accessible via
`{{ outputs.task_id.key }}` or `load_context().upstream_outputs["task_id"]`.

---

## 6. Callbacks

Same registry + resolution as plugins. A callback is a class that runs
on a lifecycle event instead of as a DAG step.

```python
from typing import ClassVar, Any
from beacon import Callback

class MyCallback(Callback):
    hook_name: ClassVar[str] = "my-callback"
    def __init__(self, webhook_url: str) -> None:
        self.webhook_url = webhook_url
    async def notify(self, event: str, data: dict[str, Any]) -> None: ...
```

```yaml
callbacks:
  - on_event: failure
    hook: "my-org/msteam-callback@1.1.0"
    inputs: { webhook_url: "https://...", channel: "alerts" }
```

| Owner         | Events                                            |
|---------------|---------------------------------------------------|
| `OnTaskEvent` | `start`, `success`, `failure`, `retry`, `skipped` |
| `OnDagEvent`  | `start`, `success`, `failure`, `finished`         |

`finished` fires on any DAG terminal state (success or failure).

---

## 7. TaskContext, Attempts, State Machine

### TaskContext

Serializable unit of work stored in the metadata store. Carries
everything needed to execute in any environment + accumulates attempt
history across retries.

```python
class TaskContext(BaseModel):
    # Identity
    run_id: str
    dag_id: str
    task_id: str
    dag_version: str

    # Time
    run_date: datetime
    logical_date: datetime
    data_interval_start: datetime
    data_interval_end: datetime

    # Inputs (fully resolved before execution)
    params: dict[str, Any]       # Deployment params (Jinja-rendered with vars)
    inputs: dict[str, Any]       # Task inputs   (Jinja-rendered with params)
    plugin_name: str

    # Execution config
    retries: int
    retry_delay: int
    execution_timeout: int | None
    exponential_backoff: bool

    # Attempts
    attempts: list[Attempt]

    # Data
    upstream_outputs: dict[str, dict[str, Any]]  # read-only, populated by worker
    outputs: dict[str, Any]                       # written on success

    @property
    def attempt_number(self) -> int: ...
```

**Not XCom.** Difference table:

| XCom (Airflow)                        | TaskContext (Beacon)                     |
|---------------------------------------|------------------------------------------|
| Data BETWEEN tasks                    | Data TO/FROM a single task               |
| Pulled by downstream at runtime       | Stored in metadata, read by executor     |
| Grows unbounded                       | Fixed structure, bounded per task        |
| Tight coupling                        | Tasks independent; share via storage     |

### Attempt

```python
class Attempt(BaseModel):
    attempt_number: int       # 1-based
    state: AttemptStatus      # running | success | failed | skipped | timed_out
    started_at: datetime
    ended_at: datetime
    duration_sec: float
    error: str | None
    error_traceback: str
    executor: str             # "local" | "docker" | "k8s" | "aws_batch"
    executor_ref: str | None  # "pod-abc123", "container-xyz", ...
```

Each retry is its own `Attempt` with its own log file. UI shows attempt
tabs with executor type+ref, duration, error/traceback, outputs.

### State machine

```text
NONE ──┬──> SCHEDULED ──> QUEUED ──> RUNNING ──┬──> SUCCESS
       │                                       ├──> FAILED
       │                                       ├──> SKIPPED  (TaskSkipped raised)
       │                                       └──> UP_FOR_RETRY ──> QUEUED (retry)
       ├──> SKIPPED            (trigger rule)
       └──> UPSTREAM_FAILED    (upstream failed)
```

| From            | To                | Triggered by                       |
|-----------------|-------------------|------------------------------------|
| `none`          | `scheduled`       | Scheduler: deps satisfied          |
| `none`          | `skipped`         | Scheduler: trigger rule skip       |
| `none`          | `upstream_failed` | Scheduler: upstream failed         |
| `none`          | `removed`         | DAG edit during run                |
| `scheduled`     | `queued`          | Scheduler: enqueue                 |
| `queued`        | `running`         | Worker: picks from queue           |
| `running`       | `success`         | Plugin completed                   |
| `running`       | `failed`          | Plugin raised; no retries left     |
| `running`       | `skipped`         | Plugin raised `TaskSkipped`        |
| `running`       | `up_for_retry`    | Plugin raised; retries remain      |
| `up_for_retry`  | `queued`          | Worker: after retry delay          |
| `up_for_retry`  | `failed`          | Manual mark / system limit         |

### run_id convention

| Prefix                           | Source                       | Example                         |
|----------------------------------|------------------------------|---------------------------------|
| `manual-{dag_id}-{uuid}`         | `dag.run()` / API trigger    | `manual-etl-a1b2c3d4`           |
| `backfill-{dag_id}-{timestamp}`  | `dag.backfill()`             | `backfill-etl-20260101T000000`  |
| `scheduled-{dag_id}-{timestamp}` | `DeploymentScheduler`        | `scheduled-etl-20260104T020000` |

Use `beacon.run_trigger(run_id)` to classify.

---

## 8. Setup & Teardown

### Two layers

```text
SIMPLE (90% of cases):              COMPLEX (shared resources):
───────────────────────             ────────────────────────────
One task, inline cleanup.           Separate tasks, DAG-level lifecycle.

- id: process                       - id: create-cluster
  uses: py                            uses: py
  inputs:                           - id: etl-1
    py_function: run_spark              upstream: [create-cluster]
    py_teardown: kill_spark         - id: destroy-cluster
                                        teardown: create-cluster
```

### Plugin-level teardown (in-task cleanup)

```python
from beacon import BasePlugin

class SparkPlugin(BasePlugin, plugin_name="spark"):
    app_id: str = ""

    async def execute(self, context):
        self.app_id = await submit_spark(...)
        return {"app_id": self.app_id}

    async def teardown(self, context):
        if self.app_id:
            await kill_spark(self.app_id)
```

`LocalExecutor.run_task()` calls `plugin.teardown(context)` in a
`finally` block — fires on success, failure, timeout.

Same effect via `py` plugin:

```yaml
- id: process
  uses: py
  inputs:
    py_statement: ./spark.py
    py_function: main
    py_teardown: cleanup       # ← always runs after main()
```

### Task-level teardown (DAG-scoped lifecycle)

Any action declares `teardown: <setup_task_id>`. The setup task is just
a normal task — it becomes "setup" implicitly because another task
declares it as its teardown target.

```yaml
- id: create-cluster
  uses: py
  inputs: { py_function: create }

- id: run-etl
  uses: py
  upstream: [create-cluster]
  inputs: { py_function: process }

- id: destroy-cluster
  uses: py
  teardown: create-cluster     # fires after ALL deps of create-cluster terminal
  inputs: { py_function: destroy }
```

**Semantics:**
1. Teardown runs after **all** tasks that (transitively) depend on its
   setup task reach terminal state — success OR failure.
2. Teardown auto-receives setup `outputs` in `upstream_outputs`.
3. Teardown failure is non-fatal — logged as warning. DagRun state
   reflects main task outcomes, not teardown failures.
4. Validated at `plan()` — referenced task must exist; no self-ref.
5. Multiple independent setup/teardown pairs supported in one DAG.

---

## 9. Templating

_Beacon_ uses Jinja2 for **value interpolation only** — not control flow.
Templates resolve to concrete values *before* a plugin runs; plugins
never see Jinja.

```yaml
# ✅ Supported
inputs:
  source: "{{ vars('source_system') }}"
  path:   "{{ vars('base_path') }}/{{ runtime.logical_date }}"
  bucket: "{{ vars('gcs_bucket') }}"
  api_key: "{{ secrets('API_KEY') }}"
  rows:   "{{ outputs.extract.row_count }}"

# ❌ Intentionally not supported (control flow in graph definitions)
{% for item in items %}        # use foreach_task instead (Phase 3)
{% if env == 'prod' %}          # use a Branch action instead
```

Control flow in DAG definitions creates unpredictable graph shapes that
can't be statically validated, versioned, or shown in a UI.

### The Renderer

One class — `beacon.core.Renderer` — handles every template in beacon.

```python
from beacon.core import Renderer, make_vars_func, make_secrets_func

variables = {"source": "postgres", "bucket": "my-bucket"}
vars_func = make_vars_func(variables)
secrets_func = make_secrets_func()

r = Renderer({
    "vars":    vars_func,
    "secrets": secrets_func,
    "outputs": {"extract": {"row_count": 42}},
    "runtime": {...},
})

r.render("{{ vars('source') }}")                   # "postgres"
r.render("{{ vars('db.host', 'localhost') }}")     # nested key with default
r.render("{{ secrets('API_KEY') }}")               # from environment
```

Properties:
- **Recursive.** Walks `dict`, `list`, `tuple`. Non-string scalars pass through.
- **Native-typed.** Pure expressions return real Python types.
  `"{{ 5 + 5 }}"` → `int(10)`, `"{{ [1,2] }}"` → `list`, `"{{ x }}"`
  with `x=None` → `None`. Mixed templates (`"prefix-{{ x }}"`) stay strings.
- **Sandboxed.** Dunder attacks (`{{ x.__class__.__mro__ }}`) raise
  `SecurityError` via `jinja2.sandbox.SandboxedEnvironment`.
- **Strict undefined.** `{{ missing }}` raises `UndefinedError` — typos
  fail loudly.
- **Cached.** Module-level template cache (size 400).

### Template Functions

| Function | Description |
|----------|-------------|
| `vars('key')` | Access variable from scoped chain |
| `vars('key.nested.subkey')` | Nested key access via dot notation |
| `vars('key', 'default')` | Access with fallback default value |
| `secrets('KEY')` | Access environment variable |
| `outputs.task_id.key` | Access upstream task output |
| `runtime.key` | Access run-time metadata |

### Namespaces

| Namespace                | When                              | What                                        |
|--------------------------|-----------------------------------|---------------------------------------------|
| `vars('KEY')`            | trigger time → enqueue            | Lookup into active stage of `variables.yml` |
| `secrets('KEY')`         | trigger time → enqueue            | Environment variable |
| `outputs.TASK_ID.KEY`    | pre-execute (worker, late-bind)   | Dict outputs returned by an upstream task   |
| `runtime.KEY`            | trigger time → enqueue            | Run identity + time (see below)             |

`runtime.*` fields:

| Key                    | Type       | Notes                                        |
|------------------------|------------|----------------------------------------------|
| `run_id`               | `str`      | `manual-…` / `scheduled-…` / `backfill-…`    |
| `dag_id`               | `str`      |                                              |
| `task_id`              | `str`      |                                              |
| `run_date`             | `datetime` | Wall-clock at trigger                        |
| `logical_date`         | `datetime` | = `data_interval_start` for cron runs        |
| `data_interval_start`  | `datetime` |                                              |
| `data_interval_end`    | `datetime` |                                              |
| `attempt_number`       | `int`      | 1-based; bumped on retry                     |

---

## 10. Metadata Store

Protocol-based. Worker, Scheduler, API depend on `MetadataProtocol` —
any implementing class works.

```python
class MetadataProtocol(Protocol):
    # DagRun operations
    async def create_dag_run(...) -> None: ...
    async def get_dag_run(self, run_id, dag_id) -> dict | None: ...
    async def update_dag_run_state(self, run_id, dag_id, state) -> None: ...
    async def list_active_runs(self, dag_id=None) -> list[dict]: ...

    # TaskContext operations
    async def put_task_context(self, run_id, dag_id, task_id, ctx) -> None: ...
    async def get_task_context(self, run_id, dag_id, task_id) -> TaskContext | None: ...
    async def get_task_outputs(self, run_id, dag_id, task_id) -> dict: ...
    async def clear_task(self, run_id, dag_id, task_id) -> None: ...

    # TaskState operations
    async def set_task_state(self, run_id, dag_id, task_id, state) -> None: ...
    async def get_task_state(self, run_id, dag_id, task_id) -> TaskState | None: ...
    async def get_all_task_states(self, run_id, dag_id) -> dict[str, TaskState]: ...

    # Deployment operations
    async def upsert_deployment(self, deployment: dict) -> None: ...
    async def get_deployment(self, deployment_id) -> dict | None: ...
    async def list_deployments(self) -> list[dict]: ...
    async def update_deployment_scheduler_state(self, deployment_id, *, last_scheduled_at) -> None: ...

    # Manual trigger queue
    async def enqueue_trigger(self, deployment_id, variables=None) -> str: ...
    async def drain_triggers(self, deployment_id=None) -> list[dict]: ...

    # Coordination (multi-instance support)
    async def try_create_scheduled_run(self, run_id, dag_id, dag_version, logical_date, deployment_id, ...) -> tuple[bool, str]: ...
    async def try_claim_trigger(self, trigger_id, deployment_id, instance_id) -> bool: ...
    async def try_update_scheduler_state(self, deployment_id, last_scheduled_at) -> bool: ...
```

### Implementations

| Store              | Persistence               | Status / Use case                |
|--------------------|---------------------------|----------------------------------|
| `LocalMetadata`    | File-based (sharded JSON) | ✅ Default, dev → 1000 DAGs       |
| `SqliteMetadata`   | Local SQLite              | Planned (Phase 2 default)        |
| `PostgresMetadata` | Postgres                  | Planned (Phase 3 multi-node)     |

### LocalMetadata Performance

- **Hive-style partitioning** — `dag_id={dag_id}/run_id={run_id}/{task_id}.json`
- **Fast filtering** — `ls {path}/dag_runs/dag_id=my-dag/`
- **Query engine compatible** — DuckDB, Spark, Trino can read directly
- **Async I/O** via `asyncio.to_thread`
- **Atomic writes** — temp file + `os.replace`
- **In-memory cache** for task states (scheduler hot path)
- **File-based coordination** — `fcntl.flock` for multi-instance support
- **Lock directory** — `{path}/.locks/` for coordination primitives

### Coordination Methods

These methods enable multiple scheduler instances to run concurrently
without creating duplicate runs:

#### `try_create_scheduled_run`

Atomically creates a scheduled run only if one doesn't already exist for
the given `(dag_id, logical_date)` combination.

```python
created, run_id = await meta.try_create_scheduled_run(
    run_id="scheduled-etl-20260606T120000",
    dag_id="etl",
    dag_version="v1",
    logical_date=datetime(2026, 6, 6, 12, 0, 0),
    deployment_id="daily-etl",
    variables={"source": "postgres"},
)
# created=True → this instance won the race
# created=False → another instance already created this run
```

#### `try_update_scheduler_state`

Atomically updates `last_scheduled_at` only if the new value is newer
than the current value. Used to claim a scheduler tick for a deployment.

```python
claimed = await meta.try_update_scheduler_state(
    "daily-etl",
    datetime(2026, 6, 6, 12, 0, 0),
)
# claimed=True → this instance gets to fire the run
# claimed=False → another instance already claimed this tick
```

#### `try_claim_trigger`

Atomically claims a manual trigger for processing.

```python
claimed = await meta.try_claim_trigger(
    trigger_id="abc123",
    deployment_id="daily-etl",
    instance_id="inst-a1b2c3",
)
# claimed=True → this instance processes the trigger
# claimed=False → another instance already claimed it
```

### Crash Recovery

When a scheduler/worker pod crashes, tasks in `RUNNING` state become
orphans ("zombies"). Beacon recovers on scheduler startup:

```text
Scheduler Startup
─────────────────
1. List all active DagRuns (state=running)
2. For each active run:
   - RUNNING tasks with no heartbeat for N seconds → FAILED (zombie)
   - QUEUED tasks stuck too long → re-queue or FAILED
3. Resume runs with non-terminal tasks
```

**Heartbeat protocol:**

Workers write `heartbeat_at` to task state files while tasks execute:

```json
{
  "run_id": "...",
  "dag_id": "...",
  "task_id": "...",
  "state": "running",
  "heartbeat_at": "2026-06-06T10:30:45.123456",
  "updated_at": "2026-06-06T10:30:45.123456"
}
```

Default zombie threshold: 5 minutes without heartbeat.

---

## 11. API Server

Beacon ships with a merged **API Server + Scheduler** that can scale
horizontally. The API provides REST endpoints for managing deployments,
triggers, and runs.

### Installation

```bash
pip install beacon[api]
```

### Running the API Server

```bash
# Single instance
beacon api ./my-bundle --port 8080

# Multiple instances for horizontal scaling
beacon api ./my-bundle --port 8080 --instance-id inst-1 &
beacon api ./my-bundle --port 8081 --instance-id inst-2 &
```

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check with instance ID |
| `/triggers` | POST | Create a manual trigger |
| `/deployments` | GET | List all deployments |
| `/deployments/{id}` | GET | Get a specific deployment |
| `/deployments` | POST | Create or update a deployment |
| `/deployments/{id}` | DELETE | Delete a deployment |
| `/deployments/{id}/enable` | PATCH | Enable a deployment |
| `/deployments/{id}/disable` | PATCH | Disable a deployment |
| `/runs` | GET | List recent DAG runs |
| `/runs/{id}` | GET | Get a specific run |
| `/runs/active` | GET | List active (non-terminal) runs |

### Example: Create Trigger via API

```bash
curl -X POST http://localhost:8080/triggers \
  -H "Content-Type: application/json" \
  -d '{"deployment_id": "daily-etl", "variables": {"source": "postgres"}}'
```

Response:

```json
{
  "trigger_id": "abc123def456",
  "deployment_id": "daily-etl",
  "message": "Trigger enqueued for daily-etl"
}
```

---

## 12. Upstream Outputs

Tasks may return a dict. Downstream tasks access upstream outputs only
through **explicit declaration** — never implicit global state.

| XCom (Airflow)              | Beacon Upstream Outputs                |
|-----------------------------|----------------------------------------|
| Any task → any task         | Only declared upstream outputs visible |
| Shared XCom table           | Stored per-task in TaskContext         |
| Unlimited size (DB bloat)   | Bounded: one dict return per plugin    |
| Implicit coupling           | Explicit: `upstream` IDs declared      |

YAML access:

```yaml
- id: extract
  uses: py
  inputs: { py_statement: ./scripts/extract.py }

- id: transform
  uses: py
  upstream: [extract]
  inputs:
    py_statement: ./scripts/transform.py
    params: { file_count: "{{ outputs.extract.row_count }}" }
```

Python access:

```python
from beacon import load_context

def main():
    ctx = load_context()
    files = ctx.upstream_outputs["extract"]["files"]
    return {"processed": len(files)}
```

Constraints:
- Read-only from downstream
- Only declared upstreams visible
- Small data only (paths, counts, IDs) — not payloads
- Resolved once at execution; same outputs across retries

---

## 13. Execution Flow End-to-End

```text
┌─ Deploy ────────────────────────────────────────────────────────────┐
│  beacon deploy --bundle ./bundle ...                                 │
│  1. Parse DAG from bundle (YAML/Python)                              │
│  2. Validate (``beacon plan`` — plugins exist, no cycles)            │
│  3. Extract variable requirements from DAG templates                 │
│  4. Serialize Deployment → Metadata                                  │
│  5. Tag Dag with bundle version (content hash / commit SHA)          │
└─────────────────────────┬───────────────────────────────────────────┘
                          ▼
┌─ Schedule trigger ─────────────────────────────────────────────────┐
│  Scheduler loop (each tick):                                         │
│    1. Drain manual triggers (with coordination)                      │
│    2. For each enabled deployment with cron:                         │
│       a. Evaluate cron expression → logical_date                     │
│       b. Try to claim tick via try_update_scheduler_state            │
│       c. If claimed: fire run via try_create_scheduled_run           │
└─────────────────────────┬───────────────────────────────────────────┘
                          ▼
┌─ Task execution ────────────────────────────────────────────────────┐
│  Worker dequeues {run_id, dag_id, task_id}:                          │
│    1. Read TaskContext                                               │
│    2. Resolve upstream_outputs                                       │
│    3. State → RUNNING; fire on_event: start                          │
│    4. executor.run_task(task_ctx)                                    │
│    5. Write updated TaskContext                                      │
│    6. SUCCESS → callbacks; FAILED + retries → UP_FOR_RETRY          │
└─────────────────────────┬───────────────────────────────────────────┘
                          ▼
┌─ DagRun resolution ─────────────────────────────────────────────────┐
│  After each terminal task:                                           │
│    1. Re-evaluate downstream (schedule / skip / upstream_failed)     │
│    2. When all tasks terminal:                                       │
│       all SUCCESS → DagRun SUCCESS → on_event: success               │
│       any FAILED  → DagRun FAILED  → on_event: failure               │
│       either way  → on_event: finished                               │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 14. Configuration

**Beacon is configured by environment variables.** No `beacon.toml`, no
`beacon.yml`, no DB-stored config.

### Inspect effective config

```bash
$ beacon config show
BEACON_METADATA_PATH                  ./metadata.db   (default)
BEACON_LOG_DIR                        /var/beacon/logs   (env)
BEACON_LOG_LEVEL                      INFO            (default)
BEACON_LOG_SINK                       file            (default)
BEACON_LOG_BATCH_SIZE                 100             (default)
BEACON_LOG_FLUSH_INTERVAL_MS          500             (default)
BEACON_SCHEDULER_TICK_SECONDS         5               (default)
BEACON_SCHEDULER_MAX_CONCURRENT_RUNS  8               (default)
```

### Reference

| Variable                                | Default          | Type | Purpose                                                                                                     |
|-----------------------------------------|------------------|------|-------------------------------------------------------------------------------------------------------------|
| `BEACON_METADATA_PATH`                  | `./metadata.db`  | str  | Dir used by `LocalMetadata` for `dag_runs/`, `task_contexts/`, `task_states/`, `deployments/`, `triggers/`. |
| `BEACON_LOG_DIR`                        | `./logs`         | str  | Root dir for JSONL logs (`{LOG_DIR}/{dag_id}/{run_id}/{task_id}/attempt_N.jsonl`). Used by `beacon logs`.   |
| `BEACON_LOG_LEVEL`                      | `INFO`           | str  | Minimum log level (`DEBUG`/`INFO`/`WARNING`/`ERROR`).                                                       |
| `BEACON_LOG_SINK`                       | `file`           | str  | `file` → `BEACON_LOG_DIR`; `memory` → in-process (testing).                                                 |
| `BEACON_LOG_BATCH_SIZE`                 | `100`            | int  | Records buffered before flush.                                                                              |
| `BEACON_LOG_FLUSH_INTERVAL_MS`          | `500`            | int  | Max ms before buffer flushes even when not full.                                                            |
| `BEACON_SCHEDULER_TICK_SECONDS`         | `5`              | int  | Deployment scheduler loop period.                                                                           |
| `BEACON_SCHEDULER_MAX_CONCURRENT_RUNS`  | `8`              | int  | Hard cap on in-flight DagRuns across all deployments in one scheduler process.                              |

### Setting them

```bash
# Shell
export BEACON_METADATA_PATH=/srv/beacon/metadata
export BEACON_LOG_DIR=/var/log/beacon
beacon api /srv/beacon/bundle --port 8080
```

```ini
# systemd
[Service]
Environment="BEACON_METADATA_PATH=/srv/beacon/metadata"
Environment="BEACON_LOG_DIR=/var/log/beacon"
Environment="BEACON_SCHEDULER_MAX_CONCURRENT_RUNS=16"
ExecStart=/usr/local/bin/beacon api /srv/beacon/bundle --port 8080
Restart=always
```

```yaml
# Docker / Compose
services:
  beacon:
    image: beacon:latest
    environment:
      BEACON_METADATA_PATH: /var/beacon/metadata
      BEACON_LOG_DIR:       /var/beacon/logs
    volumes:
      - beacon-meta:/var/beacon/metadata
      - beacon-logs:/var/beacon/logs
    command: api /bundle --port 8080
```

---

## 15. Deployment Topologies

### Single-node (Phase 1 — dag.run)

```text
┌─────────────────────────────────────────────┐
│  Single Process (dag.run / dag.backfill)    │
│  DagRunner + Worker + LocalExecutor         │
│  Metadata: LocalMetadata (local files)      │
│  Queue: asyncio.Queue                       │
│  Logging: LocalFileSink (JSONL)             │
└─────────────────────────────────────────────┘
```

### Single-node service (Phase 2 — current)

```text
┌─────────────────────────────────────────────┐
│  Single Process (beacon api)                │
│  REST API + Scheduler + Worker              │
│  Metadata: LocalMetadata (with locks)       │
│  Queue: asyncio.Queue                       │
│  Logging: LocalFileSink (JSONL)             │
└─────────────────────────────────────────────┘
```

### Horizontal scaling (Phase 2 — current)

```text
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ Instance 1   │ │ Instance 2   │ │ Instance 3   │
│ API + Sched  │ │ API + Sched  │ │ API + Sched  │
│ port 8080    │ │ port 8081    │ │ port 8082    │
└──────┬───────┘ └──────┬───────┘ └──────┬───────┘
       └────────────────┼────────────────┘
                        │
                        ▼
          Metadata: LocalMetadata (shared filesystem)
          Coordination: File locks in .locks/
```

### Distributed (Phase 3 — planned)

```text
┌──────────────┐ ┌──────────────────┐ ┌────────────────────┐
│ API (multi)  │ │ Scheduler        │ │ Worker Pool (N nodes)│
│ + LB         │ │ (can be merged)  │ │ Executor: K8s/Batch  │
└──────┬───────┘ └──────┬───────────┘ └────────┬───────────┘
       └────────────────┴───────────────────────┘
                        │
                        ▼
   Metadata: PostgresMetadata · Queue: Redis / SQS
   Logging: S3 / GCS · Bundle: GitBundle (CI/CD sync)
```

---

## 16. Key Design Decisions

| Decision                                                    | Rationale                                                                    |
|-------------------------------------------------------------|------------------------------------------------------------------------------|
| Stateless DagRunner                                         | Restart without data loss; all state in metadata                             |
| TaskContext is serializable                                 | Enables remote execution without DAG file mounts                             |
| Parse once, version-tag                                     | Eliminates Airflow's re-parse-every-heartbeat bottleneck                     |
| DAG vs Deployment separation                                | Reuse one DAG across many schedules/params                                   |
| Protocol-based MetadataStore                                | Pluggable persistence without touching runner/worker code                    |
| Sharded metadata (by dag_id)                                | Supports 1000+ DAGs without directory-listing degradation                    |
| Async-only execution                                        | Sensors don't waste worker slots; one event loop handles 100s of tasks       |
| Plugin teardown in `finally`                                | Resources always cleaned up — no leaks on failure/timeout                    |
| Merged API + Scheduler                                      | Simpler operations; horizontal scaling with coordination                     |
| Coordination via metadata store                             | No external dependencies; works with file locks or DB constraints            |
| Single `BasePlugin` for all types                           | Any plugin works with any action — no artificial restriction per action type |
| Raise strategy (`TaskFailed` / `TaskSkipped` / `TaskRetry`) | Plugins express control flow via exceptions, not return-value contracts      |
| `action.extract_outputs` normalization                      | Action owns routing logic; plugin stays ignorant of DAG topology             |
| Bounded upstream outputs                                    | Prevents unbounded XCom-style data growth                                    |
| DAG version pinning per run                                 | Mid-run DAG edits don't corrupt active instances                             |

---

## 17. What Beacon Does NOT Do (by design)

- **No unbounded cross-task data shuttle.** Upstream outputs are bounded
  (one dict per upstream), declared explicitly, read-only. For large
  data, use external storage and pass references (paths, URIs) as values.
- **No embedded Python in YAML.** YAML is configuration. Python logic
  lives in `.py` files referenced by the `py` plugin.
- **No provider installation at runtime.** Plugins resolved at parse
  time. Missing plugin → DAG fails to parse (fast feedback).
- **No DAG-level Jinja control flow.** Graph shape is static and
  inspectable.
- **No DAG-Schedule coupling.** A DAG is a template; schedules live in
  Deployments.
- **No Jinja rendering at execution time.** All templates resolved
  before TaskContext reaches the executor. Executors are dumb runners.
- **No external coordination service required.** Coordination uses the
  metadata store (file locks or DB transactions).

Full non-goals list (UI, OIDC, secrets adapter, …): see
[`roadmap.md`](./roadmap.md) §2.
