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
   *how/when* (cron, params, variable overrides). 1 DAG → N Deployments.
5. **Executor-agnostic.** `TaskContext` is serializable; the same task
   runs on local / Docker / K8s / Batch unchanged.
6. **Stateless runtime.** All state in the metadata store. Restart-safe.

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
| Scaling          | Multi-scheduler + DB tuning          | Stateless scheduler + queue           |

---

## 2. System Architecture

### Phase 1 — Local (current, shipped)

```text
┌─────────────────────────────────────────────────────────┐
│                    USER API (Dag)                       │
├─────────────────────────────────────────────────────────┤
│  dag.dryrun()     validate templates + graph            │
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
┌──────────────────────────────────────────────────────────┐
│             Metadata Store (LocalMetadata)                │
│  Sharded JSON, async I/O, LRU cache, atomic writes       │
│    {path}/dag_runs/{dag_id}/{run_id}.json                │
│    {path}/task_contexts/{dag_id}/{run_id}/{task_id}.json │
│    {path}/task_states/{dag_id}/{run_id}/{task_id}.json   │
└──────────────────────────────────────────────────────────┘
```

### Phase 2 — Production Service (planned)

```text
Client (CLI / SDK)
       │
       ▼
API Server (FastAPI) ──┐
       │               │
       ▼               ▼
Scheduler ──enqueue──> Queue ──> Worker ──> Executor
       │                                       │
       └────► Metadata Store (SQLite / PG) ◄───┘
                                               │
                                               ▼
                                          Logging Store
```

Component matrix:

| Component        | Phase 1                | Phase 2 (planned)          |
|------------------|------------------------|----------------------------|
| Metadata         | `LocalMetadata`        | `SqliteMetadata` (default) |
| Executor         | `LocalExecutor`        | + `DockerExecutor` (P3)    |
| Queue            | `asyncio.Queue`        | `asyncio.Queue` / Redis    |
| Scheduling       | `DagRunner` only       | + `DeploymentScheduler`    |
| Process          | `dag.run()` in-process | `beacon serve`             |

---

## 3. Model Hierarchy

```text
Plugin ──── execution logic (execute + teardown)
   │
   ▼
Action ──── references a plugin via `uses`, provides `inputs`
   │
   ▼         types: Task, Sensor, Branch, ShortCircuit, Group
Dag ──────── reusable template: actions + deps + params + callbacks
   │
   ▼
Deployment ─ binds a Dag to: cron + tz + params + variable_overrides
             N Deployments → 1 Dag
```

### Dag vs Deployment

```text
Dag(id="extract-load-table")
  ├── Deployment(id="daily-customers-from-postgres",
  │              cron="0 2 * * *", params={source: postgres})
  └── Deployment(id="hourly-orders-from-mysql",
                 cron="0 * * * *", params={source: mysql})
```

A `Deployment` carries:
- Identity (`Deployment.id` shown in UI)
- Schedule (`cron`, `start_date`, `end_date`, `timezone`, `catch_up`)
- Runtime params (values for `Dag.params`)
- Variable overrides (`variable_overrides` — layered on top of the
  bundle's scoped `variables.yml` / `global_variables.yml` chain)
- Version pin (`dag_version`, optional)
- Owners, labels, `enabled` flag

### Dag user API (Phase 1)

| Method                           | Purpose                              | When to use               |
|----------------------------------|--------------------------------------|---------------------------|
| `dag.dryrun()`                   | Validate graph + templates           | Before deploy / in CI     |
| `dag.run()`                      | Execute once, persist state          | Manual trigger            |
| `dag.test()`                     | Execute in tempdir, report pass/fail | Development               |
| `dag.clear(task_id=...)`         | Reset task + downstream → rerun      | Fix bug, rerun            |
| `dag.mark(state=...)`            | Force task to terminal state         | Kill stuck task / unblock |
| `dag.fail(task_id=...)`          | Shorthand for `mark(state="failed")` | Kill + clean up           |
| `dag.backfill(start, end, cron)` | Run N dates in range                 | Historical reprocessing   |

`DagRunner` is the async engine behind these — graph traversal, trigger
rule evaluation, branch / short-circuit propagation, teardown scheduling,
DAG-level callbacks, `resume=True` for re-execution of cleared tasks.
**`DagRunner` runs one DAG one time.** The future `DeploymentScheduler`
(Phase 2) sits above it and triggers runs from cron.

---

## 4. Plugin System

### Contract

Every plugin is a Pydantic model. One abstract method, one optional hook.

```python
from typing import ClassVar
from beacon.core import BasePlugin, Context

class MyPlugin(BasePlugin):
    plugin_name: ClassVar[str] = "my-plugin"

    # Typed inputs (validated by Pydantic; templated before instantiation)
    source: str
    target: str

    async def execute(self, context: Context) -> dict[str, Any]:
        """Main logic. Return outputs dict (or None)."""
        ...

    async def teardown(self, context: Context) -> None:
        """Cleanup. ALWAYS fires after execute (success or failure).
        Default: no-op. Override for resource cleanup."""
        ...
```

No inheritance chains. No mixins. No `template_fields` — every input
that's a string is templated.

### Resolution

The `uses` field resolves a plugin from the registry, in order:

1. Built-in `PLUGINS_REGISTRY` (standard provider)
2. Entry-point discovered plugins (`beacon.plugins` group in `pyproject.toml`)
3. Local `./plugins/` directory (auto-discovered from bundle path)
4. (Future) Remote registry with version pinning

```text
uses: "py"                       → built-in
uses: "gcs-extract"              → ./plugins/gcs_extract.py
uses: "my-org/etl@1.2.0"         → remote (future)
```

### Error Signaling (controls retry)

| Raises          | Behavior                             | Use case                           |
|-----------------|--------------------------------------|------------------------------------|
| Any `Exception` | Retry up to `retries`, then `FAILED` | Transient (network timeout)        |
| `TaskFailed`    | Immediately `FAILED`, skip retries   | Permanent (table missing)          |
| `TaskSkipped`   | Mark `SKIPPED`, skip retries         | Nothing to do (empty partition)    |

```python
from beacon.errors import TaskFailed, TaskSkipped

async def execute(self, context: Context) -> dict:
    if not await self.source_exists():
        raise TaskFailed("Source does not exist, no point retrying")
    rows = await self.fetch()
    if not rows:
        raise TaskSkipped("No new data this run")
    return {"rows": len(rows)}
```

### Two ways to add logic

| Pattern        | When                       | How                                   |
|----------------|----------------------------|---------------------------------------|
| `uses: py`     | One-off function, no reuse | Write `main()` in a `.py` file        |
| Custom plugin  | Reusable across DAGs       | Subclass `BasePlugin` in `./plugins/` |

Example `uses: py` flow:

```yaml
# dag.yml
- id: transform
  type: task
  uses: py
  inputs:
    py_file: ../scripts/transform.py
    py_function: main
    params: { source: "{{ params.source_system }}" }
```

```python
# scripts/transform.py
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

| Field               | Type | Default | Description                                    |
|---------------------|------|---------|------------------------------------------------|
| check_interval      | int  | 60      | Seconds between condition checks               |
| execution_timeout   | int  | None    | Max wait time before failing                   |
| exponential_backoff | bool | true    | Increase interval between checks               |
| fail_mode           | str  | soft    | `soft` = FAILED on timeout; `silent` = SKIPPED |

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
            await asyncio.sleep(context.get("check_interval", 60))
```

Executor wraps in `asyncio.timeout(execution_timeout)`. The `await
asyncio.sleep()` yields the event loop — no worker slot wasted, no
separate triggerer.

### 5.3 Branch

Chooses which downstream path(s) to schedule. Unchosen → SKIPPED.

| Field   | Type      | Description                                  |
|---------|-----------|----------------------------------------------|
| success | list[str] | Default downstream if plugin returns truthy  |
| failure | list[str] | Default downstream if plugin returns falsy   |

**Plugin output:** `{"branch": ["task-id-1", "task-id-2"]}` — only
listed IDs run; everything else in `success`+`failure` is SKIPPED.

```yaml
- id: check-quality
  type: branch
  uses: py
  inputs: { py_file: ./scripts/check_quality.py }
  success: [process-good]
  failure: [quarantine, alert]
```

### 5.4 ShortCircuit

Returns `{"continue": True|False}`. False → SKIP all downstream
**recursively**.

```yaml
- id: should-run-today
  type: short_circuit
  uses: py
  inputs: { py_file: ./scripts/check_if_needed.py }
```

### 5.5 Group

Container for nested actions. No runtime behavior — flattened by the
runner. Use to organize visually and apply shared upstream deps.

```yaml
- id: ingest-stage
  type: group
  upstream: [start]
  actions:
    - { id: extract-customers, type: task, uses: py, inputs: { py_file: ./extract_customers.py } }
    - { id: extract-orders,    type: task, uses: py, inputs: { py_file: ./extract_orders.py } }
```

### Downstream scheduling per action type

After a task completes the scheduler calls
`action.evaluate_downstream(task_ctx, downstream_ids)` → returns
`DownstreamDirective(schedule=[...], skip=[...])`.

| Action Type    | `evaluate_downstream()` logic                           |
|----------------|---------------------------------------------------------|
| Task           | Schedule all downstream                                 |
| Sensor         | Schedule all downstream (condition met)                 |
| Branch         | Schedule only `outputs["branch"]` list, skip rest       |
| ShortCircuit   | If `outputs["continue"]` is False → skip ALL downstream |
| Group          | Not invoked — flattened at parse time                   |

### Expected outputs per action type

| Action Type  | Expected output shape              | Used for                        |
|--------------|------------------------------------|---------------------------------|
| Task         | `{"key": "value", ...}` (any dict) | Upstream outputs for downstream |
| Sensor       | `{"condition_met": True, ...}`     | Proof that condition was met    |
| Branch       | `{"branch": ["task-a", ...]}`      | Which path to take              |
| ShortCircuit | `{"continue": True \| False}`      | Whether to proceed              |

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
Resolution mirrors plugins: registry name, `org/name@version`, or a
direct class reference (Python only).

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

### Retry detail

```text
Attempt 1:  QUEUED → RUNNING → error → finish_attempt(FAILED)
            attempts=[{1: failed}],  state→UP_FOR_RETRY,
            re-queue in retry_delay * 2^0 = 10s
Attempt 2:  (10s later) → ...                                    → 20s
Attempt 3:  (20s later) → plugin succeeds → finish_attempt(SUCCESS)
            attempts=[{1: failed},{2: failed},{3: success}]
            state→SUCCESS, outputs={...}
```

`TaskFailed` exits the retry loop immediately.

### run_id convention

| Prefix                           | Source                       | Example                         |
|----------------------------------|------------------------------|---------------------------------|
| `manual-{dag_id}-{uuid}`         | `dag.run()`                  | `manual-etl-a1b2c3d4`           |
| `backfill-{dag_id}-{timestamp}`  | `dag.backfill()`             | `backfill-etl-20260101T000000`  |
| `scheduled-{dag_id}-{timestamp}` | Phase 2 DeploymentScheduler  | `scheduled-etl-20260104T020000` |

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
class SparkPlugin(BasePlugin):
    plugin_name: ClassVar[str] = "spark"
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
    py_file: ./spark.py
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
4. Validated at `dryrun()` — referenced task must exist; no self-ref.
5. Multiple independent setup/teardown pairs supported in one DAG.

### Teardown guarantees

| Scenario                       | Plugin teardown         | Task-level teardown          |
|--------------------------------|-------------------------|------------------------------|
| Task succeeds                  | ✅ fires (finally)       | ✅ fires (deps terminal)      |
| Task fails                     | ✅ fires (finally)       | ✅ fires (deps terminal)      |
| Task times out                 | ✅ fires (finally)       | ✅ fires (deps terminal)      |
| Task force-failed via `mark()` | — (already done)        | ✅ auto-clears + re-fires     |
| Task cleared via `clear()`     | ✅ fires on re-execution | ✅ auto-clears + re-fires     |

---

## 9. Templating

_Beacon_ uses Jinja2 for **value interpolation only** — not control flow.
Templates resolve to concrete values *before* a plugin runs; plugins
never see Jinja.

```yaml
# ✅ Supported
inputs:
  source: "{{ params.source_system }}"
  path:   "{{ params.base_path }}/{{ params.date }}"
  bucket: "{{ vars('gcs_bucket') }}"
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
from beacon.core import Renderer
r = Renderer({
    "params":  {"source": "postgres", "date": "2026-06-04"},
    "vars":    lambda name: my_variables.get(name),
    "outputs": {"extract": {"row_count": 42}},
    "runtime": {...},
})

r.render("{{ params.source }}")                   # "postgres"
r.render({"q": "SELECT * FROM {{ params.source }}_t"})
# → {"q": "SELECT * FROM postgres_t"}
```

Properties:
- **Recursive.** Walks `dict`, `list`, `tuple`. Non-string scalars pass through.
- **Native-typed.** Pure expressions return real Python types.
  `"{{ 5 + 5 }}"` → `int(10)`, `"{{ [1,2] }}"` → `list`, `"{{ x }}"`
  with `x=None` → `None`. Mixed templates (`"prefix-{{ x }}"`) stay strings.
- **Sandboxed.** Dunder attacks (`{{ x.__class__.__mro__ }}`) raise
  `SecurityError` via `jinja2.sandbox.SandboxedEnvironment`.
- **Strict undefined.** `{{ missing }}` raises `UndefinedError` — typos
  fail loudly. (Exception: `vars('foo')` returns sentinel
  `<unresolved: vars('foo')>` to allow dryrun with partial stage data.)
- **Cached.** Module-level template cache (size 400).

### Namespaces

| Namespace                | When                              | What                                        |
|--------------------------|-----------------------------------|---------------------------------------------|
| `params.KEY`             | trigger time → enqueue            | Concrete `TaskContext.params`               |
| `vars('KEY')`            | trigger time → enqueue            | Lookup into active stage of `variables.yml` |
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

### Render pipeline (two binding sites)

```text
┌─ 1. Trigger-time render ─ beacon/runner.py: _submit_action ──────────┐
│    ctx = {params, vars, runtime, outputs={}}                         │
│    rendered = Renderer(ctx).render(task.inputs)                      │
│    → stored on TaskContext.inputs                                    │
│    Failures here (typically because input references outputs.* of    │
│    an upstream that hasn't run) are left and retried at site 2.      │
├─ 2. Pre-execute render ── beacon/worker.py: _resolve_upstream_outputs┤
│    Just before plugin call:                                          │
│    - load each upstream's TaskContext.outputs into                   │
│      task_ctx.upstream_outputs                                       │
│    - re-render task_ctx.inputs with `outputs` namespace bound        │
│      (vars is already resolved from site 1)                          │
│    → stored back on TaskContext.inputs, plugin instantiates          │
├─ 3. Dryrun render ─────── beacon/dryrun.py                           │
│    Same as site 1 but failures become `DryrunResult.warnings`.       │
└──────────────────────────────────────────────────────────────────────┘
```

By plugin execute time, `TaskContext.inputs` is **fully concrete**.
Plugins do not import `Renderer`.

### Variable resolution end-to-end

```text
variables files (scoped under dags/)
─────────────────────────────────────
dags/global_variables.yml:           gcs_bucket: my-prod-bucket
dags/extract_load/variables.yml:     source_system: postgres

Deployment (metadata store)
──────────────────────────────
id: daily-customers
dag_id: extract-load-table
variable_overrides:                  # optional pinning
  alert_path: /var/oncall
params:
  source: "{{ vars('source_system') }}"
  bucket: "{{ vars('gcs_bucket') }}"

Trigger: scheduler resolves vars against scoped chain
   (deployment overrides → dag variables.yml → group global_variables.yml
    → bundle global_variables.yml)
   → TaskContext.params = {source: "postgres", bucket: "my-prod-bucket"}

DAG action: inputs templated, then resolved
   extract.inputs   = {py_file: "./extract.py", table: "postgres_events",
                       date: datetime(2026, 6, 4, ...)}
   transform.inputs = {rows: 42}      # ← outputs.extract.row_count
```

### Native types cheat-sheet

```yaml
threshold:  "{{ 0.95 }}"           # float
batch_size: "{{ 100 * 10 }}"       # int
enabled:    "{{ True }}"           # bool
tags:       "{{ ['a', 'b'] }}"     # list
mixed:      "prefix-{{ x }}"  x=5  # "prefix-5" (mixed → str)
```

| You want…                   | Write                              |
|-----------------------------|------------------------------------|
| Stage variable              | `{{ vars('key') }}`                |
| Deployment / DAG param      | `{{ params.key }}`                 |
| Upstream task output        | `{{ outputs.task_id.key }}`        |
| Logical date                | `{{ runtime.logical_date }}`       |
| Run id                      | `{{ runtime.run_id }}`             |
| Math / coerced literal      | `{{ 1024 * 1024 }}`                |
| Inline default              | `{{ params.maybe or 'fallback' }}` |
| Jinja filter                | `{{ params.name \| upper }}`       |

### Dynamic tasks (`for_each` — Phase 3)

```yaml
actions:
  - id: "process-{{ item }}"
    type: task
    uses: py
    for_each: "{{ params.source_systems }}"
    inputs: { source_system: "{{ item }}" }
```

At trigger time the scheduler resolves `for_each` against runtime
params → gets a list → generates N `TaskContext` instances, each
independently scheduled, retried, tracked.

---

## 10. Executor

### Types

| Executor             | Use case                   | How it runs              |
|----------------------|----------------------------|--------------------------|
| `LocalExecutor`      | Dev, small workloads       | asyncio in-process       |
| `DockerExecutor`     | Isolation, reproducibility | Spawn container per task |
| `KubernetesExecutor` | Production at scale        | Create pod per task      |
| `BatchExecutor`      | AWS Batch / Cloud Batch    | Submit job per task      |

### Contract

```python
class BaseExecutor(ABC):
    executor_type: str = "base"
    async def run_task(self, task_ctx: TaskContext) -> TaskContext: ...
```

`run_task` lifecycle:
1. Resolve plugin from `plugin_name`
2. Call `task_ctx.start_attempt(executor, executor_ref)`
3. Instantiate plugin with rendered inputs
4. Run `plugin.execute(context)` (wrapped in `asyncio.timeout` if configured)
5. **Always** call `plugin.teardown(context)` in `finally`
6. Call `task_ctx.finish_attempt(state, error, outputs)`
7. Return updated `TaskContext`

### Remote executor flow (K8s / Docker / Batch)

```text
Worker:
  1. Dequeue {run_id, dag_id, task_id}
  2. Read TaskContext from metadata store
  3. executor.run_task(task_ctx):
     KubernetesExecutor:
       a. Build Pod spec (plugin image + TaskContext via env var)
       b. Submit to K8s API
       c. Poll Pod status (async, no slot waste)
       d. On completion: read logs, update TaskContext
  4. Write updated TaskContext to metadata store
```

TaskContext reaches the container via:
- `BEACON_TASK_CONTEXT` env var (JSON-serialized), OR
- Mounted file `/tmp/beacon/task_context.json`, OR
- API call (pod entrypoint fetches from beacon API)

Container runs: `beacon-runner execute --from-env`.

---

## 11. Metadata Store

Protocol-based. Worker, Scheduler, API depend on `MetadataProtocol` —
any implementing class works.

```python
class MetadataProtocol(Protocol):
    async def create_dag_run(...) -> None: ...
    async def get_dag_run(self, run_id, dag_id) -> dict | None: ...
    async def update_dag_run_state(self, run_id, dag_id, state) -> None: ...
    async def put_task_context(self, run_id, dag_id, task_id, ctx) -> None: ...
    async def get_task_context(self, run_id, dag_id, task_id) -> TaskContext | None: ...
    async def set_task_state(self, run_id, dag_id, task_id, state) -> None: ...
    async def get_task_state(self, run_id, dag_id, task_id) -> TaskState | None: ...
    async def get_all_task_states(self, run_id, dag_id) -> dict[str, TaskState]: ...
    async def get_task_outputs(self, run_id, dag_id, task_id) -> dict: ...
    async def clear_task(self, run_id, dag_id, task_id) -> None: ...
```

### Implementations

| Store              | Persistence               | Status / Use case                |
|--------------------|---------------------------|----------------------------------|
| `LocalMetadata`    | File-based (sharded JSON) | ✅ Default, dev → 1000 DAGs       |
| `SqliteMetadata`   | Local SQLite              | Pending (Phase 2 default)        |
| `PostgresMetadata` | Postgres                  | Pending (Phase 3 multi-node)     |

### LocalMetadata performance

- **Sharded by `dag_id`** — no flat 100k-file dirs
- **Async I/O** via `asyncio.to_thread`
- **Atomic writes** — temp file + `os.replace`
- **In-memory cache** for task states (scheduler hot path)
- **Bulk queries** — `get_all_task_states(run_id, dag_id)`
- **Cache eviction** — `evict_run_from_cache()` on run completion

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
  inputs: { py_file: ./scripts/extract.py }

- id: transform
  uses: py
  upstream: [extract]
  inputs:
    py_file: ./scripts/transform.py
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
│  beacon deploy ... or beacon sync /path                              │
│  1. Parse DAG from bundle (YAML/Python)                              │
│  2. Validate (plugins exist, no cycles)                              │
│  3. Serialize Dag + Deployment → Metadata                            │
│  4. Tag Dag with bundle version (content hash / commit SHA)          │
└─────────────────────────┬───────────────────────────────────────────┘
                          ▼
┌─ Schedule trigger (Phase 2) ────────────────────────────────────────┐
│  Scheduler loop:                                                     │
│    1. Find deployments where enabled and next_run <= now             │
│    2. For each due: compute logical_date, resolve vars → params      │
│    3. Create DagRun, TaskContext per action                          │
│    4. Tasks with deps met → SCHEDULED → QUEUED                       │
└─────────────────────────┬───────────────────────────────────────────┘
                          ▼
┌─ Task execution ────────────────────────────────────────────────────┐
│  Worker dequeues {run_id, dag_id, task_id}:                          │
│    1. Read TaskContext                                               │
│    2. Resolve upstream_outputs                                       │
│    3. State → RUNNING; fire on_event: start                          │
│    4. executor.run_task(task_ctx)                                    │
│    5. Write updated TaskContext                                      │
│    6. SUCCESS → callbacks; FAILED + retries → UP_FOR_RETRY;          │
│       FAILED no retries → FAILED                                     │
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

### Four operational scenarios

```text
1. NORMAL RUN
   dag.run(params={...})
2. FIX AND RERUN (task had a bug)
   dag.clear(run_id=..., task_id="bad_task", downstream=True)
   → reset task+downstream → re-execute → teardowns re-fire
   → upstreams NOT re-executed (read from metadata)
3. KILL STUCK TASK
   dag.fail(run_id=..., task_id="stuck_task")
   → mark FAILED → teardown re-fires → resource cleaned up
4. BACKFILL
   dag.backfill(start_date=..., end_date=..., cron="0 0 * * *")
   → one run per cron tick → skip existing (or reset_existing)
```

---

## 14. Bundle Layout (summary)

A team's workflow repository:

```text
my-workflow-repo/
├── dags/                    # DAG definitions (reusable templates)
│   ├── etl_pipeline.yml
│   └── reports/daily_report.yml
├── deployments/             # Deployments (per-env, per-config)
│   ├── customers-from-postgres.yml
│   └── orders-from-mysql.yml
├── plugins/                 # Custom plugins (auto-registered)
│   └── gcs_extract.py
├── scripts/                 # Python files called by `uses: py`
│   └── transform.py
└── variables.yml            # Stage variables (prod, dev, staging)
```

When a bundle changes:
1. Load new plugins (overrides existing registrations)
2. Reparse affected DAGs
3. Store new serialized DAG with new version
4. **Running instances continue on their original version**
   (TaskContext has `dag_version`)
5. New runs use the new version (unless Deployment pins `dag_version`)

Full bundle policy + variable scoping + pinned-deployment behavior:
[`deploy.md`](./deploy.md).

---

## 15. Configuration

**Beacon is configured by environment variables. No `beacon.toml`, no
`beacon.yml`, no DB-stored config.** ([roadmap.md](./roadmap.md) §2
non-goals.) Re-evaluate if the surface grows past ~20 env vars.
Today: 8.

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

First debugging step when a setting "doesn't seem to apply".

### Reference

Single source of truth: `_SPEC` in
[`beacon/cli/settings.py`](../../beacon/cli/settings.py).

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

### Per-command overrides

| Env var                | Flag (where supported)                                          |
|------------------------|-----------------------------------------------------------------|
| `BEACON_METADATA_PATH` | `--metadata-path PATH` (`deploy`, `sync`, `list`, `trigger`, …) |
| `BEACON_LOG_DIR`       | `--log-dir PATH` (`logs`)                                       |

### Setting them

```bash
# Shell
export BEACON_METADATA_PATH=/srv/beacon/metadata
export BEACON_LOG_DIR=/var/log/beacon
beacon scheduler /srv/beacon/bundle
```

```ini
# systemd
[Service]
Environment="BEACON_METADATA_PATH=/srv/beacon/metadata"
Environment="BEACON_LOG_DIR=/var/log/beacon"
Environment="BEACON_SCHEDULER_MAX_CONCURRENT_RUNS=16"
ExecStart=/usr/local/bin/beacon scheduler /srv/beacon/bundle
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
```

### Secrets

Beacon **does not** ship a secrets adapter
([roadmap.md](./roadmap.md) cut-list). Inside a `uses: py` task:
`os.environ.get("MY_API_KEY")` — let your platform (Vault, AWS Secrets
Manager, K8s secrets, 1Password) inject into the environment. Keeps
secrets out of the metadata store by construction.

### Per-DAG / per-deployment config

Not what env vars are for. Use:
- **DAG params** — declared in `dag.yml`, supplied per-deployment via
  `beacon deploy --param key=value`.
- **Scoped variables** — `dags/[<group>/]global_variables.yml` +
  `dags/<group>/<dag>/variables.yml`, layered with per-deployment
  `beacon deploy --var key=value`. See
  [`deploy.md`](./deploy.md#variable-resolution).

---

## 16. Deployment Topologies

### Single-node (Phase 1 — current)

```text
┌─────────────────────────────────────────────┐
│  Single Process (dag.run / dag.backfill)    │
│  DagRunner + Worker + LocalExecutor         │
│  Metadata: LocalMetadata (local files)       │
│  Queue: asyncio.Queue                       │
│  Logging: LocalFileSink (JSONL)             │
└─────────────────────────────────────────────┘
```

### Service mode (Phase 2 — planned)

```text
┌──────────────┐ ┌──────────────────┐ ┌──────────────────┐
│ API Server   │ │ DeploymentSched  │ │  Worker (N proc) │
│ (FastAPI)    │ │  (cron loop)     │ │  max_concurrent=50│
└──────┬───────┘ └──────┬───────────┘ └────────┬─────────┘
       └────────────────┴───────────────────────┘
                          │
                          ▼
   Metadata: SqliteMetadata · Queue: asyncio.Queue
   Logging: LocalFileSink + rotation
```

### Distributed (Phase 3 — planned)

```text
┌──────────────┐ ┌──────────────────┐ ┌────────────────────┐
│ API (multi)  │ │ DeploymentSched  │ │ Worker Pool (N nodes)│
│ + LB         │ │  (single)        │ │ Executor: K8s/Batch  │
└──────┬───────┘ └──────┬───────────┘ └────────┬───────────┘
       └────────────────┴───────────────────────┘
                          │
                          ▼
   Metadata: PostgresMetadata · Queue: Redis / SQS
   Logging: S3 / GCS · Bundle: GitBundle (CI/CD sync)
```

---

## 17. Key Design Decisions

| Decision                          | Rationale                                                               |
|-----------------------------------|-------------------------------------------------------------------------|
| Stateless DagRunner               | Restart without data loss; all state in metadata                        |
| TaskContext is serializable       | Enables remote execution without DAG file mounts                        |
| Parse once, version-tag           | Eliminates Airflow's re-parse-every-heartbeat bottleneck                |
| DAG vs Deployment separation      | Reuse one DAG across many schedules/params                              |
| Protocol-based MetadataStore      | Pluggable persistence without touching runner/worker code               |
| Sharded metadata (by dag_id)      | Supports 1000+ DAGs without directory-listing degradation               |
| Async-only execution              | Sensors don't waste worker slots; one event loop handles 100s of tasks  |
| Plugin teardown in `finally`      | Resources always cleaned up — no leaks on failure/timeout               |
| Two-pass Jinja rendering          | Outputs resolved late (worker); everything else resolved early (runner) |
| NativeEnvironment + Sandbox       | Types preserved through templates AND security enforced                 |
| `run_id` encodes trigger type     | Filter/group runs by how they were created                              |
| Auto-clear teardown on clear/mark | Prevents resource leaks on re-run                                       |
| Plugin registry (not pip install) | Fast resolution; no runtime installation; version-pinned                |
| `TaskFailed` / `TaskSkipped`      | Plugins explicitly control retry-vs-permanent-fail behavior             |
| Bounded upstream outputs          | Prevents unbounded XCom-style data growth                               |
| DAG version pinning per run       | Mid-run DAG edits don't corrupt active instances                        |

---

## 18. What Beacon Does NOT Do (by design)

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
- **No multi-tenant scheduler.** One scheduler per deployment; scale
  horizontally with separate beacon instances per team/domain.
- **No Jinja rendering at execution time.** All templates resolved
  before TaskContext reaches the executor. Executors are dumb runners.

Full non-goals list (UI, OIDC, secrets adapter, …): see
[`roadmap.md`](./roadmap.md) §2.
