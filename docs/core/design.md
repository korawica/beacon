# Design

The design document for the beacon workflow orchestration framework.
Beacon is a lean, async-first alternative to Apache Airflow — built for teams
that need production-grade orchestration without the operational complexity.

---

## Design Principles

1. **Async-first, not bolted-on** — Every action (task, sensor, branch, for-each)
   executes via `async def execute()`. No separate "deferrable" concept. No sync-to-async
   bridge. One execution model.

2. **Plugin is the unit of logic** — All execution logic lives in plugins.
   Tasks don't contain code; they reference a plugin via `uses`. This makes DAG
   definitions pure configuration.

3. **Simple path must be trivial** — A data engineer writes a Python function,
   references it via `uses: py`, and it runs. No operator inheritance, no hook
   composition, no provider installation.

4. **Scalable by default** — The architecture supports 10,000+ deployed DAGs
   through stateless workers, append-only logging, and lightweight DAG parsing.

5. **Executor-agnostic** — TaskContext is serializable. The same task runs on
   local process, Docker, Kubernetes, AWS Batch, or Cloud Batch without code changes.

---

## Overall Architecture

```text
User --> Client (CLI / SDK / YAML)
           |
           v
      API Server (FastAPI) ──────────> Metadata Store (Sqlite / Postgres)
           |                              ^       ^
           v                              |       |
      Scheduler ── enqueue ──> Queue ─────┘       |
                                 |                |
                                 v                |
                            Executor ─────────────┘
                         (Local / Docker / K8s / Batch)
                                 |
                                 v
                           Logging Store (Local / S3 / GCS)
```

**Key difference from Airflow:**

| Aspect              | Airflow                                    | Beacon                                 |
|---------------------|--------------------------------------------|----------------------------------------|
| Execution model     | Sync + Deferrable (two paths)              | Async-only (one path)                  |
| Plugin install      | pip install into runtime env               | Registry lookup or remote ref          |
| DAG parsing         | Every scheduler heartbeat re-parses        | Parse once, version-tag, cache         |
| Sensor handling     | Poke loop occupies worker slot or deferred | Async sleep, no slot waste             |
| Task context        | XCom (cross-task data shuttle)             | TaskContext (per-task, serializable)   |
| Remote execution    | KubernetesExecutor + sidecar complexity    | Executor reads TaskContext from store  |
| Scaling to 10k DAGs | Requires multiple schedulers + DB tuning   | Stateless scheduler + queue            |

---

## Plugin System

### Plugin Resolution

The `uses` field on any action resolves a plugin from the registry:

```text
uses: "py"                          --> built-in PLUGINS_REGISTRY["py"]
uses: "empty"                       --> built-in PLUGINS_REGISTRY["empty"]
uses: "my-org/etl-plugin@1.2.0"    --> external plugin (future: remote resolve)
```

**Resolution order:**

1. Built-in registry (standard provider plugins)
2. Entry-point discovered plugins (`beacon.plugins` group in pyproject.toml)
3. Local `plugins/` directory (auto-discovered from bundle path)
4. (Future) Remote registry with version pinning

### Plugin Contract

Every plugin implements one method:

```python
from typing import ClassVar
from beacon.core import BasePlugin, Context

class MyPlugin(BasePlugin):
    plugin_name: ClassVar[str] = "my-plugin"

    # Declare inputs as typed fields
    source: str
    target: str

    async def execute(self, context: Context) -> None:
        # All logic here. Has access to context (params, vars, metadata).
        ...
```

No inheritance chains. No mixin layers. One class, one method.

---

## Hook System (Callbacks)

### Unified with Plugin Registry

Hooks use the **same resolution mechanism** as `uses`. A hook is just a plugin
that executes on an event rather than as a DAG step.

```yaml
# YAML
callbacks:
  - on_event: failure
    hook: "my-org/msteam-callback@1.1.0"
    inputs:
      webhook_url: "https://my-webhook-url.com"
      channel: "alerts"
```

```python
# Python
from beacon import OnEvent
from beacon.providers.msteam.callback import MsTeamCallback

dag = Dag(
    callbacks=[
        OnEvent(
            on_event="failure",
            hook=MsTeamCallback,  # or string "msteam-adaptive-card"
            inputs={"webhook_url": "https://..."},
        ),
    ],
)
```

**Hook resolution:**

```text
hook: "msteam-adaptive-card"              --> HOOKS_REGISTRY["msteam-adaptive-card"]
hook: "my-org/callback-plugin@1.1.0"      --> external hook plugin
hook: MsTeamCallbackPlugin                --> direct class reference (Python only)
```

### Hook Plugin Contract

```python
from abc import ABC
from typing import ClassVar
from beacon.core import Templater

class BaseHookPlugin(Templater, ABC, metaclass=HookPluginMeta):
    hook_name: ClassVar[str] = "base"

    async def notify(self, task_ctx: TaskContext, event: str) -> None:
        raise NotImplementedError
```

Same pattern as `BasePlugin` but with `notify()` instead of `execute()`.
Same registry. Same version pinning. Same resolution.

---

## Task Context (NOT XCom)

### Concept

TaskContext is the **serializable unit of work** stored in the metadata store.
It carries everything a task needs to execute in any environment and accumulates
attempt history across retries.

**Key differences from Airflow's XCom:**

| XCom (Airflow)                        | TaskContext (Beacon)                     |
|---------------------------------------|------------------------------------------|
| Passes data BETWEEN tasks             | Carries data TO/FROM a single task       |
| Pulled by downstream at runtime       | Stored in metadata, read by executor     |
| Grows unbounded (data shuttle)        | Fixed structure, bounded per task        |
| Tight coupling between tasks          | Tasks are independent; share via storage |

### What TaskContext Contains

```python
class TaskContext(BaseModel):
    # Identity
    run_id: str             # DagRun this belongs to
    dag_id: str             # DAG ID
    task_id: str            # Task ID within the DAG
    dag_version: str        # Bundle version at trigger time

    # Time
    run_date: datetime
    logical_date: datetime
    data_interval_start: datetime
    data_interval_end: datetime

    # Inputs (fully resolved before execution)
    params: dict[str, Any]       # DAG params (Jinja-rendered with vars)
    inputs: dict[str, Any]       # Task inputs (Jinja-rendered with params)
    plugin_name: str             # Which plugin to run

    # Execution config
    retries: int                 # Max retry attempts
    retry_delay: int             # Base delay seconds
    execution_timeout: int | None
    exponential_backoff: bool

    # Attempt history (one per try, including retries)
    attempts: list[Attempt]

    # Outputs (written after success)
    outputs: dict[str, Any]
```

### How TaskContext Enables Remote Execution

```text
┌──────────────────────────────────────────────────────────────────┐
│ Scheduler                                                        │
│   1. Build TaskContext (resolve inputs, set plugin_name)         │
│   2. Serialize TaskContext → Metadata Store (JSON)               │
│   3. Enqueue message: {run_id, task_id} → Queue                 │
└──────────────────────────────────────────────────────────────────┘
                              │
                              v
┌──────────────────────────────────────────────────────────────────┐
│ Executor (any type: local, docker, k8s, aws batch)               │
│   1. Receive message from Queue                                  │
│   2. Read TaskContext from Metadata Store                         │
│   3. Resolve plugin from plugin_name                             │
│   4. Build Context dict (lightweight, for plugin.execute())      │
│   5. Call task_ctx.start_attempt(executor="k8s", ref="pod-xyz")  │
│   6. Run plugin.execute(context)                                 │
│   7. Call task_ctx.finish_attempt(state, error, outputs)         │
│   8. Write updated TaskContext back to Metadata Store            │
└──────────────────────────────────────────────────────────────────┘
```

The executor never needs local DAG files. It only needs:
- Access to the metadata store
- The plugin code (installed or registry-resolved)

This is what makes K8s/Docker/Batch execution work without mounting DAG volumes.

---

## Task State Machine

### States

```text
┌─────────────────────────────────────────────────────────────────────────┐
│                          NON-TERMINAL STATES                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  NONE ─── Task registered in DagRun, not yet evaluated                  │
│    │                                                                    │
│    ├──> SCHEDULED ─── Scheduler confirmed deps met, ready for queue     │
│    │       │                                                            │
│    │       └──> QUEUED ─── Message sent to executor queue               │
│    │               │                                                    │
│    │               └──> RUNNING ─── Executor picked up, plugin running  │
│    │                       │                                            │
│    │                       ├──> SUCCESS ──── (terminal)                  │
│    │                       ├──> FAILED ───── (terminal, no retries left) │
│    │                       └──> UP_FOR_RETRY ── attempt failed           │
│    │                               │             but retries remain      │
│    │                               └──> QUEUED ─── (retry loop)          │
│    │                                                                    │
│    ├──> SKIPPED ─── Trigger rule not satisfied (terminal)               │
│    └──> UPSTREAM_FAILED ─── Upstream in failed state (terminal)         │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Valid Transitions Table

| From            | To                  | Triggered By                             |
|-----------------|---------------------|------------------------------------------|
| `none`          | `scheduled`         | Scheduler: deps satisfied                |
| `none`          | `skipped`           | Scheduler: trigger rule skip             |
| `none`          | `upstream_failed`   | Scheduler: upstream failed               |
| `none`          | `removed`           | DAG edit during run                      |
| `scheduled`     | `queued`            | Scheduler: enqueue to queue              |
| `scheduled`     | `removed`           | DAG edit during run                      |
| `queued`        | `running`           | Executor: picks from queue               |
| `queued`        | `removed`           | DAG edit during run                      |
| `running`       | `success`           | Executor: plugin completed               |
| `running`       | `failed`            | Executor: no retries left                |
| `running`       | `up_for_retry`      | Executor: attempt failed, retries remain |
| `running`       | `removed`           | DAG edit during run                      |
| `up_for_retry`  | `queued`            | Scheduler: after retry delay             |
| `up_for_retry`  | `failed`            | Manual mark-failed or system limit       |
| `up_for_retry`  | `removed`           | DAG edit during run                      |

### What Happens at Each State

| State             | Actor     | Action Taken                                              |
|-------------------|-----------|-----------------------------------------------------------|
| `none`            | —         | Task exists in metadata, waiting for scheduler evaluation |
| `scheduled`       | Scheduler | Verified trigger rule + upstream states → ready           |
| `queued`          | Scheduler | Wrote message to queue. TaskContext in metadata store     |
| `running`         | Executor  | Read TaskContext → start_attempt() → plugin.execute()     |
| `success`         | Executor  | finish_attempt(SUCCESS) → write outputs → update metadata |
| `failed`          | Executor  | finish_attempt(FAILED) → write error → update metadata    |
| `up_for_retry`    | Executor  | finish_attempt(FAILED) → schedule re-queue after delay    |
| `skipped`         | Scheduler | Trigger rule evaluated to skip → no execution             |
| `upstream_failed` | Scheduler | Upstream task in terminal failed state                    |
| `removed`         | Scheduler | DAG version changed mid-run, task no longer exists        |

---

## Attempt Tracking

Each execution attempt (including retries) is recorded as an `Attempt` object
within the TaskContext:

```python
class Attempt(BaseModel):
    attempt_number: int       # 1-based
    state: AttemptStatus      # running | success | failed | timed_out
    started_at: datetime
    ended_at: datetime
    duration_sec: float
    error: str | None         # error message if failed
    error_traceback: str      # full traceback if failed
    executor: str             # "local", "docker", "k8s", "aws_batch"
    executor_ref: str | None  # "pod-abc123", "container-xyz", "job-id"
```

**Why this matters for production:**
- The UI shows all attempts with individual logs (per attempt)
- You can see which executor ran each attempt (helpful for k8s pod debugging)
- Retry delay calculation uses attempt history (exponential backoff)
- Failed attempts preserve the full traceback for debugging

---

## Executor Architecture

### Executor Types

| Executor             | Use Case                   | How It Runs              |
|----------------------|----------------------------|--------------------------|
| `LocalExecutor`      | Dev, small workloads       | asyncio in-process       |
| `DockerExecutor`     | Isolation, reproducibility | Spawn container per task |
| `KubernetesExecutor` | Production at scale        | Create pod per task      |
| `BatchExecutor`      | AWS Batch / Cloud Batch    | Submit job per task      |

### Executor Contract

All executors implement one method:

```python
class BaseExecutor(ABC):
    executor_type: str = "base"

    async def run_task(self, task_ctx: TaskContext) -> TaskContext:
        """Execute the task and return updated TaskContext."""
        ...
```

### Remote Executor Flow (K8s / Docker / Batch)

```text
┌─────────────────────────────────────────────────────────────────┐
│  Worker Process (runs in beacon server)                          │
│                                                                 │
│  1. Dequeue message {run_id, task_id}                            │
│  2. Read TaskContext from metadata store                         │
│  3. Call executor.run_task(task_ctx)                              │
│     └──> KubernetesExecutor:                                     │
│          a. Create Pod spec with plugin image + TaskContext env   │
│          b. Submit Pod to K8s API                                 │
│          c. Poll Pod status (async, no slot waste)               │
│          d. On completion: read logs, update TaskContext          │
│  4. Write updated TaskContext to metadata store                   │
│  5. Report final state to scheduler                              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

For remote executors, the TaskContext is passed to the container/pod via:
- Environment variable (`BEACON_TASK_CONTEXT` = JSON-serialized)
- Or: mounted file (`/tmp/beacon/task_context.json`)
- Or: API call (pod entrypoint fetches from beacon API)

The container runs a minimal beacon runner:
```bash
# Inside container
beacon-runner execute --from-env  # reads BEACON_TASK_CONTEXT, runs plugin, writes result
```

---

## Async Execution Model

### Why async-first eliminates deferrable complexity

In Airflow, a sensor that polls every 60s either:
- **Poke mode**: Holds a worker slot for hours (wastes resources)
- **Deferrable**: Requires a separate Triggerer process + trigger class

Beacon merges both into one model:

```python
import asyncio
from typing import ClassVar
from beacon.core import BasePlugin, Context

class MySensor(BasePlugin):
    plugin_name: ClassVar[str] = "my-sensor"
    check_interval: int = 60

    async def execute(self, context: Context) -> None:
        while not await self.condition_met():
            await asyncio.sleep(self.check_interval)
```

The async worker naturally yields the event loop during `await` — no slot wasted,
no separate process, no dual-class pattern.

### Worker Scaling for 10,000 DAGs

```text
Scheduler (stateless, single process)
    |
    | enqueue TaskContext to
    v
Message Queue (in-memory / Redis / SQS)
    |
    | consumed by
    v
Worker Pool (N async workers, each handles M concurrent tasks)
    |
    | dispatches to
    v
Executor (Local / Docker / K8s / Batch)
```

**Key design choices for scale:**

1. **DAG versioning** — Parse DAG once per bundle change. Store serialized DAG
   in metadata. Scheduler works from metadata, not filesystem.

2. **Stateless scheduler** — Scheduler reads metadata, computes "what should run
   next", enqueues. No in-memory DAG state. Can restart without data loss.

3. **Concurrency via async** — A single worker process handles hundreds of
   concurrent I/O-bound tasks (sensors, API calls, file polls) via asyncio.
   CPU-bound tasks dispatched to remote executors.

4. **Backpressure** — Queue depth per DAG is bounded. If a DAG produces tasks
   faster than workers consume, it pauses scheduling (not infinite queue growth).

---

## Execution Flow: Deploy → Schedule → Run → Result

### End-to-End Data Flow

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│ Phase 1: Deploy                                                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  CLI: beacon deploy hello-world-workflow \                                  │
│         --dag hello-world \                                                 │
│         --schedule "0 0 * * *" \                                            │
│         --variables variables.yml                                           │
│                                                                             │
│  1. Parse DAG from bundle (YAML/Python)                                     │
│  2. Resolve variables.yml → merge stage vars into Jinja context             │
│  3. Validate DAG structure (plugins exist, dependencies are acyclic)        │
│  4. Serialize DAG + schedule → store in Metadata                            │
│  5. Tag with bundle version (content hash / commit SHA)                     │
│                                                                             │
│  Metadata written:                                                          │
│    - DagRecord(id, version, serialized_dag, created_at)                     │
│    - ScheduleRecord(id, dag_id, dag_version, cron, timezone, params, vars)  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    v
┌─────────────────────────────────────────────────────────────────────────────┐
│ Phase 2: Schedule Trigger                                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Scheduler loop (every heartbeat):                                          │
│    1. Query ScheduleRecords where next_run <= now                           │
│    2. For each due schedule:                                                │
│       a. Compute logical_date, data_interval_start, data_interval_end       │
│       b. Create DagRunRecord(run_id, dag_id, dag_version, state="running")  │
│       c. For each action in DAG: build TaskContext → store in metadata      │
│       d. TaskContext.attempts = [] (empty, no execution yet)                │
│       e. Task state = NONE                                                  │
│    3. Evaluate task dependencies:                                           │
│       - Tasks with no upstream / all upstream SUCCESS → SCHEDULED → QUEUED  │
│       - Enqueue {run_id, task_id} message to Queue                          │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    v
┌─────────────────────────────────────────────────────────────────────────────┐
│ Phase 3: Task Execution                                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Worker dequeues {run_id, task_id}:                                         │
│    1. Read TaskContext from metadata store                                  │
│    2. Set task state → RUNNING                                              │
│    3. Fire on_event: start callbacks                                        │
│    4. Dispatch to executor: executor.run_task(task_ctx)                     │
│       - Executor: start_attempt(executor_type, executor_ref)                │
│       - Executor: build Context dict from TaskContext                       │
│       - Executor: plugin.execute(context)                                   │
│       - Executor: finish_attempt(state, error, outputs)                     │
│    5. Write updated TaskContext to metadata store                            │
│    6. Evaluate result:                                                      │
│       - SUCCESS → state = SUCCESS, fire success callbacks                   │
│       - FAILED + retries left → state = UP_FOR_RETRY, schedule re-queue    │
│       - FAILED + no retries → state = FAILED, fire failure callbacks        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    v
┌─────────────────────────────────────────────────────────────────────────────┐
│ Phase 4: DagRun Resolution & Logging                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  After each task reaches terminal state:                                    │
│    1. Scheduler re-evaluates downstream tasks                               │
│       - Downstream with all deps met → SCHEDULED → QUEUED                  │
│       - Downstream with upstream failed → UPSTREAM_FAILED                   │
│    2. When all tasks terminal:                                              │
│       - All SUCCESS → DagRun SUCCESS → fire dag on_event: success           │
│       - Any FAILED → DagRun FAILED → fire dag on_event: failure             │
│    3. Logs available via API:                                               │
│       GET /dags/{dag_id}/runs/{run_id}/tasks/{task_id}/logs?attempt=N       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Retry Flow (Detail)

```text
Attempt 1:
  QUEUED → RUNNING → plugin raises error → finish_attempt(FAILED)
  TaskContext.attempts = [{attempt:1, state:failed, error:"..."}]
  Task state → UP_FOR_RETRY
  Schedule re-queue after: retry_delay * 2^0 = 10s

Attempt 2:
  (after 10s) UP_FOR_RETRY → QUEUED → RUNNING → plugin raises error
  TaskContext.attempts = [{...attempt 1...}, {attempt:2, state:failed, error:"..."}]
  Task state → UP_FOR_RETRY
  Schedule re-queue after: retry_delay * 2^1 = 20s

Attempt 3:
  (after 20s) UP_FOR_RETRY → QUEUED → RUNNING → plugin succeeds
  TaskContext.attempts = [{...}, {...}, {attempt:3, state:success}]
  Task state → SUCCESS
  TaskContext.outputs = {result: ...}
```

---

## Templating

### Jinja for Variable Substitution Only

Beacon uses Jinja2 for **value interpolation**, not control flow:

Layer of macros:

- `vars`: global variables from `.global_vars.yml` merged with stage variables
- `params`: DAG parameters passed at runtime

```text
Resolution order: `vars` → `params` → `inputs`
```

```yaml
# Supported
inputs:
  source: "{{ params.source_system }}"
  path: "{{ params.base_path }}/{{ params.date }}"

# NOT supported in DAG definition (by design)
{% for item in items %}  # No.
{% if env == 'prod' %}   # No.
```

**Rationale:** Control flow in DAG definitions creates unpredictable graph shapes
that cannot be statically validated, versioned, or displayed in a UI before execution.

### Dynamic Tasks (for-each)

When you need fan-out, use a first-class `for_each` field — not Jinja loops:

```yaml
tasks:
  - id: "process-{{ item }}"
    type: task
    uses: py
    for_each: "{{ params.source_systems }}"
    inputs:
      source_system: "{{ item }}"
```

**How it works:**

1. At DAG parse time, `for_each` is stored as a template expression
2. At trigger time, the scheduler resolves it against runtime params → gets a list
3. TaskContext instances are generated — one per item in the resolved list
4. Each instance is independently scheduled, retried, and tracked
5. Each gets its own attempt history in its own TaskContext

---

## Variable Resolution Timeline

```text
Deploy time:                    Trigger time:               Execute time:
─────────────                   ──────────────              ─────────────
variables.yml parsed            vars() resolved             Plugin receives
  │                               │                         final values
  v                               v                         from Context
stages:                         Jinja renders               (already concrete)
  prod:                         schedule.params
    source_system: x            with vars context
                                  │
                                  v
                                TaskContext.params = {
                                  source_system: "x"  ← was "{{ vars('source_system') }}"
                                }
                                TaskContext.inputs = {
                                  py_file: "./process.py"
                                  params: {source_system: "x"}  ← was "{{ params.source_system }}"
                                }
```

**Two-pass rendering:**
1. **Trigger time** — `vars()` expressions resolved against variable store → becomes `params`
2. **Pre-execute (in scheduler)** — `params.*` resolved against TaskContext.params → becomes `inputs`

By execution time, TaskContext.inputs contains **fully resolved concrete values**.
The executor and plugin never do Jinja rendering.

---

## Model Hierarchy

```text
Plugin ─── defines execution logic (reusable, versioned)
  │
  v
Action ─── references a plugin via `uses`, provides `inputs`
  │         types: Task, Sensor, Branch, ForEach (future)
  v
Dag ──── declares actions + dependencies + params + callbacks
  │
  v
Schedule ── binds a Dag to a cron + timezone + variables
```

### Action Types

| Type           | Purpose                | Async Behavior                  |
|----------------|------------------------|---------------------------------|
| `task`         | Execute a unit of work | Run to completion               |
| `sensor`       | Wait for a condition   | Async poll with sleep           |
| `branch`       | Choose downstream path | Evaluate condition, return path |
| `foreach_task` | Fan-out over a list    | Spawn N task instances          |

All four share `BaseAction` and resolve plugins the same way.
All four execute via `async def execute()`.
All four use TaskContext for state persistence.

---

## Logging Store & Web UI

```text
Task execution:
  context.logger.info("Processing file X")    ─┐
  context.logger.error("Failed to connect")    │
  (stdout/stderr from remote executor)         │
                                               v
                                    ┌──────────────────┐
                                    │  Logging Store    │
                                    │  (append-only)    │
                                    ├──────────────────┤
                                    │ key format:       │
                                    │ {dag_id}/         │
                                    │   {run_id}/       │
                                    │     {task_id}/    │
                                    │       attempt_N   │
                                    └────────┬─────────┘
                                             │
                                             │ API Server reads
                                             v
                      GET /dags/{dag_id}/runs/{run_id}/tasks/{task_id}/logs?attempt=2
                                             │
                                             v
                                    ┌──────────────────┐
                                    │   Web UI          │
                                    │   - DAG graph     │
                                    │   - Task states   │
                                    │   - Log viewer    │
                                    │   - Attempt tabs  │
                                    └──────────────────┘
```

Each retry attempt gets its own log. The UI shows attempt tabs with:
- Executor type + reference (e.g., "k8s / pod-abc123")
- Duration
- Error message + traceback (if failed)
- Outputs (if success)

---

## Bundle & Production Deployment

### Repository Structure

A team's workflow repository follows this convention:

```text
my-workflow-repo/
├── dags/                    # DAG definitions
│   ├── etl_pipeline.yml
│   ├── ml_training.py
│   └── reports/
│       └── daily_report.yml
├── plugins/                 # Custom plugins (auto-registered)
│   ├── gcs_extract.py
│   └── bigquery_load.py
└── scripts/                 # Python files called by `uses: py`
    ├── transform.py
    └── validate.py
```

### GitBundle Sync Flow

```text
┌─────────────────────────────────────────────────────────────────┐
│ Developer pushes to main branch                                  │
└───────────────────────────────┬─────────────────────────────────┘
                                │ webhook / polling
                                v
┌─────────────────────────────────────────────────────────────────┐
│ API Server receives sync event                                   │
│                                                                 │
│  1. git pull → local checkout at /bundles/{name}/                │
│  2. version = git rev-parse HEAD (commit SHA)                    │
│  3. bundle.load_plugins()                                        │
│     - Scan ./plugins/*.py                                        │
│     - Import each file → PluginMeta auto-registers subclasses    │
│     - Custom plugins now available in PLUGINS_REGISTRY           │
│  4. bundle.discover_dags()                                       │
│     - Find all .yml/.yaml/.py in ./dags/                         │
│  5. For each DAG file:                                           │
│     - Parse → validate (plugins exist, no cycles)                │
│     - Serialize → store DagRecord(id, version, serialized_dag)   │
│  6. Old DAG versions preserved (running instances unaffected)    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Writing Custom Plugins

A custom plugin in `./plugins/` is just a `BasePlugin` subclass:

```python
# plugins/gcs_extract.py
from typing import ClassVar, Any
from beacon.core import BasePlugin, Context


class GcsExtractPlugin(BasePlugin):
    plugin_name: ClassVar[str] = "gcs-extract"

    bucket: str
    prefix: str

    async def execute(self, context: Context) -> dict[str, Any]:
        # Your logic here
        from google.cloud import storage
        client = storage.Client()
        blobs = list(client.list_blobs(self.bucket, prefix=self.prefix))
        return {"files": len(blobs)}
```

Then reference it in a DAG:

```yaml
# dags/etl.yml
tasks:
  - id: extract
    type: task
    uses: gcs-extract        # ← resolved from plugins/
    inputs:
      bucket: my-data-lake
      prefix: "raw/{{ params.date }}"
```

No pip install. No provider package. Just a `.py` file in `./plugins/`.

### User Python Files (py plugin)

For logic that doesn't need a reusable plugin, use the `py` plugin:

```yaml
# dags/etl.yml
tasks:
  - id: transform
    type: task
    uses: py
    inputs:
      py_file: ../scripts/transform.py
      py_function: main
      params:
        source: "{{ params.source_system }}"
```

The Python file:

```python
# scripts/transform.py
from beacon import load_context

def main(source: str):
    ctx = load_context()
    ctx.logger.info("Processing %s (attempt %d)", source, ctx.attempt_number)
    # Your logic here
    return {"rows_processed": 1000}
```

**Two levels of customization:**
- `uses: py` → simple: write a function, no ceremony
- `./plugins/` → reusable: write a plugin class, share across DAGs

### Bundle Versioning

```text
LocalBundle  ── file content hash ──> version tag
GitBundle    ── commit SHA ──────────> version tag
GcsBundle    ── object generation ───> version tag
```

When a bundle changes:

1. Load new plugins (overrides existing registrations)
2. Reparse affected DAGs
3. Store new serialized DAG with new version
4. Running instances continue on their original version (TaskContext has `dag_version`)
5. New runs use the new version

This prevents the Airflow problem where editing a DAG file mid-run corrupts
running task instances.

---

## What Beacon Does NOT Do (By Design)

- **No XCom-like cross-task data passing** — Tasks communicate via external
  storage (S3, GCS, database). TaskContext carries data TO a task, not between tasks.

- **No embedded Python in YAML** — YAML is configuration. Python logic lives in
  `.py` files referenced by the `py` plugin.

- **No provider installation at runtime** — Plugins are resolved at parse time.
  If a plugin isn't available, the DAG fails to parse (fast feedback).

- **No DAG-level Jinja control flow** — Graph shape is static and inspectable.

- **No multi-tenant scheduler** — One scheduler instance per deployment.
  Scale horizontally by deploying separate beacon instances per team/domain.

- **No Jinja rendering at execution time** — All templates resolved before
  TaskContext reaches the executor. Executors are dumb runners.

---

## Implementation Priority

| Phase  | Deliverable                                           | Status   | Validates                           |
|--------|-------------------------------------------------------|----------|-------------------------------------|
| **0**  | `PythonPlugin.execute()` end-to-end                   | ✅ Done   | Core loop                           |
| **0**  | TaskContext + Attempt + LocalExecutor                 | ✅ Done   | State persistence + retry           |
| **0**  | Bundle plugin auto-discovery (`./plugins/`)           | ✅ Done   | Custom plugin deployment            |
| **0**  | `load_context()` for user Python files                | ✅ Done   | Runtime access without coupling     |
| **0**  | Task state machine with valid transitions             | ✅ Done   | Enforced lifecycle                  |
| **1**  | Hook system with registry resolution (string + class) | ✅ Done   | Callback parity Python/YAML         |
| **1**  | Async worker with retry scheduling                    | ✅ Done   | Full lifecycle transitions          |
| **1**  | Metadata store (JsonMetadata): TaskContext CRUD       | ✅ Done   | Persistence for remote executors    |
| **2**  | GitBundle sync (webhook + git pull)                   | Pending  | Production deployment               |
| **2**  | DockerExecutor / KubernetesExecutor                   | Pending  | Remote execution                    |
| **2**  | `foreach_task` action type                            | Pending  | Dynamic parallelism                 |
| **2**  | Scheduler + metadata-based DAG versioning             | Pending  | Scale to 10k DAGs                   |
| **3**  | Remote plugin registry (`org/name@version`)           | Pending  | Ecosystem growth                    |
| **3**  | Web UI (DAG viewer + run history + log viewer)        | Pending  | Observability                       |
| **3**  | BatchExecutor (AWS Batch / Cloud Batch)               | Pending  | Cloud-native execution              |
