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
   references it via `uses: py`, and it runs. No operator inheritance, no
   callback composition, no provider installation.

4. **Scalable by default** — The architecture supports 10,000+ deployed DAGs
   through stateless workers, append-only logging, and lightweight DAG parsing.

5. **Executor-agnostic** — TaskContext is serializable. The same task runs on
   local process, Docker, Kubernetes, AWS Batch, or Cloud Batch without code changes.

6. **DAGs are reusable templates** — A DAG defines *what to do*; a Deployment
   defines *how/when to do it* (params, schedule, env). One DAG → many Deployments.

---

## Overall Architecture

```text
User --> Client (CLI / SDK / YAML)
           |
           v
      API Server (FastAPI) ──────────> Metadata Store (Json / Sqlite / Postgres)
           |                              ^       ^
           v                              |       |
      Scheduler ── enqueue ──> Queue ─────┘       |
                                 |                |
                                 v                |
                              Worker ─────────────┘
                                 |
                                 v
                            Executor
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
| DAG reuse           | One file = one DAG = one schedule          | One DAG, N Deployments (params, cron)  |
| Scaling to 10k DAGs | Requires multiple schedulers + DB tuning   | Stateless scheduler + queue            |

---

## DAG vs Deployment (Reuse Model)

The core abstraction that distinguishes Beacon from Airflow:

```text
Dag ──── reusable template (defines tasks + params schema)
  │
  └── Deployment 1 ── binds Dag to: cron="0 2 * * *", params={source: postgres}
  │
  └── Deployment 2 ── binds Dag to: cron="0 * * * *", params={source: mysql}
  │
  └── Deployment 3 ── manual-only,  params={source: snowflake}
```

A single `Dag` template (e.g., `extract-load-table`) can have many `Deployment`s,
each with its own:

- Identity (`Deployment.id` shown in the UI)
- Schedule (`cron`, `start_date`, `end_date`, `timezone`, `catch_up`)
- Runtime params (values for `Dag.params`)
- Stage variables (`variable_overrides` → per-deployment overrides layered on top of the bundle's scoped `variables.yml` / `global_variables.yml` chain)
- Version pin (`dag_version` — None = latest)
- Owners, labels, enabled flag

This eliminates Airflow's anti-pattern of duplicating DAG files just to change
a schedule or a source name.

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

### Plugin Error Signalling

Plugins control retry behavior by which exception they raise:

| Raises          | Behavior                             | Use Case                           |
|-----------------|--------------------------------------|------------------------------------|
| Any `Exception` | Retry up to `retries`, then `FAILED` | Transient errors (network timeout) |
| `TaskFailed`    | Immediately `FAILED` — skip retries  | Permanent errors (table missing)   |
| `TaskSkipped`   | Mark `SKIPPED` — skip retries        | Nothing to do (empty partition)    |

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

---

## Callback System

### Unified with Plugin Registry

Callbacks use the **same resolution mechanism** as `uses`. A callback is just
a class that runs on a lifecycle event rather than as a DAG step.

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
from beacon import OnDagEvent
from beacon.providers.msteam.callback import MsTeamCallback

dag = Dag(
    callbacks=[
        OnDagEvent(
            on_event="failure",
            hook=MsTeamCallback,  # or string "msteam-adaptive-card"
            inputs={"webhook_url": "https://..."},
        ),
    ],
)
```

**Callback resolution:**

```text
hook: "msteam-adaptive-card"              --> CALLBACKS_REGISTRY["msteam-adaptive-card"]
hook: "my-org/callback-plugin@1.1.0"      --> external callback plugin
hook: MsTeamCallback                      --> direct class reference (Python only)
```

### Callback Contract

```python
from abc import ABC
from typing import ClassVar, Any
from beacon import Callback

class MyCallback(Callback):
    hook_name: ClassVar[str] = "my-callback"

    def __init__(self, webhook_url: str) -> None:
        self.webhook_url = webhook_url

    async def notify(self, event: str, data: dict[str, Any]) -> None:
        ...
```

Same pattern as `BasePlugin` but with `notify()` instead of `execute()`.
Same registry. Same version pinning. Same resolution.

### Event Types

| Event Owner   | Events                                            |
|---------------|---------------------------------------------------|
| `OnTaskEvent` | `start`, `success`, `failure`, `retry`, `skipped` |
| `OnDagEvent`  | `start`, `success`, `failure`, `finished`         |

`finished` fires on any DAG terminal state (success or failure).

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
    params: dict[str, Any]       # Deployment params (Jinja-rendered with vars)
    inputs: dict[str, Any]       # Task inputs (Jinja-rendered with params)
    plugin_name: str             # Which plugin to run

    # Execution config
    retries: int                 # Max retry attempts
    retry_delay: int             # Base delay seconds
    execution_timeout: int | None
    exponential_backoff: bool

    # Attempt history (one per try, including retries)
    attempts: list[Attempt]

    # Upstream outputs (read-only, populated by worker)
    upstream_outputs: dict[str, dict[str, Any]]

    # Outputs (written after success)
    outputs: dict[str, Any]

    @property
    def attempt_number(self) -> int:
        """Number of the most recent attempt (1-based). 0 if not started."""
        return len(self.attempts)
```

### How TaskContext Enables Remote Execution

```text
┌──────────────────────────────────────────────────────────────────┐
│ Scheduler                                                        │
│   1. Build TaskContext (resolve inputs, set plugin_name)         │
│   2. Serialize TaskContext → Metadata Store (JSON)               │
│   3. Enqueue message: {run_id, dag_id, task_id} → Queue          │
└──────────────────────────────────────────────────────────────────┘
                              │
                              v
┌──────────────────────────────────────────────────────────────────┐
│ Worker → Executor (any type: local, docker, k8s, aws batch)      │
│   1. Receive message from Queue                                  │
│   2. Read TaskContext from Metadata Store                        │
│   3. Resolve plugin from plugin_name                             │
│   4. Call task_ctx.start_attempt(executor="k8s", ref="pod-xyz")  │
│   5. Build Context dict (lightweight, for plugin.execute())      │
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
│    │                       ├──> SUCCESS ────── (terminal)                │
│    │                       ├──> FAILED ─────── (terminal, no retries)    │
│    │                       ├──> SKIPPED ────── (terminal, TaskSkipped)   │
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
| `queued`        | `running`           | Worker: picks from queue                 |
| `queued`        | `removed`           | DAG edit during run                      |
| `running`       | `success`           | Plugin completed                         |
| `running`       | `failed`            | Plugin raised; no retries left           |
| `running`       | `skipped`           | Plugin raised `TaskSkipped`              |
| `running`       | `up_for_retry`      | Plugin raised; retries remain            |
| `running`       | `removed`           | DAG edit during run                      |
| `up_for_retry`  | `queued`            | Worker: after retry delay                |
| `up_for_retry`  | `failed`            | Manual mark-failed or system limit       |
| `up_for_retry`  | `removed`           | DAG edit during run                      |

### What Happens at Each State

| State             | Actor     | Action Taken                                              |
|-------------------|-----------|-----------------------------------------------------------|
| `none`            | —         | Task exists in metadata, waiting for scheduler evaluation |
| `scheduled`       | Scheduler | Verified trigger rule + upstream states → ready           |
| `queued`          | Scheduler | Wrote message to queue. TaskContext in metadata store     |
| `running`         | Worker    | Read TaskContext → start_attempt() → plugin.execute()     |
| `success`         | Worker    | finish_attempt(SUCCESS) → write outputs → update metadata |
| `failed`          | Worker    | finish_attempt(FAILED) → write error → update metadata    |
| `skipped`         | Worker    | finish_attempt(SKIPPED) — TaskSkipped from plugin         |
| `up_for_retry`    | Worker    | finish_attempt(FAILED) → schedule re-queue after delay    |
| `upstream_failed` | Scheduler | Upstream task in terminal failed state                    |
| `removed`         | Scheduler | DAG version changed mid-run, task no longer exists        |

---

## Attempt Tracking

Each execution attempt (including retries) is recorded as an `Attempt` object
within the TaskContext:

```python
class Attempt(BaseModel):
    attempt_number: int       # 1-based
    state: AttemptStatus      # running | success | failed | skipped | timed_out
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
│  1. Dequeue message {run_id, dag_id, task_id}                    │
│  2. Read TaskContext from metadata store                         │
│  3. Call executor.run_task(task_ctx)                             │
│     └──> KubernetesExecutor:                                     │
│          a. Create Pod spec with plugin image + TaskContext env  │
│          b. Submit Pod to K8s API                                │
│          c. Poll Pod status (async, no slot waste)               │
│          d. On completion: read logs, update TaskContext         │
│  4. Write updated TaskContext to metadata store                  │
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

## Metadata Store

### Protocol-Based (Pluggable)

The Worker, Scheduler, and API Server depend on `MetadataProtocol` — not a
concrete class. Any store implementing the protocol works.

```python
class MetadataProtocol(Protocol):
    async def create_dag_run(...) -> None: ...
    async def get_dag_run(run_id, dag_id) -> dict | None: ...
    async def update_dag_run_state(run_id, dag_id, state) -> None: ...
    async def put_task_context(run_id, dag_id, task_id, ctx) -> None: ...
    async def get_task_context(run_id, dag_id, task_id) -> TaskContext | None: ...
    async def set_task_state(run_id, dag_id, task_id, state) -> None: ...
    async def get_task_state(run_id, dag_id, task_id) -> TaskState | None: ...
```

### Implementations

| Store              | Persistence        | Use Case                         |
|--------------------|--------------------|----------------------------------|
| `JsonMetadata`     | Sharded JSON files | Default, dev → 1000 DAGs         |
| `SqliteMetadata`   | Local SQLite       | Single-node production (pending) |
| `PostgresMetadata` | Postgres           | Multi-node production (pending)  |

### JsonMetadata Performance

Optimized for 1000+ DAG workloads without an external database:

- **Sharded by `dag_id`** — `{base}/{dag_runs|task_contexts|task_states}/{dag_id}/...`
  keeps each directory small (no flat 100k-file dirs)
- **Async I/O** via `asyncio.to_thread` — file reads/writes don't block the event loop
- **Atomic writes** — temp file + `os.replace` prevents partial reads
- **In-memory cache** for task states (scheduler hot path)
- **Bulk queries** — `get_all_task_states(run_id, dag_id)` for dependency evaluation
- **Cache eviction** — `evict_run_from_cache()` frees memory when runs complete

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
│  CLI: beacon deploy daily-customers-from-postgres \                         │
│         --dag extract-load-table \                                          │
│         --cron "0 2 * * *" \                                                │
│         --params source=postgres target=customers \                         │
│         --var alert_path=/var/oncall                                        │
│                                                                             │
│  1. Parse DAG from bundle (YAML/Python) — only if not already deployed      │
│  2. Validate Deployment.dag_id resolves to a known Dag                      │
│  3. Validate DAG structure (plugins exist, dependencies are acyclic)        │
│  4. Serialize Dag + Deployment → store in Metadata                          │
│  5. Tag Dag with bundle version (content hash / commit SHA)                 │
│                                                                             │
│  Metadata written:                                                          │
│    - DagRecord(id, version, serialized_dag, created_at)                     │
│    - DeploymentRecord(id, dag_id, dag_version, cron, timezone,              │
│                       params, variable_overrides, start_date, end_date)     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    v
┌─────────────────────────────────────────────────────────────────────────────┐
│ Phase 2: Schedule Trigger                                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Scheduler loop (every heartbeat):                                          │
│    1. Query DeploymentRecords where enabled=True and next_run <= now        │
│    2. For each due deployment:                                              │
│       a. Compute logical_date, data_interval_start, data_interval_end       │
│       b. Resolve vars() against scoped chain → final params                 │
│       c. Create DagRunRecord(run_id, dag_id, deployment_id, dag_version)    │
│       d. For each action in DAG: build TaskContext → store in metadata      │
│       e. TaskContext.attempts = [] (empty, no execution yet)                │
│       f. Task state = NONE                                                  │
│    3. Evaluate task dependencies:                                           │
│       - Tasks with no upstream / all upstream SUCCESS → SCHEDULED → QUEUED  │
│       - Enqueue {run_id, dag_id, task_id} message to Queue                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    v
┌─────────────────────────────────────────────────────────────────────────────┐
│ Phase 3: Task Execution                                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Worker dequeues {run_id, dag_id, task_id}:                                 │
│    1. Read TaskContext from metadata store                                  │
│    2. Resolve upstream_outputs from declared upstreams                      │
│    3. Set task state → RUNNING                                              │
│    4. Fire on_event: start callbacks                                        │
│    5. Dispatch to executor: executor.run_task(task_ctx)                     │
│       - Executor: start_attempt(executor_type, executor_ref)                │
│       - Executor: build Context dict from TaskContext                       │
│       - Executor: plugin.execute(context)                                   │
│       - Executor: finish_attempt(state, error, outputs)                     │
│    6. Write updated TaskContext to metadata store                           │
│    7. Evaluate result:                                                      │
│       - SUCCESS  → state=SUCCESS,  fire success callbacks                   │
│       - SKIPPED  → state=SKIPPED,  fire skipped callbacks                   │
│       - FAILED + retries left → state=UP_FOR_RETRY, schedule re-queue       │
│       - FAILED + no retries   → state=FAILED, fire failure callbacks        │
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
│       - Downstream with all deps met → SCHEDULED → QUEUED                   │
│       - Downstream with upstream failed → UPSTREAM_FAILED                   │
│    2. When all tasks terminal:                                              │
│       - All SUCCESS → DagRun SUCCESS → fire dag on_event: success           │
│       - Any FAILED → DagRun FAILED → fire dag on_event: failure             │
│       - Either way → fire dag on_event: finished                            │
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

If the plugin raises `TaskFailed` instead of a generic exception, retries are
skipped and the task transitions directly to `FAILED`.

---

## Templating

### Jinja for Variable Substitution Only

Beacon uses Jinja2 for **value interpolation**, not control flow.

Layer of macros:

- `vars`: stage variables loaded from `variables.yml`, scoped per deployment
- `params`: Deployment params passed at trigger time
- `outputs`: upstream task outputs (resolved at execution time)

```text
Resolution order: vars → params → outputs → inputs
```

```yaml
# Supported
inputs:
  source: "{{ params.source_system }}"
  path: "{{ params.base_path }}/{{ params.date }}"
  bucket: "{{ vars('gcs_bucket') }}"
  upstream_count: "{{ outputs.extract.row_count }}"

# NOT supported in DAG definition (by design)
{% for item in items %}  # No control flow
{% if env == 'prod' %}   # No conditionals
```

**Rationale:** Control flow in DAG definitions creates unpredictable graph shapes
that cannot be statically validated, versioned, or displayed in a UI before execution.

See [`templating.md`](./templating.md) for the full rendering pipeline.

### Dynamic Tasks (for-each)

When you need fan-out, use a first-class `for_each` field — not Jinja loops:

```yaml
actions:
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
Deploy time:                Trigger time:               Execute time:
─────────────               ──────────────              ─────────────
variables.yml parsed        vars() resolved             Plugin receives
  │                           │                         final concrete
  v                           v                         values from
stages:                     Jinja renders               Context (no
  prod:                     deployment.params           Jinja rendering)
    source_system: x        with vars(prod stage)
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
1. **Trigger time** — `vars()` expressions resolved against the bundle's
   scoped variables chain (with deployment `variable_overrides` on top)
   → becomes `params`
2. **Pre-execute (in scheduler)** — `params.*` resolved against TaskContext.params
   → becomes `inputs`

By execution time, TaskContext.inputs contains **fully resolved concrete values**.
The executor and plugin never do Jinja rendering.

---

## Model Hierarchy

```text
Plugin ─── defines execution logic (reusable, versioned)
  │
  v
Action ─── references a plugin via `uses`, provides `inputs`
  │         types: Task, Sensor, Branch, ShortCircuit, Group, ForEach (future)
  v
Dag ──── reusable template: actions + dependencies + params schema + callbacks
  │
  v
Deployment ── binds a Dag to: cron + timezone + params values + variable_overrides
              many Deployments can reference one Dag
```

### Action Types

| Type            | Purpose                | Async Behavior                  |
|-----------------|------------------------|---------------------------------|
| `task`          | Execute a unit of work | Run to completion               |
| `sensor`        | Wait for a condition   | Async poll with sleep           |
| `branch`        | Choose downstream path | Evaluate condition, return path |
| `short_circuit` | Skip all downstream    | Evaluate boolean, skip if false |
| `group`         | Bundle nested actions  | Container, not a runtime unit   |
| `foreach_task`  | Fan-out over a list    | Spawn N task instances (future) |

All share `BaseAction` and resolve plugins the same way.
All execute via `async def execute()`.
All use TaskContext for state persistence.

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
                                    │   - Deployments   │
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

**UI primary list is Deployments, not DAGs.** Users think in terms of
"daily-customers-from-postgres" (a deployment), not the underlying reusable
`extract-load-table` DAG. Click into a deployment to see its DAG graph and runs.

---

## Bundle & Production Deployment

### Repository Structure

A team's workflow repository follows this convention:

```text
my-workflow-repo/
├── dags/                    # DAG definitions (reusable templates)
│   ├── etl_pipeline.yml
│   ├── ml_training.py
│   └── reports/
│       └── daily_report.yml
├── deployments/             # Deployments (per-environment, per-config)
│   ├── customers-from-postgres.yml
│   └── orders-from-mysql.yml
├── plugins/                 # Custom plugins (auto-registered)
│   ├── gcs_extract.py
│   └── bigquery_load.py
├── scripts/                 # Python files called by `uses: py`
│   ├── transform.py
│   └── validate.py
└── variables.yml            # Stage variables (prod, dev, staging)
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
│  6. bundle.discover_deployments()                                │
│     - Find all .yml in ./deployments/                            │
│     - Validate each Deployment.dag_id resolves to a known DAG    │
│     - Store DeploymentRecord                                     │
│  7. Old DAG versions preserved (running instances unaffected)    │
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
actions:
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
actions:
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
5. New runs use the new version (unless a Deployment pins `dag_version`)

This prevents the Airflow problem where editing a DAG file mid-run corrupts
running task instances.

---

## Upstream Output Transfer

### Concept

Tasks can produce outputs (return a dict from `execute()`). Downstream tasks
can access upstream outputs — but only through explicit declaration, not
implicit global state like Airflow's XCom.

**How it differs from XCom:**

| XCom (Airflow)                    | Upstream Outputs (Beacon)              |
|-----------------------------------|----------------------------------------|
| Any task can push/pull from any   | Only declared upstream outputs visible |
| Stored in shared XCom table       | Stored per-task in TaskContext         |
| Unlimited size (DB bloat risk)    | Bounded: only dict return from plugin  |
| Implicit coupling                 | Explicit: upstream_task_ids declared   |

### Usage in YAML

```yaml
actions:
  - id: extract
    type: task
    uses: py
    inputs:
      py_file: ./scripts/extract.py

  - id: transform
    type: task
    uses: py
    upstream: [extract]
    inputs:
      py_file: ./scripts/transform.py
      params:
        file_count: "{{ outputs.extract.row_count }}"
```

### Usage in Python (via load_context)

```python
# scripts/transform.py
from beacon import load_context

def main():
    ctx = load_context()
    extract_out = ctx.upstream_outputs["extract"]
    files = extract_out["files"]
    ctx.logger.info("Processing %d files", len(files))
    return {"processed": len(files)}
```

### How It Works

1. Upstream task completes → outputs stored in TaskContext in metadata
2. Scheduler enqueues downstream task with `upstream_task_ids=["extract"]`
3. Worker resolves: reads upstream TaskContexts from metadata
4. Populates `task_ctx.upstream_outputs = {"extract": {...}}`
5. Plugin receives outputs via `context["upstream_outputs"]` or `load_context()`

### Design Constraints

- **Read-only** — downstream cannot modify upstream outputs
- **Only from declared upstreams** — no arbitrary cross-DAG access
- **Small data only** — outputs should be metadata (paths, counts, IDs), not payloads
- **Resolved once** — at execution time, not re-read on retry (same outputs for all attempts)

---

## DAG User Journey (`Dag.run()` / `Dag.test()` / `Dag.dryrun()`)

The `Dag` model provides three methods that form the developer workflow:

```text
dag = Dag(...)
     │
     ├── dag.dryrun()   → Validate structure + render templates (no execution)
     │
     ├── dag.test()     → Execute all tasks, report pass/fail per task (temp storage)
     │
     └── dag.run()      → Execute full DAG locally, persist state, return outputs
```

### `dag.dryrun(params=..., variables=...)`

Pre-deployment validation. Checks plugins exist, graph is acyclic, and Jinja
templates render correctly with the given params/variables. **No plugin code runs.**

```python
dag = Dag(id="etl", owners=["de"], actions=[...])
result = dag.dryrun(params={"source": "postgres"}, variables={"bucket": "prod"})
print(result.print())   # Shows resolved inputs per task
assert result.is_valid  # Fails fast if misconfigured
```

### `dag.test(params=...)`

Executes all tasks in a temporary metadata store. Reports pass/fail per task.
Use this to verify plugins can be instantiated and produce outputs.

```python
result = dag.test(params={"source": "postgres"})
assert result["passed"]  # All tasks succeeded
print(result["tasks"])   # {"extract": {"state": "success", "outputs": {...}}, ...}
```

### `dag.run(params=..., metadata_path=...)`

Full local execution with state persistence. Tasks run in topological order.
Upstream outputs flow to downstream tasks. Returns run_id, states, and outputs.

```python
result = dag.run(params={"source": "postgres"})
print(result["outputs"]["transform"])  # {"rows_processed": 1000}
```

---

## Setup & Teardown Tasks (Action-Level)

### Problem

Many data pipelines need **resource provisioning** before tasks run and
**resource cleanup** after tasks complete (regardless of success or failure):

- **Provision a Spark/Dataproc cluster** → run tasks → **tear down cluster**
- **Create a staging table** → load data → **drop staging table**
- **Acquire a connection slot** → run queries → **release slot**

### Design: `teardown` Field on Any Action

Instead of DAG-level `setup`/`teardown` lists, Beacon uses an **action-level
`teardown` field** — any task can declare itself as the teardown for another
specific task. This is flexible and composable (like Airflow's `as_teardown()`).

```yaml
actions:
  - id: create-cluster
    type: task
    uses: py
    inputs:
      py_file: ../scripts/cluster.py
      py_function: create

  - id: run-etl
    type: task
    uses: py
    upstream: [create-cluster]
    inputs:
      py_file: ../scripts/etl.py
      py_function: main
      params:
        cluster: "{{ outputs.create-cluster.endpoint }}"

  - id: destroy-cluster
    type: task
    uses: py
    teardown: create-cluster          # ← "I am the teardown for create-cluster"
    inputs:
      py_file: ../scripts/cluster.py
      py_function: destroy
      params:
        cluster: "{{ outputs.create-cluster.endpoint }}"
```

### Semantics

```text
┌────────────────────────────────────────────────────────────────────┐
│  Task: create-cluster (setup task — referenced by teardown)        │
│    Runs first (no upstream).                                       │
│    Outputs: { endpoint: "cluster-abc.internal:8080" }              │
└───────────────────────────┬────────────────────────────────────────┘
                            │
                            v
┌────────────────────────────────────────────────────────────────────┐
│  Task: run-etl (normal task, upstream: [create-cluster])           │
│    Uses cluster endpoint from outputs.                             │
└───────────────────────────┬────────────────────────────────────────┘
                            │ after ALL dependents of create-cluster
                            │ reach terminal state (success OR failure)
                            v
┌────────────────────────────────────────────────────────────────────┐
│  Task: destroy-cluster (teardown: create-cluster)                  │
│    ALWAYS runs — even if run-etl failed.                           │
│    Can access create-cluster outputs.                              │
└────────────────────────────────────────────────────────────────────┘
```

### Key Rules

1. **`teardown: <task_id>`** — marks this task as teardown for that task.
2. **The setup task is just a normal task** — no special field needed. It becomes
   a "setup" implicitly because another task declares it as its teardown target.
3. **Teardown always runs** — regardless of whether dependents of its setup task
   succeeded or failed. It uses `trigger_rule: ALL_DONE` semantics implicitly.
4. **Teardown runs last** — after ALL tasks that depend (directly or transitively)
   on the setup task have reached terminal state.
5. **Teardown can access setup outputs** — the setup task_id is added to its
   `upstream_outputs` automatically so it can read provisioned resources.
6. **Teardown failure is non-fatal** — logged as warning. The DagRun state
   reflects the main task outcomes, not teardown failures.
7. **Validated at dryrun** — the referenced task_id must exist in the DAG.
   Self-reference is rejected.

### Multiple Setup/Teardown Pairs

You can have multiple independent setup/teardown pairs in the same DAG:

```yaml
actions:
  - id: create-cluster
    type: task
    uses: py
    inputs: { py_file: ./cluster.py, py_function: create }

  - id: create-staging-table
    type: task
    uses: py
    inputs: { py_file: ./staging.py, py_function: create }

  - id: extract
    type: task
    uses: py
    upstream: [create-cluster, create-staging-table]
    inputs: { py_file: ./extract.py, py_function: main }

  - id: transform
    type: task
    uses: py
    upstream: [extract]
    inputs: { py_file: ./transform.py, py_function: main }

  # Teardowns — each cleans up its own setup
  - id: destroy-cluster
    type: task
    uses: py
    teardown: create-cluster
    inputs: { py_file: ./cluster.py, py_function: destroy }

  - id: drop-staging-table
    type: task
    uses: py
    teardown: create-staging-table
    inputs: { py_file: ./staging.py, py_function: drop }
```

### Python API

```python
from beacon import Dag, Task

dag = Dag(
    id="etl-with-cluster",
    owners=["de"],
    actions=[
        Task(
            id="create-cluster",
            uses="py",
            inputs={"py_file": "./cluster.py", "py_function": "create"},
        ),
        Task(
            id="run-etl",
            uses="py",
            upstream=["create-cluster"],
            inputs={"py_file": "./etl.py", "py_function": "main"},
        ),
        Task(
            id="destroy-cluster",
            uses="py",
            teardown="create-cluster",
            inputs={"py_file": "./cluster.py", "py_function": "destroy"},
        ),
    ],
)

# Dryrun validates the teardown reference exists
result = dag.dryrun()
assert result.is_valid
```

### Validation (in `dryrun`)

The `dryrun()` function validates teardown references:

- **Referenced task must exist** — `teardown: "nonexistent"` → error
- **No self-reference** — `teardown: "self-id"` → error
- Included alongside existing upstream reference validation (Check 3)

### Comparison with Airflow

| Aspect         | Airflow                               | Beacon                        |
|----------------|---------------------------------------|-------------------------------|
| Syntax         | `task.as_teardown(setups=setup_task)` | `teardown: "setup-task-id"`   |
| Scope          | Task-level (decorator-like)           | Action-level (field in model) |
| Multiple pairs | Supported                             | Supported                     |
| Always-run     | `trigger_rule=ALL_DONE_SETUP_SUCCESS` | Implicit for teardown tasks   |
| Validation     | Runtime error if setup missing        | dryrun catches at parse time  |

### Implementation Plan

| Step                                      | Description                                 | Verify                                 |
|-------------------------------------------|---------------------------------------------|----------------------------------------|
| 1. ✅ Add `teardown` field to `BaseAction` | Field accepted on all action types          | Unit: Task(teardown="x") parses        |
| 2. ✅ Validate teardown ref in `dryrun()`  | Catches missing/self references             | Unit: dryrun errors for bad refs       |
| 3. ✅ Scheduler resolves teardown scheduling | Teardown runs after all dependents terminal | E2E: teardown runs after failure       |
| 4. ✅ Teardown accesses setup outputs       | via upstream_outputs                        | E2E: reads cluster endpoint            |
| 5. ✅ Teardown failure is non-fatal         | DagRun state ignores teardown failures      | E2E: dag SUCCESS despite teardown fail |

---

## What Beacon Does NOT Do (By Design)

- **No unbounded cross-task data shuttle** — Upstream outputs are bounded (one dict
  per upstream), declared explicitly, and read-only. For large data, use external
  storage and pass references (paths, URIs) as output values.

- **No embedded Python in YAML** — YAML is configuration. Python logic lives in
  `.py` files referenced by the `py` plugin.

- **No provider installation at runtime** — Plugins are resolved at parse time.
  If a plugin isn't available, the DAG fails to parse (fast feedback).

- **No DAG-level Jinja control flow** — Graph shape is static and inspectable.

- **No DAG-Schedule coupling** — A DAG is a template; schedules live in Deployments.
  Reuse a DAG N times with N different Deployments instead of duplicating files.

- **No multi-tenant scheduler** — One scheduler instance per deployment.
  Scale horizontally by deploying separate beacon instances per team/domain.

- **No Jinja rendering at execution time** — All templates resolved before
  TaskContext reaches the executor. Executors are dumb runners.

---

## Implementation Priority

[Implement Plan](./implement_plan.md)
