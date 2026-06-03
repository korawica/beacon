# Architecture

An architecture overview of the Beacon workflow orchestration framework.

Beacon is a lean, async-first alternative to Apache Airflow — built for teams
that need production-grade orchestration without the operational complexity.

---

## System Architecture

```text
                            ┌───────────────────────────────────────────┐
                            │           Client (CLI / SDK)               │
                            └─────────────────┬─────────────────────────┘
                                              │
                                              v
┌─────────────────┐         ┌───────────────────────────────────────────┐
│  Web Server     │<------->│           API Server (FastAPI)             │
│  (Frontend UI)  │         └──────┬────────────────┬───────────────────┘
└─────────────────┘                │                │
                                   v                v
                    ┌──────────────────┐   ┌────────────────┐
                    │    Scheduler     │   │ Metadata Store  │
                    │  (stateless)     │──>│                 │
                    └────────┬─────────┘   └───────┬────────┘
                             │                     ^
                             │ enqueue             │ read/write
                             v                     │
                    ┌──────────────────┐           │
                    │   Task Queue     │           │
                    │ (memory/Redis)   │           │
                    └────────┬─────────┘           │
                             │                     │
                             v                     │
                    ┌──────────────────┐           │
                    │   Async Worker   │───────────┘
                    │                  │
                    │ ┌──────────────┐ │
                    │ │   Executor   │ │──────> Logging Store
                    │ └──────────────┘ │
                    └──────────────────┘
```

---

## Components

### Client

Entry point for users to interact with Beacon.

- **CLI**: Deploy bundles, register deployments, trigger runs, inspect status
- **SDK**: Python API for programmatic DAG / Deployment definition and triggering

### API Server

Stateless HTTP service (FastAPI) handling:

- Bundle deployment (parse, validate, version-tag, store DAGs and Deployments)
- DAG run triggers (manual or API-driven)
- Run/task status queries
- Log retrieval (per-task, per-attempt)

### Scheduler

Stateless process that evaluates "what should run next":

- Iterates `enabled=True` Deployments, computes due runs from cron
- Builds TaskContexts and stores them in Metadata
- Evaluates trigger rules and upstream dependencies
- Transitions tasks: `NONE → SCHEDULED → QUEUED`
- Enqueues `{run_id, dag_id, task_id}` messages to Task Queue
- Handles retry re-scheduling (`UP_FOR_RETRY → QUEUED` after delay)
- Computes `logical_date`, `data_interval_start/end` for scheduled runs

**Stateless design**: Can restart without data loss. All state lives in Metadata Store.

### Task Queue

Decouples scheduling from execution:

- **MemoryQueue**: Default for development (in-process `asyncio.Queue`)
- **RedisQueue**: Production distributed queue (future)
- **SQS/PubSub**: Cloud-native queue (future)

Messages are lightweight: `{run_id, dag_id, task_id}` — the full TaskContext
is read from Metadata Store by the worker.

### Async Worker

Consumes messages from Task Queue and orchestrates execution:

1. Dequeue message `{run_id, dag_id, task_id}`
2. Read TaskContext from Metadata Store
3. Resolve upstream outputs (if declared)
4. Transition state: `QUEUED → RUNNING`
5. Fire `on_event: start` callbacks
6. Dispatch to Executor
7. Evaluate result → `SUCCESS` / `FAILED` / `SKIPPED` / `UP_FOR_RETRY`
8. Fire appropriate callbacks (`success` / `failure` / `retry` / `skipped`)
9. Write updated TaskContext back to Metadata Store

**Concurrency**: Bounded by semaphore (`max_concurrent`). A single worker
process handles hundreds of I/O-bound tasks via asyncio. CPU-bound tasks
are dispatched to remote executors.

**Protocol-driven**: The Worker depends on `MetadataProtocol`, not a concrete
class. Any compliant metadata store (JsonMetadata, SqliteMetadata, PostgresMetadata)
works without code changes.

### Executor

Runs the actual plugin logic in a target environment:

| Executor             | Environment         | Use Case                       |
|----------------------|---------------------|--------------------------------|
| `LocalExecutor`      | In-process asyncio  | Development, lightweight tasks |
| `DockerExecutor`     | Container per task  | Isolation, reproducibility     |
| `KubernetesExecutor` | Pod per task        | Production at scale            |
| `BatchExecutor`      | AWS/Cloud Batch job | Large compute tasks            |

All executors implement one method:

```python
async def run_task(self, task_ctx: TaskContext) -> TaskContext
```

The executor resolves the plugin, calls `plugin.execute(context)`, records
the attempt, and returns the updated TaskContext.

### Metadata Store

Persists all runtime state. Source of truth for the system.

All stores implement `MetadataProtocol`:

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

| Store              | Persistence               | Use Case                 |
|--------------------|---------------------------|--------------------------|
| `JsonMetadata`     | File-based (sharded JSON) | Default, dev → 1000 DAGs |
| `SqliteMetadata`   | Local SQLite              | Single-node production (pending) |
| `PostgresMetadata` | Postgres                  | Multi-node production (pending)  |

**What it stores**:

| Entity          | Key                       | Content                                   |
|-----------------|---------------------------|-------------------------------------------|
| DagRecord       | `dag_id/dag_version`      | Serialized DAG, version, timestamps       |
| DeploymentRecord| `deployment_id`           | Cron, params, variables_ref, dag_id ref   |
| DagRun          | `dag_id/run_id`           | State, params, deployment_id, timestamps  |
| TaskContext     | `dag_id/run_id/task_id`   | Full execution context, attempts, outputs |
| TaskState       | `dag_id/run_id/task_id`   | Current state machine position            |

**Performance characteristics (JsonMetadata)**:
- Sharded by `dag_id` to avoid large flat directories
- Async I/O via `asyncio.to_thread` (non-blocking)
- In-memory cache for task states (scheduler hot path)
- Atomic writes (temp file + rename)
- Bulk queries (`get_all_task_states`) for dependency evaluation

### Logging Store

Append-only log storage, keyed per task per attempt:

| Store                      | Use Case                          |
|----------------------------|-----------------------------------|
| `LocalFileLogging`         | Development (files on disk)       |
| `S3Logging` / `GCSLogging` | Production (cloud object storage) |

Key format: `{dag_id}/{run_id}/{task_id}/attempt_{N}.log`

Each retry attempt gets its own log. The UI shows per-attempt log tabs.

### Web Server (UI)

Frontend for observability:

- **Deployments list** (primary view — what users care about)
- DAG graph visualization (task dependencies)
- Run history with state timeline
- Task state indicators (color-coded)
- Per-attempt log viewer with error highlighting
- Callback/alert history

---

## Callback System

Callbacks fire on lifecycle events using the same plugin registry resolution.

```text
OnTaskEvent.on_event: start | success | failure | retry | skipped
OnDagEvent.on_event:  start | success | failure | finished
```

Callbacks are resolved via `CALLBACKS_REGISTRY`:
- Built-in registry (`json-file`, `log`, etc.)
- Entry-point plugins (`beacon.callbacks` group)
- Remote references (`my-org/msteam-callback@1.1.0`) — future

A `Callback` is just a class with `async def notify(event, data)`:

```python
from beacon import Callback

class MyCallback(Callback):
    hook_name: ClassVar[str] = "my-callback"
    async def notify(self, event: str, data: dict) -> None: ...
```

---

## Bundle & DAG Parsing

### Bundle Types

| Bundle        | Version Source    | Use Case           |
|---------------|-------------------|--------------------|
| `LocalBundle` | File content hash | Development        |
| `GitBundle`   | Commit SHA        | Production (CI/CD) |
| `GcsBundle`   | Object generation | Cloud-native       |

### Parsing Flow

```text
Bundle Source (local / git / GCS)
    │
    v
Parse DAGs (YAML / Python)
    │
    ├── Discover custom plugins (./plugins/*.py)
    ├── Parse DAG definitions (./dags/*.yml)
    ├── Parse Deployment definitions (./deployments/*.yml)
    ├── Validate: plugins exist, no cycles, deployment.dag_id resolves
    ├── Tag with bundle version
    │
    v
Store serialized DAG + Deployments + version in Metadata
```

**Key design**: DAGs are parsed once per version change, not on every scheduler
heartbeat. Running instances continue on their original version — editing a DAG
mid-run does not corrupt active task instances.

---

## Model Hierarchy

```text
Plugin ─── defines execution logic (reusable, versioned)
  │
  v
Action ─── references a plugin via `uses`, provides `inputs`
  │         types: Task, Sensor, Branch, ShortCircuit, Group, ForEach (future)
  v
Dag ────── reusable template: actions + dependencies + params schema + callbacks
  │
  v
Deployment ── binds a Dag to: cron + timezone + params values + variables_ref
              many Deployments can reference one Dag
```

### DAG vs Deployment

A `Dag` is a **reusable template**. A `Deployment` is a specific binding of
that template to a schedule, params, env, and identity. One `Dag` can have many
`Deployment`s — each shows under its own name in the UI.

```text
Dag(id="extract-load-table")
  ├── Deployment(id="daily-customers-from-postgres",
  │              cron="0 2 * * *", params={source: postgres})
  └── Deployment(id="hourly-orders-from-mysql",
                 cron="0 * * * *", params={source: mysql})
```

This eliminates Airflow's anti-pattern of duplicating DAG files just to change
a schedule or source name.

### Action Types

| Type           | Purpose                | Async Behavior                  |
|----------------|------------------------|---------------------------------|
| `task`         | Execute a unit of work | Run to completion               |
| `sensor`       | Wait for a condition   | Async poll with `await sleep`   |
| `branch`       | Choose downstream path | Evaluate condition, return path |
| `short_circuit`| Skip all downstream    | Evaluate boolean, skip if false |
| `group`        | Container for actions  | Not runtime — flattened by scheduler |
| `foreach_task` | Fan-out over a list    | Spawn N task instances (future) |

---

## Execution Flow

### End-to-End: Deploy → Schedule → Execute → Complete

```text
Phase 1: Deploy
─────────────────
  CLI/SDK → API Server → Parse Bundle → Validate
  → Store DagRecord(id, version, serialized_dag)
  → Store DeploymentRecord(id, dag_id, cron, params, variables_ref, ...)

Phase 2: Schedule Trigger
───────────���──────────────
  Scheduler heartbeat → Query enabled Deployments where next_run <= now
  → Resolve vars() against deployment.variables_ref stage
  → Create DagRun(run_id, dag_id, deployment_id, dag_version)
  → Build TaskContexts → Evaluate dependencies → Enqueue ready tasks

Phase 3: Task Execution
────────────────────────
  Worker dequeue → Read TaskContext → Resolve upstream outputs
  → RUNNING → Executor.run_task() → Plugin.execute()
  → SUCCESS / FAILED / SKIPPED / UP_FOR_RETRY

Phase 4: DAG Resolution
────────────────────────
  Terminal task → Scheduler re-evaluates downstream
  → All tasks terminal → DagRun SUCCESS/FAILED
  → Fire DAG-level callbacks (success/failure/finished)
```

### Task State Machine

```text
NONE ──→ SCHEDULED ──→ QUEUED ──→ RUNNING ──→ SUCCESS
  │                                   │
  ├──→ SKIPPED                        ├──→ FAILED
  │                                   ├──→ SKIPPED (TaskSkipped raised)
  └──→ UPSTREAM_FAILED                └──→ UP_FOR_RETRY ──→ QUEUED (retry)
```

---

## Production Deployment Topology

### Single-Node (Small Scale: <100 DAGs)

```text
┌─────────────────────────────────────────────┐
│  Single Process                              │
│                                              │
│  API Server + Scheduler + Worker             │
│  Metadata: JsonMetadata (local files)        │
│  Queue: asyncio.Queue (in-memory)            │
│  Logging: LocalFileLogging                   │
└─────────────────────────────────────────────┘
```

### Multi-Process (Medium Scale: 100–1000 DAGs)

```text
┌──────────────┐  ┌──────────────┐  ┌──────────────────┐
│ API Server   │  │  Scheduler   │  │  Worker (N proc)  │
│ (FastAPI)    │  │  (single)    │  │  max_concurrent=50│
└──────┬───────┘  └──────┬───────┘  └────────┬─────────┘
       │                 │                    │
       v                 v                    v
┌─────────────────────────────────────────────────────────┐
│  Metadata: SqliteMetadata or JsonMetadata (shared fs)    │
│  Queue: Redis                                            │
│  Logging: LocalFileLogging or S3                         │
└─────────────────────────────────────────────────────────┘
```

### Distributed (Large Scale: 1000–10,000+ DAGs)

```text
┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐
│ API Server   │  │  Scheduler   │  │  Worker Pool (N nodes)│
│ (multiple)   │  │  (single)    │  │  Executor: K8s/Batch  │
│ + LB         │  │  stateless   │  │                       │
└──────┬───────┘  └──────┬───────┘  └────────┬─────────────┘
       │                 │                    │
       v                 v                    v
┌────────────────────────────────────────────────────────���────┐
│  Metadata: PostgresMetadata                                  │
│  Queue: Redis / SQS / PubSub                                 │
│  Logging: S3 / GCS                                           │
│  Bundle: GitBundle (webhook-triggered sync)                   │
└─────────────────────────────────────────────────────────────┘
```

---

## Key Design Decisions for Production

| Decision                          | Rationale                                                                                |
|-----------------------------------|------------------------------------------------------------------------------------------|
| Stateless scheduler               | Restart without data loss; no in-memory DAG state                                        |
| TaskContext is serializable       | Enables remote execution without DAG file mounts                                         |
| Parse once, version-tag           | Eliminates Airflow's re-parse-every-heartbeat bottleneck                                 |
| DAG vs Deployment separation      | Reuse one DAG across many schedules/params instead of duplicating files                  |
| Protocol-based MetadataStore      | Pluggable persistence (Json / Sqlite / Postgres) without touching worker/scheduler code  |
| Sharded metadata                  | Supports 1000+ DAGs without directory listing degradation                                |
| Async-only execution              | Sensors don't waste worker slots; single event loop handles hundreds of concurrent tasks |
| Plugin registry (not pip install) | Fast resolution; no runtime installation; version-pinned                                 |
| Attempt history in TaskContext    | Full retry debugging without separate query                                              |
| TaskFailed / TaskSkipped errors   | Plugins explicitly control retry-vs-permanent-fail behavior                              |
| Bounded upstream outputs          | Prevents unbounded XCom-style data growth                                                |
| DAG version pinning per run       | Mid-run DAG edits don't corrupt active instances                                         |
