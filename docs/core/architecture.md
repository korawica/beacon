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

- **CLI**: Deploy bundles, trigger runs, inspect status
- **SDK**: Python API for programmatic DAG definition and triggering

### API Server

Stateless HTTP service (FastAPI) handling:

- Bundle deployment (parse, validate, version-tag, store)
- DAG run triggers (manual or API-driven)
- Run/task status queries
- Log retrieval (per-task, per-attempt)

### Scheduler

Stateless process that evaluates "what should run next":

- Reads active DAG runs from Metadata Store
- Evaluates trigger rules and upstream dependencies
- Transitions tasks: `NONE → SCHEDULED → QUEUED`
- Enqueues `{run_id, task_id}` messages to Task Queue
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
7. Evaluate result → `SUCCESS` / `FAILED` / `UP_FOR_RETRY`
8. Fire appropriate callbacks (`success` / `failure` / `retry`)
9. Write updated TaskContext back to Metadata Store

**Concurrency**: Bounded by semaphore (`max_concurrent`). A single worker
process handles hundreds of I/O-bound tasks via asyncio. CPU-bound tasks
are dispatched to remote executors.

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

Persists all runtime state. Source of truth for the system:

| Store              | Persistence               | Use Case                 |
|--------------------|---------------------------|--------------------------|
| `JsonMetadata`     | File-based (sharded JSON) | Default, dev/small-scale |
| `SqliteMetadata`   | Local SQLite              | Single-node production   |
| `PostgresMetadata` | Postgres                  | Multi-node production    |

**What it stores**:

| Entity      | Key                     | Content                                   |
|-------------|-------------------------|-------------------------------------------|
| DagRun      | `dag_id/run_id`         | State, params, timestamps                 |
| TaskContext | `dag_id/run_id/task_id` | Full execution context, attempts, outputs |
| TaskState   | `dag_id/run_id/task_id` | Current state machine position            |

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

- DAG graph visualization (task dependencies)
- Run history with state timeline
- Task state indicators (color-coded)
- Per-attempt log viewer with error highlighting
- Callback/alert history

---

## Callback (Hook) System

Hooks fire on lifecycle events using the same plugin registry resolution:

```text
on_event: start | success | failure | retry
```

Hooks are resolved via:
- Built-in registry (`json-file`, `log`, etc.)
- Entry-point plugins (`beacon.hooks` group)
- Remote references (`my-org/msteam-callback@1.1.0`) — future

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
    ├── Validate DAG structure (acyclic, plugins exist)
    ├── Tag with bundle version
    │
    v
Store serialized DAG + version in Metadata
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
  │         types: Task, Sensor, Branch, ForEach
  v
Group ──── optional: treats multiple actions as a single unit
  │
  v
Dag ────── declares actions + dependencies + params + callbacks
  │
  v
Schedule ── binds a Dag to a cron + timezone + variables
```

### Action Types

| Type           | Purpose                | Async Behavior                  |
|----------------|------------------------|---------------------------------|
| `task`         | Execute a unit of work | Run to completion               |
| `sensor`       | Wait for a condition   | Async poll with `await sleep`   |
| `branch`       | Choose downstream path | Evaluate condition, return path |
| `foreach_task` | Fan-out over a list    | Spawn N task instances          |

---

## Execution Flow

### End-to-End: Deploy → Schedule → Execute → Complete

```text
Phase 1: Deploy
─────────────────
  CLI/SDK → API Server → Parse Bundle → Validate → Store (DagRecord + ScheduleRecord)

Phase 2: Schedule Trigger
──────────────────────────
  Scheduler heartbeat → Query due schedules → Create DagRun → Build TaskContexts
  → Evaluate dependencies → Enqueue ready tasks

Phase 3: Task Execution
────────────────────────
  Worker dequeue → Read TaskContext → Resolve upstream outputs
  → RUNNING → Executor.run_task() → Plugin.execute()
  → SUCCESS / FAILED / UP_FOR_RETRY

Phase 4: DAG Resolution
────────────────────────
  Terminal task → Scheduler re-evaluates downstream
  → All tasks terminal → DagRun SUCCESS/FAILED → Fire DAG-level callbacks
```

### Task State Machine

```text
NONE ──→ SCHEDULED ──→ QUEUED ──→ RUNNING ──→ SUCCESS
  │                                   │
  ├──→ SKIPPED                        ├──→ FAILED
  │                                   │
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
┌─────────────────────────────────────────────────────────────┐
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
| Sharded metadata                  | Supports 1000+ DAGs without directory listing degradation                                |
| Async-only execution              | Sensors don't waste worker slots; single event loop handles hundreds of concurrent tasks |
| Plugin registry (not pip install) | Fast resolution; no runtime installation; version-pinned                                 |
| Attempt history in TaskContext    | Full retry debugging without separate query                                              |
| Bounded upstream outputs          | Prevents unbounded XCom-style data growth                                                |
| DAG version pinning per run       | Mid-run DAG edits don't corrupt active instances                                         |
