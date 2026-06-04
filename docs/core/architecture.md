# Architecture

Beacon is a lean, async-first workflow orchestration framework — built for data
engineering teams that need production-grade orchestration without the operational
complexity of Apache Airflow.

---

## System Architecture

### Phase 1 — Local (current, shipped)

```text
┌─────────────────────────────────────────────────────────┐
│                    USER API (Dag)                        │
├─────────────────────────────────────────────────────────┤
│  dag.dryrun()     validate templates + graph            │
│  dag.run()        one-shot execution                    │
│  dag.test()       tempdir one-shot + pass/fail          │
│  dag.clear()      fix bug → rerun task(s)              │
│  dag.mark()       force state → fire teardowns          │
│  dag.fail()       shorthand for mark(state="failed")    │
│  dag.backfill()   run N dates (skip/reset existing)     │
└──────────────────────────┬──────────────────────────────┘
                           │
┌──────────────────────────┴──────────────────────────────┐
│                  DagRunner (async)                       │
├─────────────────────────────────────────────────────────┤
│  run(resume=False)  fresh execution                     │
│  run(resume=True)   continue existing (skip terminal)   │
│  clear(task_ids, downstream)   reset + auto-teardown    │
│  mark(task_ids, state)         set state + auto-teardown│
└──────────────────────────┬──────────────────────────────┘
                           │
┌──────────────────────────┴──────────────────────────────┐
│                Worker + Executor                         │
├─────────────────────────────────────────────────────────┤
│  Worker: queue → resolve upstream outputs → dispatch    │
│  LocalExecutor.run_task():                              │
│    try:                                                 │
│      plugin.execute(context)   ← main logic             │
│    finally:                                             │
│      plugin.teardown(context)  ← always fires           │
└──────────────────────────┬──────────────────────────────┘
                           │
┌──────────────────────────┴──────────────────────────────┐
│               Plugin (BasePlugin)                        │
├─────────────────────────────────────────────────────────┤
│  execute(context) → dict     abstractmethod             │
│  teardown(context) → None    default no-op              │
│                                                         │
│  PythonPlugin (uses: py):                               │
│    py_function: "main"       → calls main()             │
│    py_teardown: "cleanup"    → calls cleanup() always   │
└──────────────────────────┬──────────────────────────────┘
                           │
┌──────────────────────────┴──────────────────────────────┐
│             Metadata Store (JsonMetadata)                │
├─────────────────────────────────────────────────────────┤
│  Sharded JSON files, async I/O, LRU cache               │
│  Atomic writes (temp + rename)                          │
│  Structure:                                             │
│    {path}/dag_runs/{dag_id}/{run_id}.json               │
│    {path}/task_contexts/{dag_id}/{run_id}/{task_id}.json │
│    {path}/task_states/{dag_id}/{run_id}/{task_id}.json   │
└─────────────────────────────────────────────────────────┘
```

### Phase 2 — Production Service (planned)

```text
                            ┌───────────────────────────────────────────┐
                            │           Client (CLI / SDK)               │
                            └─────────────────┬─────────────────────────┘
                                              │
                                              v
                            ┌───────────────────────────────────────────┐
                            │           API Server (FastAPI)             │
                            └──────┬────────────────┬───────────────────┘
                                   │                │
                                   v                v
                    ┌──────────────────┐   ┌────────────────┐
                    │ DeploymentSched  │   │ Metadata Store  │
                    │  (cron loop)     │──>│ (Sqlite/PG)     │
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

### Dag (User API)

The public interface for defining and operating DAGs.

| Method                           | Purpose                              | When to use               |
|----------------------------------|--------------------------------------|---------------------------|
| `dag.dryrun()`                   | Validate graph + templates           | Before deploy / in CI     |
| `dag.run()`                      | Execute once, persist state          | Manual trigger            |
| `dag.test()`                     | Execute in tempdir, report pass/fail | Development               |
| `dag.clear(task_id=...)`         | Reset task + downstream → rerun      | Fix bug, rerun            |
| `dag.mark(state=...)`            | Force task to terminal state         | Kill stuck task / unblock |
| `dag.fail(task_id=...)`          | Shorthand for `mark(state="failed")` | Kill + clean up           |
| `dag.backfill(start, end, cron)` | Run N dates in range                 | Historical reprocessing   |

### DagRunner

Async engine that owns the DAG graph traversal for a single execution:

- Evaluates trigger rules + upstream dependencies
- Handles branch / short-circuit propagation
- Cascades `UPSTREAM_FAILED` / `SKIPPED`
- Schedules teardown tasks after all dependents terminate
- Fires DAG-level callbacks
- Supports `resume=True` for re-execution of cleared tasks

**Not a scheduler.** `DagRunner` runs one DAG one time. The future
`DeploymentScheduler` (Phase 2) sits above it and triggers runs from cron.

### Worker

Consumes messages from Task Queue and orchestrates single-task execution:

1. Dequeue message `{run_id, dag_id, task_id}`
2. Read TaskContext from Metadata Store
3. Resolve upstream outputs (two-pass Jinja rendering)
4. Transition state: `QUEUED → RUNNING`
5. Fire `on_event: start` callbacks
6. Dispatch to Executor
7. Evaluate result → `SUCCESS` / `FAILED` / `SKIPPED` / `UP_FOR_RETRY`
8. Fire appropriate callbacks
9. Write updated TaskContext back to Metadata Store

**Concurrency**: Bounded by semaphore (`max_concurrent`). A single worker
process handles hundreds of I/O-bound tasks via asyncio.

### Executor

Runs the actual plugin logic in a target environment:

| Executor             | Environment         | Status  |
|----------------------|---------------------|---------|
| `LocalExecutor`      | In-process asyncio  | ✅ Done  |
| `DockerExecutor`     | Container per task  | Pending |
| `KubernetesExecutor` | Pod per task        | Pending |
| `BatchExecutor`      | AWS/Cloud Batch job | Pending |

All executors implement:

```python
async def run_task(self, task_ctx: TaskContext) -> TaskContext
```

The executor:
1. Resolves plugin from registry
2. Instantiates plugin with rendered inputs
3. Calls `plugin.execute(context)` (with timeout if configured)
4. **Always** calls `plugin.teardown(context)` in a `finally` block
5. Records attempt result (success/failed/skipped/timed_out)

### Plugin System

Every plugin is a Pydantic model with two methods:

```python
class MyPlugin(BasePlugin):
    plugin_name: ClassVar[str] = "my-plugin"

    # Typed inputs (validated by Pydantic)
    source: str
    target: str

    async def execute(self, context: Context) -> dict[str, Any]:
        """Main logic. Return outputs dict."""
        ...

    async def teardown(self, context: Context) -> None:
        """Cleanup — ALWAYS fires after execute (success or failure).
        Default: no-op. Override for resource cleanup."""
        ...
```

### Metadata Store

Persists all runtime state. Source of truth.

| Store              | Persistence               | Status  |
|--------------------|---------------------------|---------|
| `JsonMetadata`     | File-based (sharded JSON) | ✅ Done  |
| `SqliteMetadata`   | Local SQLite              | Pending |
| `PostgresMetadata` | Postgres                  | Pending |

Protocol methods:

```python
async def create_dag_run(...)
async def get_dag_run(run_id, dag_id) -> dict | None
async def update_dag_run_state(run_id, dag_id, state)
async def put_task_context(run_id, dag_id, task_id, ctx)
async def get_task_context(run_id, dag_id, task_id) -> TaskContext | None
async def set_task_state(run_id, dag_id, task_id, state)
async def get_task_state(run_id, dag_id, task_id) -> TaskState | None
async def get_all_task_states(run_id, dag_id) -> dict[str, TaskState]
async def get_task_outputs(run_id, dag_id, task_id) -> dict
async def clear_task(run_id, dag_id, task_id)
```

---

## Operations — The Four Scenarios

Every DAG operation for data engineers falls into one of these:

```text
┌─────────────────────────────────────────────────────────────────────┐
│  1. NORMAL RUN                                                      │
│     dag.run(params={...})                                           │
│     → execute all tasks → teardowns fire → done                     │
├─────────────────────────────────────────────────────────────────────┤
│  2. FIX AND RERUN (task had a bug)                                  │
│     dag.clear(run_id=..., task_id="bad_task", downstream=True)      │
│     → reset bad_task + downstream → re-execute → teardowns re-fire  │
│     → upstreams NOT re-executed (read from metadata)                │
├─────────────────────────────────────────────────────────────────────┤
│  3. KILL STUCK TASK (force-fail + cleanup)                          │
│     dag.fail(run_id=..., task_id="stuck_task")                      │
│     → mark FAILED → teardown re-fires → resource cleaned up        │
├─────────────────────────────────────────────────────────────────────┤
│  4. BACKFILL (reprocess historical dates)                           │
│     dag.backfill(start_date=..., end_date=..., cron="0 0 * * *")   │
│     → one run per cron tick → skip existing (or reset_existing)     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## run_id Convention

Run IDs encode how the run was triggered:

| Prefix                           | Source                      | Example                         |
|----------------------------------|-----------------------------|---------------------------------|
| `manual-{dag_id}-{uuid}`         | `dag.run()`                 | `manual-etl-a1b2c3d4`           |
| `backfill-{dag_id}-{timestamp}`  | `dag.backfill()`            | `backfill-etl-20260101T000000`  |
| `scheduled-{dag_id}-{timestamp}` | Phase 2 DeploymentScheduler | `scheduled-etl-20260104T020000` |

Use `beacon.run_trigger(run_id)` to classify.

---

## Teardown Architecture

### Two Layers

```text
SIMPLE (90% of cases):                  COMPLEX (shared resources):
───────────────────────                 ──────────────────────────────
One task, inline cleanup.               Separate tasks, DAG-level lifecycle.

- id: process                           - id: create-cluster
  uses: py                                uses: py
  inputs:                               - id: etl-1
    py_function: run_spark                  upstream: [create-cluster]
    py_teardown: kill_spark             - id: etl-2
                                            upstream: [create-cluster]
                                        - id: destroy-cluster
                                            teardown: create-cluster
```

### Plugin-Level Teardown

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

Or with the `py` plugin:

```yaml
- id: process
  uses: py
  inputs:
    py_file: ./spark.py
    py_function: main
    py_teardown: cleanup       # ← fires ALWAYS after main()
    params:
      cluster: "{{ params.cluster }}"
```

### Task-Level Teardown

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
  teardown: create-cluster     # ← fires after ALL dependents of create-cluster
  inputs: { py_function: destroy }
```

### Teardown Guarantees

| Scenario                       | Plugin teardown         | Task-level teardown         |
|--------------------------------|-------------------------|-----------------------------|
| Task succeeds                  | ✅ fires (finally)       | ✅ fires (all deps terminal) |
| Task fails                     | ✅ fires (finally)       | ✅ fires (all deps terminal) |
| Task times out                 | ✅ fires (finally)       | ✅ fires (all deps terminal) |
| Task force-failed via `mark()` | — (already done)        | ✅ auto-clears + re-fires    |
| Task cleared via `clear()`     | ✅ fires on re-execution | ✅ auto-clears + re-fires    |

---

## Renderer: Two-Pass Template Resolution

```text
Pass 1 (trigger time — DagRunner._enqueue):
  Binds: params, vars(), runtime.*
  Defers: outputs.* (not yet available)

Pass 2 (pre-execute — Worker._resolve_upstream_outputs):
  Binds: outputs.upstream_task.key (now available from metadata)
  Result: final concrete values given to plugin
```

Plugins **never** see Jinja. They receive fully-resolved, correctly-typed values.
The `NativeEnvironment` + `SandboxedEnvironment` renderer preserves Python types:

```
"{{ x }}" with x=5      → int(5)      (not "5")
"{{ x }}" with x=[1,2]  → [1, 2]      (not "[1, 2]")
"{{ x }}" with x=False  → False       (not "False")
"prefix-{{ x }}" x=5    → "prefix-5"  (mixed → str)
```

---

## Callback System

```text
OnTaskEvent.on_event: start | success | failure | retry | skipped
OnDagEvent.on_event:  start | success | failure | finished
```

A `Callback` is a class with `async def notify(event, data)`:

```python
from beacon import Callback

class MyCallback(Callback):
    hook_name: ClassVar[str] = "my-callback"
    async def notify(self, event: str, data: dict) -> None: ...
```

---

## Model Hierarchy

```text
Plugin ─── defines execution logic (execute + teardown)
  │
  v
Action ─── references a plugin via `uses`, provides `inputs`
  │         types: Task, Sensor, Branch, ShortCircuit, Group
  v
Dag ────── reusable template: actions + dependencies + params + callbacks
  │
  v
Deployment ── binds a Dag to: cron + timezone + params + variable_overrides
              many Deployments → one Dag
```

### DAG vs Deployment

```text
Dag(id="extract-load-table")
  ├── Deployment(id="daily-customers-from-postgres",
  │              cron="0 2 * * *", params={source: postgres})
  └── Deployment(id="hourly-orders-from-mysql",
                 cron="0 * * * *", params={source: mysql})
```

### Action Types

| Type            | Purpose                | Async Behavior              |
|-----------------|------------------------|-----------------------------|
| `task`          | Execute a unit of work | Run to completion           |
| `sensor`        | Wait for a condition   | Async poll with sleep       |
| `branch`        | Choose downstream path | Return path list            |
| `short_circuit` | Skip all downstream    | Return boolean              |
| `group`         | Container for actions  | Flattened by runner         |

---

## Task State Machine

```text
NONE ──→ SCHEDULED ──→ QUEUED ──→ RUNNING ──→ SUCCESS
  │                                   │
  ├──→ SKIPPED                        ├──→ FAILED
  │                                   ├──→ SKIPPED (TaskSkipped raised)
  └──→ UPSTREAM_FAILED                └──→ UP_FOR_RETRY ──→ QUEUED (retry)
```

---

## Production Deployment Topology

### Single-Node (Phase 1 — current)

```text
┌─────────────────────────────────────────────┐
│  Single Process (dag.run / dag.backfill)     │
│                                              │
│  DagRunner + Worker + LocalExecutor          │
│  Metadata: JsonMetadata (local files)        │
│  Queue: asyncio.Queue (in-memory)            │
│  Logging: LocalFileSink (JSONL)              │
└─────────────────────────────────────────────┘
```

### Service Mode (Phase 2 — planned)

```text
┌──────────────┐  ┌──────────────────┐  ┌──────────────────┐
│ API Server   │  │ DeploymentSched  │  │  Worker (N proc)  │
│ (FastAPI)    │  │  (cron loop)     │  │  max_concurrent=50│
└──────┬───────┘  └──────┬───────────┘  └────────┬─────────┘
       │                 │                       │
       v                 v                       v
┌─────────────────────────────────────────────────────────┐
│  Metadata: SqliteMetadata                                │
│  Queue: asyncio.Queue                                    │
│  Logging: LocalFileSink + rotation                       │
└─────────────────────────────────────────────────────────┘
```

### Distributed (Phase 3 — planned)

```text
┌──────────────┐  ┌──────────────────┐  ┌──────────────────────┐
│ API Server   │  │ DeploymentSched  │  │  Worker Pool (N nodes)│
│ (multiple)   │  │  (single)        │  │  Executor: K8s/Batch  │
│ + LB         │  │  stateless       │  │                       │
└──────┬───────┘  └──────┬───────────┘  └────────┬─────────────┘
       │                 │                       │
       v                 v                       v
┌──────────────────────────────────────────────────────────────┐
│  Metadata: PostgresMetadata                                   │
│  Queue: Redis / SQS                                           │
│  Logging: S3 / GCS                                            │
│  Bundle: GitBundle (CI/CD sync)                               │
└──────────────────────────────────────────────────────────────┘
```

---

## Key Design Decisions

| Decision                          | Rationale                                                               |
|-----------------------------------|-------------------------------------------------------------------------|
| Stateless DagRunner               | Restart without data loss; all state in metadata                        |
| TaskContext is serializable       | Enables remote execution without DAG file mounts                        |
| Parse once, version-tag           | Eliminates Airflow's re-parse-every-heartbeat bottleneck                |
| DAG vs Deployment separation      | Reuse one DAG across many schedules/params                              |
| Protocol-based MetadataStore      | Pluggable persistence without touching runner/worker code               |
| Sharded metadata (by dag_id)      | Supports 1000+ DAGs without directory listing degradation               |
| Async-only execution              | Sensors don't waste worker slots; one event loop handles 100s of tasks  |
| Plugin teardown in finally        | Resources always cleaned up — no leaks on failure/timeout               |
| Two-pass Jinja rendering          | Outputs resolved late (worker), everything else resolved early (runner) |
| NativeEnvironment + Sandbox       | Types preserved through templates AND security enforced                 |
| run_id encodes trigger type       | Filter/group runs by how they were created                              |
| Auto-clear teardown on clear/mark | Prevents resource leaks on re-run                                       |
| Plugin registry (not pip install) | Fast resolution; no runtime installation; version-pinned                |
| TaskFailed / TaskSkipped errors   | Plugins explicitly control retry-vs-permanent-fail behavior             |
| Bounded upstream outputs          | Prevents unbounded XCom-style data growth                               |
| DAG version pinning per run       | Mid-run DAG edits don't corrupt active instances                        |
