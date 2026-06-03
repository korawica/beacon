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

---

## Overall Architecture

```text
User --> Client (CLI / SDK / YAML)
           |
           v
      API Server (FastAPI) ──────────> Metadata Store (Sqlite / Postgres)
           |                                    ^
           v                                    |
      Scheduler ── enqueue ──> Async Worker ────┘
                                    |
                                    v
                              Logging Store (Local / S3 / GCS)
```

**Key difference from Airflow:**

| Aspect              | Airflow                                    | Beacon                         |
|---------------------|--------------------------------------------|--------------------------------|
| Execution model     | Sync + Deferrable (two paths)              | Async-only (one path)          |
| Plugin install      | pip install into runtime env               | Registry lookup or remote ref  |
| DAG parsing         | Every scheduler heartbeat re-parses        | Parse once, version-tag, cache |
| Sensor handling     | Poke loop occupies worker slot or deferred | Async sleep, no slot waste     |
| Scaling to 10k DAGs | Requires multiple schedulers + DB tuning   | Stateless scheduler + queue    |

---

## Plugin System

### Plugin Resolution

The `uses` field on any action resolves a plugin from the registry:

```text
uses: "py"                          --> built-in PLUGINS_REGISTRY["py"]
uses: "empty"                       --> built-in PLUGINS_REGISTRY["empty"]
uses: "my-org/etl-plugin@1.2.0"     --> external plugin (future: remote resolve)
```

**Resolution order:**

1. Built-in registry (standard provider plugins)
2. Entry-point discovered plugins (`beacon.plugins` group in pyproject.toml)
3. Local `plugins/` directory (auto-discovered from bundle path)
4. (Future) Remote registry with version pinning

### Plugin Contract

Every plugin implements one method:

```python
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
from beacon import Context, Templater

class BaseHookPlugin(Templater, ABC, metaclass=HookPluginMeta):
    hook_name: ClassVar[str] = "base"

    async def notify(self, context: Context, event: str) -> None:
        raise NotImplementedError
```

Same pattern as `BasePlugin` but with `notify()` instead of `execute()`.
Same registry. Same version pinning. Same resolution.

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
from beacon import BasePlugin, Context

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
    | enqueue task instances to
    v
Message Queue (in-memory / Redis / SQS)
    |
    | consumed by
    v
Worker Pool (N async workers, each handles M concurrent tasks)
```

**Key design choices for scale:**

1. **DAG versioning** — Parse DAG once per bundle change. Store serialized DAG
   in metadata. Scheduler works from metadata, not filesystem.

2. **Stateless scheduler** — Scheduler reads metadata, computes "what should run
   next", enqueues. No in-memory DAG state. Can restart without data loss.

3. **Concurrency via async** — A single worker process handles hundreds of
   concurrent I/O-bound tasks (sensors, API calls, file polls) via asyncio.
   CPU-bound tasks use `asyncio.to_thread()` or subprocess.

4. **Backpressure** — Queue depth per DAG is bounded. If a DAG produces tasks
   faster than workers consume, it pauses scheduling (not infinite queue growth).

---

## Templating

### Jinja for Variable Substitution Only

Beacon uses Jinja2 for **value interpolation**, not control flow:

```yaml
# Supported
inputs:
  source: "{{ params.source_system }}"
  path: "{{ vars('base_path') }}/{{ params.date }}"

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
2. At execution time, the scheduler resolves it against runtime params
3. Task instances are generated — one per item in the resolved list
4. Each instance is independently scheduled, retried, and tracked

This gives dynamic parallelism while keeping the DAG definition declarative
and the graph shape inspectable.

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

| Type       | Purpose                | Async Behavior                  |
|------------|------------------------|---------------------------------|
| `task`     | Execute a unit of work | Run to completion               |
| `sensor`   | Wait for a condition   | Async poll with sleep           |
| `branch`   | Choose downstream path | Evaluate condition, return path |
| `for_each` | Fan-out over a list    | Spawn N task instances          |

All four share `BaseAction` and resolve plugins the same way.
All four execute via `async def execute()`.

---

## Bundle & Versioning

```text
LocalBundle  ── file content hash ──> version tag
GitBundle    ── commit SHA ──────────> version tag
GcsBundle    ── object generation ───> version tag
```

When a bundle changes:

1. Reparse affected DAGs
2. Store new serialized DAG with new version
3. Running instances continue on their original version
4. New runs use the new version

This prevents the Airflow problem where editing a DAG file mid-run corrupts
running task instances.

---

## What Beacon Does NOT Do (By Design)

- **No XCom-like cross-task data passing** — Tasks communicate via external
  storage (S3, GCS, database). The orchestrator orchestrates; it doesn't shuttle data.

- **No embedded Python in YAML** — YAML is configuration. Python logic lives in
  `.py` files referenced by the `py` plugin.

- **No provider installation at runtime** — Plugins are resolved at parse time.
  If a plugin isn't available, the DAG fails to parse (fast feedback).

- **No DAG-level Jinja control flow** — Graph shape is static and inspectable.

- **No multi-tenant scheduler** — One scheduler instance per deployment.
  Scale horizontally by deploying separate beacon instances per team/domain.

---

## Implementation Priority

| Phase  | Deliverable                                           | Validates                           |
|--------|-------------------------------------------------------|-------------------------------------|
| **0**  | `PythonPlugin.execute()` works end-to-end             | Core loop                           |
| **0**  | Hook system with registry resolution (string + class) | Callback parity between Python/YAML |
| **1**  | LocalWorker with asyncio task runner                  | Async execution model               |
| **1**  | Entry-point plugin discovery                          | External plugin story               |
| **2**  | `for_each` action type                                | Dynamic parallelism                 |
| **2**  | Scheduler + metadata-based DAG versioning             | Scale to 10k DAGs                   |
| **3**  | Remote plugin registry (`org/name@version`)           | Ecosystem growth                    |
| **3**  | Web UI (read-only DAG viewer + run history)           | Observability                       |
