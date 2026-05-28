# Design

## Plugin Type Serialization Across Executor Boundaries

### The Problem

When beacon dispatches a task to a remote executor (K8s pod, Celery worker, webhook
server), it cannot transmit the user's custom Pydantic `Annotated` type across the
wire. Python types are not serializable — you can't pickle a `TypeAdapter` or send a
class definition over HTTP.

```python
# This type lives only in the orchestrator's process:
MyTask = Annotated[BigQueryCount | CloudStorageWithPrefix, Field(discriminator="uses")]
```

### What Gets Transmitted

Only a plain JSON-safe dict is sent to the remote executor:

```json
{
  "task_id": "extract-1",
  "uses": "bigquery_count",
  "inputs": {
    "source_system": "example",
    "bucket": "my-bucket",
    "prefix": "year=2026/month=01/day=01/hour=00"
  }
}
```

Templates (`{{ params.source_system }}`) are resolved by the orchestrator **before**
dispatch, so the remote executor always receives concrete values.

### How the Remote Executor Reconstructs the Task

The remote worker (pod image, Celery worker, webhook server) must have the **same
plugin code installed**. It uses a string-based registry to map `uses` → class:

```
uses="bigquery_count"  →  registry lookup  →  BigQueryCount(inputs={...}).execute(ctx)
```

There is no way around this constraint — it is the same requirement as Airflow
(same DAG code deployed to all workers), Celery (same task module on all workers),
or any distributed system. The Pydantic discriminated union is a **definition-time
validation tool only**, not a runtime transmission format.

### Lifecycle by Executor Type

```
LocalExecutor (same process):
  Orchestrator: validate with TypeAdapter → resolve templates → call execute()
  Worker:       same process, no serialization needed

WebhookExecutor / PodExecutor (remote):
  Orchestrator: validate with TypeAdapter → resolve templates → POST JSON spec
  Worker:       receive JSON → registry.lookup(uses) → instantiate → execute()
```

### Plugin Registry on the Worker Side

For remote executors, the worker needs a registry. Two approaches:

**Option A — module path in `uses`**:

```python
uses = "myapp.plugins.BigQueryCount"  # importable dotted path
```

Worker does `importlib.import_module(...)` to get the class. Zero extra
registration code, but couples the plugin name to its import path.

**Option B — explicit registry**:

```python
# worker startup
registry.register("bigquery_count", BigQueryCount)
```

Worker does `registry[uses]`. Decoupled names, but requires matching
registration on both orchestrator and worker.

**Recommendation**: start with Option A (module path) for the remote executor
because it requires no coordination. Option B can be layered on top as an alias
system later.

### Callable `uses` on Remote Executors

```python
Task(uses=some_python_code, ...)  # callable, not a string
```

A callable **cannot be dispatched to a remote executor**. The `LocalExecutor`
handles it directly. Any other executor must receive a string `uses`. The runner
should raise at dispatch time if a callable task is sent to a non-local executor.
