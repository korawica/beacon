# Templating

Beacon uses [Jinja2](https://jinja.palletsprojects.com/) for **value
interpolation only** — not control flow. Templates resolve to concrete
values *before* a plugin executes; plugins never see Jinja syntax.

```yaml
# ✅ Supported in DAG inputs
inputs:
  source: "{{ params.source_system }}"
  path:   "{{ params.base_path }}/{{ params.date }}"
  bucket: "{{ vars('gcs_bucket') }}"
  rows:   "{{ outputs.extract.row_count }}"

# ❌ Intentionally not supported (control flow in graph definitions)
{% for item in items %}        # use foreach_task instead (Phase 3)
{% if env == 'prod' %}          # use a Branch action instead
```

**Why no control flow?** Graphs whose shape depends on Jinja conditions
can't be statically validated, versioned, or shown in a UI before
execution. Use [`Branch`](../examples/yaml_with_standard.md) for
fork-on-condition and `foreach_task` for fan-out.

---

## The Renderer

One class — `beacon.core.Renderer` — handles every template in beacon.

```python
from beacon.core import Renderer

r = Renderer({
    "params":   {"source": "postgres", "date": "2026-06-04"},
    "vars":     lambda name: my_variables.get(name),
    "outputs":  {"extract": {"row_count": 42}},
    "runtime":  {...},
})

r.render("{{ params.source }}")                   # "postgres"
r.render({"q": "SELECT * FROM {{ params.source }}_t"})
# → {"q": "SELECT * FROM postgres_t"}
r.render(["x", "{{ params.date }}", 5])
# → ["x", "2026-06-04", 5]
```

Properties:

- **Recursive.** Walks `dict`, `list`, `tuple`. Non-string scalars pass
  through unchanged.
- **Native-typed.** Pure expressions return real Python types — `"{{ 5 + 5 }}"`
  → `int(10)`, `"{{ [1, 2] }}"` → `list`, `"{{ x }}"` with `x=None`
  → `None`. Mixed templates (`"prefix-{{ x }}"`) stay strings.
- **Sandboxed.** Dunder / attribute attacks (`{{ x.__class__.__mro__ }}`)
  raise `SecurityError`. `is_safe_attribute` / `is_safe_callable` from
  `jinja2.sandbox.SandboxedEnvironment` apply.
- **Strict undefined.** `{{ missing }}` raises `UndefinedError` — typos
  fail loudly, not silently.
- **Module-level template cache** (size 400) — repeated renders of the
  same string skip re-parsing.

`Renderer` is the only Jinja contact point in beacon. There is no plugin
mixin, no `template_fields` declaration, no `@renderable` decorator.
Plugins receive already-resolved values via `Context.params` and the
inputs you set on `Task(inputs={...})`.

---

## Available Namespaces

| Namespace                | When                              | What                                                         |
|--------------------------|-----------------------------------|--------------------------------------------------------------|
| `params.KEY`             | trigger time → enqueue            | Concrete `TaskContext.params` (Deployment params merged with overrides) |
| `vars('KEY')`            | trigger time → enqueue            | Lookup into the active stage of `variables.yml`              |
| `outputs.TASK_ID.KEY`    | pre-execute (worker, late-bind)   | Dict outputs returned by an upstream task                    |
| `runtime.KEY`            | trigger time → enqueue            | Run identity + time (see below)                              |

### `runtime.*` fields

All present in every render context:

| Key                       | Type        | Notes                                                |
|---------------------------|-------------|------------------------------------------------------|
| `run_id`                  | `str`       | DagRun id (`manual-…`, `scheduled-…`, `backfill-…`)  |
| `dag_id`                  | `str`       |                                                      |
| `task_id`                 | `str`       |                                                      |
| `run_date`                | `datetime`  | Wall-clock at trigger                                |
| `logical_date`            | `datetime`  | = `data_interval_start` for cron runs                |
| `data_interval_start`     | `datetime`  |                                                      |
| `data_interval_end`       | `datetime`  |                                                      |
| `attempt_number`          | `int`       | 1-based; bumped on retry                             |

### Unresolved `vars()` is non-fatal

If a `vars('foo')` key isn't in the active stage, the renderer
substitutes the sentinel string `<unresolved: vars('foo')>` instead of
raising. This lets `beacon dryrun` show templates with partial stage
data and surfaces missing keys in a single readable place rather than
blowing up the first time they're referenced.

Unresolved `params.*`, `outputs.*`, and `runtime.*` **do** raise (typed
typos must fail loudly).

---

## Render Pipeline (where each pass happens)

There is no abstract "Pass 1 / Pass 2" — there are two distinct binding
*sites* in the runtime, plus dryrun:

```text
┌──────────────────────────────────────────────────────────────────────┐
│  1. Trigger-time render        beacon/runner.py: _submit_action      │
│     ────────────────────                                              │
│     ctx = {params, vars, runtime, outputs={}}                         │
│     rendered = Renderer(ctx).render(task.inputs)                      │
│     → stored on TaskContext.inputs                                    │
│                                                                       │
│     Anything that fails here (typically because the input references  │
│     outputs.* of an upstream that hasn't run) is left untouched and   │
│     re-tried at site 2.                                               │
├──────────────────────────────────────────────────────────────────────┤
│  2. Pre-execute render         beacon/worker.py: _resolve_upstream_   │
│     ────────────────────                         outputs              │
│     Just before the worker calls the plugin:                          │
│     - load each upstream's TaskContext.outputs into                   │
│       task_ctx.upstream_outputs                                       │
│     - re-render task_ctx.inputs with the new outputs namespace        │
│       bound (vars is bound to a sentinel — it was already resolved    │
│       at site 1)                                                      │
│     → stored back on TaskContext.inputs, then plugin instantiates    │
├──────────────────────────────────────────────────────────────────────┤
│  3. Dryrun render              beacon/dryrun.py                       │
│     ──────────────                                                    │
│     Same as site 1 but error-tolerant: failures become                │
│     ``DryrunResult.warnings`` instead of exceptions.                  │
└──────────────────────────────────────────────────────────────────────┘
```

By the time a plugin's `execute()` runs, **`TaskContext.inputs` contains
fully concrete values**. Plugins do not import `Renderer`.

---

## Two-Pass Flow End-to-End

```text
variables files (scoped under dags/)
─────────────────────────────────────
dags/global_variables.yml:           gcs_bucket: my-prod-bucket
dags/extract_load/variables.yml:     source_system: postgres

Deployment (metadata store)
──────────────────────────────
id: daily-customers
dag_id: extract-load-table
variable_overrides:                  # optional; pinning happens here
  alert_path: /var/oncall
params:
  source: "{{ vars('source_system') }}"   ← templated at deploy author time
  bucket: "{{ vars('gcs_bucket') }}"

Trigger (scheduled or manual)
──────────────────────────────
Scheduler resolves vars against the scoped chain (deployment overrides
→ dag variables.yml → group global_variables.yml → bundle
global_variables.yml). The merged TaskContext.params becomes:
  params = {source: "postgres", bucket: "my-prod-bucket"}

DAG action
──────────
- id: extract
  uses: py
  inputs:
    py_file: ./scripts/extract.py
    table: "{{ params.source }}_events"        ← site 1 (trigger-time)
    date:  "{{ runtime.logical_date }}"        ← site 1

- id: transform
  uses: py
  upstream: [extract]
  inputs:
    rows: "{{ outputs.extract.row_count }}"    ← site 2 (pre-execute)

What the plugin actually receives
──────────────────────────────────
extract.inputs   = {py_file: "./scripts/extract.py",
                    table:   "postgres_events",
                    date:    datetime(2026, 6, 4, ...)}

transform.inputs = {rows: 42}      # outputs.extract.row_count
```

---

## Native Types

Pure-expression templates return their underlying Python value, not a
stringification. This matters more often than it looks:

```yaml
threshold:  "{{ 0.95 }}"             # float 0.95
batch_size: "{{ 100 * 10 }}"         # int 1000
enabled:    "{{ True }}"             # bool True
nothing:    "{{ x }}"  with x=None   # None (not "None")
tags:       "{{ ['a', 'b'] }}"       # list ["a", "b"]
items:      "{{ params.items }}"     # whatever type `params.items` is
mixed:      "prefix-{{ x }}"  x=5    # "prefix-5"  (mixed → str, correct)
```

Plugin authors never need to coerce types — declare a Pydantic field
with the expected type and the input arrives correctly typed.

---

## Writing a Custom Plugin

Plugins are plain Pydantic `BaseModel`s. The runner pre-renders inputs;
the plugin just declares the shape it expects.

```python
from typing import ClassVar, Any
from beacon import BasePlugin, Context


class GcsExtractPlugin(BasePlugin):
    plugin_name: ClassVar[str] = "gcs-extract"

    # Plain Pydantic fields. ALL of them can be set from templated YAML
    # — beacon resolves the template before instantiating this model.
    bucket: str
    prefix: str
    max_files: int = 100

    async def execute(self, context: Context) -> dict[str, Any]:
        # self.bucket / self.prefix / self.max_files are concrete values.
        from google.cloud import storage
        client = storage.Client()
        blobs = list(client.list_blobs(self.bucket, prefix=self.prefix))
        return {"file_count": min(len(blobs), self.max_files)}
```

Usage in YAML:

```yaml
- id: extract
  uses: gcs-extract
  inputs:
    bucket: "{{ vars('gcs_bucket') }}"
    prefix: "raw/{{ runtime.logical_date.strftime('%Y-%m-%d') }}/events"
    max_files: 50
```

There is no `template_fields` to declare. Everything in `inputs` is
fair game for templating; what *isn't* a string passes through untouched.

---

## Cheat-sheet

| You want…                                | Write                                       |
|------------------------------------------|---------------------------------------------|
| Stage variable                           | `{{ vars('key') }}`                         |
| Deployment / DAG param                   | `{{ params.key }}`                          |
| Upstream task output                     | `{{ outputs.task_id.key }}`                 |
| Logical date                             | `{{ runtime.logical_date }}`                |
| Run id                                   | `{{ runtime.run_id }}`                      |
| Math / coerced literal                   | `{{ 1024 * 1024 }}`                         |
| Inline default                           | `{{ params.maybe or 'fallback' }}`          |
| Jinja filter                             | `{{ params.name | upper }}`                 |
| Pass a literal `{` to a plugin           | use a non-string type, or `{{ "{" }}`       |
