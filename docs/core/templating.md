# Templating

This document describes how the Beacon template system works, including the
rendering pipeline, available macros, and how values flow from variables through
to task inputs at execution time.

---

## Overview

Beacon uses [Jinja2](https://jinja.palletsprojects.com/) for **value interpolation only** —
not control flow. Templates are resolved at deploy/trigger time so that by the time a task
reaches the executor, all inputs contain fully concrete values.

```yaml
# ✅ Supported: value interpolation
inputs:
  source: "{{ params.source_system }}"
  path: "{{ params.base_path }}/{{ params.date }}"
  bucket: "{{ vars('gcs_bucket') }}"

# ❌ NOT supported in DAG definitions (by design)
{% for item in items %}   # No control flow
{% if env == 'prod' %}    # No conditionals
```

**Why no control flow?** — Control flow in DAG definitions creates unpredictable
graph shapes that cannot be statically validated, versioned, or displayed in a
UI before execution. Use `for_each` for dynamic fan-out instead.

---

## Rendering Pipeline

Templates are resolved in two passes before execution:

```text
Deploy/Trigger Time                      Pre-Execute (Scheduler)
────────────────────                     ───────────────────────
variables.yml                            TaskContext.params
    │                                        │
    v                                        v
vars() resolved against                  params.* resolved against
variable store (per stage)               TaskContext.params
    │                                        │
    v                                        v
schedule.params = {                      TaskContext.inputs = {
  source: "postgres"  ← was "{{vars}}"    table: "postgres"  ← was "{{params}}"
}                                        }
```

### Pass 1: `vars()` → `params` (at trigger time)

Variables from `variables.yml` (stage-specific) are resolved into DAG params:

```yaml
# variables.yml
stages:
  prod:
    source_system: postgres
    gcs_bucket: my-prod-bucket
  dev:
    source_system: sqlite
    gcs_bucket: my-dev-bucket
```

```yaml
# schedule params (before rendering)
params:
  source: "{{ vars('source_system') }}"
  bucket: "{{ vars('gcs_bucket') }}"

# After Pass 1 (trigger in prod stage):
params:
  source: "postgres"
  bucket: "my-prod-bucket"
```

### Pass 2: `params.*` → `inputs` (pre-execute in scheduler)

Task inputs referencing `params` are resolved into concrete values stored in TaskContext:

```yaml
# Task definition
tasks:
  - id: extract
    uses: py
    inputs:
      py_file: ./scripts/extract.py
      params:
        source: "{{ params.source }}"
        bucket: "{{ params.bucket }}/raw"

# After Pass 2 (stored in TaskContext.inputs):
inputs:
  py_file: "./scripts/extract.py"
  params:
    source: "postgres"
    bucket: "my-prod-bucket/raw"
```

### Pass 3: `outputs.*` → `inputs` (upstream output references)

Downstream tasks can reference upstream outputs via the `outputs` namespace:

```yaml
tasks:
  - id: transform
    uses: py
    upstream: [extract]
    inputs:
      py_file: ./scripts/transform.py
      params:
        row_count: "{{ outputs.extract.row_count }}"
```

These are resolved at execution time when the worker populates `upstream_outputs`.

---

## Available Template Macros

| Macro                 | Scope  | Description                                |
|-----------------------|--------|--------------------------------------------|
| `vars('key')`         | Pass 1 | Reads from stage variables (variables.yml) |
| `params.key`          | Pass 2 | Reads from resolved DAG params             |
| `outputs.task_id.key` | Pass 3 | Reads from upstream task outputs           |

### Built-in Variables (available in all templates)

These are automatically available when templates are rendered:

| Variable       | Type     | Description                            |
|----------------|----------|----------------------------------------|
| `params.*`     | dict     | All DAG params (after vars resolution) |
| `vars('name')` | function | Lookup function for stage variables    |

---

## How It Works Internally

### Components

```text
┌─────────────────────────────────────────────────────────────┐
│                    Template System                            │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  JinjaRender (beacon.core.renderer)                          │
│  ├── Creates Jinja2 Environment (NativeEnvironment)          │
│  ├── Manages user_defined_macros (vars, params, etc.)        │
│  ├── Manages user_defined_filters                            │
│  └── Renders any value recursively (str, dict, list, tuple)  │
│                                                              │
│  Templater (beacon.core.templater)                           │
│  ├── Pydantic BaseModel mixin                                │
│  ├── Declares template_fields (which fields to render)       │
│  ├── model_validator(mode="before") triggers rendering       │
│  └── Renders declared fields before Pydantic validation      │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### JinjaRender

The core rendering engine. Wraps Jinja2 with:

- **NativeEnvironment** (default): Returns Python native types (int, list, dict)
  instead of always returning strings. `"{{ 42 }}"` → `42` (int), not `"42"`.
- **SandboxedEnvironment** (optional): For untrusted templates.
- **Recursive rendering**: Traverses dicts, lists, tuples, and sets to render
  any nested string containing `{{ ... }}`.

```python
from beacon.core.renderer import JinjaRender

renderer = JinjaRender(
    user_defined_macros={
        "params": {"source": "postgres", "date": "2026-06-03"},
        "vars": lambda key: variables.get(key),
    },
)

# Renders a nested structure
result = renderer.render({
    "table": "{{ params.source }}_events",
    "date": "{{ params.date }}",
    "count": "{{ 5 * 10 }}",
})
# → {"table": "postgres_events", "date": "2026-06-03", "count": 50}
```

### Templater (Pydantic Mixin)

Any Pydantic model that extends `Templater` gets automatic template rendering
on declared fields **before** Pydantic validation runs:

```python
from typing import ClassVar
from beacon.core.templater import Templater

class MyPlugin(Templater):
    template_fields: ClassVar[tuple[str, ...]] = ("source", "target")

    source: str
    target: str
    retries: int  # Not in template_fields — never rendered
```

When this model is validated with a `jinja_renderer` in context:

```python
plugin = MyPlugin.model_validate(
    {"source": "{{ params.db }}", "target": "output", "retries": 3},
    context={"jinja_renderer": renderer},
)
# plugin.source = "postgres"  (rendered)
# plugin.target = "output"    (no template, unchanged)
# plugin.retries = 3          (not in template_fields, unchanged)
```

### Template Field Extensions

For fields that reference template **files** (e.g., SQL files):

```python
class SqlPlugin(Templater):
    template_fields: ClassVar[tuple[str, ...]] = ("query",)
    template_fields_ext: ClassVar[dict[str, tuple[str, ...]]] = {
        "query": (".sql", ".jinja2"),
    }

    query: str
```

When `query = "transform.sql"` and the file extension matches, the renderer
loads the file from `template_searchpath` and renders it as a Jinja template.

---

## Global Variables Protection

The system detects unresolved global variables (`glob_*` pattern) and raises
an error rather than silently passing broken templates:

```python
# If vars('glob_start_date') is not set, this raises ValueError:
# "The Global variables 'glob_start_date' are not settled yet."
```

This ensures misconfigured variables are caught at deploy/trigger time, not
at execution time when debugging is harder.

---

## Resolution Timeline Summary

```text
Deploy time              Trigger time            Pre-execute            Execute time
────────────             ────────────            ───────────            ────────────
variables.yml parsed     vars() resolved         params.* resolved      Plugin receives
  │                        │                       │                    final concrete
  v                        v                       v                    values from
stages:                  schedule.params =       TaskContext.inputs =   Context dict
  prod:                  {                       {                      (no Jinja
    source: postgres       source: "postgres"      table: "postgres"     rendering)
    bucket: my-bucket      bucket: "my-bucket"     path: "my-bucket/x"
                         }                       }
```

**Key principle**: By the time the executor runs a plugin, `TaskContext.inputs`
contains **fully resolved concrete values**. The executor and plugin never
perform Jinja rendering.

---

## Plugin Author Guide

### Declaring Template Fields

If you write a custom plugin with fields that should support `{{ }}` syntax:

```python
from typing import ClassVar
from beacon.core import BasePlugin, Context


class GcsExtractPlugin(BasePlugin):
    plugin_name: ClassVar[str] = "gcs-extract"

    # These fields will be Jinja-rendered before validation
    template_fields: ClassVar[tuple[str, ...]] = ("bucket", "prefix")

    bucket: str
    prefix: str
    max_files: int = 100  # Not templated — always literal

    async def execute(self, context: Context) -> dict:
        # bucket and prefix are already resolved concrete strings
        files = await list_gcs_files(self.bucket, self.prefix)
        return {"file_count": len(files[:self.max_files])}
```

Usage in YAML:
```yaml
tasks:
  - id: extract
    uses: gcs-extract
    inputs:
      bucket: "{{ params.gcs_bucket }}"
      prefix: "raw/{{ params.date }}/events"
      max_files: 50
```

### What Gets Rendered

Only fields listed in `template_fields` are rendered. This is intentional:

- **Security**: Prevents injection via fields that shouldn't be templates
- **Performance**: Only scans fields that need it
- **Clarity**: Makes it explicit which fields support dynamic values

### Native Type Rendering

Because Beacon uses Jinja2's `NativeEnvironment`, templates can return
non-string types:

```yaml
inputs:
  threshold: "{{ 0.95 }}"         # → float 0.95 (not string "0.95")
  batch_size: "{{ 100 * 10 }}"    # → int 1000
  enabled: "{{ True }}"           # → bool True
  tags: "{{ ['a', 'b', 'c'] }}"   # → list ['a', 'b', 'c']
```

This eliminates the need for type coercion in plugin code.
