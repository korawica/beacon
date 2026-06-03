# Beacon — Code Review & Improvement Plan

Audience: maintainer who wants beacon to stay **lean, simple, async-first,
production-grade** (NOT an Airflow replacement). Focus is on **core** —
not API server, not Web UI.

Reviewed at commit-state: 2026-06-03. ~4,200 LoC across `beacon/`.

> **Implementation status (this branch):** §1.1–1.4, 1.6, 1.7, 1.8 done;
> §2.5 (dead code) done; §2.6, 2.7, 2.9, 2.10, 2.11, 2.13, 2.14, 2.15, 2.16
> done; §3 done via new `beacon/scheduler.py` (`LocalScheduler`); §5 done.
> Deferred (need design call): §2.1 (renderer rewrite), §2.2 (drop
> Templater from BasePlugin), §2.3 (unify Jinja), §2.4 (`build_task_context`
> kept on `BaseAction` — used by scheduler), §2.8 (heap topo), §4.x
> (ergonomics), §6 SQLite store.
>
> **166 tests pass** (134 previous + 32 new).

Legend
- **P0** — bug / correctness / wrong behavior. Fix now.
- **P1** — leanness / clarity / scaling risk. Fix before adding more surface.
- **P2** — nice-to-have polish.

---

## 1. Bugs (P0)

### 1.1 `GitBundle.local` is infinite recursion
`beacon/core/bundle.py:199-206`
```python
@property
def local(self) -> LocalBundle:
    if self._local is None:
        ...
        self._local = LocalBundle(name=self.name, path=root)
    return self.local        # ← recurses forever
```
**Fix:** `return self._local`. Also add a unit test that imports `GitBundle`
and reads `.local`.

### 1.2 `Dag.run()` does not respect the DAG graph
`beacon/models/dag.py:109-164`
- Creates a brand-new `Worker(meta, max_concurrent=1)` **per task** inside
  the topological loop, calls `worker.run()` + a `sleep(0.5)` shutdown — i.e.
  every task pays ~500 ms of polling latency and the DAG runs strictly
  serially even when the graph allows parallelism.
- Ignores `Branch.evaluate_downstream` / `ShortCircuit.evaluate_downstream`
  → downstreams of a branch always run, breaking branching semantics.
- Stops only on `FAILED` — does not propagate `UPSTREAM_FAILED` or `SKIPPED`.
- Does not fire DAG-level callbacks (`start`, `success`, `failure`,
  `finished`).
- Does not evaluate trigger rules.

This method is the **developer entry point** (`dag.run() / dag.test()`)
referenced in `design.md` — and it bypasses almost every primitive the
rest of the codebase defines. Right now `Dag.run()` is essentially a toy
runner. **Fix:** write a single `LocalScheduler` (see §5) and have
`Dag.run()` delegate to it.

### 1.3 `pyproject.toml` pytest `pythonpath`
`pyproject.toml:21`
```toml
pythonpath = ["beacon"]
```
This makes tests `import core`, not `import beacon.core`. Should be
`pythonpath = ["."]` (or removed — the package is installable).

### 1.4 `BaseAction.desc` / `Dag.desc` type lies
`beacon/core/action.py:40`, `beacon/models/dag.py:19`
```python
desc: str = Field(default=None, ...)
```
Default is `None` but the annotation is `str`. Pydantic v2 will accept it,
but static typing breaks. Use `str | None`.

### 1.5 `Worker._fire` swallows exceptions silently per callback but is OK; however `OnDagEvent` is never invoked anywhere
There is no code path that fires `OnDagEvent.notify` — DAG-level callbacks
are dead. Design says they fire on DAG terminal state. Tied to the missing
scheduler (§5).

### 1.6 Plugin registration silently overrides
`beacon/core/plugin.py:46-56`
```python
if plugin_name in PLUGINS_REGISTRY:
    logger.debug("Overriding plugin registry with %s", plugin_name)
with _lock:
    PLUGINS_REGISTRY[plugin_name] = cls
```
Bundle plugins silently shadow built-ins. Design says built-ins come first;
bundle plugins should either log a **warning** or fail unless an explicit
override flag is set. Today an accidentally-named `py` in user `./plugins/`
will replace `PythonPlugin` with zero warning.

### 1.7 `BasePlugin` metaclass registers every subclass
`beacon/core/plugin.py:79-83`
```python
register_plugin(pydantic_cls,
                attrs.get("plugin_name", to_snake_case(pydantic_cls.__name__)))
```
Any intermediate / abstract subclass (e.g. `class MyBasePlugin(BasePlugin): ...`
in user code, or test fixtures) auto-registers. The class attribute check
later (`!= BASE_PLUGIN_NAME`) only protects `BasePlugin` itself.

**Fix:** only register when `plugin_name` is **explicitly declared** in the
class body (`"plugin_name" in attrs`). Drop the implicit `to_snake_case`
fallback — it's a foot-gun.

### 1.8 `core/action.py:130 wrap_execute` is dead code
The worker (`beacon/worker.py:_process`) duplicates the lifecycle logic
inline; nothing calls `BaseAction.wrap_execute`, `_transition`, or
`_fire_callbacks`. Remove the whole method or migrate Worker to call it
(pick one — having both invites them to drift).

### 1.9 `Worker._process` holds the semaphore across the whole execution including remote attempts
`beacon/worker.py:119` — fine for `LocalExecutor`, but once
`DockerExecutor`/`KubernetesExecutor` lands the slot is held while polling
a pod (potentially hours). Sensors that just `await asyncio.sleep(60)` will
still hold a slot. The whole point of "async-first eliminates deferrable"
(design §"Async Execution Model") is to **not** hold slots for I/O waits.

**Fix:** semaphore is unnecessary for async I/O-bound tasks. Drop it for
sensors; keep a separate CPU-bound cap (much larger, e.g. 1000) for normal
tasks. Or remove entirely and rely on the async runtime.

### 1.10 `runtime.RuntimeContext.run_date` is `datetime | None = None` — but no `from __future__ import annotations`
Works only because the project targets Python 3.14 (PEP 649). Worth
documenting in README so contributors don't downgrade.

---

## 2. Leanness — Things to Delete or Shrink (P1)

### 2.1 `core/renderer.py` is 375 lines of Airflow heritage
- Mentions Airflow in docstrings (`Airflow Jinja templating`, `Airflow can
  evaluate it at task run-time`, `preserve mode`).
- Supports `NativeEnvironment`, `SandboxedEnvironment`, `FileSystemLoader`,
  `_seen` cycle protection, `template_ext`, `template_searchpath`,
  `copy_override`, `set_globals`, `DebugUndefined` preserve mode.
- The design says: **"All templates resolved before TaskContext reaches the
  executor."** The renderer's complexity is justified for an Airflow plugin,
  not for beacon.

**Recommendation:** rewrite as a ~80-line module that does only:
1. `render(value, ctx)` recursing through `str / list / dict` with a
   single `SandboxedEnvironment` (no native, no file loader).
2. Two pass: trigger-time (`vars()` + `params`) → execute-time (`outputs.*`).

Delete `copy_override`, `preserve mode`, `KeepUndefined`, `_seen`. None of
these are referenced anywhere outside the renderer itself.

### 2.2 `core/templater.py` couples plugin model to Jinja
`Templater` is a `BaseModel` subclass with a `model_validator(mode="before")`
that pre-renders fields using a renderer injected via Pydantic validation
context. Plugins inherit from it.

Problems:
- Plugins now have a non-obvious "validation can render templates"
  side-channel. Most plugins never use it.
- `LocalExecutor` already calls `model_validate(inputs, context={"jinja_renderer": None})`
  — meaning execute-time rendering is intentionally disabled, but the
  machinery is still there.
- It duplicates `dryrun.py:_render_inputs` (a separate, simpler
  implementation using raw `jinja2.Template`).

**Recommendation:** remove `Templater` from `BasePlugin`'s inheritance.
Rendering happens **only** in the scheduler before `TaskContext` is built,
and **only** in dryrun for previewing. Plugins are plain Pydantic models.

### 2.3 Two Jinja rendering implementations
- `core/renderer.py` — feature-rich, plugin-side.
- `dryrun.py:_render_inputs` — minimal, dryrun-side. Doesn't recurse lists.

Pick one (a small one) and use it everywhere. The dryrun version is closer
to what design.md wants.

### 2.4 `core/action.py` carries scheduler logic in the wrong place
`BaseAction.build_task_context`, `wrap_execute`, `_transition`,
`_fire_callbacks` belong to the scheduler/worker, not the model.
A `BaseAction` is a pydantic *definition*; making it own state-machine
methods means tests have to construct workers to test models.

**Recommendation:** move `build_task_context` to a `scheduler.py` helper.
Delete `wrap_execute` / `_transition` / `_fire_callbacks`.

### 2.5 Dead / vestigial code
- `beacon/models/task.py:44-52` — commented-out `outputs()` method. Delete.
- `beacon/models/global_variable.py` — defined but never imported / used.
- `beacon/models/variable.py` — defined but never imported / used.
- `beacon/models/task.py:8 TaskOutput` — defined, unused.
- `beacon/core/plugin.py:48` — commented-out line. Delete.
- `beacon/utils.py:load_all_plugins` — unused (`grep_search` shows zero
  callers). Bundle.py does its own discovery.
- `beacon/core/bundle.py:217 GcsBundle` — stub with no implementation,
  but listed alongside production classes. Either implement or remove
  until Phase 2.
- `beacon/core/action.py:5 from typing import ... Type # noqa` — use lowercase `type`.

### 2.6 `OnTaskEvent` / `OnDagEvent` re-instantiate the hook on every fire
`callback.py:_resolve_hook` creates a fresh `Callback` instance per event.
For a DAG with 100 tasks emitting 4 events each = 400 instantiations.
Cache the resolved instance on the `OnTaskEvent` model (e.g. private
`_resolved` slot or `functools.cached_property`).

### 2.7 `threading.Lock` in async-only modules
- `core/plugin.py:24 _lock` — registry mutated only at import time + bundle
  load. No thread contention possible in async-only design.
- `callback.py:18 _lock` — same.
- `metadata/json_store.py` uses `threading.Lock` around in-memory dicts —
  again, async-only single loop. Replace with no lock, or document that the
  store is intended to be safe across thread pools.

If you keep them, that's defensible — but they're a leanness smell. Two
sentences in the module docstring would justify "I kept the lock for safety
when called from `asyncio.to_thread` callbacks" — currently undocumented.

### 2.8 `dryrun._topological_sort` uses `O(N²)` sort-then-pop
`beacon/dryrun.py:336-343`
```python
while queue:
    queue.sort()      # ← every iteration
    node = queue.pop(0)
```
Switch to `heapq.heappush/heappop` for `O(N log N)` and identical
determinism. Matters at 10k tasks.

### 2.9 `JsonMetadata._state_cache` has no eviction
`_CACHE_SIZE` only **prevents** writes once full; old keys stay forever
unless `evict_run_from_cache` is called manually. Use
`collections.OrderedDict` with LRU semantics, or just `functools.lru_cache`
on a wrapped read.

### 2.10 `JsonMetadata._async_read` TOCTOU
```python
def _do_read():
    if not path.exists():
        return None
    return json.loads(path.read_text())
```
Two syscalls + race window. Use:
```python
try:
    return json.loads(path.read_text())
except FileNotFoundError:
    return None
```

### 2.11 Sequential awaits where `asyncio.gather` is correct
- `worker.py:225-230 _resolve_upstream_outputs` — N upstreams = N awaits in
  a Python `for` loop. Gather.
- `json_store.py:218 get_all_task_states` and `:157 get_all_task_contexts`
  — same pattern; gather.

Modest perf win, but trivial and matters for fan-in tasks.

### 2.12 `bundle.py:_compute_version` reads every file twice (once for hash, again at parse)
Hash from `Path.stat().st_mtime_ns` + size of relevant files would suffice
for cache invalidation; full content hash should only happen on commit (or
just use the git SHA in `GitBundle`).

### 2.13 `bundle.py` module-name collisions
`module_name = f"_beacon_bundle_{self.name}_{py_file.stem}"` — two plugin
files named `extract.py` in different subdirs will collide. Use the
relative path: `py_file.relative_to(self.plugins_path).with_suffix("").as_posix().replace("/", "_")`.

### 2.14 `bundle.LocalBundle.load_plugins` registration detection is O(modules × len(PLUGINS_REGISTRY))
For each loaded module, it walks `dir(module)` and checks every attribute
against `PLUGINS_REGISTRY`. Snapshot the registry keyset **before** import,
diff **after**. ~10 lines, much faster, no false positives.

### 2.15 `worker.py:96 self._semaphore._value` is private API
Used for a log message only. Just store `max_concurrent` on the instance.

### 2.16 `worker.py:run` busy-polls with `wait_for(timeout=0.5)`
Use a real shutdown event:
```python
shutdown = asyncio.Event()
get = asyncio.create_task(self._queue.get())
done, _ = await asyncio.wait({get, asyncio.create_task(shutdown.wait())},
                              return_when=FIRST_COMPLETED)
```
Or simpler: put a sentinel `None` on shutdown (already done) and just
`msg = await self._queue.get()` without the timeout. The current code burns
2 wakeups/sec doing nothing.

---

## 3. Gaps vs. design.md (P0/P1)

These are documented as core but missing or incomplete:

| Design Section | Status | Gap |
|---|---|---|
| Scheduler (`Phase 2`) | **Missing** | No scheduler module exists. `Dag.run()` improvises (see §1.2). |
| Trigger-rule evaluation in run loop | **Missing** | `evaluate_trigger_rule` exists but is never called. |
| `UPSTREAM_FAILED` propagation | **Missing** | Worker doesn't mark downstreams as `UPSTREAM_FAILED` on parent failure. |
| Branch / ShortCircuit downstream skip | **Missing in runtime** | `evaluate_downstream` exists on models but is never invoked. |
| Teardown scheduling | **Missing** | `teardown` field is parsed (dryrun validates the ref), but no runtime path runs teardowns after dependents finish. |
| DAG-level callbacks (`OnDagEvent`) | **Missing fire path** | Class exists; no caller. |
| Variables resolution two-pass (`vars` then `params` then `outputs`) | **Partial** | Only ad-hoc in dryrun; not in any runtime path. |
| Deployment scheduling (cron, catch-up, intervals) | **Missing** | `Deployment` model parses; no scheduler ticks it. |
| `foreach_task` action | **Pending** (acknowledged) | Stays in design only — OK for now. |

The single biggest hole is **there is no scheduler**. The worker is a queue
consumer that knows nothing about DAG topology. Without a scheduler, none
of the lifecycle promises in design.md ("after each task reaches terminal
state… evaluate downstream… fire DAG callbacks…") can be kept.

---

## 4. API Ergonomics (P1)

### 4.1 `BasePlugin` requires Pydantic + Templater inheritance
For "Simple path must be trivial", the user-facing contract should be:
```python
class MyPlugin(BasePlugin):
    plugin_name = "my"
    source: str
    async def execute(self, ctx):
        ...
```
Today this works, but the inheritance chain is
`Templater(BaseModel, model_validator) → ABC → metaclass(type(BaseModel))`.
If a user does `class MyPlugin(BasePlugin)` with `__init__`, Pydantic and
Templater fight. Document or simplify.

### 4.2 `Context` is a `TypedDict` with `total=False`
Every plugin must do `context.get("dag_id", "")` defensively (see
`python.py:111-124`). Either make it `total=True` (all fields required
when executor builds it — which it already does), or move to a tiny dataclass.

### 4.3 `load_context()` requires a `from beacon import load_context` while the docs example uses `from beacon.runtime import load_context`
`beacon/__init__.py` re-exports it — good. Pick one path in all docs.

### 4.4 `Dag(owners=[...])` is required but most local dev examples don't need owners
Make optional or default to `[]`. Forcing owners on `dag.test()` / `dag.run()`
violates "simple path is trivial".

### 4.5 `Deployment.start_date` is required, no default
For local manual triggers (`cron=None`), `start_date` is irrelevant.
Make `start_date: datetime | None = None`.

### 4.6 `dag.test()` calls `dag.run()` which calls `dryrun()` then runs everything
Double validation cost + writes to a tempdir. `test()` and `run()` should
share a common executor and differ only in where they store metadata
(tempdir vs. user path).

---

## 5. Proposed Refactor — One LeanScheduler

A single `beacon/scheduler.py` (~250 lines) would close most of §3 and
let `Dag.run()` become 20 lines:

```python
class LocalScheduler:
    def __init__(self, dag, meta, executor, callbacks=()):
        self.dag = dag
        self.meta = meta
        self.executor = executor
        self.callbacks = callbacks
        self.task_map, self.downstream = build_indexes(dag)

    async def run(self, *, run_id, params, run_date, ...):
        # 1. Fire dag.start
        # 2. Initialize all task states = NONE
        # 3. Enqueue tasks with no upstream into worker
        # 4. After each task terminal:
        #     a. evaluate Branch/ShortCircuit -> mark skip set
        #     b. for each downstream:
        #         - if upstream failed -> UPSTREAM_FAILED
        #         - elif in skip set    -> SKIPPED
        #         - elif trigger rule met -> enqueue
        # 5. When all terminal: fire dag.success/failure + finished
        # 6. Run teardowns last
```

This collapses:
- `Dag.run()` boilerplate (§1.2)
- `BaseAction.wrap_execute` duplicate (§1.8)
- Missing branch/short-circuit/teardown runtime (§3)
- DAG-level callback firing (§1.5)

Worker stays a dumb queue → executor dispatcher. Scheduler owns the graph.

---

## 6. Suggested Order

1. **Fix bugs** §1.1–1.4, 1.6, 1.7 (1 day).
2. **Delete dead code** §2.5 + §1.8 (1 hour).
3. **Build `LocalScheduler`** (§5) and rewrite `Dag.run()` / `dag.test()`
   on top of it (2–3 days). This is where the design becomes real.
4. **Trim `renderer.py` + drop `Templater` from `BasePlugin`** (§2.1, 2.2)
   (1 day) — easier once scheduler owns rendering.
5. **Tighten worker** §1.9, 2.15, 2.16 + gather upstream reads §2.11
   (½ day).
6. **JsonMetadata polish** §2.9, 2.10, 2.11 (½ day).
7. **Plugin registry strictness** §1.6, 1.7, 2.14 (½ day).

Total: ~1 working week to close the lean-but-correct gap. Web UI and API
server remain firmly Phase 2/3 as you wanted.

---

## 7. Questions for You (need answers before I touch code)

1. **Scheduler placement**: do you want a single `beacon/scheduler.py` (in-process,
   async) for now, with the door open for a separate process later — or do you
   want it as a separate subprocess from day one?
2. **Worker concurrency**: drop the semaphore for sensors (§1.9), or keep
   per-executor caps (e.g. one cap for local CPU-bound, another for I/O)?
3. **Registry override policy**: should bundle `./plugins/` be allowed to
   override built-ins silently (current), with warning, or never (fail)?
4. **Rendering engine**: are you OK dropping `NativeEnvironment` and
   `preserve mode` from the renderer entirely? They exist for Airflow-style
   late binding which beacon doesn't need.
5. **Metadata default**: stick with `JsonMetadata` as the only Phase-1 store,
   or jump straight to SQLite for `dag.run()`/`dag.test()` (fewer files,
   simpler atomic semantics, single-file deletion)?
6. **`Templater` on plugins**: confirm you're OK removing the
   `model_validator(mode="before")` Jinja machinery from `BasePlugin`. Any
   existing user plugin relying on `template_fields` would break.
7. **`compatible_actions`**: the only consumer is `dryrun.py`. Keep, or
   delete and let runtime errors signal incompatibility?

Answer:

1. Yes, please do it.
2. I do not sure a trade-off about it. but if I make this package scalable and do not have a concern, please do it.
3. Yes, I want it silently with logging a warning. Because it will raise when the dag run.
4. yes
5. I want to stay on this phase.
6. Yes, please do it. I think it is better to separate the responsibility of plugin and renderer.
7. It use for checking the plugin compatibility or not. Did you have any plan for it? because action can use same plugin by user.
