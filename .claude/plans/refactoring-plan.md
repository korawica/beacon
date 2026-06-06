# Beacon Refactoring Plan

## Goal
Refactor and lean the Beacon codebase to improve maintainability while preserving the architectural principles from `docs/core/reference.md`.

## Key Principles from reference.md
1. **Async-first** - One execution model
2. **Plugin is the unit of logic** - DAGs are pure configuration
3. **Simple path is trivial** - Write a function, reference with `uses: py`
4. **Executor-agnostic** - TaskContext is serializable
5. **Stateless runtime** - All state in metadata store

## Current Codebase Analysis

### What's Working Well (Don't Touch)
- `BasePlugin` with metaclass auto-registration
- Action hierarchy (Task, Sensor, Branch, ShortCircuit, Group)
- `TaskContext` as serializable unit
- `Renderer` two-pass architecture
- `LocalMetadata` with sharding and caching
- Remote plugin isolation via uv

### Areas for Improvement

#### 1. `runner.py` is too large (~860 lines)
**Problem**: Mixes graph operations, orchestration, API methods, and callbacks.
**Solution**: Extract graph operations to `core/graph.py`.

#### 2. State management is implicit
**Problem**: State transitions happen across runner/worker without clear boundaries.
**Solution**: Add a lightweight state transition helper in `core/state.py`.

#### 3. `plan.py` validation logic could be cleaner
**Problem**: Mixes validation, detection, and rendering logic.
**Solution**: Extract detection functions to `core/validation.py`.

#### 4. Protocol placement
**Problem**: `MetadataProtocol` lives in `context.py` alongside runtime helpers.
**Solution**: Move to dedicated `core/protocols.py`.

#### 5. Duplicate graph logic in `Dag.backfill`
**Problem**: `Dag.backfill` calls `_build_graph` from runner.
**Solution**: Move `_build_graph` to `core/graph.py` and re-export.

---

## Refactoring Steps

### Phase 1: Extract Graph Operations (Low Risk)

**Files to create:**
- `beacon/core/graph.py` - Graph topology utilities

**Files to modify:**
- `beacon/runner.py` - Import from `core/graph.py`
- `beacon/models/dag.py` - Import from `core/graph.py`

**Extract from `runner.py`:**
```python
# Move to core/graph.py
- _flatten_actions()
- _build_graph()
- _Graph dataclass
- _collect_self_and_downstream()
- _detect_cycle() (from plan.py)
- _topological_sort() (from plan.py)
```

**Benefits:**
- Single source of truth for graph operations
- Easier to test graph logic independently
- `Dag.backfill` doesn't need to import from `runner`

---

### Phase 2: Extract Validation Utilities (Low Risk)

**Files to create:**
- `beacon/core/validation.py` - Detection and validation utilities

**Files to modify:**
- `beacon/plan.py` - Import from `core/validation.py`

**Extract from `plan.py`:**
```python
# Move to core/validation.py
- _detect_required_variables()
- _detect_required_secrets()
- RequiredVariable dataclass
- RequiredSecret dataclass
```

**Benefits:**
- Validation logic can be reused outside `plan.py`
- Cleaner separation of concerns

---

### Phase 3: Reorganize Protocols (Low Risk)

**Files to create:**
- `beacon/core/protocols.py` - Protocol definitions

**Files to modify:**
- `beacon/core/context.py` - Remove `MetadataProtocol`
- Files importing `MetadataProtocol` - Update imports

**Move from `context.py`:**
```python
# Move to core/protocols.py
- MetadataProtocol
```

**Benefits:**
- Clear separation between runtime context and storage protocol
- Easier to find protocol definitions

---

### Phase 4: Consolidate State Logic (Medium Risk)

**Files to modify:**
- `beacon/core/state.py` - Add transition helpers

**Add to `state.py`:**
```python
def is_terminal(state: TaskState) -> bool:
    return state in TERMINAL_STATES

def can_transition(from_state: TaskState, to_state: TaskState) -> bool:
    """Validate state transitions (for logging/debugging, not enforcement)."""
    VALID_TRANSITIONS = {
        TaskState.NONE: {TaskState.SCHEDULED, TaskState.SKIPPED, TaskState.UPSTREAM_FAILED},
        TaskState.SCHEDULED: {TaskState.QUEUED},
        TaskState.QUEUED: {TaskState.RUNNING},
        TaskState.RUNNING: {TaskState.SUCCESS, TaskState.FAILED, TaskState.SKIPPED, TaskState.UP_FOR_RETRY},
        TaskState.UP_FOR_RETRY: {TaskState.QUEUED, TaskState.FAILED},
    }
    return to_state in VALID_TRANSITIONS.get(from_state, set())
```

**Benefits:**
- Self-documenting state machine
- Easier debugging

---

### Phase 5: Simplify Runner (Low Risk)

**After Phase 1, `runner.py` will be ~650 lines.**

Further simplifications:
1. Move `_fire_dag_callbacks` logic to `callback.py` as a helper
2. Reduce inline comments (the reference.md documents the design)
3. Consider extracting `DagRunResult` to a separate file if it grows

---

### Phase 6: Clean Up Imports and Exports (Low Risk)

**Files to modify:**
- `beacon/__init__.py` - Ensure clean public API
- `beacon/core/__init__.py` - Consolidate core exports

**Verify:**
- All public symbols are in `__all__`
- No circular imports
- Consistent import style (absolute vs relative)

---

## What NOT to Do (Non-Goals from reference.md)

1. **No abstraction for single-use code** - Don't create base classes for things used once
2. **No "flexibility" that wasn't requested** - Don't add configuration options
3. **No speculative features** - Don't add "might be useful" utilities
4. **No provider installation at runtime** - Keep plugin resolution at parse time
5. **No UI, OIDC, secrets adapter** - Explicitly out of scope

---

## File Structure After Refactoring

```
beacon/
├── __init__.py              # Public API (unchanged)
├── callback.py              # Callbacks + OnTaskEvent/OnDagEvent (unchanged)
├── errors.py                # TaskFailed, TaskSkipped, TaskRetry (unchanged)
├── logging.py               # Logging infrastructure (unchanged)
├── plan.py                  # DAG validation (~400 lines, reduced)
├── runner.py                # DAG execution (~600 lines, reduced)
├── runtime.py               # load_context() (unchanged)
├── scheduler.py             # DeploymentScheduler (unchanged)
├── utils.py                 # Utilities (unchanged)
├── worker.py                # Task worker (unchanged)
├── core/
│   ├── __init__.py          # Core exports
│   ├── action.py            # BaseAction (unchanged)
│   ├── assets.py            # Bundle context (unchanged)
│   ├── bundle.py            # LocalBundle (unchanged)
│   ├── context.py           # Context TypedDict (reduced)
│   ├── executor.py          # Executors (unchanged)
│   ├── graph.py             # NEW: Graph operations
│   ├── plugin.py            # BasePlugin (unchanged)
│   ├── protocols.py         # NEW: MetadataProtocol
│   ├── renderer.py          # Jinja rendering (unchanged)
│   ├── remote_plugin.py     # Remote plugin runner (unchanged)
│   ├── state.py             # TaskState + helpers (enhanced)
│   ├── task_context.py      # TaskContext (unchanged)
│   ├── template.py          # Template utilities (unchanged)
│   ├── trigger_rule.py      # Trigger rules (unchanged)
│   ├── validation.py        # NEW: Detection utilities
│   └── variables.py         # Variable scope (unchanged)
├── metadata/
│   ├── __init__.py
│   └── json_store.py        # LocalMetadata (unchanged)
├── models/
│   ├── __init__.py
│   ├── branch.py            # Branch action (unchanged)
│   ├── dag.py               # Dag model (unchanged, imports from core/graph)
│   ├── deployment.py        # Deployment model (unchanged)
│   ├── group.py             # Group action (unchanged)
│   ├── sensor.py            # Sensor action (unchanged)
│   ├── short_circuit.py     # ShortCircuit action (unchanged)
│   └── task.py              # Task action (unchanged)
├── cli/                     # CLI commands (unchanged)
└── providers/
    └── standard/            # Built-in plugins (unchanged)
```

---

## Implementation Order

1. **Phase 1**: Extract `core/graph.py` - Highest impact, lowest risk
2. **Phase 4**: Add state helpers to `core/state.py` - Simple addition
3. **Phase 3**: Create `core/protocols.py` - Clean separation
4. **Phase 2**: Extract `core/validation.py` - Moderate refactoring
5. **Phase 5**: Simplify runner - After graph extraction
6. **Phase 6**: Clean up imports - Final polish

---

## Testing Strategy

After each phase:
1. Run existing tests: `pytest tests/`
2. Verify imports work: `python -c "import beacon; print(beacon.__all__)"`
3. Check for circular imports: `python -c "from beacon import Dag, DagRunner"`

---

## Success Criteria

1. **No behavior changes** - All tests pass
2. **Smaller files** - Largest file < 500 lines (currently runner.py is 860)
3. **Clear responsibilities** - Each module has one job
4. **Easier navigation** - Find graph logic in `graph.py`, not `runner.py`
5. **Maintained simplicity** - No new abstractions or patterns

---

## Estimated Impact

| File | Before | After | Reduction |
|------|--------|-------|-----------|
| runner.py | ~860 lines | ~600 lines | 30% |
| plan.py | ~610 lines | ~400 lines | 34% |
| context.py | ~160 lines | ~100 lines | 37% |

Total code moved to dedicated modules: ~300 lines
New modules created: 3 (graph.py, protocols.py, validation.py)
