# Design: Template Extension Support for Plugins

## Summary

Add `template_ext` class variable to plugins that signals which file extensions should trigger file-loading + Jinja rendering. This enables `py_statement` to accept both file paths (ending in `.py`) and inline code strings.

## Goals

1. Rename `py_file` → `py_statement` in PythonPlugin
2. `py_statement` accepts:
   - **File path** (ends with `.py`): Load file → render with Jinja → execute
   - **Inline code** (no `.py` suffix): Render as Jinja string → execute
3. Add `template_ext: ClassVar[tuple[str, ...]]` to BasePlugin (default empty)
4. PythonPlugin sets `template_ext = (".py",)`
5. Jinja rendering of file contents uses `{{ }}` syntax; users escape with `{{{{`

## Non-Goals

- Adding Jinja's `{% include %}` or `{% extends %}` template inheritance
- Changing how the Renderer works for non-file inputs
- Adding this to the core Renderer (remains plugin-internal)

## Architecture Decision

**Where does file-loading + rendering happen?**

**Decision: Plugin-level implementation (Option B from Q6)**

Rationale:
1. The Renderer is designed for value interpolation, not I/O
2. File loading is specific to certain plugins (py, potentially bash, sql)
3. Plugin already has context needed for asset resolution
4. Keeps Renderer simple and fast for the common case

## Implementation Plan

### 1. Add `template_ext` to BasePlugin

**File:** `beacon/core/plugin.py`

```python
class BasePlugin(BaseModel, ABC, metaclass=PluginMeta):
    plugin_name: ClassVar[str] = BASE_PLUGIN_NAME
    compatible_actions: ClassVar[tuple[str, ...]] = ()
    template_ext: ClassVar[tuple[str, ...]] = ()  # NEW: extensions that trigger file rendering
```

### 2. Update PythonPlugin

**File:** `beacon/providers/standard/plugins/task/python.py`

Changes:
- Rename `py_file: str` → `py_statement: str`
- Add `template_ext: ClassVar[tuple[str, ...]] = (".py",)`
- Update `execute()` to:
  1. Check if `py_statement.endswith(".py")`
  2. If yes: resolve asset path → read file → render with Jinja → execute
  3. If no: render `py_statement` as inline code → execute
- Update `teardown()` similarly

### 3. Add file rendering helper

**File:** `beacon/providers/standard/plugins/task/python.py` (internal function)

```python
def _render_code(code: str, context: dict[str, Any]) -> str:
    """Render code string with Jinja.

    Uses the same Renderer as the scheduler but for file contents.
    """
    from beacon.core.renderer import Renderer

    renderer = Renderer(context)
    return renderer.render(code)
```

### 4. Update asset resolution for template search path

**File:** `beacon/core/assets.py`

The existing `resolve_asset()` already implements the search path:
1. `<dag_folder>/assets/<path>`
2. `<bundle_root>/assets/<path>`

No changes needed here.

### 5. Context for Jinja rendering

The file/inline rendering needs access to the same context as trigger-time rendering:
- `params`
- `vars` (already resolved into params at trigger time)
- `runtime`
- `outputs` (upstream outputs)

This is already available in `PythonPlugin.execute()` via the `context` parameter.

### 6. Update documentation

**Files to update:**
- `docs/core/reference.md` - Update `py_file` → `py_statement`
- Docstrings in PythonPlugin

## File Changes Summary

### Core Changes

| File | Change |
|------|--------|
| `beacon/core/plugin.py` | Add `template_ext` ClassVar to BasePlugin |
| `beacon/providers/standard/plugins/task/python.py` | Rename `py_file` → `py_statement`, add inline code support, add Jinja rendering for file contents |

### Test Files (rename `py_file` → `py_statement`)

| File |
|------|
| `tests/unit/test_task_context.py` |
| `tests/e2e/test_bundle_e2e.py` |
| `tests/e2e/test_phase1_e2e.py` |
| `tests/e2e/test_py_plugin_logging.py` |
| `tests/e2e/test_all_events.py` |
| `tests/e2e/test_upstream_outputs.py` |
| `tests/e2e/test_parallel_100_dags.py` |
| `tests/e2e/test_plugin_teardown.py` |
| `tests/e2e/test_backfill_upstream_outputs.py` |
| `tests/e2e/test_py_plugin_e2e.py` |

### Documentation Files (update field name)

| File |
|------|
| `docs/core/reference.md` |
| `docs/core/deploy.md` |
| `docs/core/roadmap.md` |
| `docs/examples/yaml_with_standard.md` |
| `docs/examples/py_with_standard.md` |

## Backward Compatibility

**Breaking change:** `py_file` renamed to `py_statement`

Migration:
```yaml
# Before
inputs:
  py_file: ./script.py

# After
inputs:
  py_statement: ./script.py
```

No backward compatibility shim needed since Beacon is pre-1.0.

## Testing Plan

1. **Unit tests** for `_render_code()` helper
2. **E2E tests** for:
   - File path with `.py` extension
   - Inline code string
   - File with Jinja `{{ params.x }}` → rendered
   - File with escaped `{{{{ params.x }}}}` → literal `{{ params.x }}`
   - File not found error
   - Inline code with Jinja syntax error

## Risks

1. **Performance**: File I/O + rendering at execute time (not trigger time)
   - Mitigation: This is the intended behavior (lazy loading)

2. **Security**: User code with Jinja templates
   - Mitigation: Same sandbox as existing Renderer

3. **Breaking change**: `py_file` → `py_statement`
   - Mitigation: Clear migration docs; pre-1.0 allows breaking changes
