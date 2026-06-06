# Plan: Variable Requirements Tracking for Deployments

## Problem
When deploying a DAG, the system doesn't track what variables the DAG requires.
When triggering a deployment with custom variables, there's no validation before
runtime - errors only surface when Jinja rendering fails during execution.

Example: DAG defines `inputs: {source: "vars('key')"}` but:
- Deploy doesn't track that `key` is required
- Trigger allows any `--var` without validation
- Runtime error occurs when `key` is missing

## Solution Overview
1. Store variable requirements on Deployment at deploy time
2. Validate custom variables at trigger time against requirements
3. Surface validation errors early, not at runtime

## Implementation Plan

### 1. Extend Deployment Model (`beacon/models/deployment.py`)

Add a new field to store variable requirements:

```python
class Deployment(BaseModel):
    # ... existing fields ...

    required_variables: dict[str, dict] = Field(
        default_factory=dict,
        description=(
            "Variable requirements extracted from DAG templates. "
            "Keys are variable names, values contain: "
            "'has_default' (bool), 'default_value' (Any). "
            "Populated at deploy time by running plan analysis."
        ),
    )
```

Structure: `{"key": {"has_default": False}, "bucket": {"has_default": True, "default_value": "fallback"}}`

### 2. Update Deploy Command (`beacon/cli/commands/deploy_cmd.py`)

Add optional `--bundle` flag. When provided, analyze DAG and store requirements:

```python
@click.option(
    "--bundle",
    "bundle_path",
    default=None,
    type=click.Path(exists=True, file_okay=False),
    help="Bundle directory to analyze DAG variable requirements. Recommended.",
)
def deploy(..., bundle_path: str | None):
    # 1. Validate cron (existing)

    # 2. Load DAG from bundle (if provided)
    required_vars = {}
    if bundle_path:
        dag = _load_dag_for_deployment(dag_id, bundle_path)
        if dag:
            plan_result = plan(dag, variables={})
            required_vars = {
                v.key: {"has_default": v.has_default, "default_value": v.default_value}
                for v in plan_result.required_variables
            }
        else:
            click.echo(f"Warning: DAG {dag_id!r} not found in bundle", err=True)
    else:
        click.echo(
            "Tip: Use --bundle to analyze variable requirements at deploy time",
            err=True,
        )

    # 3. Create Deployment with requirements
    dep = Deployment(
        ...,
        variable_overrides=parse_kv_options(variable_overrides),
        required_variables=required_vars,
    )
```

**Backwards compatible**: If `--bundle` not provided, `required_variables` is empty and validation is skipped.

### 3. Add DAG Loading Helper (`beacon/cli/commands/deploy_cmd.py`)

```python
from ...core.bundle import LocalBundle
from ..loader import load_dags

def _load_dag_for_deployment(dag_id: str, bundle_path: str) -> Dag | None:
    """Load a specific DAG from bundle for analysis."""
    bundle = LocalBundle(name=Path(bundle_path).name, path=Path(bundle_path))
    dags = load_dags(bundle_path)
    for d in dags:
        if d.id == dag_id:
            return d
    return None
```

### 4. Update Trigger Command (`beacon/cli/commands/trigger_cmd.py`)

Validate custom variables against requirements before enqueuing:

```python
def trigger(deployment_id, variables, metadata_path):
    dep = asyncio.run(meta.get_deployment(deployment_id))

    # Validate incoming variables
    parsed_vars = parse_kv_options(variables)
    errors = _validate_trigger_variables(dep, parsed_vars)
    if errors:
        for e in errors:
            click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    # Enqueue trigger
    tid = asyncio.run(meta.enqueue_trigger(deployment_id, parsed_vars))

def _validate_trigger_variables(dep: dict, variables: dict) -> list[str]:
    """Validate trigger variables against deployment requirements."""
    errors = []
    required = dep.get("required_variables", {})

    for key, spec in required.items():
        has_default = spec.get("has_default", False)
        provided = key in variables or key in dep.get("variable_overrides", {})

        if not has_default and not provided:
            errors.append(
                f"Required variable {key!r} not provided. "
                f"Use --var {key}=VALUE"
            )

    return errors
```

### 4. Update Scheduler Variable Resolution (`beacon/scheduler.py`)

The scheduler already merges variables correctly:
1. Bundle scoped variables
2. Deployment `variable_overrides`
3. Trigger-time overrides

Add validation when processing triggers:

```python
async def _process_trigger(self, trigger: dict, dep: dict):
    override_vars = trigger.get("variables", {})

    # Validate before running
    errors = _validate_trigger_variables(dep, override_vars)
    if errors:
        logger.error("Invalid trigger for %s: %s", dep["id"], errors)
        return  # Skip this trigger

    # Proceed with run
    ...
```

## File Changes Summary

| File | Change |
|------|--------|
| `beacon/models/deployment.py` | Add `required_variables` field |
| `beacon/cli/commands/deploy_cmd.py` | Add `--bundle` option, load DAG, run plan, store requirements |
| `beacon/cli/commands/trigger_cmd.py` | Validate variables against requirements before enqueue |
| `beacon/scheduler.py` | Validate variables when processing triggers (defense in depth) |
| `docs/core/reference.md` | Document variable requirements in Deployment section |

## Edge Cases

1. **DAG not found at deploy time**: Allow deploy with empty `required_variables`, warn user
2. **Deployment created before this feature**: `required_variables` defaults to `{}`, validation is lenient
3. **Nested variable keys** (`vars("db.host")`): Stored as flat key `"db.host"`
4. **Variables with defaults**: Tracked but not required at trigger time

## Alternatives Considered

1. **Validate at runtime only**: Rejected - defeats the purpose of early validation
2. **Store full PlanResult**: Rejected - only need variable requirements, not full plan
3. **Re-analyze at trigger time**: Rejected - requires DAG to be available at trigger time

## Testing

1. Unit test: Deployment model with `required_variables`
2. Unit test: `_validate_trigger_variables` helper
3. Integration test: Deploy → Trigger with missing required var
4. Integration test: Deploy → Trigger with valid vars
