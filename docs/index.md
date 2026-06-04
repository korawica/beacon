# Beacon

An everyday workflow orchestrator — small, async, file-on-disk,
zero-config.

## Start here

- **[Configuration](./core/config.md)** — the 8 `BEACON_*` env vars
  and why there is no `beacon.toml`.
- **[Deploy flow](./core/deploy.md)** — bundle layout, scoped
  variables, deployment pinning, `beacon sync`.
- **[Architecture](./core/architecture.md)** — components, lifecycle,
  data flow.
- **[Design](./core/design.md)** — DAG vs Deployment, templating,
  state model.
- **[Templating](./core/templating.md)** — `vars()`, `params`, the
  two-pass render.
- **[Action types](./core/action.md)** — Task, Branch, Sensor,
  Group, ShortCircuit.

## Planning

- **[Production plan](./core/production_plan.md)** — what
  "production-ready" means + the non-goals discipline.
- **[Implement plan](./core/implement_plan.md)** — status of every
  deliverable.

## Examples

- **[Python with the standard provider](./examples/py_with_standard.md)**
- **[Python with custom plugins](./examples/py_with_plugin.md)**
- **[YAML with the standard provider](./examples/yaml_with_standard.md)**
