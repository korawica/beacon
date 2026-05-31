# Beacon

An everyday workflow orchestrator that simple and easy to customize via YAML
template.

---

## Examples

This is the full example of a workflow that this beacon package support.

```yaml title="hello_world.yml"
id: hello-world
type: dag
owners: ["de"]
desc: An example workflow description
tasks:
  - id: start
    type: task
    uses: empty
  - id: end
    type: task
    uses: empty
    upstream: ["start"]
```

```python
from beacon import Schedule

schedule = Schedule(
    cron="0 0 * * *",
    timezone="UTC",
    dag="hello_world.yml",
)
schedule.run()
```
