# Beacon

An everyday workflow orchestrator that simple and easy to customize.

---

## Examples

This is the full example of a workflow that this beacon package support.

```yaml
id: hello-world
type: dag
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
