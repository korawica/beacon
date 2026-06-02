# Beacon

An everyday async workflow orchestrator that simple and easy to customize via YAML
template.

- Simple with less component, Webserver, API server, Database, and Worker
- Scalable with concept write once and run reuse it.

---

## Examples

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
    id="hello-world-schedule",
    cron="10 0 * * *",
    timezone="UTC",
    catchup="{{ vars('catchup', 'false') }}",
    dag="hello_world.yml",
    variables="variables.yml",
)
schedule.run()
```

claude --resume 6f86578e-740d-4705-9e96-222a2f6a45ab
