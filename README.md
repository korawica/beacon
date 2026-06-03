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

## :speech_balloon: Contribute

I do not think this project will go around the world because it has specific propose
and you can create by your coding without this project dependency for long term
solution. So, on this time, you can open [the GitHub issue on this project :raised_hands:](https://github.com/korawica/beacon/issues)
for fix bug or request new feature if you want it.
