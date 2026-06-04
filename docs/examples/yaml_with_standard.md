# Example: YAML with Standard

## Define Dag

```yaml title="dag.yml"
id: hello-world
owners: ["de"]
desc: An example workflow description
params:
  - name: source_system
    type: str
    default: example
callbacks:
  - on_event: success
    hook: "beacon.providers.msteam.msteam_adaptive_card"
    hook_args:
      url: "https://my-webhook-url.com"
  - on_event: failure
    hook: "beacon.providers.smtp.send_mail"
    hook_args:
      to: "on-call@email.com"
tasks:
  - id: start
    uses: empty
  - id: process
    upstream: [start]
    uses: py
    inputs:
      py_statement: ./example_py_statement.py
      py_function: main
      params:
        source_system: "{{ params.source_system }}"
    retries: 3
    execution_timeout: 3600
```

## Defining Python File

```python title="example_py_statement.py"
from google.storage import Client

def main(source_system: str):
    client = Client()
    # Your code here
```

## Run Dag

```bash
beacon run hello-world --param source_system=example
```

## Deploy Dag to Schedule

```yaml title="variables.yml"
type: variable
stages:
  dev:
    source_system: example_dev
  prod:
    source_system: example_prod
```

```bash
beacon deploy hello-world-workflow \
    --dag hello-world \
    --schedule "0 0 * * *" \
    --start-date "2024-01-01T00:00:00Z" \
    --end-date "2024-12-31T00:00:00Z" \
    --catchup false \
    --timezone "UTC" \
    --params source_system="{{ vars('source_system') }}" \
    --variables variables.yml
```
