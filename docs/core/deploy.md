# Deploy Flow

## Repo

The project file structure should be organized as follows:

```text
|- plugins/
|    gcs/
|      sensor.py               <-- Global plugin directory.
|- workflows/
     domain/
       propose/
         {workflow-id}/
           assets/             <-- Local plugin directory, only for this workflow.
             plugin.py
           workflow.yml
           variables.yml
       global_variables.yml    <-- Global variables, accessible by all workflows.
     global_variables.yml      <-- Global variables, accessible by all workflows.
```

## File Lists

### Workflow File

```yaml
id: workflow-id
type: workflow
desc: A description of the workflow's purpose and behavior.
params:
  - name: source_system
    type: str
    default: example
tasks:
  - id: task-id
    uses: plugin_name
    inputs:
      source_system: "{{ params.source_system }}"
      bucket: "{{ vars('bucket') }}"
      prefix: "{{ runtime.data_interval_start | utc | fmt('year=%Y/month=%m/day=%d/hour=%H') }}"
```

### Variables File

```yaml
type: variable
stages:
  dev:
    bucket: bucket-dev
  prod:
    bucket: bucket-prod
```

### Plugin Code

```python title="plugin.py"
from typing import ClassVar
from beacon import BasePlugin, Context


class PluginName(BasePlugin):
    plugin_name: ClassVar[str] = "plugin_name"

    source_system: str
    bucket: str
    prefix: str

    def execute(self, context: Context):
        # plugin logic here
        pass
```

## Execution
