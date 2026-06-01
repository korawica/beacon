# Action

An action is the first core beacon interface.

=== "YAML Example"

    ```yaml
    tasks:
      - id: ???
        type: task
        upstream: ["action_id_1", "action_id_2"]
        uses: plugin_name
        inputs:
          param1: value1
          param2: "{{ params.param2 }}"
        callbacks:
          - on_event: failure
            hook: hook_name
          - on_event: start
            hook: hook_name
    ```

=== "Python Example"

    ```python
    from beacon.callback import OnTaskEvent
    from beacon import BasePlugin, Context

    class PluginName(BasePlugin):
        plugin_name = "plugin_name"

        def execute(self, context: Context):
            # NOTE: The plugin logic here
            pass

    task = Task(
        id="action_id",
        upstream=["action_id_1", "action_id_2"],
        uses="plugin_name",
        inputs={
            "param1": "value1",
            "param2": "{{ params.param2 }}"
        },
        callbacks=[
            OnTaskEvent(on_event="failure", hook="hook_name"),
            OnTaskEvent(on_event="start", hook="hook_name)]
        ],
        retries=2,
        retry_delay=10,
        execution_timeout=60,
    )
    ```

## Core Action Fields

These fields are common for all types of actions, including tasks, sensors, and
branches.

| Field       | Type                       | Description                                                                                                                                                                                                        |
|-------------|----------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| id          | str                        | Unique identifier for the action within the workflow.                                                                                                                                                              |
| type        | `task`, `sensor`, `branch` | Type of the action, such as "task", "sensor", or "branch".                                                                                                                                                         |
| uses        | str                        | The plugin name that this action uses. This should correspond to a plugin defined in the plugins directory.                                                                                                        |
| inputs      | dict                       | A dictionary of input parameters for the plugin. The values can be static or templated using Jinja syntax.                                                                                                         |
| upstream    | list                       | A list of action IDs that must be completed before this action can run. This defines the dependencies between actions.                                                                                             |
| callbacks   | dict                       | A dictionary of callback actions to be executed on specific events, such as "on_success", "on_failure", etc. The keys are event names and the values are lists of action IDs to be executed when the event occurs. |

---

## Standard Action

Type of Standard Actions:

- Task
- Sensor
- Branch

### Task

| Field               | Type   | Description                                                                                          |
|---------------------|--------|------------------------------------------------------------------------------------------------------|
| retries             | int    | Number of times to retry the task in case of failure.                                                |
| retry_delay         | int    | Delay in seconds between retries.                                                                    |
| execution_timeout   | int    | Maximum time in seconds that the task is allowed to run before it is terminated.                     |
| exponential_backoff | bool   | Whether to use exponential backoff for retry delays. If true, the delay will double with each retry. |

### Sensor

| Field                | Type              | Description                                                                                                 |
|----------------------|-------------------|-------------------------------------------------------------------------------------------------------------|
| mode                 | str               | A mode of the sensor.                                                                                       |
| check_interval       | int               | Time in seconds that the sensor waits between checks.                                                       |
| execution_timeout    | int               | Maximum time in seconds that the sensor will keep checking before it times out.                             |
| exponential_backoff  | bool              | Whether to use exponential backoff for check intervals. If true, the interval will double with each check.  |
| fail_mode            | `soft`, `slient`  | A mode of fail event if it handle from a plugin.                                                            |

### Branch

| Field   | Type  | Description                                                             |
|---------|-------|-------------------------------------------------------------------------|
| success | list  | A list of action IDs to be executed if the branch condition is met.     |
| failure | list  | A list of action IDs to be executed if the branch condition is not met. |
