"""Test dry-run validation."""

from beacon import Dag, Task, Param
from beacon.dryrun import dryrun
from beacon.models.branch import Branch

# Ensure plugins registered
from beacon.providers.standard.plugins import EmptyPlugin  # noqa: F401
from beacon.providers.standard.plugins.task.python import PythonPlugin  # noqa: F401
from beacon.providers.standard.plugins.branch.by_hours import ByHourBranchPlugin  # noqa: F401


class TestPluginExistence:
    def test_valid_plugin(self):
        dag = Dag(
            id="test",
            owners=["de"],
            actions=[
                Task(id="t1", uses="py"),
            ],
        )
        result = dryrun(dag)
        assert result.is_valid

    def test_missing_plugin(self):
        dag = Dag(
            id="test",
            owners=["de"],
            actions=[
                Task(id="t1", uses="nonexistent-plugin"),
            ],
        )
        result = dryrun(dag)
        assert not result.is_valid
        assert result.errors[0].category == "plugin"
        assert "not found" in result.errors[0].message


class TestPluginActionCompatibility:
    def test_branch_plugin_with_branch_action(self):
        """by_hours plugin used with branch action → valid."""
        dag = Dag(
            id="test",
            owners=["de"],
            actions=[
                Branch(
                    id="b1", uses="by_hours", success=["t1"], failure=["t2"]
                ),
                Task(id="t1", uses="empty", upstream=["b1"]),
                Task(id="t2", uses="empty", upstream=["b1"]),
            ],
        )
        result = dryrun(dag)
        assert result.is_valid

    def test_branch_plugin_with_task_action(self):
        """by_hours plugin used with task action → error."""
        dag = Dag(
            id="test",
            owners=["de"],
            actions=[
                Task(id="t1", uses="by_hours"),
            ],
        )
        result = dryrun(dag)
        assert not result.is_valid
        assert result.errors[0].category == "compatibility"
        assert "only compatible with" in result.errors[0].message

    def test_generic_plugin_with_any_action(self):
        """py plugin (no compatible_actions restriction) works with any action."""
        dag = Dag(
            id="test",
            owners=["de"],
            actions=[
                Task(id="t1", uses="py"),
            ],
        )
        result = dryrun(dag)
        assert result.is_valid


class TestGraphValidation:
    def test_missing_upstream(self):
        dag = Dag(
            id="test",
            owners=["de"],
            actions=[
                Task(id="t1", uses="empty", upstream=["nonexistent"]),
            ],
        )
        result = dryrun(dag)
        assert not result.is_valid
        assert result.errors[0].category == "graph"
        assert "does not exist" in result.errors[0].message

    def test_valid_graph(self):
        dag = Dag(
            id="test",
            owners=["de"],
            actions=[
                Task(id="start", uses="empty"),
                Task(id="process", uses="empty", upstream=["start"]),
                Task(id="end", uses="empty", upstream=["process"]),
            ],
        )
        result = dryrun(dag)
        assert result.is_valid
        assert result.task_order == ["start", "process", "end"]

    def test_cycle_detection(self):
        dag = Dag(
            id="test",
            owners=["de"],
            actions=[
                Task(id="a", uses="empty", upstream=["c"]),
                Task(id="b", uses="empty", upstream=["a"]),
                Task(id="c", uses="empty", upstream=["b"]),
            ],
        )
        result = dryrun(dag)
        assert not result.is_valid
        assert any(
            e.category == "graph" and "Cycle" in e.message
            for e in result.errors
        )


class TestTemplateRendering:
    def test_renders_params(self):
        dag = Dag(
            id="test",
            owners=["de"],
            params=[Param(name="source", type="str", default="default_src")],
            actions=[
                Task(
                    id="t1",
                    uses="py",
                    inputs={
                        "py_file": "./script.py",
                    },
                ),
            ],
        )
        result = dryrun(dag, params={"source": "orders"})
        assert result.is_valid
        t = result.resolved_tasks[0]
        assert t.inputs["py_file"] == "./script.py"

    def test_renders_jinja_params(self):
        dag = Dag(
            id="test",
            owners=["de"],
            params=[Param(name="source", type="str", default="x")],
            actions=[
                Task(
                    id="t1",
                    uses="empty",
                    inputs={
                        "value": "{{ params.source }}",
                    },
                ),
            ],
        )
        result = dryrun(dag, params={"source": "orders"})
        assert result.is_valid
        assert result.resolved_tasks[0].inputs["value"] == "orders"

    def test_renders_variables(self):
        dag = Dag(
            id="test",
            owners=["de"],
            actions=[
                Task(
                    id="t1",
                    uses="empty",
                    inputs={
                        "bucket": "{{ vars('bucket') }}",
                    },
                ),
            ],
        )
        result = dryrun(dag, variables={"bucket": "prod-bucket"})
        assert result.is_valid
        assert result.resolved_tasks[0].inputs["bucket"] == "prod-bucket"


class TestDryRunOutput:
    def test_print_format(self):
        dag = Dag(
            id="etl-pipeline",
            owners=["de"],
            actions=[
                Task(id="start", uses="empty"),
                Task(
                    id="process",
                    uses="py",
                    upstream=["start"],
                    inputs={
                        "py_file": "./process.py",
                    },
                ),
            ],
        )
        result = dryrun(dag)
        output = result.print()
        assert "etl-pipeline" in output
        assert "start → process" in output
        assert "PASS" in output
