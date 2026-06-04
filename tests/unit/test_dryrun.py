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

    def test_teardown_references_nonexistent_task(self):
        """Teardown referencing a task_id that doesn't exist in DAG → error."""
        dag = Dag(
            id="test",
            owners=["de"],
            actions=[
                Task(id="run-etl", uses="empty"),
                Task(
                    id="destroy-cluster", uses="empty", teardown="nonexistent"
                ),
            ],
        )
        result = dryrun(dag)
        assert not result.is_valid
        assert any(
            e.category == "graph" and "nonexistent" in e.message
            for e in result.errors
        )

    def test_teardown_self_reference(self):
        """A task cannot be a teardown for itself."""
        dag = Dag(
            id="test",
            owners=["de"],
            actions=[
                Task(id="task1", uses="empty", teardown="task1"),
            ],
        )
        result = dryrun(dag)
        assert not result.is_valid
        assert any(
            "cannot be a teardown for itself" in e.message
            for e in result.errors
        )

    def test_valid_teardown_reference(self):
        """Teardown referencing an existing task_id → valid."""
        dag = Dag(
            id="test",
            owners=["de"],
            actions=[
                Task(id="create-cluster", uses="empty"),
                Task(id="run-etl", uses="empty", upstream=["create-cluster"]),
                Task(
                    id="destroy-cluster",
                    uses="empty",
                    teardown="create-cluster",
                ),
            ],
        )
        result = dryrun(dag)
        assert result.is_valid

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
                        "py_statement": "./script.py",
                    },
                ),
            ],
        )
        result = dryrun(dag, params={"source": "orders"})
        assert result.is_valid
        t = result.resolved_tasks[0]
        assert t.inputs["py_statement"] == "./script.py"

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

    def test_renders_data_interval_from_cron(self):
        """When cron is provided, data_interval_start/end are computed and
        available in runtime context for template rendering."""
        dag = Dag(
            id="test",
            owners=["de"],
            actions=[
                Task(
                    id="t1",
                    uses="empty",
                    inputs={
                        "start": "{{ runtime.data_interval_start }}",
                        "end": "{{ runtime.data_interval_end }}",
                    },
                ),
            ],
        )
        from datetime import datetime

        logical = datetime(2026, 6, 3, 2, 0, 0)
        result = dryrun(dag, logical_date=logical, cron="0 2 * * *")
        assert result.is_valid
        t = result.resolved_tasks[0]
        # With daily cron "0 2 * * *" and logical_date=2026-06-03 02:00,
        # data_interval_start = logical_date, data_interval_end = next day 02:00.
        # Renderer is native-typed: datetime flows through as datetime.
        assert t.inputs["start"] == datetime(2026, 6, 3, 2, 0, 0)
        assert t.inputs["end"] == datetime(2026, 6, 4, 2, 0, 0)

    def test_renders_data_interval_without_cron(self):
        """Without cron, data_interval_start/end both equal logical_date."""
        dag = Dag(
            id="test",
            owners=["de"],
            actions=[
                Task(
                    id="t1",
                    uses="empty",
                    inputs={
                        "start": "{{ runtime.data_interval_start }}",
                        "end": "{{ runtime.data_interval_end }}",
                    },
                ),
            ],
        )
        from datetime import datetime

        logical = datetime(2026, 6, 3, 2, 0, 0)
        result = dryrun(dag, logical_date=logical)
        assert result.is_valid
        t = result.resolved_tasks[0]
        # Both should be the same date when no cron
        assert t.inputs["start"] == t.inputs["end"]


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
                        "py_statement": "./process.py",
                    },
                ),
            ],
        )
        result = dryrun(dag)
        output = result.print()
        assert "etl-pipeline" in output
        assert "start → process" in output
        assert "PASS" in output
