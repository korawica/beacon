"""Tests for beacon.plan — DAG plan-time validation and template rendering."""

from datetime import datetime

from beacon import Dag, Task
from beacon.plan import plan, PlanResult, PlannedTask
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
        result = plan(dag)
        assert result.is_valid

    def test_missing_plugin(self):
        dag = Dag(
            id="test",
            owners=["de"],
            actions=[
                Task(id="t1", uses="nonexistent-plugin"),
            ],
        )
        result = plan(dag)
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
        result = plan(dag)
        assert result.is_valid

    def test_branch_plugin_with_task_action(self):
        """Any plugin can be used with any action type — no compatibility errors."""
        dag = Dag(
            id="test",
            owners=["de"],
            actions=[
                Task(id="t1", uses="by_hours"),
            ],
        )
        result = plan(dag)
        assert result.is_valid

    def test_generic_plugin_with_any_action(self):
        """py plugin works with any action type."""
        dag = Dag(
            id="test",
            owners=["de"],
            actions=[
                Task(id="t1", uses="py"),
            ],
        )
        result = plan(dag)
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
        result = plan(dag)
        assert not result.is_valid
        assert result.errors[0].category == "graph"
        assert "does not exist" in result.errors[0].message

    def test_teardown_references_nonexistent_task(self):
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
        result = plan(dag)
        assert not result.is_valid
        assert any(
            e.category == "graph" and "nonexistent" in e.message
            for e in result.errors
        )

    def test_teardown_self_reference(self):
        dag = Dag(
            id="test",
            owners=["de"],
            actions=[
                Task(id="task1", uses="empty", teardown="task1"),
            ],
        )
        result = plan(dag)
        assert not result.is_valid
        assert any(
            "cannot be a teardown for itself" in e.message
            for e in result.errors
        )

    def test_valid_teardown_reference(self):
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
        result = plan(dag)
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
        result = plan(dag)
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
        result = plan(dag)
        assert not result.is_valid
        assert any(
            e.category == "graph" and "Cycle" in e.message
            for e in result.errors
        )


class TestTemplateRendering:
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
        result = plan(dag, variables={"bucket": "prod-bucket"})
        assert result.is_valid
        assert result.planned_tasks[0].inputs["bucket"] == "prod-bucket"

    def test_renders_data_interval_from_cron(self):
        """cron computes data_interval_start/end from logical_date."""
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
        logical = datetime(2026, 6, 3, 2, 0, 0)
        result = plan(dag, logical_date=logical, cron="0 2 * * *")
        assert result.is_valid
        t = result.planned_tasks[0]
        assert t.inputs["start"] == datetime(2026, 6, 3, 2, 0, 0)
        assert t.inputs["end"] == datetime(2026, 6, 4, 2, 0, 0)

    def test_renders_data_interval_without_cron(self):
        """Without cron or explicit intervals, both default to logical_date."""
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
        logical = datetime(2026, 6, 3, 2, 0, 0)
        result = plan(dag, logical_date=logical)
        assert result.is_valid
        t = result.planned_tasks[0]
        assert t.inputs["start"] == t.inputs["end"]

    def test_explicit_data_interval_overrides_cron(self):
        """Explicit data_interval_start/end take priority over cron."""
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
        logical = datetime(2026, 6, 3, 2, 0, 0)
        explicit_start = datetime(2026, 6, 1, 0, 0, 0)
        explicit_end = datetime(2026, 6, 7, 0, 0, 0)
        result = plan(
            dag,
            logical_date=logical,
            cron="0 2 * * *",  # should be ignored when explicit args given
            data_interval_start=explicit_start,
            data_interval_end=explicit_end,
        )
        assert result.is_valid
        t = result.planned_tasks[0]
        assert t.inputs["start"] == explicit_start
        assert t.inputs["end"] == explicit_end


class TestPlanOutput:
    def test_str_format(self):
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
        result = plan(dag)
        output = str(result)
        assert "etl-pipeline" in output
        assert "start → process" in output
        assert "PASS" in output

    def test_result_types(self):
        """PlanResult, PlanIssue, PlannedTask are the canonical names."""
        dag = Dag(
            id="test", owners=["de"], actions=[Task(id="t1", uses="empty")]
        )
        result = plan(dag)
        assert isinstance(result, PlanResult)
        assert isinstance(result.planned_tasks[0], PlannedTask)
