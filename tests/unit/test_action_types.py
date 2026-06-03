"""Unit tests for action type evaluate_downstream() behavior."""

from datetime import datetime

from beacon.core import TaskContext
from beacon.models.branch import Branch
from beacon.models.short_circuit import ShortCircuit
from beacon.models.task import Task


def _ctx(outputs: dict) -> TaskContext:
    """Minimal TaskContext with given outputs."""
    return TaskContext(
        run_id="r1",
        dag_id="d1",
        task_id="t1",
        dag_version="v1",
        run_date=datetime(2026, 6, 3),
        logical_date=datetime(2026, 6, 3),
        data_interval_start=datetime(2026, 6, 3),
        data_interval_end=datetime(2026, 6, 4),
        inputs={},
        plugin_name="py",
        outputs=outputs,
    )


class TestTaskEvaluateDownstream:
    def test_schedules_all_downstream(self):
        task = Task(id="t1", uses="py")
        ctx = _ctx({"rows": 100})
        d = task.evaluate_downstream(ctx, ["t2", "t3"])
        assert d.schedule == ["t2", "t3"]
        assert d.skip == []


class TestBranchEvaluateDownstream:
    def test_explicit_branch_choice(self):
        branch = Branch(
            id="b1",
            uses="py",
            success=["good"],
            failure=["bad"],
        )
        ctx = _ctx({"branch": ["good"]})
        d = branch.evaluate_downstream(ctx, ["good", "bad"])
        assert d.schedule == ["good"]
        assert d.skip == ["bad"]

    def test_explicit_branch_multiple(self):
        branch = Branch(
            id="b1",
            uses="py",
            success=["a", "b"],
            failure=["c"],
        )
        ctx = _ctx({"branch": ["a", "c"]})
        d = branch.evaluate_downstream(ctx, ["a", "b", "c"])
        assert set(d.schedule) == {"a", "c"}
        assert d.skip == ["b"]

    def test_fallback_truthy_outputs(self):
        branch = Branch(
            id="b1",
            uses="py",
            success=["good"],
            failure=["bad"],
        )
        ctx = _ctx({"some_data": True})  # truthy but no "branch" key
        d = branch.evaluate_downstream(ctx, ["good", "bad"])
        assert d.schedule == ["good"]
        assert d.skip == ["bad"]

    def test_fallback_empty_outputs(self):
        branch = Branch(
            id="b1",
            uses="py",
            success=["good"],
            failure=["bad"],
        )
        ctx = _ctx({})  # empty = falsy
        d = branch.evaluate_downstream(ctx, ["good", "bad"])
        assert d.schedule == ["bad"]
        assert d.skip == ["good"]


class TestShortCircuitEvaluateDownstream:
    def test_continue_true_schedules_all(self):
        sc = ShortCircuit(id="sc1", uses="py")
        ctx = _ctx({"continue": True})
        d = sc.evaluate_downstream(ctx, ["t2", "t3", "t4"])
        assert d.schedule == ["t2", "t3", "t4"]
        assert d.skip == []

    def test_continue_false_skips_all(self):
        sc = ShortCircuit(id="sc1", uses="py")
        ctx = _ctx({"continue": False})
        d = sc.evaluate_downstream(ctx, ["t2", "t3", "t4"])
        assert d.schedule == []
        assert d.skip == ["t2", "t3", "t4"]

    def test_default_continues_if_no_key(self):
        sc = ShortCircuit(id="sc1", uses="py")
        ctx = _ctx({"some_output": 42})  # no "continue" key → defaults True
        d = sc.evaluate_downstream(ctx, ["t2"])
        assert d.schedule == ["t2"]
        assert d.skip == []
