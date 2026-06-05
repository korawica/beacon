"""Unit tests for action type extract_outputs() and evaluate_downstream() behavior."""

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


class TestBranchExtractOutputs:
    """Tests for Branch.extract_outputs — the new normalization layer."""

    def _branch(self):
        return Branch(id="b1", uses="py", success=["good"], failure=["bad"])

    def test_none_return_defaults_to_success(self):
        # Plugin returned None → executor stores {} → success path
        assert self._branch().extract_outputs({}) == {"branch": ["good"]}

    def test_true_return_takes_success_path(self):
        assert self._branch().extract_outputs({"_result": True}) == {
            "branch": ["good"]
        }

    def test_false_return_takes_failure_path(self):
        assert self._branch().extract_outputs({"_result": False}) == {
            "branch": ["bad"]
        }

    def test_list_return_used_directly(self):
        assert self._branch().extract_outputs({"_result": ["good", "bad"]}) == {
            "branch": ["good", "bad"]
        }

    def test_str_return_wrapped_in_list(self):
        assert self._branch().extract_outputs({"_result": "good"}) == {
            "branch": ["good"]
        }

    def test_explicit_branch_dict_passed_through(self):
        assert self._branch().extract_outputs({"branch": ["good"]}) == {
            "branch": ["good"]
        }


class TestBranchEvaluateDownstream:
    """Tests for Branch.evaluate_downstream — operates on already-normalised outputs."""

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

    def test_full_flow_none_return_goes_to_success(self):
        """Simulates the runner calling extract_outputs then evaluate_downstream."""
        branch = Branch(id="b1", uses="py", success=["good"], failure=["bad"])
        ctx = _ctx({})  # None return from plugin → {} stored by executor
        ctx.outputs = branch.extract_outputs(ctx.outputs)
        d = branch.evaluate_downstream(ctx, ["good", "bad"])
        assert d.schedule == ["good"]
        assert d.skip == ["bad"]

    def test_full_flow_false_return_goes_to_failure(self):
        branch = Branch(id="b1", uses="py", success=["good"], failure=["bad"])
        ctx = _ctx({"_result": False})
        ctx.outputs = branch.extract_outputs(ctx.outputs)
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


class TestShortCircuitExtractOutputs:
    """Tests for ShortCircuit.extract_outputs."""

    def _sc(self):
        return ShortCircuit(id="sc1", uses="py")

    def test_none_return_defaults_to_continue(self):
        assert self._sc().extract_outputs({}) == {"continue": True}

    def test_false_return_stops(self):
        assert self._sc().extract_outputs({"_result": False}) == {
            "continue": False
        }

    def test_true_return_continues(self):
        assert self._sc().extract_outputs({"_result": True}) == {
            "continue": True
        }

    def test_explicit_continue_dict_passed_through(self):
        assert self._sc().extract_outputs({"continue": False}) == {
            "continue": False
        }
