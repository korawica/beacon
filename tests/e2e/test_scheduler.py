"""Tests for DagRunner — the heart of beacon's local lifecycle."""

import asyncio
from pathlib import Path
from typing import Any, ClassVar

import pytest

from beacon import (
    BasePlugin,
    Branch,
    Callback,
    Dag,
    DagRunner,
    OnDagEvent,
    ShortCircuit,
    Task,
)
from beacon.callback import CALLBACKS_REGISTRY
from beacon.core import TaskState
from beacon.core.plugin import PLUGINS_REGISTRY
from beacon.metadata import LocalMetadata


# ─── inline plugins for tests ────────────────────────────────────────────


class _RecordingPlugin(BasePlugin):
    """A plugin that records each invocation and returns deterministic output."""

    plugin_name: ClassVar[str] = "_rec"
    value: str = ""

    async def execute(self, context):
        _RECORD.append((context["task_id"], self.value))
        return {"value": self.value}


class _BranchDecider(BasePlugin):
    """Branch plugin that returns {"branch": [...]} based on chosen ids."""

    plugin_name: ClassVar[str] = "_branch_decider"
    compatible_actions: ClassVar[tuple[str, ...]] = ("branch",)
    chosen: list[str] = []

    async def execute(self, context):
        return {"branch": list(self.chosen)}


class _ShortCircuit(BasePlugin):
    plugin_name: ClassVar[str] = "_sc"
    compatible_actions: ClassVar[tuple[str, ...]] = ("short_circuit",)
    keep_going: bool = True

    async def execute(self, context):
        return {"continue": self.keep_going}


class _FailingPlugin(BasePlugin):
    plugin_name: ClassVar[str] = "_fail"

    async def execute(self, context):
        raise RuntimeError("boom")


class _FlakyPlugin(BasePlugin):
    """Fails on first call(s) then succeeds. Per-task-id counter."""

    plugin_name: ClassVar[str] = "_flaky"
    fail_attempts: int = 1

    async def execute(self, context):
        tid = context["task_id"]
        _FLAKY_COUNT[tid] = _FLAKY_COUNT.get(tid, 0) + 1
        if _FLAKY_COUNT[tid] <= self.fail_attempts:
            raise RuntimeError(f"flaky {_FLAKY_COUNT[tid]}")
        return {"attempts": _FLAKY_COUNT[tid]}


_RECORD: list[tuple[str, str]] = []
_FLAKY_COUNT: dict[str, int] = {}


@pytest.fixture(autouse=True)
def _reset():
    _RECORD.clear()
    _FLAKY_COUNT.clear()
    yield


# ─── helpers ─────────────────────────────────────────────────────────────


def _run(dag: Dag, tmp_path: Path, **kwargs) -> dict[str, Any]:
    meta = LocalMetadata(tmp_path / "meta")
    sched = DagRunner(dag, meta=meta)
    return asyncio.run(sched.run(**kwargs))


# ─── tests ───────────────────────────────────────────────────────────────


def test_linear_dag_success(tmp_path):
    dag = Dag(
        id="linear",
        actions=[
            Task(id="a", uses="_rec", inputs={"value": "A"}),
            Task(id="b", uses="_rec", upstream=["a"], inputs={"value": "B"}),
            Task(id="c", uses="_rec", upstream=["b"], inputs={"value": "C"}),
        ],
    )
    result = _run(dag, tmp_path)
    assert result.state == "success"
    assert result.states["a"] == TaskState.SUCCESS
    assert result.states["b"] == TaskState.SUCCESS
    assert result.states["c"] == TaskState.SUCCESS
    # Order respects topology
    order = [tid for tid, _ in _RECORD]
    assert order.index("a") < order.index("b") < order.index("c")


def test_fan_out_executes_in_parallel(tmp_path):
    dag = Dag(
        id="fanout",
        actions=[
            Task(id="root", uses="_rec", inputs={"value": "R"}),
            *[
                Task(
                    id=f"leaf{i}",
                    uses="_rec",
                    upstream=["root"],
                    inputs={"value": f"L{i}"},
                )
                for i in range(5)
            ],
        ],
    )
    result = _run(dag, tmp_path)
    assert result.state == "success"
    for i in range(5):
        assert result.states[f"leaf{i}"] == TaskState.SUCCESS


def test_failure_propagates_upstream_failed(tmp_path):
    dag = Dag(
        id="fail-cascade",
        actions=[
            Task(id="bad", uses="_fail"),
            Task(
                id="downstream",
                uses="_rec",
                upstream=["bad"],
                inputs={"value": "x"},
            ),
        ],
    )
    result = _run(dag, tmp_path)
    assert result.state == "failed"
    assert result.states["bad"] == TaskState.FAILED
    assert result.states["downstream"] == TaskState.UPSTREAM_FAILED


def test_trigger_rule_all_done_runs_after_failure(tmp_path):
    dag = Dag(
        id="all-done",
        actions=[
            Task(id="bad", uses="_fail"),
            Task(
                id="cleanup",
                uses="_rec",
                upstream=["bad"],
                inputs={"value": "cleanup"},
                trigger_rule="all_done",
            ),
        ],
    )
    result = _run(dag, tmp_path)
    # bad failed, but cleanup ran
    assert result.states["bad"] == TaskState.FAILED
    assert result.states["cleanup"] == TaskState.SUCCESS
    # DAG state is failed because bad failed (cleanup is normal task)
    assert result.state == "failed"


def test_branch_skips_unchosen_path(tmp_path):
    dag = Dag(
        id="branch-dag",
        actions=[
            Branch(
                id="decide",
                uses="_branch_decider",
                inputs={"chosen": ["take_a"]},
                success=["take_a"],
                failure=["take_b"],
            ),
            Task(
                id="take_a",
                uses="_rec",
                upstream=["decide"],
                inputs={"value": "A"},
            ),
            Task(
                id="take_b",
                uses="_rec",
                upstream=["decide"],
                inputs={"value": "B"},
            ),
        ],
    )
    result = _run(dag, tmp_path)
    assert result.state == "success"
    assert result.states["decide"] == TaskState.SUCCESS
    assert result.states["take_a"] == TaskState.SUCCESS
    assert result.states["take_b"] == TaskState.SKIPPED


def test_short_circuit_skips_downstream_transitively(tmp_path):
    dag = Dag(
        id="sc-dag",
        actions=[
            ShortCircuit(
                id="gate",
                uses="_sc",
                inputs={"keep_going": False},
            ),
            Task(
                id="middle",
                uses="_rec",
                upstream=["gate"],
                inputs={"value": "M"},
            ),
            Task(
                id="leaf",
                uses="_rec",
                upstream=["middle"],
                inputs={"value": "L"},
            ),
        ],
    )
    result = _run(dag, tmp_path)
    assert result.states["gate"] == TaskState.SUCCESS
    assert result.states["middle"] == TaskState.SKIPPED
    # Cascade: leaf's upstream middle is SKIPPED → ALL_SUCCESS not met
    assert result.states["leaf"] == TaskState.SKIPPED
    assert result.state == "success"


def test_short_circuit_passes_when_continue_true(tmp_path):
    dag = Dag(
        id="sc-ok",
        actions=[
            ShortCircuit(id="g", uses="_sc", inputs={"keep_going": True}),
            Task(id="next", uses="_rec", upstream=["g"], inputs={"value": "N"}),
        ],
    )
    result = _run(dag, tmp_path)
    assert result.states["g"] == TaskState.SUCCESS
    assert result.states["next"] == TaskState.SUCCESS


def test_retry_then_succeed(tmp_path):
    dag = Dag(
        id="retry",
        actions=[
            Task(
                id="t",
                uses="_flaky",
                retries=3,
                retry_delay=0,
                exponential_backoff=False,
                inputs={"fail_attempts": 2},
            ),
        ],
    )
    result = _run(dag, tmp_path)
    assert result.state == "success"
    assert result.states["t"] == TaskState.SUCCESS
    assert _FLAKY_COUNT["t"] == 3  # 1 fail + 1 fail + 1 success


def test_dag_callbacks_fire(tmp_path):
    fired: list[tuple[str, str]] = []

    class _Spy(Callback):
        hook_name: ClassVar[str] = "_dag_spy"

        async def notify(self, event, data):
            fired.append((event, data.get("state", "")))

    try:
        dag = Dag(
            id="cb-dag",
            actions=[Task(id="t", uses="_rec", inputs={"value": "x"})],
            callbacks=[
                OnDagEvent(on_event="start", hook="_dag_spy"),
                OnDagEvent(on_event="success", hook="_dag_spy"),
                OnDagEvent(on_event="failure", hook="_dag_spy"),
                OnDagEvent(on_event="finished", hook="_dag_spy"),
            ],
        )
        result = _run(dag, tmp_path)
        events = [e for e, _ in fired]
        assert events == ["start", "success", "finished"]
        assert result.state == "success"
    finally:
        CALLBACKS_REGISTRY.pop("_dag_spy", None)


def test_dag_failure_fires_failure_callback(tmp_path):
    fired: list[str] = []

    class _SpyF(Callback):
        hook_name: ClassVar[str] = "_dag_fail_spy"

        async def notify(self, event, data):
            fired.append(event)

    try:
        dag = Dag(
            id="cb-fail",
            actions=[Task(id="t", uses="_fail")],
            callbacks=[
                OnDagEvent(on_event="success", hook="_dag_fail_spy"),
                OnDagEvent(on_event="failure", hook="_dag_fail_spy"),
                OnDagEvent(on_event="finished", hook="_dag_fail_spy"),
            ],
        )
        result = _run(dag, tmp_path)
        assert result.state == "failed"
        assert "failure" in fired
        assert "finished" in fired
        assert "success" not in fired
    finally:
        CALLBACKS_REGISTRY.pop("_dag_fail_spy", None)


def test_teardown_runs_after_normal_task_failure(tmp_path):
    """Teardown must run even when its setup's dependent failed."""
    dag = Dag(
        id="td",
        actions=[
            Task(id="create_cluster", uses="_rec", inputs={"value": "create"}),
            Task(
                id="etl",
                uses="_fail",
                upstream=["create_cluster"],
            ),
            Task(
                id="destroy_cluster",
                uses="_rec",
                teardown="create_cluster",
                inputs={"value": "destroy"},
            ),
        ],
    )
    result = _run(dag, tmp_path)
    assert result.states["create_cluster"] == TaskState.SUCCESS
    assert result.states["etl"] == TaskState.FAILED
    # Teardown ran despite failure
    assert result.states["destroy_cluster"] == TaskState.SUCCESS
    # DAG state derives from non-teardown tasks → FAILED
    assert result.state == "failed"


def test_teardown_can_read_setup_outputs(tmp_path):
    """Teardown should see the setup task's outputs in upstream_outputs."""

    class _CaptureUpstream(BasePlugin):
        plugin_name: ClassVar[str] = "_capture_upstream"

        async def execute(self, context):
            _RECORD.append(
                ("captured", str(context.get("upstream_outputs", {})))
            )
            return {}

    try:
        dag = Dag(
            id="td-out",
            actions=[
                Task(id="setup", uses="_rec", inputs={"value": "ENDPOINT"}),
                Task(
                    id="tear",
                    uses="_capture_upstream",
                    teardown="setup",
                ),
            ],
        )
        _run(dag, tmp_path)
        captures = [r for r in _RECORD if r[0] == "captured"]
        assert captures, "teardown did not run"
        assert "ENDPOINT" in captures[-1][1]
    finally:
        PLUGINS_REGISTRY.pop("_capture_upstream", None)


def test_teardown_failure_does_not_fail_dag(tmp_path):
    """If only teardown fails, DAG state stays SUCCESS."""
    dag = Dag(
        id="td-fail",
        actions=[
            Task(id="setup", uses="_rec", inputs={"value": "s"}),
            Task(
                id="work",
                uses="_rec",
                upstream=["setup"],
                inputs={"value": "w"},
            ),
            Task(id="cleanup", uses="_fail", teardown="setup"),
        ],
    )
    result = _run(dag, tmp_path)
    assert result.states["setup"] == TaskState.SUCCESS
    assert result.states["work"] == TaskState.SUCCESS
    assert result.states["cleanup"] == TaskState.FAILED
    assert result.state == "success"  # Teardown failures ignored


def test_dag_run_method_returns_dict(tmp_path):
    """Dag.run() public API returns a flat dict."""
    dag = Dag(
        id="api",
        actions=[Task(id="t", uses="_rec", inputs={"value": "x"})],
    )
    result = dag.run(metadata_path=str(tmp_path))
    assert result["state"] == "success"
    assert result["states"]["t"] == TaskState.SUCCESS
    assert result["outputs"]["t"] == {"value": "x"}


def test_dag_test_method(tmp_path):
    dag = Dag(
        id="api-test",
        actions=[
            Task(id="t1", uses="_rec", inputs={"value": "a"}),
            Task(id="t2", uses="_fail"),
        ],
    )
    result = dag.test()
    assert result["passed"] is False
    assert result["tasks"]["t1"]["passed"] is True
    assert result["tasks"]["t2"]["passed"] is False


def test_upstream_outputs_flow_to_downstream(tmp_path):
    """Downstream task should see upstream's outputs in upstream_outputs."""

    captured: dict[str, Any] = {}

    class _Consumer(BasePlugin):
        plugin_name: ClassVar[str] = "_consumer"

        async def execute(self, context):
            captured.update(context.get("upstream_outputs", {}))
            return {}

    try:
        dag = Dag(
            id="upout",
            actions=[
                Task(id="prod", uses="_rec", inputs={"value": "PAYLOAD"}),
                Task(id="cons", uses="_consumer", upstream=["prod"]),
            ],
        )
        _run(dag, tmp_path)
        assert captured.get("prod") == {"value": "PAYLOAD"}
    finally:
        PLUGINS_REGISTRY.pop("_consumer", None)


def test_dag_run_state_persisted_in_metadata(tmp_path):
    """DagRun state in metadata reflects scheduler outcome."""
    meta = LocalMetadata(tmp_path / "meta")
    dag = Dag(
        id="persist",
        actions=[Task(id="t", uses="_rec", inputs={"value": "x"})],
    )
    sched = DagRunner(dag, meta=meta)
    result = asyncio.run(sched.run(run_id="run-persist-1"))

    async def _check():
        dr = await meta.get_dag_run("run-persist-1", "persist")
        assert dr["state"] == "success"

    asyncio.run(_check())
    assert result.state == "success"


def test_empty_dag_succeeds(tmp_path):
    """A DAG with no actions trivially succeeds."""
    dag = Dag(id="empty", actions=[])
    result = _run(dag, tmp_path)
    assert result.state == "success"
    assert result.states == {}


def test_single_root_task(tmp_path):
    dag = Dag(
        id="single",
        actions=[Task(id="only", uses="_rec", inputs={"value": "x"})],
    )
    result = _run(dag, tmp_path)
    assert result.state == "success"
    assert result.states["only"] == TaskState.SUCCESS


def test_grouped_actions_are_flattened(tmp_path):
    """Tasks nested in a Group should be discovered by the scheduler."""
    from beacon import Group

    dag = Dag(
        id="grouped",
        actions=[
            Task(id="root", uses="_rec", inputs={"value": "R"}),
            Group(
                id="g1",
                actions=[
                    Task(
                        id="g_a",
                        uses="_rec",
                        upstream=["root"],
                        inputs={"value": "A"},
                    ),
                    Task(
                        id="g_b",
                        uses="_rec",
                        upstream=["g_a"],
                        inputs={"value": "B"},
                    ),
                ],
            ),
        ],
    )
    result = _run(dag, tmp_path)
    assert result.state == "success"
    assert result.states["root"] == TaskState.SUCCESS
    assert result.states["g_a"] == TaskState.SUCCESS
    assert result.states["g_b"] == TaskState.SUCCESS
