"""Tests for the lean Renderer and scheduler-time template binding."""

import asyncio
from typing import Any, ClassVar

import pytest

from beacon import BasePlugin, Dag, DagRunner, Task
from beacon.core import Renderer
from beacon.metadata import JsonMetadata


# ─── Renderer unit tests ─────────────────────────────────────────────────


def test_renderer_passes_through_non_strings():
    r = Renderer({"x": 1})
    assert r.render(42) == 42
    assert r.render(None) is None
    assert r.render(True) is True


def test_renderer_passes_through_plain_strings():
    r = Renderer({})
    assert r.render("hello world") == "hello world"


def test_renderer_resolves_simple_var():
    assert Renderer({"x": "hi"}).render("{{ x }}") == "hi"


def test_renderer_recurses_dict():
    out = Renderer({"x": "X"}).render({"a": "{{ x }}", "b": [1, "{{ x }}"]})
    assert out == {"a": "X", "b": [1, "X"]}


def test_renderer_recurses_list_and_tuple():
    assert Renderer({"a": "A"}).render(["{{ a }}", "b"]) == ["A", "b"]
    assert Renderer({"a": "A"}).render(("{{ a }}", 2)) == ("A", 2)


def test_renderer_vars_macro():
    ctx = {"vars": {"bucket": "prod-bucket"}.get}
    assert Renderer(ctx).render("{{ vars('bucket') }}") == "prod-bucket"


def test_renderer_undefined_raises_strict():
    """StrictUndefined should raise on unknown names."""
    from jinja2 import UndefinedError

    with pytest.raises(UndefinedError):
        Renderer({}).render("{{ missing }}")


def test_renderer_sandbox_blocks_attribute_attacks():
    """SandboxedEnvironment should forbid access to dangerous attributes."""
    from jinja2.exceptions import SecurityError

    with pytest.raises(SecurityError):
        Renderer({"x": 1}).render("{{ x.__class__.__mro__ }}")


# ─── Scheduler-level rendering ───────────────────────────────────────────


_CAPTURED: dict[str, Any] = {}


class _Capture(BasePlugin):
    plugin_name: ClassVar[str] = "_render_capture"
    value: str = ""

    async def execute(self, context):
        _CAPTURED[context["task_id"]] = self.value
        return {"value": self.value}


@pytest.fixture(autouse=True)
def _reset():
    _CAPTURED.clear()
    yield


def test_scheduler_renders_params_at_enqueue(tmp_path):
    dag = Dag(
        id="render-params",
        actions=[
            Task(
                id="t",
                uses="_render_capture",
                inputs={"value": "src={{ params.src }}"},
            ),
        ],
    )
    meta = JsonMetadata(tmp_path / "meta")
    sched = DagRunner(dag, meta=meta)
    asyncio.run(sched.run(params={"src": "postgres"}))
    assert _CAPTURED["t"] == "src=postgres"


def test_scheduler_renders_vars_at_enqueue(tmp_path):
    dag = Dag(
        id="render-vars",
        actions=[
            Task(
                id="t",
                uses="_render_capture",
                inputs={"value": "{{ vars('bucket') }}"},
            ),
        ],
    )
    meta = JsonMetadata(tmp_path / "meta")
    sched = DagRunner(dag, meta=meta, variables={"bucket": "my-bucket"})
    asyncio.run(sched.run())
    assert _CAPTURED["t"] == "my-bucket"


def test_scheduler_renders_runtime(tmp_path):
    dag = Dag(
        id="render-runtime",
        actions=[
            Task(
                id="t",
                uses="_render_capture",
                inputs={"value": "{{ runtime.dag_id }}/{{ runtime.task_id }}"},
            ),
        ],
    )
    sched = DagRunner(dag, meta=JsonMetadata(tmp_path / "meta"))
    asyncio.run(sched.run())
    assert _CAPTURED["t"] == "render-runtime/t"


def test_worker_late_binds_upstream_outputs(tmp_path):
    """`{{ outputs.X.Y }}` should resolve at upstream-resolution time."""
    dag = Dag(
        id="late-bind",
        actions=[
            Task(
                id="prod",
                uses="_render_capture",
                inputs={"value": "PAYLOAD"},
            ),
            Task(
                id="cons",
                uses="_render_capture",
                upstream=["prod"],
                inputs={"value": "got={{ outputs.prod.value }}"},
            ),
        ],
    )
    sched = DagRunner(dag, meta=JsonMetadata(tmp_path / "meta"))
    asyncio.run(sched.run())
    assert _CAPTURED["cons"] == "got=PAYLOAD"


def test_dag_run_accepts_variables(tmp_path):
    dag = Dag(
        id="api-vars",
        actions=[
            Task(
                id="t",
                uses="_render_capture",
                inputs={"value": "{{ vars('env') }}"},
            ),
        ],
    )
    result = dag.run(variables={"env": "prod"}, metadata_path=str(tmp_path))
    assert result["state"] == "success"
    assert _CAPTURED["t"] == "prod"
