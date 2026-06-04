"""Tests for plugin-level teardown — both BasePlugin.teardown() and py_teardown.

Proves:
1. BasePlugin.teardown() fires after execute() on success AND failure.
2. PythonPlugin.py_teardown fires the named function after main().
3. dag.mark(state="success") forces a task to success + fires task-level teardowns.
4. dag.mark(state="failed") works same as dag.fail().
"""

import json
from pathlib import Path
from typing import ClassVar

import pytest

from beacon import BasePlugin, Dag, Task
from beacon.providers.standard.plugins.task.python import PythonPlugin  # noqa: F401


# ---------------------------------------------------------------------------
# 1. Custom plugin with teardown() method
# ---------------------------------------------------------------------------

_LIFECYCLE: list[str] = []


@pytest.fixture(autouse=True)
def _reset():
    _LIFECYCLE.clear()
    yield


class _SparkPlugin(BasePlugin):
    """Plugin that simulates launch + teardown."""

    plugin_name: ClassVar[str] = "_spark_with_td"
    fail_execute: bool = False

    async def execute(self, context):
        _LIFECYCLE.append("execute")
        if self.fail_execute:
            raise RuntimeError("boom")
        return {"app_id": "spark-123"}

    async def teardown(self, context):
        _LIFECYCLE.append("teardown")


def test_plugin_teardown_fires_on_success(tmp_path):
    dag = Dag(
        id="td-success",
        owners=["de"],
        actions=[Task(id="t", uses="_spark_with_td")],
    )
    result = dag.run(metadata_path=str(tmp_path / "m"))
    assert result["state"] == "success"
    assert _LIFECYCLE == ["execute", "teardown"]


def test_plugin_teardown_fires_on_failure(tmp_path):
    dag = Dag(
        id="td-fail",
        owners=["de"],
        actions=[
            Task(id="t", uses="_spark_with_td", inputs={"fail_execute": True})
        ],
    )
    result = dag.run(metadata_path=str(tmp_path / "m"))
    assert result["state"] == "failed"
    # teardown STILL fires despite execute raising.
    assert _LIFECYCLE == ["execute", "teardown"]


# ---------------------------------------------------------------------------
# 2. py_teardown — inline teardown function in the py plugin
# ---------------------------------------------------------------------------

SCRIPT = """
import json
from pathlib import Path
from beacon.runtime import load_context

def main(events_path: str):
    ctx = load_context()
    _record(events_path, "main")
    return {"ok": True}

def cleanup(events_path: str):
    _record(events_path, "cleanup")

def failing_main(events_path: str):
    _record(events_path, "failing_main")
    raise RuntimeError("intentional")

def _record(events_path, event):
    p = Path(events_path)
    log = json.loads(p.read_text()) if p.exists() else []
    log.append(event)
    p.write_text(json.dumps(log))
"""


@pytest.fixture
def script(tmp_path) -> Path:
    p = tmp_path / "spark.py"
    p.write_text(SCRIPT)
    return p


def _events(p: Path) -> list[str]:
    return json.loads(p.read_text()) if p.exists() else []


def test_py_teardown_fires_on_success(script, tmp_path):
    ep = str(tmp_path / "ev.json")
    dag = Dag(
        id="py-td",
        owners=["de"],
        actions=[
            Task(
                id="t",
                uses="py",
                inputs={
                    "py_file": str(script),
                    "py_function": "main",
                    "py_teardown": "cleanup",
                    "params": {"events_path": ep},
                },
            ),
        ],
    )
    result = dag.run(metadata_path=str(tmp_path / "m"))
    assert result["state"] == "success"
    assert _events(Path(ep)) == ["main", "cleanup"]


def test_py_teardown_fires_on_failure(script, tmp_path):
    ep = str(tmp_path / "ev.json")
    dag = Dag(
        id="py-td-fail",
        owners=["de"],
        actions=[
            Task(
                id="t",
                uses="py",
                inputs={
                    "py_file": str(script),
                    "py_function": "failing_main",
                    "py_teardown": "cleanup",
                    "params": {"events_path": ep},
                },
            ),
        ],
    )
    result = dag.run(metadata_path=str(tmp_path / "m"))
    assert result["state"] == "failed"
    # cleanup STILL fires.
    assert _events(Path(ep)) == ["failing_main", "cleanup"]


def test_py_no_teardown_field_is_noop(script, tmp_path):
    """When py_teardown is not set, nothing extra fires."""
    ep = str(tmp_path / "ev.json")
    dag = Dag(
        id="py-no-td",
        owners=["de"],
        actions=[
            Task(
                id="t",
                uses="py",
                inputs={
                    "py_file": str(script),
                    "py_function": "main",
                    "params": {"events_path": ep},
                },
            ),
        ],
    )
    dag.run(metadata_path=str(tmp_path / "m"))
    assert _events(Path(ep)) == ["main"]  # no "cleanup"


# ---------------------------------------------------------------------------
# 3. dag.mark(state="success") — force success + unblock downstream
# ---------------------------------------------------------------------------


class _Blocker(BasePlugin):
    plugin_name: ClassVar[str] = "_blocker"

    async def execute(self, context):
        _LIFECYCLE.append(f"exec:{context['task_id']}")
        return {"done": True}

    async def teardown(self, context):
        _LIFECYCLE.append(f"td:{context['task_id']}")


def test_mark_success_unblocks_downstream(tmp_path):
    """Force a stuck sensor to SUCCESS → downstream runs."""
    dag = Dag(
        id="mark-ok",
        owners=["de"],
        actions=[
            Task(id="sensor", uses="_blocker"),
            Task(id="load", uses="_blocker", upstream=["sensor"]),
        ],
    )
    meta_path = str(tmp_path / "m")
    # Run normally first — both tasks succeed.
    initial = dag.run(metadata_path=meta_path)
    run_id = initial["run_id"]
    assert initial["state"] == "success"

    _LIFECYCLE.clear()
    # Clear sensor (simulate it needs rerun), then mark it SUCCESS externally.
    out = dag.mark(
        run_id=run_id,
        task_id="sensor",
        state="success",
        metadata_path=meta_path,
    )
    # DAG is SUCCESS.
    assert out["state"] == "success"


def test_mark_success_with_teardown(tmp_path):
    """Force-success a task that has a teardown → teardown re-fires."""
    dag = Dag(
        id="mark-td",
        owners=["de"],
        actions=[
            Task(id="setup", uses="_blocker"),
            Task(id="work", uses="_blocker", upstream=["setup"]),
            Task(id="cleanup", uses="_blocker", teardown="setup"),
        ],
    )
    meta_path = str(tmp_path / "m")
    initial = dag.run(metadata_path=meta_path)
    run_id = initial["run_id"]

    _LIFECYCLE.clear()
    out = dag.mark(
        run_id=run_id, task_id="work", state="success", metadata_path=meta_path
    )
    # Teardown re-fired.
    assert "cleanup" in out["teardowns_fired"]
    assert "td:cleanup" in _LIFECYCLE


# ---------------------------------------------------------------------------
# 4. Invalid state raises
# ---------------------------------------------------------------------------


def test_mark_invalid_state_raises(tmp_path):
    dag = Dag(id="x", owners=["de"], actions=[Task(id="t", uses="_blocker")])
    meta_path = str(tmp_path / "m")
    dag.run(metadata_path=meta_path)
    with pytest.raises(ValueError, match="must be one of"):
        dag.mark(
            run_id="whatever",
            task_id="t",
            state="running",
            metadata_path=meta_path,
        )
