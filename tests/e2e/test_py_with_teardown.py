"""End-to-end: setup / main / teardown with the real `py` plugin.

Proves the user-facing pattern:
    one .py file, three functions (launch / process / stop),
    three tasks (launch / process / stop with `teardown: launch`).

The cleanup task ALWAYS runs (even when the main task fails) and reads
the setup's outputs via ``ctx.upstream_outputs``.
"""

import json
from pathlib import Path

import pytest

from beacon import Dag, Task

# Register the py plugin
from beacon.providers.standard.plugins.task.python import PythonPlugin  # noqa: F401


# ---------------------------------------------------------------------------
# Helper: write the user's spark_pipeline.py module
# ---------------------------------------------------------------------------


SPARK_PIPELINE_PY = '''
"""User's Spark pipeline — setup / main / teardown in one file."""
import json
from pathlib import Path
from beacon.runtime import load_context


def _record(event):
    ctx = load_context()
    p = Path(ctx.params["events_path"])
    log = json.loads(p.read_text()) if p.exists() else []
    log.append(event)
    p.write_text(json.dumps(log))


def launch(events_path: str):
    """Setup: launch a Spark application; return its id."""
    ctx = load_context()
    app_id = f"app-{ctx.run_id}"
    _record({"phase": "launch", "app_id": app_id, "attempt": ctx.attempt_number})
    return {"app_id": app_id}


def process(events_path: str, fail_main: bool = False):
    """Main: use the cluster. Optionally fails to exercise teardown-on-failure."""
    ctx = load_context()
    app_id = ctx.upstream_outputs["launch"]["app_id"]
    _record({"phase": "process", "app_id": app_id, "attempt": ctx.attempt_number})
    if fail_main:
        raise RuntimeError("simulated main failure")
    return {"processed_with": app_id}


def stop(events_path: str):
    """Teardown: ALWAYS runs; reads app_id from setup outputs."""
    ctx = load_context()
    app_id = ctx.upstream_outputs["launch"]["app_id"]
    _record({"phase": "stop", "app_id": app_id, "attempt": ctx.attempt_number})
    return {"stopped": app_id}
'''


@pytest.fixture
def pipeline_script(tmp_path: Path) -> Path:
    """Materialize the user's pipeline file and return its path."""
    script = tmp_path / "spark_pipeline.py"
    script.write_text(SPARK_PIPELINE_PY)
    return script


@pytest.fixture
def events_path(tmp_path: Path) -> Path:
    p = tmp_path / "events.json"
    return p


def _events(p: Path) -> list[dict]:
    return json.loads(p.read_text()) if p.exists() else []


def _make_dag(script: Path, events_path: Path) -> Dag:
    """Three tasks, one file, three functions — the documented pattern."""
    common = {"py_file": str(script)}
    ep = str(events_path)
    return Dag(
        id="spark-py",
        owners=["de"],
        actions=[
            Task(
                id="launch",
                uses="py",
                inputs={
                    **common,
                    "py_function": "launch",
                    "params": {"events_path": ep},
                },
            ),
            Task(
                id="process",
                uses="py",
                upstream=["launch"],
                inputs={
                    **common,
                    "py_function": "process",
                    "params": {"events_path": ep},
                },
            ),
            Task(
                id="stop",
                uses="py",
                teardown="launch",
                inputs={
                    **common,
                    "py_function": "stop",
                    "params": {"events_path": ep},
                },
            ),
        ],
    )


# ---------------------------------------------------------------------------
# Happy path: setup → process → teardown, in order, ONCE.
# ---------------------------------------------------------------------------


def test_py_teardown_happy_path(pipeline_script, events_path, tmp_path):
    dag = _make_dag(pipeline_script, events_path)
    result = dag.run(
        params={"events_path": str(events_path)},
        metadata_path=str(tmp_path / "meta"),
    )
    assert result["state"] == "success"

    phases = [e["phase"] for e in _events(events_path)]
    assert phases == ["launch", "process", "stop"]

    # stop saw the same app_id launch produced.
    ev = _events(events_path)
    assert ev[0]["app_id"] == ev[2]["app_id"]


# ---------------------------------------------------------------------------
# Failure path: main raises → teardown STILL runs.
# This is the whole reason setup/teardown exists.
# ---------------------------------------------------------------------------


def test_py_teardown_runs_on_main_failure(
    pipeline_script, events_path, tmp_path
):
    dag = _make_dag(pipeline_script, events_path)
    # Make process() fail by adding fail_main to its params.
    dag.actions[1].inputs["params"] = {
        "events_path": str(events_path),
        "fail_main": True,
    }

    result = dag.run(
        metadata_path=str(tmp_path / "meta"),
    )
    assert result["state"] == "failed"
    assert result["states"]["process"].value == "failed"
    # Teardown still ran even though main failed.
    assert result["states"]["stop"].value == "success"

    phases = [e["phase"] for e in _events(events_path)]
    assert phases == ["launch", "process", "stop"]


# ---------------------------------------------------------------------------
# Force-clear `process` → teardown auto-re-fires (the spark-sensor fix).
# ---------------------------------------------------------------------------


def test_py_teardown_re_fires_when_main_is_cleared(
    pipeline_script, events_path, tmp_path
):
    dag = _make_dag(pipeline_script, events_path)
    meta_path = str(tmp_path / "meta")

    # Initial run.
    initial = dag.run(
        metadata_path=meta_path,
    )
    run_id = initial["run_id"]
    assert [e["phase"] for e in _events(events_path)] == [
        "launch",
        "process",
        "stop",
    ]

    # Force-clear `process`. The auto-teardown rule should also clear `stop`.
    out = dag.clear(
        run_id=run_id,
        task_id="process",
        metadata_path=meta_path,
    )
    assert set(out["cleared"]) == {"process", "stop"}, (
        "FAIL: clearing the main task did not re-queue its teardown — "
        "the resource cleanup would be missed."
    )

    # Inspect events: launch ran ONCE (not re-cleared), process+stop ran TWICE.
    by_phase: dict[str, int] = {}
    for ev in _events(events_path):
        by_phase[ev["phase"]] = by_phase.get(ev["phase"], 0) + 1
    assert by_phase == {"launch": 1, "process": 2, "stop": 2}


# ---------------------------------------------------------------------------
# Best-practice check: 3 tasks share one file. Verify the file is imported
# afresh per task (no leaked module state across functions).
# ---------------------------------------------------------------------------


def test_py_teardown_each_task_has_own_attempt_log(
    pipeline_script, events_path, tmp_path
):
    dag = _make_dag(pipeline_script, events_path)
    result = dag.run(
        metadata_path=str(tmp_path / "meta"),
    )
    assert result["state"] == "success"

    # Each task gets its own attempt_number == 1 (independent retry budget).
    attempts_by_phase = {e["phase"]: e["attempt"] for e in _events(events_path)}
    assert attempts_by_phase == {"launch": 1, "process": 1, "stop": 1}
