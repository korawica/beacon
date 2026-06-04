"""Force-clear semantics with teardown — the Spark sensor scenario.

User story (verbatim):
    "I implement trigger spark application action and if I clear task
    that is sensoring spark app, then it should trigger stop spark app
    if it implement teardown logic — after force clear."

Translation: when we clear a task that's a transitive dependent of a
setup task (whose teardown cleans up a resource), the teardown MUST also
re-run on resume. Otherwise the original teardown's side effects are
stale (e.g. the spark cluster was already stopped) and the re-execution
fails or leaks a new resource.
"""

import asyncio
from typing import ClassVar

import pytest

from beacon import BasePlugin, Dag, DagRunner, Task
from beacon.runner import run_trigger
from beacon.metadata import JsonMetadata


# --- a fake "spark cluster" so we can observe lifecycle events --------------

_EVENTS: list[str] = []


@pytest.fixture(autouse=True)
def _reset_events():
    _EVENTS.clear()
    yield


class _LaunchSpark(BasePlugin):
    """Setup: 'launches' a spark app, returns its id."""

    plugin_name: ClassVar[str] = "_launch_spark"

    async def execute(self, context):
        app_id = f"spark-{context['run_id']}-{len(_EVENTS)}"
        _EVENTS.append(f"launch:{app_id}")
        return {"app_id": app_id}


class _SenseSpark(BasePlugin):
    """Sensor: waits for spark to finish (instant for the test)."""

    plugin_name: ClassVar[str] = "_sense_spark"
    app_id: str = ""

    async def execute(self, context):
        _EVENTS.append(f"sense:{self.app_id}")
        return {"sensed": self.app_id}


class _ProcessResults(BasePlugin):
    plugin_name: ClassVar[str] = "_process_results"

    async def execute(self, context):
        _EVENTS.append("process")
        return {"processed": True}


class _StopSpark(BasePlugin):
    """Teardown: 'stops' the spark app."""

    plugin_name: ClassVar[str] = "_stop_spark"
    app_id: str = ""

    async def execute(self, context):
        _EVENTS.append(f"stop:{self.app_id}")
        return {"stopped": self.app_id}


def _make_spark_dag() -> Dag:
    return Dag(
        id="spark-pipeline",
        owners=["de"],
        actions=[
            Task(id="launch", uses="_launch_spark"),
            Task(
                id="sense",
                uses="_sense_spark",
                upstream=["launch"],
                inputs={"app_id": "{{ outputs.launch.app_id }}"},
            ),
            Task(
                id="process",
                uses="_process_results",
                upstream=["sense"],
            ),
            Task(
                id="stop",
                uses="_stop_spark",
                teardown="launch",  # ← cleanup for the setup
                inputs={"app_id": "{{ outputs.launch.app_id }}"},
            ),
        ],
    )


# ---------------------------------------------------------------------------
# Baseline: initial run fires every lifecycle step exactly once
# ---------------------------------------------------------------------------


def test_initial_run_fires_full_lifecycle(tmp_path):
    dag = _make_spark_dag()
    runner = DagRunner(dag, meta=JsonMetadata(tmp_path / "meta"))
    result = asyncio.run(runner.run(run_id="manual-spark-pipeline-week1"))

    assert result.state == "success"
    # launch → sense → process → stop, in order.
    kinds = [e.split(":", 1)[0] for e in _EVENTS]
    assert kinds == ["launch", "sense", "process", "stop"]


# ---------------------------------------------------------------------------
# THE BUG THE USER FLAGGED:
# Clearing the sensor MUST also re-trigger the teardown, otherwise the
# rerun sensors a long-gone spark app and the cluster leak goes uncleaned.
# ---------------------------------------------------------------------------


def test_clearing_sensor_auto_re_fires_teardown(tmp_path):
    dag = _make_spark_dag()
    meta = JsonMetadata(tmp_path / "meta")
    runner = DagRunner(dag, meta=meta)

    # Original run.
    asyncio.run(runner.run(run_id="manual-spark-pipeline-week1"))
    assert _EVENTS == [
        "launch:spark-manual-spark-pipeline-week1-0",
        "sense:spark-manual-spark-pipeline-week1-0",
        "process",
        "stop:spark-manual-spark-pipeline-week1-0",
    ]

    # Force-clear the sensor only.
    cleared = asyncio.run(
        runner.clear(run_id="manual-spark-pipeline-week1", task_ids="sense")
    )
    # The runner MUST auto-include `stop` because the teardown's dep set
    # (launch + sense + process) intersects the cleared set ({sense}).
    assert set(cleared) == {"sense", "stop"}
    assert "stop" in cleared, (
        "FAIL: clearing a sensor did NOT re-queue its teardown. "
        "Resource cleanup would be missed."
    )

    _EVENTS.clear()

    # Resume.
    result = asyncio.run(
        runner.run(run_id="manual-spark-pipeline-week1", resume=True)
    )
    assert result.state == "success"

    # On resume: launch + process stayed SUCCESS (not re-run). sense + stop
    # re-fired. The teardown DID stop the spark app again.
    kinds = [e.split(":", 1)[0] for e in _EVENTS]
    assert kinds == ["sense", "stop"]
    assert "launch" not in kinds  # setup unchanged
    assert "process" not in kinds  # process not in clear set


# ---------------------------------------------------------------------------
# Clearing the setup itself also re-fires the teardown
# (because clearing the setup disturbs its dep set trivially).
# ---------------------------------------------------------------------------


def test_clearing_setup_re_fires_teardown(tmp_path):
    dag = _make_spark_dag()
    runner = DagRunner(dag, meta=JsonMetadata(tmp_path / "meta"))
    asyncio.run(runner.run(run_id="manual-spark-pipeline-x"))

    cleared = asyncio.run(
        runner.clear(
            run_id="manual-spark-pipeline-x",
            task_ids="launch",
            downstream=True,
        )
    )
    # launch + (sense + process via downstream) + stop (via teardown rule)
    assert set(cleared) == {"launch", "sense", "process", "stop"}


# ---------------------------------------------------------------------------
# Clearing a task that's NOT in any teardown's dep set leaves teardowns alone.
# ---------------------------------------------------------------------------


def test_clearing_unrelated_task_does_not_touch_teardown(tmp_path):
    """A DAG with two independent setup/teardown brackets — clearing a task
    in bracket A must not re-queue bracket B's teardown."""
    dag = Dag(
        id="two-brackets",
        owners=["de"],
        actions=[
            Task(id="setupA", uses="_launch_spark"),
            Task(id="useA", uses="_process_results", upstream=["setupA"]),
            Task(id="downA", uses="_stop_spark", teardown="setupA"),
            Task(id="setupB", uses="_launch_spark"),
            Task(id="useB", uses="_process_results", upstream=["setupB"]),
            Task(id="downB", uses="_stop_spark", teardown="setupB"),
        ],
    )
    meta = JsonMetadata(tmp_path / "meta")
    runner = DagRunner(dag, meta=meta)
    asyncio.run(runner.run(run_id="manual-two-brackets-x"))

    # Clearing useA must touch downA but NOT downB.
    cleared = asyncio.run(
        runner.clear(run_id="manual-two-brackets-x", task_ids="useA")
    )
    assert set(cleared) == {"useA", "downA"}
    assert "downB" not in cleared
    assert "setupB" not in cleared
    assert "useB" not in cleared


# ---------------------------------------------------------------------------
# Clearing a teardown alone is allowed (re-runs the cleanup, nothing else).
# Used for: "the cleanup itself failed, let me just retry the cleanup."
# ---------------------------------------------------------------------------


def test_clearing_teardown_alone_only_reruns_teardown(tmp_path):
    dag = _make_spark_dag()
    runner = DagRunner(dag, meta=JsonMetadata(tmp_path / "meta"))
    asyncio.run(runner.run(run_id="manual-spark-pipeline-y"))

    cleared = asyncio.run(
        runner.clear(run_id="manual-spark-pipeline-y", task_ids="stop")
    )
    assert cleared == ["stop"]

    _EVENTS.clear()
    asyncio.run(runner.run(run_id="manual-spark-pipeline-y", resume=True))
    kinds = [e.split(":", 1)[0] for e in _EVENTS]
    assert kinds == ["stop"]


# ---------------------------------------------------------------------------
# run_id trigger convention: manual / backfill / scheduled / unknown
# ---------------------------------------------------------------------------


def test_run_trigger_classifies_run_id():
    assert run_trigger("manual-etl-a1b2c3d4") == "manual"
    assert run_trigger("backfill-etl-20260101T000000") == "backfill"
    assert run_trigger("scheduled-etl-20260101T000000") == "scheduled"
    assert run_trigger("legacy-id") == "unknown"
    assert run_trigger("run-deadbeef") == "unknown"


def test_dag_run_emits_manual_prefix(tmp_path):
    dag = Dag(
        id="prefix-test",
        owners=["de"],
        actions=[Task(id="t", uses="_process_results")],
    )
    out = dag.run(metadata_path=str(tmp_path / "meta"))
    assert out["run_id"].startswith("manual-prefix-test-")
    assert run_trigger(out["run_id"]) == "manual"


def test_dag_backfill_emits_backfill_prefix(tmp_path):
    from datetime import datetime

    dag = Dag(
        id="prefix-bf",
        owners=["de"],
        actions=[Task(id="t", uses="_process_results")],
    )
    results = dag.backfill(
        start_date=datetime(2026, 1, 1),
        end_date=datetime(2026, 1, 1),
        cron="0 0 * * *",
        metadata_path=str(tmp_path / "meta"),
    )
    assert len(results) == 1
    assert results[0]["run_id"].startswith("backfill-prefix-bf-")
    assert run_trigger(results[0]["run_id"]) == "backfill"
